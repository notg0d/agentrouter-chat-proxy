"""Regression tests for resumed Responses sessions and Azure capacity failures.

All upstream traffic is mocked; no credentials or live model requests are used.
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import json
import os
import sys
import time
import unittest
from unittest.mock import patch

import httpx

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import chat_proxy
import proxy


class _Stream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    async def aclose(self):
        self.closed = True


def _sse(*chunks):
    return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=_Stream(chunks))


def _complete():
    return httpx.Response(200, json={
        "id": "resp_new", "status": "completed",
        "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "OK"}]}],
    })


COMPLETED = b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n'
DELTA = b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"partial"}\n\n'
MISMATCH = "The requested item was created under a different Azure OpenAI resource. Use the same resource that created the item to access it."


class RecoveryRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.script = []
        self.calls = []
        self.old_client = proxy._SHARED_CLIENT
        self.upstream = httpx.AsyncClient(transport=httpx.MockTransport(self.respond))
        proxy._SHARED_CLIENT = self.upstream
        self.settings = patch.multiple(
            proxy, RATE_LIMIT_BASE_DELAY_S=0.01, RATE_LIMIT_MAX_DELAY_S=0.02,
            MAX_RATE_LIMIT_WAIT_S=2.0, MAX_TRANSIENT_RETRIES=2,
            RECOVERY_STAGGER_S=0.005, RETRY_DELAY_MS=1,
            SSE_PRIME_CREATED_GRACE_S=0.05, SSE_PRIME_FIRST_TIMEOUT_S=0.1,
        )
        self.settings.start()
        self.jitter = patch.object(proxy.random, "uniform", return_value=0.0)
        self.jitter.start()
        self.proxy_stats = patch.dict(proxy.STATS, {k: 0 for k in proxy.STATS if k != "start_time"})
        self.chat_stats = patch.dict(chat_proxy.STATS, {k: 0 for k in chat_proxy.STATS if k != "start_time"})
        self.proxy_stats.start()
        self.chat_stats.start()
        proxy.reset_rate_limit_state()
        self.local = httpx.AsyncClient(transport=httpx.ASGITransport(app=chat_proxy.app), base_url="http://local")

    async def asyncTearDown(self):
        await self.local.aclose()
        await self.upstream.aclose()
        proxy._SHARED_CLIENT = self.old_client
        self.settings.stop()
        self.jitter.stop()
        self.proxy_stats.stop()
        self.chat_stats.stop()
        proxy.reset_rate_limit_state()

    async def respond(self, request):
        self.calls.append(json.loads(request.content) if request.content else None)
        if not self.script:
            raise AssertionError("Unexpected extra upstream attempt")
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def request_body(self, endpoint, stream=False):
        if "chat" in endpoint:
            return {"model": "gpt-6.1-sol", "messages": [{"role": "user", "content": "hi"}], "stream": stream}
        return {"model": "gpt-6.1-sol", "input": "hi", "stream": stream}

    async def test_resumed_native_session_recovers_on_both_response_routes(self):
        original = {
            "model": "gpt-6.1-sol", "previous_response_id": "resp_yesterday", "stream": False,
            "input": [
                {"type": "message", "role": "user", "content": "Yesterday's question"},
                {"type": "reasoning", "id": "rs_old", "encrypted_content": "foreign-resource-state"},
                {"type": "message", "role": "assistant", "id": "msg_old", "status": "completed", "content": "Yesterday's answer"},
                {"type": "function_call", "id": "fc_old", "call_id": "call_old", "name": "read", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "call_old", "output": "file contents"},
                {"type": "message", "role": "user", "content": "Continue today"},
            ],
        }
        snapshot = copy.deepcopy(original)
        for endpoint in ("/v1/responses", "/responses"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                self.script.extend([httpx.Response(400, json={"error": {"message": MISMATCH}}), _complete()])
                result = await self.local.post(endpoint, json=original)
                self.assertEqual(result.status_code, 200)
                sent = self.calls[start + 1]
                self.assertNotIn("previous_response_id", sent)
                self.assertEqual([item["type"] for item in sent["input"]], ["message", "message", "function_call", "function_call_output", "message"])
                self.assertEqual(sent["input"][1]["content"], "Yesterday's answer")
                self.assertEqual(sent["input"][2]["call_id"], "call_old")
                self.assertEqual(sent["input"][3]["call_id"], "call_old")
                self.assertTrue(all("id" not in item for item in sent["input"]))
                self.assertEqual(original, snapshot)

        health = (await self.local.get("/_health")).json()
        self.assertIn("responses", health["supported_apis"])
        self.assertEqual(health["stats"]["responses_requests"], 2)
        self.assertEqual(health["stats"]["chat_requests"], 0)
        self.assertEqual(health["responses_stats"]["stateless_fallbacks_applied"], 2)

    async def test_native_sse_mismatch_recovers_without_leaking_failed_attempt(self):
        failed = {"type": "response.failed", "response": {"status": "failed", "error": {"message": MISMATCH}}}
        self.script.extend([_sse(b"data: " + json.dumps(failed).encode() + b"\n\n"), _sse(COMPLETED)])
        result = await self.local.post("/v1/responses", json={
            "model": "gpt-6.1-sol", "stream": True,
            "input": [{"type": "message", "role": "user", "id": "msg_old", "content": "Continue"}],
        })
        self.assertEqual(result.status_code, 200)
        self.assertNotIn("different Azure", result.text)
        self.assertNotIn("id", self.calls[1]["input"][0])

    async def test_rate_limit_after_empty_item_is_caught_before_commit(self):
        setup = b'event: response.output_item.added\ndata: {"type":"response.output_item.added","item":{"type":"reasoning","id":"rs_old"}}\n\n'
        failed = b'event: response.failed\ndata: {"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"Token rate limit in eastus2"}}}\n\n'
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                proxy.reset_rate_limit_state()
                self.script.extend([_sse(setup, failed), _sse(COMPLETED)])
                result = await self.local.post(endpoint, json=self.request_body(endpoint, stream=True))
                self.assertEqual(result.status_code, 200)
                self.assertNotIn("rate_limit_exceeded", result.text)
                self.assertNotIn("rs_old", result.text)

    async def test_long_retry_after_returns_429_and_blocks_next_caller(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                proxy.reset_rate_limit_state()
                start = len(self.calls)
                self.script.append(httpx.Response(429, headers={"retry-after": "60"}, json={"error": {"code": "rate_limit_exceeded", "message": "Token rate limit"}}))
                result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 429)
                self.assertGreaterEqual(int(result.headers["retry-after"]), 60)
                self.assertEqual(len(self.calls), start + 1)
                self.assertGreater(proxy._RATE_LIMIT_STATE["cooldown_until"] - time.monotonic(), 59)
                proxy._clear_rate_limit_cooldown()
                again = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(again.status_code, 429)
                self.assertEqual(len(self.calls), start + 1)

    async def test_http_500_and_transport_timeouts_have_one_bounded_retry_loop(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            for failure in (httpx.ReadTimeout("busy"), httpx.PoolTimeout("busy"), httpx.Response(500, json={"error": {"message": "server error"}})):
                with self.subTest(endpoint=endpoint, failure=type(failure).__name__):
                    proxy.reset_rate_limit_state()
                    start = len(self.calls)
                    self.script.extend([failure, _complete()])
                    result = await self.local.post(endpoint, json=self.request_body(endpoint))
                    self.assertEqual(result.status_code, 200)
                    self.assertEqual(len(self.calls), start + 2)

    async def test_repeated_timeouts_stop_at_configured_retry_count(self):
        self.script.extend([httpx.ReadTimeout("busy") for _ in range(3)])
        result = await self.local.post("/v1/chat/completions", json=self.request_body("chat"))
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.calls), 3)  # Initial send + 2, not 5 nested attempts each.

    async def test_blank_transport_errors_name_attempts_without_changing_retry_count(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                self.script.extend([httpx.ReadTimeout("") for _ in range(3)])
                with self.assertLogs(proxy.logger, level="WARNING") as captured:
                    result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 502)
                self.assertEqual(result.json()["error"]["message"], "Proxy upstream communication error: ReadTimeout")
                self.assertEqual(result.json()["error"]["type"], "proxy_error")
                self.assertEqual(len(self.calls), start + 3)
                attempts = [record.getMessage() for record in captured.records if record.levelname == "ERROR"]
                retries = [record.getMessage() for record in captured.records if "Retrying" in record.getMessage()]
                self.assertEqual(attempts, ["[Attempt Exception] ReadTimeout"] * 3)
                self.assertEqual(len(retries), 2)
                self.assertTrue(all(message.endswith(": ReadTimeout") for message in retries))

    async def test_communication_error_details_are_redacted_in_logs_and_responses(self):
        with patch.object(proxy, "RETRY_AMBIGUOUS_ERRORS", False):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                with self.subTest(endpoint=endpoint):
                    self.script.append(httpx.ReadError("connection failed: Bearer diagnostic-token sk-test-key"))
                    with self.assertLogs(proxy.logger, level="ERROR") as captured:
                        result = await self.local.post(endpoint, json=self.request_body(endpoint))
                    self.assertEqual(result.status_code, 502)
                    diagnostic = result.text + "\n" + "\n".join(captured.output)
                    self.assertIn("ReadError: connection failed", diagnostic)
                    self.assertIn("[redacted]", diagnostic)
                    self.assertNotIn("diagnostic-token", diagnostic)
                    self.assertNotIn("sk-test-key", diagnostic)

    async def test_precommit_stream_exception_details_are_not_blank(self):
        with patch.object(proxy, "MAX_TRANSIENT_RETRIES", 0):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                with self.subTest(endpoint=endpoint):
                    start = len(self.calls)
                    upstream = _sse(httpx.RemoteProtocolError(""))
                    stream = upstream.stream
                    self.script.append(upstream)
                    with self.assertLogs(proxy.logger, level="ERROR") as captured:
                        result = await self.local.post(endpoint, json=self.request_body(endpoint, stream=True))
                    self.assertEqual(result.status_code, 502)
                    self.assertIn("RemoteProtocolError", result.json()["error"]["message"])
                    self.assertIn("RemoteProtocolError", "\n".join(captured.output))
                    self.assertEqual(len(self.calls), start + 1)
                    self.assertTrue(stream.closed)

    async def test_partial_stream_exception_details_are_redacted_without_replay(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            for message in ("", "stream closed: Bearer diagnostic-token sk-test-key"):
                with self.subTest(endpoint=endpoint, message=message):
                    start = len(self.calls)
                    self.script.append(_sse(DELTA, httpx.ReadTimeout(message)))
                    with self.assertLogs(level="WARNING") as captured:
                        result = await self.local.post(endpoint, json=self.request_body(endpoint, stream=True))
                    self.assertIn("partial", result.text)
                    self.assertIn("upstream_stream_interrupted", result.text)
                    self.assertIn("ReadTimeout", result.text)
                    diagnostic = result.text + "\n" + "\n".join(captured.output)
                    self.assertNotIn("diagnostic-token", diagnostic)
                    self.assertNotIn("sk-test-key", diagnostic)
                    self.assertNotIn('"finish_reason": "stop"', result.text)
                    self.assertEqual(len(self.calls), start + 1)

    async def test_passthrough_exception_details_are_named_and_redacted(self):
        with patch.object(proxy, "RETRY_AMBIGUOUS_ERRORS", False):
            for app in (proxy.app, chat_proxy.app):
                with self.subTest(app=app):
                    start = len(self.calls)
                    self.script.append(httpx.ReadTimeout("read failed: Bearer diagnostic-token sk-test-key"))
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
                        with self.assertLogs(level="ERROR") as captured:
                            result = await client.get("/v1/models")
                    self.assertEqual(result.status_code, 502)
                    diagnostic = result.text + "\n" + "\n".join(captured.output)
                    self.assertIn("ReadTimeout: read failed", diagnostic)
                    self.assertNotIn("diagnostic-token", diagnostic)
                    self.assertNotIn("sk-test-key", diagnostic)
                    self.assertEqual(len(self.calls), start + 1)

    async def test_transport_retry_log_preserves_redacted_details(self):
        self.script.extend([httpx.ConnectError("connect failed: Bearer diagnostic-token"), _complete()])
        with self.assertLogs(proxy.logger, level="WARNING") as captured:
            result = await proxy._send_with_transport_retry(
                self.upstream, "POST", "https://upstream.invalid/v1/responses", {}, content=b"{}", max_retries=2,
            )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(len(self.calls), 2)
        diagnostic = "\n".join(captured.output)
        self.assertIn("ConnectError: connect failed", diagnostic)
        self.assertIn("[redacted]", diagnostic)
        self.assertNotIn("diagnostic-token", diagnostic)

    async def test_nonstream_read_timeout_closes_connection_before_retry(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                stream = _Stream([httpx.ReadTimeout("read timeout")])
                self.script.extend([httpx.Response(200, headers={"content-type": "application/json"}, stream=stream), _complete()])
                result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 200)
                self.assertTrue(stream.closed)

    async def test_partial_stream_timeout_is_not_reported_as_success_or_replayed(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                self.script.append(_sse(DELTA, httpx.ReadTimeout("stream timeout")))
                result = await self.local.post(endpoint, json=self.request_body(endpoint, stream=True))
                self.assertIn("partial", result.text)
                self.assertIn("upstream_stream_interrupted", result.text)
                self.assertNotIn('"finish_reason": "stop"', result.text)
                self.assertEqual(len(self.calls), start + 1)

    async def test_split_terminal_frame_does_not_trigger_false_stream_error(self):
        self.script.append(_sse(DELTA, COMPLETED[:15], COMPLETED[15:]))
        result = await self.local.post("/v1/responses", json=self.request_body("responses", stream=True))
        self.assertIn("response.completed", result.text)
        self.assertNotIn("upstream_stream_interrupted", result.text)

    async def test_permanent_bad_request_is_not_retried(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                self.script.append(httpx.Response(400, json={"error": {"message": "bad tool schema"}}))
                result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 400)
                self.assertEqual(len(self.calls), start + 1)

    async def test_native_invalid_json_object_does_not_reach_upstream(self):
        result = await self.local.post("/v1/responses", json=[])
        self.assertEqual(result.status_code, 400)
        self.assertEqual(self.calls, [])

    async def test_exhausted_failed_json_is_not_counted_as_successful_recovery(self):
        failure = {"status": "failed", "error": {"code": "server_error", "message": "Upstream busy"}}
        self.script.extend([httpx.Response(200, json=failure) for _ in range(3)])
        result = await self.local.post("/v1/responses", json=self.request_body("responses"))
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(proxy.STATS["successful_recoveries"], 0)


class ExceptionDescriptionTests(unittest.TestCase):
    def test_blank_exception_messages_keep_the_type(self):
        for error_type in (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ReadError, httpx.RemoteProtocolError):
            for message in ("", " \t "):
                with self.subTest(error_type=error_type.__name__, message=message):
                    self.assertEqual(proxy.describe_exception(error_type(message)), error_type.__name__)

    def test_explicit_cause_is_included_without_duplicates_or_recursion(self):
        error = httpx.ConnectError("")
        error.__cause__ = ConnectionResetError(10054, "Connection reset by peer")
        description = proxy.describe_exception(error)
        self.assertIn("ConnectError (caused by ConnectionResetError:", description)
        self.assertIn("10054", description)
        error = httpx.ReadTimeout("")
        error.__cause__ = httpx.ReadTimeout("")
        self.assertEqual(proxy.describe_exception(error), "ReadTimeout")
        error.__cause__ = error
        self.assertEqual(proxy.describe_exception(error), "ReadTimeout")

    def test_exception_message_and_cause_redact_known_credentials(self):
        error = httpx.ReadError("Bearer diagnostic-token sk-test-key local-secret fallback-secret desktop-secret")
        error.__cause__ = OSError("upstream-secret")
        with patch.object(proxy, "PROXY_AUTH_TOKEN", "local-secret"), patch.dict(os.environ, {
            "CODEX_GATEWAY_API_KEY": "fallback-secret", "AGENTROUTER_API_KEY": "upstream-secret",
            "CODEX_AGENTROUTER_GATEWAY_KEY": "desktop-secret",
        }):
            description = proxy.describe_exception(error)
        self.assertIn("ReadError:", description)
        self.assertIn("caused by OSError:", description)
        for secret in ("diagnostic-token", "sk-test-key", "local-secret", "fallback-secret", "desktop-secret", "upstream-secret"):
            self.assertNotIn(secret, description)

    def test_suppressed_context_is_not_reported(self):
        error = httpx.ReadTimeout("")
        error.__context__ = OSError("unrelated exception")
        error.__suppress_context__ = True
        self.assertEqual(proxy.describe_exception(error), "ReadTimeout")


class RetryHintTests(unittest.TestCase):
    def test_provider_hints_are_never_capped_by_local_backoff(self):
        cases = [
            ({"Retry-After": "60"}, "", 60.0, 60),
            ({"retry-after-ms": "60000"}, "", 60.0, 60),
            ({}, "Please retry after 200 milliseconds.", 0.2, 1),
            ({}, "Try again in 60 seconds.", 60.0, 60),
            ({"x-ratelimit-reset-tokens": "1s", "x-ratelimit-reset-requests": "1m30s"}, "", 90.0, 90),
            ({"x-ratelimit-reset-tokens": "1m"}, "", 60.0, 60),
        ]
        with patch.object(proxy, "RATE_LIMIT_MAX_DELAY_S", 15.0), patch.object(proxy.random, "uniform", return_value=0.0):
            for headers, text, expected, hint in cases:
                with self.subTest(headers=headers, text=text):
                    delay, seconds = proxy._extract_retry_delay_s(headers, text, 1)
                    self.assertAlmostEqual(delay, expected)
                    self.assertEqual(seconds, hint)

    def test_retry_after_http_date_is_honored(self):
        date = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=120), usegmt=True)
        with patch.object(proxy.random, "uniform", return_value=0.0):
            delay, hint = proxy._extract_retry_delay_s({"Retry-After": date}, "", 1)
        self.assertGreater(delay, 118)
        self.assertGreater(hint, 118)


if __name__ == "__main__":
    unittest.main()
