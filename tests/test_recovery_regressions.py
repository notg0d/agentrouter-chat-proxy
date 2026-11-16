"""Regression tests for resumed Responses sessions and Azure capacity failures.

All upstream traffic is mocked; no credentials or live model requests are used.
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import json
import os
import ssl
import sys
import time
import unittest
from unittest.mock import patch

import anyio
import httpcore
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


def _nested_read_error(message="Connection reset by peer"):
    cause = ConnectionResetError(10054, message)
    for error in (anyio.BrokenResourceError(), httpcore.ReadError(""), httpx.ReadError("")):
        error.__cause__ = cause
        cause = error
    return cause


COMPLETED = b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n'
DELTA = b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"partial"}\n\n'
MISMATCH = "The requested item was created under a different Azure OpenAI resource. Use the same resource that created the item to access it."


class _Clock:
    """Advance proxy time without changing asyncio's real clock or sleeping."""
    time = staticmethod(time.time)

    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    async def sleep(self, delay, request):
        self.now += delay
        return True

    def after(self, delay, result):
        def respond(request):
            self.now += delay
            return result
        return respond


class RecoveryRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.script = []
        self.calls = []
        self.old_client = proxy._SHARED_CLIENT
        self.upstream = httpx.AsyncClient(transport=httpx.MockTransport(self.respond))
        proxy._SHARED_CLIENT = self.upstream
        self.settings = patch.multiple(
            proxy, RATE_LIMIT_BASE_DELAY_S=0.01, RATE_LIMIT_MAX_DELAY_S=0.02,
            MAX_RATE_LIMIT_WAIT_S=2.0, MAX_TRANSPORT_RECOVERY_WAIT_S=2.0, MAX_TRANSIENT_RETRIES=2,
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
        if callable(item):
            item = item(request)
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

    async def test_slow_first_transport_failure_gets_its_own_recovery_budget(self):
        for endpoint in ("/v1/responses", "/responses", "/v1/chat/completions", "/chat/completions"):
            for stream in (False, True):
                with self.subTest(endpoint=endpoint, stream=stream):
                    proxy.reset_rate_limit_state()
                    start = len(self.calls)
                    clock = _Clock()
                    failure = _sse(httpx.ReadError("broken stream")) if stream else _nested_read_error()
                    if not stream:
                        failure.__cause__.__cause__.__cause__ = OSError("[WinError 121] The semaphore timeout period has expired")
                    success = _sse(COMPLETED) if stream else _complete()
                    self.script.extend([clock.after(142.4, failure), success])
                    payload = self.request_body(endpoint, stream=stream)
                    history = [
                        {"role": "user", "content": "Earlier question"},
                        {"role": "assistant", "content": "Earlier answer"},
                        {"role": "user", "content": "Continue the same task"},
                    ]
                    payload["messages" if "chat" in endpoint else "input"] = history
                    if "chat" not in endpoint:
                        payload["previous_response_id"] = "resp_existing_session"
                    with patch.object(proxy, "time", clock), patch.object(proxy, "_sleep_unless_disconnected", clock.sleep), patch.multiple(
                        proxy, MAX_RATE_LIMIT_WAIT_S=90.0, MAX_TRANSPORT_RECOVERY_WAIT_S=90.0,
                    ):
                        result = await self.local.post(endpoint, json=payload)
                    self.assertEqual(result.status_code, 200)
                    self.assertEqual(len(self.calls), start + 2)
                    self.assertEqual(self.calls[start], self.calls[start + 1])
                    self.assertNotIn("broken stream", result.text)

    async def test_transport_recovery_deadline_does_not_reset_after_each_failure(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                clock = _Clock()
                self.script.extend([clock.after(delay, httpx.ReadError("network read failed")) for delay in (142.4, 45.0, 45.0)])
                with patch.object(proxy, "time", clock), patch.object(proxy, "_sleep_unless_disconnected", clock.sleep), patch.multiple(
                    proxy, MAX_RATE_LIMIT_WAIT_S=90.0, MAX_TRANSPORT_RECOVERY_WAIT_S=90.0, MAX_TRANSIENT_RETRIES=6,
                ):
                    result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 502)
                self.assertEqual(len(self.calls), start + 3)
                self.assertEqual(result.json()["error"]["retries"], 2)
                self.assertEqual(result.json()["error"]["code"], "upstream_transport_retries_exhausted")
                self.assertIn("same OpenCode session", result.json()["error"]["message"])

    async def test_http_errors_do_not_start_a_fresh_transport_budget(self):
        for status in (429, 503):
            with self.subTest(status=status):
                proxy.reset_rate_limit_state()
                start = len(self.calls)
                clock = _Clock()
                code = "rate_limit_exceeded" if status == 429 else "server_error"
                self.script.append(clock.after(142.4, httpx.Response(status, json={"error": {"code": code, "message": "busy"}})))
                with patch.object(proxy, "time", clock), patch.object(proxy, "_sleep_unless_disconnected", clock.sleep), patch.multiple(
                    proxy, MAX_RATE_LIMIT_WAIT_S=90.0, MAX_TRANSPORT_RECOVERY_WAIT_S=90.0,
                ):
                    result = await self.local.post("/v1/responses", json=self.request_body("responses"))
                self.assertEqual(result.status_code, status)
                self.assertEqual(len(self.calls), start + 1)

    async def test_transport_and_http_failures_share_the_retry_count(self):
        self.script.extend([
            httpx.ReadError("first failure"),
            httpx.Response(503, json={"error": {"code": "server_error", "message": "busy"}}),
            httpx.ReadError("last failure"),
        ])
        result = await self.local.post("/v1/responses", json=self.request_body("responses"))
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(result.json()["error"]["retries"], 2)
        self.assertIn("last failure", result.json()["error"]["message"])

    async def test_transport_recovery_still_honors_a_shared_rate_limit(self):
        clock = _Clock()

        def fail(request):
            clock.now += 142.4
            proxy._RATE_LIMIT_STATE["cooldown_until"] = clock.now + 100.0
            return httpx.ReadError("connection failed")

        self.script.append(fail)
        with patch.object(proxy, "time", clock), patch.object(proxy, "_sleep_unless_disconnected", clock.sleep), patch.multiple(
            proxy, MAX_RATE_LIMIT_WAIT_S=90.0, MAX_TRANSPORT_RECOVERY_WAIT_S=90.0,
        ):
            result = await self.local.post("/v1/responses", json=self.request_body("responses"))
        self.assertEqual(result.status_code, 429)
        self.assertGreaterEqual(int(result.headers["retry-after"]), 100)
        self.assertEqual(len(self.calls), 1)

    async def test_expired_transport_budget_does_not_launch_another_attempt(self):
        clock = _Clock()

        async def delayed_sleep(delay, request):
            clock.now += 91.0
            return True

        self.script.append(httpx.ReadError("network read failed"))
        with patch.object(proxy, "time", clock), patch.object(proxy, "_sleep_unless_disconnected", delayed_sleep), patch.object(
            proxy, "MAX_TRANSPORT_RECOVERY_WAIT_S", 90.0,
        ):
            result = await self.local.post("/v1/responses", json=self.request_body("responses"))
        self.assertEqual(result.status_code, 502)
        self.assertEqual(result.json()["error"]["code"], "upstream_transport_retries_exhausted")
        self.assertEqual(len(self.calls), 1)

    async def test_content_blocked_preserves_upstream_details_without_retry_or_history_changes(self):
        request_id = "20261116163640840765114lh28gBMSYDeFO"
        error = {
            "code": "content-blocked", "type": "agent_router_api_error", "param": "",
            "message": f"content-blocked (request id: {request_id})",
        }
        for endpoint in ("/v1/responses", "/responses", "/v1/chat/completions", "/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                payload = self.request_body(endpoint)
                original = copy.deepcopy(payload)
                self.script.append(httpx.Response(400, json={"error": error}))
                result = await self.local.post(endpoint, json=payload)
                explained = result.json()["error"]
                self.assertEqual(result.status_code, 400)
                self.assertEqual(explained["code"], error["code"])
                self.assertEqual(explained["type"], error["type"])
                self.assertEqual(explained["param"], error["param"])
                self.assertEqual(explained["upstream_message"], error["message"])
                self.assertEqual(explained["request_id"], request_id)
                self.assertFalse(explained["retryable"])
                self.assertIn("not retried", explained["message"])
                self.assertEqual(len(self.calls), start + 1)
                self.assertEqual(payload, original)
                self.assertEqual(proxy._RATE_LIMIT_STATE["streak"], 0)
                self.assertEqual(proxy.STATS["stateless_fallbacks_applied"], 0)

    async def test_initial_json_and_sse_content_rejections_return_400_not_retryable_502(self):
        error = {"code": "content_filter", "message": "content filter rejected this request"}
        failed = {"type": "response.failed", "response": {"status": "failed", "error": error}}
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            for stream in (False, True):
                with self.subTest(endpoint=endpoint, stream=stream):
                    start = len(self.calls)
                    upstream = _sse(b"data: " + json.dumps(failed).encode() + b"\n\n") if stream else httpx.Response(200, json=failed)
                    self.script.append(upstream)
                    result = await self.local.post(endpoint, json=self.request_body(endpoint, stream=stream))
                    self.assertEqual(result.status_code, 400)
                    self.assertEqual(result.json()["error"]["code"], "content_filter")
                    self.assertFalse(result.json()["error"]["retryable"])
                    self.assertEqual(len(self.calls), start + 1)
                    self.assertTrue(upstream.is_closed)
        self.assertEqual(proxy.STATS["permanent_errors_total"], 4)
        self.assertEqual(proxy.STATS["successful_recoveries"], 0)

    async def test_policy_rejection_never_activates_resource_mismatch_recovery(self):
        self.script.append(httpx.Response(400, json={"error": {"code": "content-blocked", "message": MISMATCH}}))
        result = await self.local.post("/v1/responses", json={**self.request_body("responses"), "previous_response_id": "resp_old"})
        self.assertEqual(result.status_code, 400)
        self.assertEqual(result.json()["error"]["code"], "content-blocked")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(proxy.STATS["stateless_fallbacks_applied"], 0)
        self.assertEqual(self.calls[0]["previous_response_id"], "resp_old")

    async def test_content_rejection_diagnostics_log_counts_not_content_or_credentials(self):
        self.script.append(httpx.Response(400, headers={"x-request-id": "request-123"}, json={"error": {
            "code": "content-blocked", "message": "content-blocked: private-prompt Bearer diagnostic-token",
        }}))
        payload = {
            "model": "test", "instructions": "private-instructions",
            "input": [{"role": "user", "content": [
                {"type": "input_text", "text": "private-prompt"},
                {"type": "input_image", "image_url": "https://example.invalid/private-image"},
            ]}],
        }
        with self.assertLogs(proxy.logger, level="WARNING") as captured:
            result = await self.local.post("/v1/responses", json=payload)
        self.assertEqual(result.json()["error"]["request_id"], "request-123")
        self.assertEqual(result.headers["x-request-id"], "request-123")
        self.assertNotIn("diagnostic-token", result.text)
        diagnostic = "\n".join(captured.output)
        self.assertIn("request_bytes=", diagnostic)
        self.assertIn("input_items=1 images=1", diagnostic)
        for secret in ("private-prompt", "private-instructions", "private-image", "diagnostic-token"):
            self.assertNotIn(secret, diagnostic)

    async def test_content_rejection_after_transport_retry_is_not_a_successful_recovery(self):
        self.script.extend([httpx.ReadError("network read failed"), httpx.Response(400, json={"error": {
            "code": "content-blocked", "message": "content-blocked",
        }})])
        result = await self.local.post("/v1/responses", json=self.request_body("responses"))
        self.assertEqual(result.status_code, 400)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(proxy.STATS["successful_recoveries"], 0)

    async def test_content_rejection_after_partial_output_is_forwarded_without_retry(self):
        failed = b'event: error\ndata: {"type":"error","error":{"code":"content-blocked","message":"content-blocked"}}\n\n'
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                self.script.append(_sse(DELTA, failed))
                result = await self.local.post(endpoint, json=self.request_body(endpoint, stream=True))
                self.assertEqual(result.status_code, 200)
                self.assertIn("partial", result.text)
                self.assertIn("content-blocked", result.text)
                self.assertEqual(len(self.calls), start + 1)

    async def test_responses_only_app_uses_same_recovery_and_reports_active_setting(self):
        clock = _Clock()
        self.script.extend([
            clock.after(142.4, httpx.ReadError("network read failed")), _complete(),
            httpx.Response(400, json={"error": {"code": "content-blocked", "message": "content-blocked"}}),
        ])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=proxy.app), base_url="http://local") as client:
            with patch.object(proxy, "time", clock), patch.object(proxy, "_sleep_unless_disconnected", clock.sleep), patch.multiple(
                proxy, MAX_RATE_LIMIT_WAIT_S=90.0, MAX_TRANSPORT_RECOVERY_WAIT_S=90.0,
            ):
                self.assertEqual((await client.post("/v1/responses", json=self.request_body("responses"))).status_code, 200)
                rejection = await client.post("/responses", json=self.request_body("responses"))
                self.assertEqual(rejection.status_code, 400)
                self.assertFalse(rejection.json()["error"]["retryable"])
                for health_client in (client, self.local):
                    health = (await health_client.get("/_health")).json()
                    self.assertEqual(health["transport_recovery_wait_seconds"], 90.0)
        self.assertEqual(len(self.calls), 3)

    async def test_blank_transport_errors_name_attempts_without_changing_retry_count(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                self.script.extend([httpx.ReadTimeout("") for _ in range(3)])
                with self.assertLogs(proxy.logger, level="WARNING") as captured:
                    result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 502)
                self.assertTrue(result.json()["error"]["message"].startswith("Proxy upstream communication error: ReadTimeout."))
                self.assertEqual(result.json()["error"]["code"], "upstream_transport_retries_exhausted")
                self.assertIn("same OpenCode session", result.json()["error"]["message"])
                self.assertEqual(result.json()["error"]["type"], "proxy_error")
                self.assertEqual(len(self.calls), start + 3)
                attempts = [record.getMessage() for record in captured.records if record.levelname == "ERROR"]
                retries = [record.getMessage() for record in captured.records if "Retrying" in record.getMessage()]
                self.assertEqual(len(attempts), 3)
                self.assertTrue(all(message.startswith("[Attempt Exception] ReadTimeout (") for message in attempts))
                self.assertTrue(all("phase=send_request" in message for message in attempts))
                self.assertEqual(len(retries), 2)
                self.assertTrue(all(message.endswith(": ReadTimeout") for message in retries))

    async def test_nested_read_error_logs_phase_and_cause_without_extra_retries(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                error = _nested_read_error("reset: Bearer diagnostic-token sk-test-key")
                self.script.extend([error, _complete()])
                with self.assertLogs(proxy.logger, level="WARNING") as captured:
                    result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 200)
                self.assertEqual(len(self.calls), start + 2)
                diagnostic = "\n".join(captured.output)
                self.assertIn("BrokenResourceError", diagnostic)
                self.assertIn("ConnectionResetError", diagnostic)
                self.assertIn("10054", diagnostic)
                self.assertIn("phase=send_request", diagnostic)
                self.assertIn("elapsed=", diagnostic)
                self.assertNotIn("diagnostic-token", diagnostic)
                self.assertNotIn("sk-test-key", diagnostic)

    async def test_response_body_read_error_logs_http_status_and_closes_stream(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                start = len(self.calls)
                stream = _Stream([_nested_read_error()])
                self.script.extend([
                    httpx.Response(200, headers={"content-type": "application/json"}, stream=stream),
                    _complete(),
                ])
                with self.assertLogs(proxy.logger, level="WARNING") as captured:
                    result = await self.local.post(endpoint, json=self.request_body(endpoint))
                self.assertEqual(result.status_code, 200)
                self.assertEqual(len(self.calls), start + 2)
                self.assertTrue(stream.closed)
                diagnostic = "\n".join(captured.output)
                self.assertIn("phase=response_body", diagnostic)
                self.assertIn("http_status=200", diagnostic)
                self.assertIn("ConnectionResetError", diagnostic)

    async def test_precommit_read_error_preserves_cause_when_retries_are_disabled(self):
        with patch.object(proxy, "RETRY_AMBIGUOUS_ERRORS", False):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                with self.subTest(endpoint=endpoint):
                    start = len(self.calls)
                    stream = _Stream([_nested_read_error("reset: Bearer diagnostic-token")])
                    self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
                    result = await self.local.post(endpoint, json=self.request_body(endpoint, stream=True))
                    self.assertEqual(result.status_code, 502)
                    self.assertIn("no retry was permitted", result.text)
                    self.assertIn("ConnectionResetError", result.text)
                    self.assertNotIn("diagnostic-token", result.text)
                    self.assertEqual(len(self.calls), start + 1)
                    self.assertTrue(stream.closed)

    async def test_trace_records_safe_stage_without_logging_trace_payload(self):
        for fresh_connection in (False, True):
            with self.subTest(fresh_connection=fresh_connection):
                diagnostics = proxy.UpstreamAttemptDiagnostics()

                async def respond(request):
                    trace = request.extensions["trace"]
                    secret_info = {"headers": [(b"authorization", b"Bearer diagnostic-token")]}
                    if fresh_connection:
                        await trace("connection.connect_tcp.started", secret_info)
                        await trace("connection.start_tls.started", secret_info)
                    await trace("http11.send_request_headers.started", secret_info)
                    await trace("http11.send_request_body.started", secret_info)
                    await trace("http11.receive_response_headers.started", secret_info)
                    await trace("http11.response_closed.started", secret_info)
                    raise _nested_read_error()

                async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                    with self.assertRaises(httpx.ReadError):
                        await proxy._send_with_transport_retry(
                            client, "POST", "https://upstream.invalid/v1/responses", {},
                            content=b"{}", max_retries=1, diagnostics=diagnostics,
                        )
                summary = diagnostics.summary()
                self.assertIn("phase=response_headers", summary)
                self.assertIn("connection=new" if fresh_connection else "connection=reused", summary)
                self.assertNotIn("diagnostic-token", summary)
                self.assertNotIn("authorization", summary)

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

    def test_nested_httpx_cause_chain_reaches_socket_error(self):
        description = proxy.describe_exception(_nested_read_error())
        self.assertTrue(description.startswith("ReadError (caused by BrokenResourceError -> ConnectionResetError:"))
        self.assertEqual(description.count("ReadError"), 1)
        self.assertIn("10054", description)
        self.assertIn("Connection reset by peer", description)

    def test_nested_tls_read_error_preserves_ssl_cause(self):
        error = _nested_read_error()
        error.__cause__.__cause__.__cause__ = ssl.SSLEOFError(8, "Unexpected TLS EOF")
        description = proxy.describe_exception(error)
        self.assertIn("SSLEOFError", description)
        self.assertIn("Unexpected TLS EOF", description)

    def test_duplicate_wrappers_do_not_hide_deeper_redacted_details(self):
        error = _nested_read_error("Bearer diagnostic-token sk-test-key desktop-secret")
        with patch.dict(os.environ, {"CODEX_AGENTROUTER_GATEWAY_KEY": "desktop-secret"}):
            description = proxy.describe_exception(error)
        self.assertIn("ConnectionResetError", description)
        self.assertIn("[redacted]", description)
        for secret in ("diagnostic-token", "sk-test-key", "desktop-secret"):
            self.assertNotIn(secret, description)

    def test_multi_exception_cycle_is_bounded(self):
        error = httpx.ReadError("")
        cause = OSError("socket failed")
        error.__cause__ = cause
        cause.__cause__ = error
        self.assertEqual(proxy.describe_exception(error), "ReadError (caused by OSError: socket failed)")

    def test_excessive_cause_depth_is_marked_and_bounded(self):
        error = httpx.ReadError("")
        current = error
        for index in range(30):
            current.__cause__ = OSError(f"layer-{index}")
            current = current.__cause__
        description = proxy.describe_exception(error)
        self.assertIn("cause chain truncated", description)
        self.assertNotIn("layer-29", description)
        self.assertLess(description.count("layer-"), 30)

    def test_unsuppressed_implicit_context_is_not_reported(self):
        error = httpx.ReadError("")
        error.__context__ = OSError("unrelated previous error")
        self.assertEqual(proxy.describe_exception(error), "ReadError")

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
