"""Mocked security, shared recovery, stream bounds, and launcher regressions."""

from __future__ import annotations

import asyncio
import copy
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import httpx

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import chat_proxy
import proxy
from request_guard import ProxyRequestGuard, allowed_methods
import service
import updater


def frame(event, **data):
    return f"event: {event}\ndata: ".encode() + json.dumps({"type": event, **data}).encode() + b"\n\n"


DELTA = frame("response.output_text.delta", delta="partial")
COMPLETED = frame("response.completed", response={"status": "completed"})
MISMATCH = "The requested item was created under a different Azure OpenAI resource."


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks=(), *, stall=False):
        self.chunks = chunks
        self.stall = stall
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk
        if self.stall:
            await asyncio.Event().wait()

    async def aclose(self):
        self.closed = True


def sse(stream):
    return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)


def complete():
    return httpx.Response(200, json={"id": "resp_test", "status": "completed", "output": []})


class RequestGuardTests(unittest.IsolatedAsyncioTestCase):
    async def run_guard(self, messages, *, headers=(), path="/v1/responses", method="POST", token="", limit=8, timeout=0.05):
        sent, received = [], []
        stats = {"requests_rejected_total": 0}
        items = iter(messages)

        async def receive():
            item = next(items, None)
            if item is None:
                await asyncio.Event().wait()
            return item

        async def app(scope, receive, send):
            received.append(await receive())
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        async def send(message):
            sent.append(message)

        guard = ProxyRequestGuard(app, token=lambda: token, max_body_bytes=lambda: limit,
                                  upload_timeout_s=lambda: timeout, stats=stats)
        await guard({"type": "http", "path": path, "method": method, "headers": headers}, receive, send)
        return sent, received, stats

    async def test_declared_oversized_body_is_rejected_before_receive(self):
        sent, received, stats = await self.run_guard([], headers=[(b"content-length", b"9")])
        self.assertEqual(sent[0]["status"], 413)
        self.assertEqual(received, [])
        self.assertEqual(stats["requests_rejected_total"], 1)

    async def test_chunked_body_limit_and_exact_boundary(self):
        for size, status in ((4, 200), (5, 413)):
            with self.subTest(size=size):
                messages = [{"type": "http.request", "body": b"abcd", "more_body": True},
                            {"type": "http.request", "body": b"x" * size}]
                sent, received, _ = await self.run_guard(messages)
                self.assertEqual(sent[0]["status"], status)
                if status == 200:
                    self.assertEqual(received[0]["body"], b"abcdxxxx")
                    self.assertFalse(received[0]["more_body"])
                else:
                    self.assertEqual(received, [])

    async def test_invalid_and_duplicate_content_lengths(self):
        for values in ((b"-1",), (b"abc",), (b"2", b"2"), (b"9" * 5000,)):
            with self.subTest(values=tuple(v[:10] for v in values)):
                sent, received, _ = await self.run_guard([], headers=[(b"content-length", v) for v in values])
                self.assertEqual(sent[0]["status"], 400)
                self.assertEqual(received, [])

    async def test_stalled_upload_has_one_total_deadline(self):
        sent, received, _ = await self.run_guard([{"type": "http.request", "body": b"a", "more_body": True}])
        self.assertEqual(sent[0]["status"], 408)
        self.assertIn(b"request_upload_timeout", sent[1]["body"])
        self.assertEqual(received, [])

    async def test_disconnected_upload_does_not_start_application(self):
        sent, received, stats = await self.run_guard([{"type": "http.disconnect"}])
        self.assertEqual((sent, received), ([], []))
        self.assertEqual(stats["requests_rejected_total"], 0)

    async def test_route_method_and_duplicate_token_rejections(self):
        cases = [
            ("/admin", "POST", (), 404),
            ("/v1/responses", "GET", ((b"x-proxy-token", b"secret"),), 405),
            ("/v1/responses", "POST", ((b"x-proxy-token", b"secret"), (b"x-proxy-token", b"secret")), 401),
        ]
        for path, method, headers, status in cases:
            with self.subTest(path=path, status=status):
                sent, received, _ = await self.run_guard([], path=path, method=method, headers=headers, token="secret")
                self.assertEqual(sent[0]["status"], status)
                self.assertEqual(received, [])
                if status == 405:
                    self.assertIn((b"allow", b"POST"), sent[0]["headers"])

    def test_api_allowlist_is_explicit(self):
        for path in ("/v1/models", "/models/model-id"):
            self.assertEqual(allowed_methods(path), {"GET"})
        for path in ("/v1/responses/compact", "/v1/messages", "/v1/images/edits", "/v1/audio/speech"):
            self.assertEqual(allowed_methods(path), {"POST"})
        for path in ("/admin", "/v1/files", "/v1/models/id/delete", "/v1/responses/id", "/v1/responses/"):
            self.assertFalse(allowed_methods(path))


class SharedRecoverySafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.script, self.calls = [], []
        self.upstream = httpx.AsyncClient(transport=httpx.MockTransport(self.respond))
        self.settings = patch.multiple(
            proxy, _SHARED_CLIENT=self.upstream, PROXY_AUTH_TOKEN="", RETRY_ON_5XX=True,
            RETRY_AMBIGUOUS_ERRORS=True, RATE_LIMIT_BASE_DELAY_S=0.001, RATE_LIMIT_MAX_DELAY_S=0.002,
            MAX_RATE_LIMIT_WAIT_S=1.0, MAX_TRANSIENT_RETRIES=2, RECOVERY_STAGGER_S=0,
            SSE_PRIME_CREATED_GRACE_S=0.03, SSE_PRIME_FIRST_TIMEOUT_S=0.03,
        )
        self.settings.start()
        self.addCleanup(self.settings.stop)
        self.stat_patch = patch.dict(proxy.STATS, {key: 0 for key in proxy.STATS if key != "start_time"})
        self.stat_patch.start()
        self.addCleanup(self.stat_patch.stop)
        proxy.reset_rate_limit_state()
        self.local = httpx.AsyncClient(transport=httpx.ASGITransport(app=chat_proxy.app), base_url="http://local")

    async def asyncTearDown(self):
        await self.local.aclose()
        await self.upstream.aclose()
        proxy.reset_rate_limit_state()

    async def respond(self, request):
        self.calls.append(request)
        if not self.script:
            raise AssertionError("Unexpected upstream request")
        result = self.script.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def body(self, endpoint, *, stream=False):
        field = {"messages": [{"role": "user", "content": "hi"}]} if "chat" in endpoint else {"input": "hi"}
        return {"model": "test-model", "stream": stream, **field}

    async def test_authentication_and_token_stripping_on_both_applications(self):
        with patch.object(proxy, "PROXY_AUTH_TOKEN", "local-secret"):
            for app in (proxy.app, chat_proxy.app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
                    for path in ("/_stats", "/v1/models", "/v1/responses"):
                        result = await client.request("POST" if "responses" in path else "GET", path)
                        self.assertEqual(result.status_code, 401)
                    health = await client.get("/_health")
                    self.assertEqual(health.status_code, 200)
                    self.assertTrue(health.json()["proxy_auth_enabled"])
                    self.script.append(complete())
                    result = await client.post("/v1/responses", json=self.body("responses"),
                                               headers={"X-Proxy-Token": "local-secret", "Authorization": "Bearer upstream-only"})
                    self.assertEqual(result.status_code, 200)
                    self.assertNotIn("x-proxy-token", self.calls[-1].headers)
                    self.assertEqual(self.calls[-1].headers["authorization"], "Bearer upstream-only")
        self.assertEqual(len(self.calls), 2)

    async def test_fallback_key_and_local_token_are_independent(self):
        with patch.object(proxy, "PROXY_AUTH_TOKEN", "local-secret"), patch.dict(os.environ, {"CODEX_GATEWAY_API_KEY": "fallback-only"}):
            self.script.append(httpx.Response(200, json={"data": []}))
            response = await self.local.get("/v1/models", headers={"X-Proxy-Token": "local-secret"})
            self.assertEqual(response.status_code, 200)
            self.assertNotIn("x-proxy-token", self.calls[0].headers)
            self.assertEqual(self.calls[0].headers["authorization"], "Bearer fallback-only")

    async def test_unknown_routes_and_wrong_methods_never_reach_upstream(self):
        for method, path, status in (("GET", "/admin", 404), ("DELETE", "/v1/models", 405),
                                     ("GET", "/v1/responses", 405), ("OPTIONS", "/v1/chat/completions", 405)):
            result = await self.local.request(method, path)
            self.assertEqual(result.status_code, status)
        self.assertEqual(self.calls, [])

    async def test_invalid_model_stream_and_messages_are_rejected_locally(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            for changes in ({"model": ""}, {"model": []}, {"stream": "false"}):
                result = await self.local.post(endpoint, json={**self.body(endpoint), **changes})
                self.assertEqual(result.status_code, 400)
        for messages in ([], [None], [{"role": "unknown"}]):
            result = await self.local.post("/v1/chat/completions", json={"model": "test", "messages": messages})
            self.assertEqual(result.status_code, 400)
        self.assertEqual(self.calls, [])

    async def test_image_limit_keeps_newest_images_for_both_apis(self):
        with patch.object(proxy, "MAX_IMAGES_PER_REQUEST", 2):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                image_type = "image_url" if "chat" in endpoint else "input_image"
                images = [{"type": image_type, "image_url": f"https://example.invalid/{i}"} for i in range(4)]
                payload = self.body(endpoint)
                payload["messages" if "chat" in endpoint else "input"] = [{"role": "user", "content": images}]
                original = copy.deepcopy(payload)
                self.script.append(complete())
                self.assertEqual((await self.local.post(endpoint, json=payload)).status_code, 200)
                parts = json.loads(self.calls[-1].content)["input"][0]["content"]
                self.assertEqual([p["image_url"] for p in parts if "image_url" in p],
                                 ["https://example.invalid/2", "https://example.invalid/3"])
                self.assertEqual(payload, original)
        self.assertEqual(proxy.STATS["images_pruned_total"], 4)

    async def test_converted_payload_is_also_size_bounded(self):
        payload = self.body("chat")
        raw = json.dumps(payload, separators=(",", ":")).encode()
        converted = json.dumps(chat_proxy.build_responses_payload(payload)).encode()
        self.assertGreater(len(converted), len(raw))
        with patch.object(proxy, "MAX_BODY_BYTES", len(raw)):
            result = await self.local.post("/v1/chat/completions", content=raw)
        self.assertEqual(result.status_code, 413)
        self.assertEqual(self.calls, [])

    async def test_permanent_account_policy_and_auth_errors_do_not_retry(self):
        failures = [(429, "insufficient_quota", "Quota exhausted"),
                    (500, "sensitive_words", "content blocked"),
                    (503, "", "no available channel"),
                    (401, "rate_limit_exceeded", "rate limit"),
                    (400, "context_length_exceeded", "too many tokens"),
                    (400, "", "too many images")]
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            for status, code, message in failures:
                with self.subTest(endpoint=endpoint, status=status, code=code):
                    start = len(self.calls)
                    self.script.append(httpx.Response(status, json={"error": {"code": code, "message": message}}))
                    result = await self.local.post(endpoint, json=self.body(endpoint))
                    self.assertEqual(result.status_code, status)
                    self.assertEqual(len(self.calls), start + 1)
                    self.assertEqual(proxy._RATE_LIMIT_STATE["streak"], 0)

    async def test_conservative_5xx_mode_preserves_http_and_json_errors(self):
        with patch.object(proxy, "RETRY_ON_5XX", False):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                for status in (500, 502, 503, 504, 200):
                    start = len(self.calls)
                    self.script.append(httpx.Response(status, json={"error": {"code": "server_error", "message": "busy"}}))
                    result = await self.local.post(endpoint, json=self.body(endpoint))
                    expected = 502 if status == 200 and "chat" in endpoint else status
                    self.assertEqual(result.status_code, expected)
                    self.assertEqual(len(self.calls), start + 1)

    async def test_conservative_transport_mode_stops_read_write_and_protocol_replays(self):
        with patch.object(proxy, "RETRY_AMBIGUOUS_ERRORS", False):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                for error in (httpx.ReadTimeout("read"), httpx.WriteError("write"), httpx.RemoteProtocolError("protocol")):
                    start = len(self.calls)
                    self.script.append(error)
                    result = await self.local.post(endpoint, json=self.body(endpoint))
                    self.assertEqual(result.status_code, 502)
                    self.assertEqual(len(self.calls), start + 1)

    async def test_conservative_mode_still_retries_connect_and_pool_failures(self):
        with patch.object(proxy, "RETRY_AMBIGUOUS_ERRORS", False):
            for error in (httpx.ConnectError("connect"), httpx.ConnectTimeout("connect"), httpx.PoolTimeout("pool")):
                start = len(self.calls)
                self.script.extend([error, complete()])
                result = await self.local.post("/v1/responses", json=self.body("responses"))
                self.assertEqual(result.status_code, 200)
                self.assertEqual(len(self.calls), start + 2)

    async def test_conservative_mode_does_not_replay_empty_stalled_or_failed_streams(self):
        with patch.object(proxy, "RETRY_AMBIGUOUS_ERRORS", False):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                for stream in (Stream(), Stream(stall=True), Stream([httpx.ReadTimeout("read")])):
                    start = len(self.calls)
                    self.script.append(sse(stream))
                    result = await self.local.post(endpoint, json=self.body(endpoint, stream=True))
                    self.assertEqual(result.status_code, 502)
                    self.assertEqual(len(self.calls), start + 1)
                    self.assertTrue(stream.closed)

    async def test_missing_history_and_orphaned_tool_output_fail_with_clear_code(self):
        inputs = [None, [], [{"type": "item_reference", "id": "msg_old"}],
                  [{"type": "compaction", "encrypted_content": "foreign"}],
                  [{"type": "function_call_output", "call_id": "missing", "output": "result"}]]
        for input_items in inputs:
            with self.subTest(input=input_items):
                start = len(self.calls)
                payload = {"model": "test", "previous_response_id": "resp_old", "input": input_items}
                self.script.append(httpx.Response(400, json={"error": {"message": MISMATCH}}))
                result = await self.local.post("/v1/responses", json=payload)
                self.assertEqual(result.status_code, 400)
                self.assertEqual(result.json()["error"]["code"], "recovery_history_missing")
                self.assertEqual(len(self.calls), start + 1)

    async def test_text_input_remains_replayable_after_resource_mismatch(self):
        self.script.extend([httpx.Response(400, json={"error": {"message": MISMATCH}}), complete()])
        result = await self.local.post("/v1/responses", json={**self.body("responses"), "previous_response_id": "resp_old"})
        self.assertEqual(result.status_code, 200)
        replay = json.loads(self.calls[1].content)
        self.assertEqual(replay["input"], "hi")
        self.assertNotIn("previous_response_id", replay)

    async def test_metadata_before_rate_limit_does_not_commit_or_leak(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            proxy.reset_rate_limit_state()
            self.script.extend([sse(Stream([frame("billing.summary", value="old-attempt"),
                                            frame("error", code="rate_limit_exceeded", message="rate limit")])),
                                sse(Stream([COMPLETED]))])
            result = await self.local.post(endpoint, json=self.body(endpoint, stream=True))
            self.assertEqual(result.status_code, 200)
            self.assertNotIn("old-attempt", result.text)
            self.assertNotIn("rate_limit_exceeded", result.text)

    async def test_oversized_frames_fail_before_commit_without_retry(self):
        with patch.object(proxy, "MAX_SSE_FRAME_BYTES", 128):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                for chunks in ([b"data: " + b"x" * 200], [b"data: " + b"x" * 200 + b"\n\n"]):
                    start = len(self.calls)
                    stream = Stream(chunks)
                    self.script.append(sse(stream))
                    result = await self.local.post(endpoint, json=self.body(endpoint, stream=True))
                    self.assertEqual(result.status_code, 502)
                    self.assertEqual(result.json()["error"]["code"], "upstream_sse_frame_too_large")
                    self.assertEqual(len(self.calls), start + 1)
                    self.assertTrue(stream.closed)

    async def test_oversized_frame_after_output_emits_error_without_replay(self):
        with patch.object(proxy, "MAX_SSE_FRAME_BYTES", 128):
            for endpoint in ("/v1/responses", "/v1/chat/completions"):
                start = len(self.calls)
                stream = Stream([DELTA, b"data: " + b"x" * 200])
                self.script.append(sse(stream))
                result = await self.local.post(endpoint, json=self.body(endpoint, stream=True))
                self.assertEqual(result.status_code, 200)
                self.assertIn("partial", result.text)
                self.assertIn("upstream_sse_frame_too_large", result.text)
                self.assertEqual(len(self.calls), start + 1)
                self.assertTrue(stream.closed)

    async def test_initial_metadata_buffer_has_aggregate_limit(self):
        metadata = frame("billing.summary", value="metadata")
        with patch.object(proxy, "MAX_SSE_PRIME_BYTES", len(metadata) * 2):
            stream = Stream([metadata] * 3)
            self.script.append(sse(stream))
            result = await self.local.post("/v1/responses", json=self.body("responses", stream=True))
        self.assertEqual(result.status_code, 502)
        self.assertEqual(result.json()["error"]["code"], "upstream_sse_prime_too_large")
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(stream.closed)

    async def test_terminal_events_close_an_upstream_that_stays_open(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            stream = Stream([DELTA, COMPLETED], stall=True)
            self.script.append(sse(stream))
            result = await asyncio.wait_for(self.local.post(endpoint, json=self.body(endpoint, stream=True)), timeout=1)
            self.assertEqual(result.status_code, 200)
            self.assertNotIn("upstream_stream_interrupted", result.text)
            self.assertTrue(stream.closed)

    async def test_malformed_terminal_event_does_not_hide_truncation(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            self.script.append(sse(Stream([DELTA, b"event: response.completed\ndata: invalid-json\n\n"])))
            result = await self.local.post(endpoint, json=self.body(endpoint, stream=True))
            self.assertIn("upstream_stream_interrupted", result.text)
            self.assertNotIn('"finish_reason": "stop"', result.text)

    async def test_consumer_close_after_first_chunk_releases_upstream(self):
        for translate in (False, True):
            stream = Stream([DELTA], stall=True)
            response = sse(stream)
            if translate:
                iterator = chat_proxy._translate_responses_stream(
                    response, primed_chunks=None, remainder=b"", queue=None, producer_task=None, model="test")
            else:
                iterator = proxy._stream_generator(response)
            await anext(iterator)
            await iterator.aclose()
            self.assertTrue(stream.closed)

    async def test_nonloopback_startup_requires_local_token_for_both_apps(self):
        with patch.object(proxy, "PROXY_AUTH_TOKEN", ""):
            for module in (proxy, chat_proxy):
                with patch.object(module, "LISTEN_HOST", "0.0.0.0"):
                    with self.assertRaisesRegex(RuntimeError, "PROXY_AUTH_TOKEN"):
                        async with module._app_lifespan(module.app):
                            self.fail("Non-loopback application started without a token")


class OpenCodePresentationTests(unittest.IsolatedAsyncioTestCase):
    def test_api_docs_use_opencode_name(self):
        for module in (proxy, chat_proxy):
            with self.subTest(module=module.__name__):
                self.assertEqual(module.app.title, "AgentRouter OpenCode Proxy")
                self.assertIn("OpenCode", module.app.description)
                self.assertNotIn("Codex", module.app.description)

    def test_cli_help_uses_opencode_without_starting_or_updating(self):
        with patch.object(updater, "load_env_file"), patch.object(updater, "check_for_update") as check, patch.object(updater, "apply_update") as apply:
            for parse in (proxy._parse_proxy_cli_args, service._parse_args, updater.main):
                with self.subTest(parser=parse.__name__):
                    output = io.StringIO()
                    with redirect_stdout(output), self.assertRaises(SystemExit) as exited:
                        parse(["--help"])
                    self.assertEqual(exited.exception.code, 0)
                    self.assertIn("OpenCode", output.getvalue())
                    self.assertNotIn("Codex", output.getvalue())
            check.assert_not_called()
            apply.assert_not_called()

    async def test_legacy_health_identities_remain_compatible(self):
        for module, identity in ((proxy, "agentrouter-codex-proxy"), (chat_proxy, "agentrouter-chat-proxy")):
            with self.subTest(module=module.__name__):
                self.assertEqual((await module.health())["proxy"], identity)

    def test_upstream_user_agent_remains_compatible(self):
        headers = proxy._filter_headers({"user-agent": "curl/8.0"})
        self.assertEqual(headers["user-agent"], "Codex Desktop")


class ConfigurationAndServiceTests(unittest.TestCase):
    def test_modes_select_matching_workers_and_ports(self):
        with patch.dict(os.environ, {}, clear=True):
            for mode, port, module in (("responses", 8788, "proxy:app"), ("combined", 8789, "chat_proxy:app")):
                args = service._parse_args(["--mode", mode])
                self.assertEqual(args.port, port)
                self.assertIn(module, service._worker_command("python", args))
            self.assertEqual(service._parse_args([]).mode, "responses")
        with patch.dict(os.environ, {"PROXY_PORT": "9111", "CHAT_PROXY_PORT": "9222"}):
            self.assertEqual(service._parse_args([]).port, 9111)
            self.assertEqual(service._parse_args(["--mode", "combined"]).port, 9222)
            self.assertEqual(service._parse_args(["--mode", "combined", "--port", "9333"]).port, 9333)

    def test_health_probe_checks_identity_address_and_api_capabilities(self):
        good = {"status": "ok", "proxy": "agentrouter-chat-proxy", "listen": "127.0.0.1:8789",
                "supported_apis": ["responses", "chat.completions"]}
        for payload, expected in ((good, True), ({**good, "supported_apis": ["responses"]}, False),
                                  ({**good, "proxy": "other"}, False), ({**good, "listen": "0.0.0.0:8789"}, False),
                                  ({**good, "supported_apis": [[]]}, False), ([], False)):
            with patch.object(service.urllib.request, "urlopen", return_value=io.BytesIO(json.dumps(payload).encode())):
                self.assertEqual(service._probe_our_health(8789, mode="combined"), expected)

    def test_supervisor_respawn_preserves_combined_mode(self):
        args = service._parse_args(["--mode", "combined", "--port", "9000", "--no-auto-update"])
        with patch.object(service.sys, "platform", "win32"), patch.object(service, "_resolve_service_python", return_value="pythonw.exe"), patch.object(service.subprocess, "Popen") as spawn:
            service._respawn_self(args)
        command = spawn.call_args.args[0]
        self.assertEqual(command[command.index("--mode") + 1], "combined")
        self.assertEqual(command[command.index("--port") + 1], "9000")
        self.assertIn("--no-auto-update", command)

    def test_supervisor_rejects_unauthenticated_nonloopback_before_worker_setup(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(service.updater, "load_env_file"), patch.object(service, "RotatingFileLogger"), patch.object(service, "_is_port_busy", return_value=False), patch.object(service, "_ensure_dependencies") as dependencies, patch.object(service.subprocess, "Popen") as spawn:
            self.assertEqual(service.main(["--mode", "combined", "--host", "0.0.0.0"]), 1)
            dependencies.assert_not_called()
            spawn.assert_not_called()

    def test_supervisor_leaves_existing_listener_untouched_and_checks_mode(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(service.updater, "load_env_file"), patch.object(service, "RotatingFileLogger"), patch.object(service, "_is_port_busy", return_value=True), patch.object(service, "_probe_our_health", return_value=False) as probe, patch.object(service.subprocess, "Popen") as spawn:
            self.assertEqual(service.main(["--mode", "combined"]), 2)
            probe.assert_called_once_with(8789, "127.0.0.1", mode="combined")
            probe.return_value = True
            self.assertEqual(service.main(["--mode", "combined"]), 0)
            spawn.assert_not_called()

    def test_combined_uvicorn_flags_override_imported_bind_settings(self):
        result = proxy._extract_uvicorn_bind_overrides(
            ["__main__.py", "chat_proxy:app", "--host=0.0.0.0", "--port=9001"], "127.0.0.1", 8789)
        self.assertEqual(result, ("0.0.0.0", 9001))

    def test_new_positive_settings_reject_invalid_values(self):
        for value in ("0", "-1", "nan", "inf", "bad"):
            with patch.dict(os.environ, {"PROXY_TEST_LIMIT": value}):
                with self.assertRaises(ValueError):
                    proxy._positive_setting("PROXY_TEST_LIMIT", "1")

    def test_local_token_and_common_keys_are_redacted(self):
        with patch.object(proxy, "PROXY_AUTH_TOKEN", "local-secret"), patch.dict(os.environ, {"CODEX_GATEWAY_API_KEY": "fallback-secret"}):
            text = proxy.redact_sensitive("Bearer arbitrary-key sk-example123 local-secret fallback-secret")
        for secret in ("arbitrary-key", "sk-example123", "local-secret", "fallback-secret"):
            self.assertNotIn(secret, text)

    def test_updater_compiles_all_runtime_modules(self):
        self.assertTrue({"proxy.py", "chat_proxy.py", "request_guard.py", "service.py", "updater.py"}.issubset(updater._CORE_PYTHON_FILES))


if __name__ == "__main__":
    unittest.main()
