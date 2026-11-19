"""Tests for native Xingchi routing, credential isolation, and stream ownership."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import chat_proxy
import proxy
import xingchi_proxy
from request_guard import allowed_methods


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks, *, stall=False):
        self.chunks = chunks
        self.stall = stall
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.stall:
            await asyncio.Event().wait()

    async def aclose(self):
        self.closed = True


class XingchiRouteTests(unittest.TestCase):
    def test_explicit_routes_and_methods(self):
        for path in ("/xingchi/models", "/xingchi/v1/models/gpt-test", "/xingchi/usage",
                     "/xingchi/v1/sub2api/billing", "/xingchi/v1beta/models/gemini-test",
                     "/xingchi/antigravity/v1/models", "/xingchi/backend-api/codex/models",
                     "/xingchi/images/tasks/task_123"):
            with self.subTest(path=path):
                self.assertEqual(allowed_methods(path), {"GET"})
        for path in ("/xingchi/responses", "/xingchi/chat/completions", "/xingchi/messages",
                     "/xingchi/v1/responses/compact", "/xingchi/v1/responses/input_tokens",
                     "/xingchi/v1/messages/count_tokens", "/xingchi/antigravity/v1/messages",
                     "/xingchi/v1beta/models/gemini-test:generateContent",
                     "/xingchi/antigravity/v1beta/models/gemini-test:streamGenerateContent",
                     "/xingchi/v1beta/models/gemini-test:countTokens",
                     "/xingchi/v1beta/models/gemini-test:embedContent",
                     "/xingchi/images/generations/async"):
            with self.subTest(path=path):
                self.assertEqual(allowed_methods(path), {"POST"})
        for path in ("/xingchi", "/xingchi/admin", "/xingchi/api/v1/keys", "/xingchi/v1/files",
                     "/xingchi/v1/responses/id", "/xingchi/v1/models/..", "/xingchi/models/id/delete",
                     "/xingchi/v1beta/models/a:delete", "/xingchi/v1beta/models/a:b:generateContent",
                     "/xingchi/v1beta/models/a/other:generateContent", "/xingchi/v1/models/"):
            with self.subTest(path=path):
                self.assertFalse(allowed_methods(path))
        self.assertFalse(allowed_methods("/v1beta/models"))
        self.assertEqual(allowed_methods("/v1/responses"), {"POST"})

    def test_upstream_normalization_and_validation(self):
        for raw in ("https://api.xingchiapi.com", "https://api.xingchiapi.com/", "https://api.xingchiapi.com/v1/"):
            self.assertEqual(xingchi_proxy._upstream_base(raw), "https://api.xingchiapi.com")
        for raw in ("https://key@example.com", "https://example.com?key=secret", "https://example.com#fragment",
                    "https://example.com/v1beta", "file:///test", "example.com"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                xingchi_proxy._upstream_base(raw)


class XingchiRelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls = []
        self.script = []
        self.upstream = httpx.AsyncClient(transport=httpx.MockTransport(self.respond))
        settings = patch.multiple(proxy, _SHARED_CLIENT=self.upstream, PROXY_AUTH_TOKEN="",
                                  MAX_BODY_BYTES=20971520, MAX_SSE_FRAME_BYTES=8388608)
        settings.start()
        self.addCleanup(settings.stop)
        upstream_url = patch.object(xingchi_proxy, "UPSTREAM", "https://api.xingchiapi.com")
        upstream_url.start()
        self.addCleanup(upstream_url.stop)
        env = patch.dict(os.environ, {}, clear=True)
        env.start()
        self.addCleanup(env.stop)
        proxy.reset_rate_limit_state()

    async def asyncTearDown(self):
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

    def client(self, app):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local")

    async def test_native_requests_and_exact_paths_on_both_apps(self):
        cases = (
            ("POST", "/xingchi/responses", "/v1/responses"),
            ("POST", "/xingchi/messages", "/v1/messages"),
            ("POST", "/xingchi/v1beta/models/gemini-test:generateContent", "/v1beta/models/gemini-test:generateContent"),
            ("POST", "/xingchi/antigravity/v1/messages", "/antigravity/v1/messages"),
            ("POST", "/xingchi/backend-api/codex/responses/compact", "/backend-api/codex/responses/compact"),
            ("GET", "/xingchi/models", "/v1/models"),
            ("GET", "/xingchi/usage", "/v1/usage"),
            ("GET", "/xingchi/sub2api/billing", "/v1/sub2api/billing"),
        )
        body = b'{"model":"unchanged","reasoning_effort":"xhigh","messages":[{"role":"user","content":"hi"}],"previous_response_id":"keep"}'
        output = b'{"provider":"xingchi","data":[]}'
        for app in (proxy.app, chat_proxy.app):
            async with self.client(app) as client:
                for method, path, target in cases:
                    with self.subTest(app=app, path=path):
                        self.script.append(httpx.Response(200, content=output, headers={"content-type": "application/json"}))
                        result = await client.request(method, path, content=body)
                        self.assertEqual(result.status_code, 200)
                        self.assertEqual(result.content, output)
                        self.assertEqual(str(self.calls[-1].url), "https://api.xingchiapi.com" + target)
                        self.assertEqual(self.calls[-1].content, body)

    async def test_chat_completions_convert_directly_to_responses(self):
        completion = {"id": "resp-1", "model": "test", "status": "completed",
                      "output": [{"type": "message", "content": [{"type": "output_text", "text": "hello"}]}]}
        body = {"model": "test", "messages": [{"role": "user", "content": "hi"}]}
        for app in (proxy.app, chat_proxy.app):
            calls_before = len(self.calls)
            self.script.append(httpx.Response(200, json=completion))
            async with self.client(app) as client:
                result = await client.post("/xingchi/v1/chat/completions", json=body)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()["choices"][0]["message"]["content"], "hello")
            self.assertEqual(len(self.calls), calls_before + 1)
            self.assertEqual(self.calls[-1].url.path, "/v1/responses")
            self.assertEqual(json.loads(self.calls[-1].content)["input"][0]["content"], "hi")

    async def test_fallback_credentials_are_isolated_and_native_headers_win(self):
        with patch.dict(os.environ, {"CODEX_GATEWAY_API_KEY": "agent-only", "AGENTROUTER_API_KEY": "agent-other"}):
            async with self.client(chat_proxy.app) as client:
                for headers, expected in (({}, None), ({"x-api-key": "native-key"}, None),
                                          ({"x-goog-api-key": "google-key"}, None),
                                          ({"authorization": "Bearer client-key"}, "Bearer client-key")):
                    self.script.append(httpx.Response(200, json={}))
                    await client.get("/xingchi/models", headers=headers)
                    self.assertEqual(self.calls[-1].headers.get("authorization"), expected)
                with patch.dict(os.environ, {"XINGCHI_API_KEY": "xingchi-only"}):
                    for headers, expected in (({}, "Bearer xingchi-only"), ({"x-api-key": "native-key"}, None),
                                              ({"x-goog-api-key": "google-key"}, None),
                                              ({"authorization": "Bearer client-key"}, "Bearer client-key")):
                        self.script.append(httpx.Response(200, json={}))
                        await client.get("/xingchi/models", headers=headers)
                        self.assertEqual(self.calls[-1].headers.get("authorization"), expected)
                    self.script.append(httpx.Response(200, json={"data": []}))
                    await client.get("/v1/models")
                    self.assertEqual(self.calls[-1].headers["authorization"], "Bearer agent-only")
                    self.assertNotIn("xingchi", self.calls[-1].url.host)
        for request in self.calls[:4]:
            self.assertNotEqual(request.headers["user-agent"], "Codex Desktop")

    async def test_preserves_queries_session_headers_and_strips_local_token(self):
        with patch.object(proxy, "PROXY_AUTH_TOKEN", "local-only"):
            self.script.append(httpx.Response(200, json={}))
            async with self.client(proxy.app) as client:
                result = await client.get("/xingchi/v1beta/models?pageSize=2&tag=a&tag=b", headers={
                    "X-Proxy-Token": "local-only", "x-goog-api-key": "native-key", "session_id": "session-1",
                    "anthropic-version": "2023-06-01", "connection": "x-remove", "x-remove": "not-upstream",
                })
            self.assertEqual(result.status_code, 200)
            sent = self.calls[-1]
            self.assertEqual(sent.url.params.get_list("tag"), ["a", "b"])
            self.assertEqual(sent.headers["session_id"], "session-1")
            self.assertEqual(sent.headers["anthropic-version"], "2023-06-01")
            self.assertEqual(sent.headers["x-goog-api-key"], "native-key")
            self.assertNotIn("x-proxy-token", sent.headers)
            self.assertNotIn("x-remove", sent.headers)

    async def test_request_guard_still_applies_to_xingchi(self):
        for app in (proxy.app, chat_proxy.app):
            async with self.client(app) as client:
                for method, path, status in (("GET", "/xingchi/admin", 404), ("GET", "/xingchi/responses", 405),
                                             ("DELETE", "/xingchi/models", 405)):
                    self.assertEqual((await client.request(method, path)).status_code, status)
                with patch.object(proxy, "PROXY_AUTH_TOKEN", "secret"):
                    self.assertEqual((await client.get("/xingchi/models")).status_code, 401)
                    duplicate = await client.get("/xingchi/models", headers=[("X-Proxy-Token", "secret")] * 2)
                    self.assertEqual(duplicate.status_code, 401)
                    self.assertEqual((await client.get("/_health")).status_code, 200)
                with patch.object(proxy, "MAX_BODY_BYTES", 4):
                    self.assertEqual((await client.post("/xingchi/messages", content=b"12345")).status_code, 413)
        self.assertEqual(self.calls, [])

    async def test_http_errors_are_preserved_without_retries_or_cooldown(self):
        async with self.client(chat_proxy.app) as client:
            for status in (400, 401, 403, 429, 500, 503):
                self.script.append(httpx.Response(status, json={"code": "API_KEY_REQUIRED", "message": "original"},
                                                 headers={"retry-after": "60"}))
                result = await client.post("/xingchi/responses", json={"model": "test", "input": "hi"})
                self.assertEqual(result.status_code, status)
                self.assertEqual(result.json()["message"], "original")
                self.assertEqual(result.headers["retry-after"], "60")
        self.assertEqual(len(self.calls), 6)
        self.assertEqual(proxy._RATE_LIMIT_STATE["cooldown_until"], 0)

    async def test_transport_error_is_redacted_and_retries_are_bounded(self):
        self.script.extend([httpx.ConnectError("xingchi-secret") for _ in range(3)])
        with patch.dict(os.environ, {"XINGCHI_API_KEY": "xingchi-secret"}), patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock):
            async with self.client(proxy.app) as client:
                result = await client.get("/xingchi/models")
        self.assertEqual(result.status_code, 502)
        self.assertIn("ConnectError", result.text)
        self.assertNotIn("xingchi-secret", result.text)
        self.assertEqual(len(self.calls), 3)

    async def test_winerror_64_recovers_before_headers_on_both_apps(self):
        for app in (proxy.app, chat_proxy.app):
            start = len(self.calls)
            error = httpx.ReadError('')
            error.__cause__ = ConnectionResetError(64, 'The specified network name is no longer available')
            self.script.extend([error, httpx.Response(200, json={'ok': True})])
            with patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock), self.assertLogs(xingchi_proxy.logger, level='INFO') as logs:
                async with self.client(app) as client:
                    result = await client.post('/xingchi/responses', content=b'{"input":"unchanged"}')
            self.assertEqual(result.status_code, 200)
            self.assertEqual(len(self.calls), start + 2)
            self.assertEqual(self.calls[-1].content, self.calls[-2].content)
            self.assertIn('ConnectionResetError', '\n'.join(logs.output))
            self.assertIn('[Xingchi Recovered]', '\n'.join(logs.output))

    async def test_429_and_transport_failures_share_retry_count(self):
        self.script.extend([httpx.Response(429, json={}), httpx.ReadError('reset'), httpx.ConnectError('reset')])
        with patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock) as wait:
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(wait.await_count, 2)

    async def test_conservative_mode_does_not_retry_ambiguous_read_failure(self):
        self.script.append(httpx.ReadError('reset'))
        with patch.object(proxy, 'RETRY_AMBIGUOUS_ERRORS', False), patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock) as wait:
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.calls), 1)
        wait.assert_not_awaited()

    async def test_disconnect_during_transport_delay_stops_retry(self):
        self.script.append(httpx.ReadError('reset'))
        with patch.object(xingchi_proxy, '_wait_retry', side_effect=proxy._ClientDisconnected):
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(result.status_code, 499)
        self.assertEqual(len(self.calls), 1)

    async def test_transport_budget_expiry_after_wait_stops_retry(self):
        self.script.append(httpx.ReadError('reset'))
        clock = [0.0]
        async def wait(*args):
            clock[0] = 9.0
        with patch.object(xingchi_proxy, '_wait_retry', side_effect=wait), patch.object(xingchi_proxy.time, 'monotonic', side_effect=lambda: clock[0]):
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.calls), 1)

    async def test_read_error_after_partial_output_is_never_replayed(self):
        class ResetStream(ByteStream):
            async def __aiter__(self):
                yield b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n'
                raise httpx.ReadError('connection reset')
        stream = ResetStream([])
        self.script.append(httpx.Response(200, headers={'content-type':'text/event-stream'}, stream=stream))
        with patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock) as wait:
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={'stream': True})
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(result.content.count(b'partial'), 1)
        self.assertIn(b'upstream_stream_interrupted', result.content)
        self.assertTrue(stream.closed)
        wait.assert_not_awaited()

    async def test_429_retries_preserve_payload_close_response_and_log_recovery(self):
        for app in (proxy.app, chat_proxy.app):
            with self.subTest(app=app):
                start = len(self.calls)
                stream = ByteStream([b'{"error":{"code":"rate_limit_exceeded"}}'])
                self.script.extend([httpx.Response(429, stream=stream, headers={"retry-after": "7"}), httpx.Response(200, json={"ok": True})])
                async def wait(backend, request, delay):
                    self.assertTrue(stream.closed)
                    self.assertGreaterEqual(delay, 7)
                body = b'{"model":"test","input":"unchanged"}'
                with patch.object(xingchi_proxy, "_wait_retry", side_effect=wait), self.assertLogs(xingchi_proxy.logger, level="INFO") as logs:
                    async with self.client(app) as client:
                        result = await client.post("/xingchi/responses?tag=a&tag=b", content=body, headers={"authorization": "Bearer test-only"})
                self.assertEqual(result.status_code, 200)
                self.assertEqual(len(self.calls), start + 2)
                self.assertEqual(self.calls[-1].content, body)
                self.assertEqual(self.calls[-1].headers['authorization'], 'Bearer test-only')
                self.assertEqual(self.calls[-1].url.params.get_list('tag'), ['a', 'b'])
                self.assertIn('[Xingchi Retry 1/2]', '\n'.join(logs.output))
                self.assertIn('[Xingchi Recovered]', '\n'.join(logs.output))
                self.assertEqual(proxy._RATE_LIMIT_STATE['cooldown_until'], 0)

    async def test_429_exhaustion_returns_last_body_headers_and_exact_attempt_count(self):
        self.script.extend([httpx.Response(429, content=f'limited-{i}'.encode(), headers={'retry-after': '1', 'x-request-id': str(i)}) for i in range(3)])
        with patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock) as wait, self.assertLogs(xingchi_proxy.logger, level='WARNING') as logs:
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(wait.await_count, 2)
        self.assertEqual(result.status_code, 429)
        self.assertEqual(result.content, b'limited-2')
        self.assertEqual(result.headers['retry-after'], '1')
        self.assertEqual(result.headers['x-request-id'], '2')
        self.assertIn('[Xingchi Retry Exhausted]', '\n'.join(logs.output))

    async def test_long_hint_and_permanent_quota_are_not_retried(self):
        self.assertLess(xingchi_proxy.MAX_429_RECOVERY_S, 10)
        for headers, payload in (({'retry-after': '40'}, {'error': {'code': 'rate_limit_exceeded'}}), ({'retry-after': '10'}, {}), ({}, {'error': {'code': 'insufficient_quota'}})):
            start = len(self.calls)
            self.script.append(httpx.Response(429, headers=headers, json=payload))
            with patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock) as wait:
                async with self.client(chat_proxy.app) as client:
                    result = await client.post('/xingchi/responses', json={})
            self.assertEqual(result.status_code, 429)
            self.assertEqual(result.json(), payload)
            self.assertEqual(len(self.calls), start + 1)
            wait.assert_not_awaited()

    async def test_disconnect_during_429_delay_does_not_launch_retry(self):
        self.script.append(httpx.Response(429, json={}))
        with patch.object(xingchi_proxy, '_wait_retry', side_effect=proxy._ClientDisconnected):
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(result.status_code, 499)
        self.assertEqual(len(self.calls), 1)

    async def test_budget_expiry_during_wait_does_not_launch_retry(self):
        self.script.append(httpx.Response(429, content=b'last rejection'))
        clock = [0.0]
        async def wait(*args):
            clock[0] = xingchi_proxy.MAX_429_RECOVERY_S + 1
        with patch.object(xingchi_proxy, '_wait_retry', side_effect=wait), patch.object(xingchi_proxy.time, 'monotonic', side_effect=lambda: clock[0]):
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(result.status_code, 429)
        self.assertEqual(result.content, b'last rejection')
        self.assertEqual(len(self.calls), 1)

    async def test_retry_wait_observes_disconnect_without_agentrouter_cooldown(self):
        class Request:
            async def is_disconnected(self):
                return True
        with self.assertRaises(proxy._ClientDisconnected):
            await xingchi_proxy._wait_retry(proxy, Request(), 60)
        self.assertEqual(proxy._RATE_LIMIT_STATE['cooldown_until'], 0)

    async def test_retried_stream_is_not_replayed_after_partial_output(self):
        partial = b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n'
        self.script.extend([httpx.Response(429, json={}), httpx.Response(200, headers={'content-type':'text/event-stream'}, stream=ByteStream([partial]))])
        with patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock):
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={'stream': True})
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(result.content.count(b'partial'), 1)
        self.assertIn(b'upstream_stream_interrupted', result.content)

    async def test_502_after_429_is_not_retried_or_logged_as_recovery(self):
        self.script.extend([httpx.Response(429, json={}), httpx.Response(502, content=b'upstream failed')])
        with patch.object(xingchi_proxy, '_wait_retry', new_callable=AsyncMock), self.assertLogs(xingchi_proxy.logger, level='INFO') as logs:
            async with self.client(chat_proxy.app) as client:
                result = await client.post('/xingchi/responses', json={})
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.calls), 2)
        self.assertIn('[Xingchi Retry Stopped]', '\n'.join(logs.output))
        self.assertNotIn('[Xingchi Recovered]', '\n'.join(logs.output))

    def test_retry_delay_hints_dates_invalid_values_and_backoff(self):
        with patch.object(xingchi_proxy.random, 'uniform', return_value=0):
            self.assertEqual(xingchi_proxy._retry_delay(httpx.Headers({'retry-after-ms':'500'}), 0), 0.5)
            self.assertEqual(xingchi_proxy._retry_delay(httpx.Headers({'retry-after':'7'}), 0), 7)
            for raw in ('NaN', 'inf', '-1', 'invalid'):
                self.assertEqual(xingchi_proxy._retry_delay(httpx.Headers({'retry-after':raw}), 1), 2)
            date = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
            delay = xingchi_proxy._retry_delay(httpx.Headers({'retry-after':date}), 0)
            self.assertGreater(delay, 28)
            self.assertLessEqual(delay, 30)

    async def test_native_sse_formats_are_preserved(self):
        streams = (
            ("/xingchi/messages", b'event: content_block_delta\ndata: {"type":"content_block_delta","delta":{"text":"hello"}}\n\nevent: message_stop\ndata: {"type":"message_stop"}\n\n'),
            ("/xingchi/responses", b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello"}\n\nevent: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n'),
            ("/xingchi/v1beta/models/gemini-test:streamGenerateContent", b'data: {"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}]}\n\ndata: {"usageMetadata":{"totalTokenCount":12}}\n\n'),
            ("/xingchi/images/generations", b'event: image_generation.completed\ndata: {"type":"image_generation.completed","b64_json":"test-only"}\n\n'),
            ("/xingchi/images/edits", b'data: {"type":"image_edit.completed","b64_json":"test-only"}\n\n'),
        )
        for app in (proxy.app, chat_proxy.app):
            async with self.client(app) as client:
                for path, content in streams:
                    stream = ByteStream([content[:17], content[17:]])
                    self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
                    result = await client.post(path, json={"stream": True})
                    self.assertEqual(result.content, content)
                    self.assertTrue(stream.closed)

    async def test_multipart_images_and_response_headers_are_preserved(self):
        body = b'--boundary\r\nContent-Disposition: form-data; name="image"; filename="test.png"\r\nContent-Type: image/png\r\n\r\n\x89PNG\x00\r\n--boundary--\r\n'
        self.script.append(httpx.Response(200, content=b"image-output", headers={
            "content-type": "image/png", "connection": "x-private", "x-private": "not-downstream",
            "x-request-id": "request-123",
        }))
        async with self.client(proxy.app) as client:
            result = await client.post("/xingchi/images/edits", content=body,
                                       headers={"content-type": "multipart/form-data; boundary=boundary"})
        self.assertEqual(self.calls[-1].content, body)
        self.assertEqual(self.calls[-1].headers["content-type"], "multipart/form-data; boundary=boundary")
        self.assertEqual(result.content, b"image-output")
        self.assertEqual(result.headers["x-request-id"], "request-123")
        self.assertNotIn("x-private", result.headers)

    async def test_valid_terminal_events_close_upstreams_that_stay_open(self):
        for path, content in (
            ("/xingchi/messages", b'event: message_stop\ndata: {"type":"message_stop"}\n\n'),
            ("/xingchi/responses", b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n'),
        ):
            stream = ByteStream([content], stall=True)
            self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
            async with self.client(chat_proxy.app) as client:
                result = await asyncio.wait_for(client.post(path, json={}), timeout=2)
            self.assertEqual(result.content, content)
            self.assertTrue(stream.closed)

    async def test_malformed_terminal_metadata_does_not_hide_truncation(self):
        for content in (b'event: response.completed\ndata: not-json\n\n', b'event: message_stop\ndata: not-json\n\n'):
            self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=ByteStream([content])))
            async with self.client(proxy.app) as client:
                result = await client.post("/xingchi/responses", json={})
            self.assertIn("upstream_stream_interrupted", result.text)

    async def test_agentrouter_cooldown_does_not_delay_xingchi(self):
        with patch.dict(proxy._RATE_LIMIT_STATE, {"cooldown_until": float("inf")}):
            self.script.append(httpx.Response(200, json={}))
            async with self.client(proxy.app) as client:
                result = await asyncio.wait_for(client.get("/xingchi/models"), timeout=2)
            self.assertEqual(result.status_code, 200)

    async def test_preheader_disconnect_cancels_send_or_buffered_read(self):
        for app in (proxy.app, chat_proxy.app):
            for stalled_read in (False, True):
                started = asyncio.Event()
                cancelled = asyncio.Event()
                stream = ByteStream([], stall=True)

                async def respond(request):
                    started.set()
                    if stalled_read:
                        return httpx.Response(200, headers={"content-type": "application/json"}, stream=stream)
                    try:
                        await asyncio.Event().wait()
                    finally:
                        cancelled.set()

                messages = asyncio.Queue()
                await messages.put({"type": "http.request", "body": b"{}", "more_body": False})
                sent = []

                async def send(message):
                    sent.append(message)

                scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET",
                         "scheme": "http", "path": "/xingchi/models", "raw_path": b"/xingchi/models",
                         "query_string": b"", "root_path": "", "headers": [], "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 2)}
                async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as upstream:
                    with patch.object(proxy, "_SHARED_CLIENT", upstream):
                        task = asyncio.create_task(app(scope, messages.get, send))
                        try:
                            await asyncio.wait_for(started.wait(), timeout=2)
                            await messages.put({"type": "http.disconnect"})
                            await asyncio.wait_for(task, timeout=2)
                            self.assertEqual(sent[0]["status"], 499)
                            self.assertTrue(stream.closed if stalled_read else cancelled.is_set())
                            self.assertFalse(upstream.is_closed)
                        finally:
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)

    async def test_sse_errors_and_truncation_do_not_become_success_or_replay(self):
        native_error = b'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n\n'
        self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=ByteStream([native_error])))
        async with self.client(chat_proxy.app) as client:
            self.assertEqual((await client.post("/xingchi/messages", json={})).content, native_error)
            partial = b'event: response.output_text.delta\ndata: {"delta":"partial"}\n\n'
            self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=ByteStream([partial])))
            result = await client.post("/xingchi/chat/completions", json={
                "model": "test", "stream": True, "messages": [{"role": "user", "content": "hi"}],
            })
            self.assertIn("partial", result.text)
            self.assertIn("upstream_stream_interrupted", result.text)
            self.assertIn("[DONE]", result.text)
        self.assertEqual(len(self.calls), 2)

    async def test_oversized_sse_frame_is_bounded_and_closed(self):
        stream = ByteStream([b"data: " + b"x" * 100])
        self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
        with patch.object(proxy, "MAX_SSE_FRAME_BYTES", 64):
            async with self.client(proxy.app) as client:
                result = await client.post("/xingchi/responses", json={})
        self.assertIn("upstream_sse_frame_too_large", result.text)
        self.assertTrue(stream.closed)

    async def test_stream_delivers_before_completion_and_disconnect_closes_upstream(self):
        for app in (proxy.app, chat_proxy.app):
            stream = ByteStream([b'event: response.output_text.delta\ndata: {"delta":"first"}\n\n'], stall=True)
            self.script.append(httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
            messages = asyncio.Queue()
            await messages.put({"type": "http.request", "body": b'{"model":"test","messages":[{"role":"user","content":"hi"}],"stream":true}', "more_body": False})
            first = asyncio.Event()

            async def send(message):
                if b"first" in message.get("body", b""):
                    first.set()

            scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "POST",
                     "scheme": "http", "path": "/xingchi/chat/completions", "raw_path": b"/xingchi/chat/completions",
                     "query_string": b"", "root_path": "", "headers": [], "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 2)}
            task = asyncio.create_task(app(scope, messages.get, send))
            try:
                await asyncio.wait_for(first.wait(), timeout=2)
                self.assertFalse(task.done())
                await messages.put({"type": "http.disconnect"})
                await asyncio.wait_for(task, timeout=2)
                self.assertTrue(stream.closed)
                self.assertFalse(self.upstream.is_closed)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_health_exposes_additional_upstream_without_changing_identity(self):
        for module, identity in ((proxy, "dev-api-proxy"), (chat_proxy, "dev-api-proxy")):
            data = await module.health()
            self.assertEqual(data["proxy"], identity)
            self.assertEqual(data["additional_upstreams"]["xingchi"], "https://api.xingchiapi.com")


if __name__ == "__main__":
    unittest.main()
