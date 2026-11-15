"""Transport fault injection with mocks and ephemeral loopback servers only."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import ssl
import sys
import unittest
from unittest.mock import patch

import anyio
import httpcore
import httpx

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import chat_proxy
import proxy


def suppressed_error(error):
    """Match HTTPcore pool/stream cleanup, which clears an explicit cause."""
    try:
        raise error from None
    except Exception as caught:
        return caught


class TraceCauseTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_trace_retains_redacted_causes_after_httpcore_cleanup(self):
        cases = (
            ("connection.connect_tcp", httpcore.ConnectError, httpx.ConnectError, "connect_tcp"),
            ("connection.start_tls", httpcore.ConnectError, httpx.ConnectError, "tls_handshake"),
            ("http11.receive_response_headers", httpcore.ReadError, httpx.ReadError, "response_headers"),
            ("http11.receive_response_body", httpcore.ReadError, httpx.ReadError, "response_body"),
            ("http2.receive_response_body", httpcore.ReadError, httpx.ReadError, "response_body"),
        )
        for event, core_type, mapped_type, phase in cases:
            with self.subTest(event=event):
                diagnostics = proxy.UpstreamAttemptDiagnostics()
                cause = anyio.BrokenResourceError()
                cause.__cause__ = ConnectionResetError(10054, "reset Bearer diagnostic-token sk-test-key")
                error = core_type("")
                error.__cause__ = cause
                await diagnostics.trace(event + ".started", {})
                await diagnostics.trace(event + ".failed", {
                    "exception": error, "headers": [("authorization", "Bearer trace-header-secret")],
                })
                suppressed_error(error)
                self.assertIsNone(error.__cause__)
                mapped = mapped_type("")
                mapped.__cause__ = error
                description = proxy.describe_exception(mapped)
                self.assertIn("BrokenResourceError", description)
                self.assertIn("ConnectionResetError", description)
                self.assertIn("10054", description)
                self.assertEqual(description.count(mapped_type.__name__), 1)
                self.assertIn("phase=" + phase, diagnostics.summary())
                self.assertIsInstance(error._proxy_cause_summaries, tuple)
                self.assertTrue(all(isinstance(value, str) for value in error._proxy_cause_summaries))
                for secret in ("diagnostic-token", "sk-test-key", "trace-header-secret", "authorization"):
                    self.assertNotIn(secret, description + repr(error._proxy_cause_summaries))

    async def test_repeated_failure_trace_does_not_overwrite_saved_tls_cause(self):
        diagnostics = proxy.UpstreamAttemptDiagnostics()
        error = httpcore.ConnectError("")
        error.__cause__ = ssl.SSLCertVerificationError(1, "certificate verify failed")
        await diagnostics.trace("connection.start_tls.failed", {"exception": error})
        suppressed_error(error)
        await diagnostics.trace("connection.start_tls.failed", {"exception": error})
        description = proxy.describe_exception(error)
        self.assertEqual(description.count("SSLCertVerificationError"), 1)
        self.assertIn("certificate verify failed", description)

    async def test_cleanup_failures_and_cancellation_are_not_transport_causes(self):
        diagnostics = proxy.UpstreamAttemptDiagnostics()
        await diagnostics.trace("http11.receive_response_headers.started", {})
        error = OSError("unrelated cleanup error")
        await diagnostics.trace("http11.response_closed.failed", {"exception": error})
        self.assertFalse(hasattr(error, "_proxy_cause_summaries"))
        cancelled = asyncio.CancelledError()
        await diagnostics.trace("http11.receive_response_headers.failed", {"exception": cancelled})
        self.assertFalse(hasattr(cancelled, "_proxy_cause_summaries"))
        self.assertIn("phase=response_headers", diagnostics.summary())

    async def test_trace_snapshots_have_bounded_depth_and_message_size(self):
        diagnostics = proxy.UpstreamAttemptDiagnostics()
        error = httpcore.ReadError("")
        current = error
        for index in range(30):
            current.__cause__ = OSError(f"layer-{index}: " + "x" * 2000)
            current = current.__cause__
        await diagnostics.trace("http11.receive_response_body.failed", {"exception": error})
        suppressed_error(error)
        self.assertLessEqual(len(error._proxy_cause_summaries), 16)
        self.assertTrue(all(len(value) <= 1024 for value in error._proxy_cause_summaries))
        self.assertIn("cause chain truncated", proxy.describe_exception(error))


class StalledStream(httpx.AsyncByteStream):
    def __init__(self):
        self.started = asyncio.Event()
        self.cancelled = False
        self.closed = False

    async def __aiter__(self):
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled = True
        yield b""  # Keep this method an async generator.

    async def aclose(self):
        await asyncio.sleep(0)  # A real close may have cancellation checkpoints.
        self.closed = True


class DisconnectTests(unittest.IsolatedAsyncioTestCase):
    async def run_disconnected_request(self, app, endpoint, *, stalled_body=False, status=200, cancel_task=False):
        stream = StalledStream()
        started = stream.started if stalled_body else asyncio.Event()
        cancelled = asyncio.Event()
        calls = []
        messages = asyncio.Queue()
        sent = []
        body = {"model": "gpt-6.1-sol", "input": "hi", "stream": False}
        if "chat" in endpoint:
            body = {"model": "gpt-6.1-sol", "messages": [{"role": "user", "content": "hi"}], "stream": False}
        await messages.put({"type": "http.request", "body": json.dumps(body).encode(), "more_body": False})

        async def respond(request):
            calls.append(request)
            if stalled_body:
                return httpx.Response(status, headers={"content-type": "application/json"}, stream=stream)
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async def send(message):
            sent.append(message)

        scope = {
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": "POST", "scheme": "http", "path": endpoint,
            "raw_path": endpoint.encode(), "query_string": b"", "root_path": "",
            "headers": [(b"content-type", b"application/json")],
            "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 2),
        }
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(proxy, "_SHARED_CLIENT", client), patch.object(proxy, "PROXY_AUTH_TOKEN", ""):
                task = asyncio.create_task(app(scope, messages.get, send))
                try:
                    await asyncio.wait_for(started.wait(), timeout=2)
                    if cancel_task:
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(task, timeout=2)
                    else:
                        await messages.put({"type": "http.disconnect"})
                        with self.assertLogs(level="INFO") as logs:
                            await asyncio.wait_for(task, timeout=2)
                        self.assertIn("[Client Disconnected]", "\n".join(logs.output))
                        self.assertNotIn("[Attempt Exception]", "\n".join(logs.output))
                        self.assertNotIn("Retrying", "\n".join(logs.output))
                        self.assertEqual(sent[0]["status"], 499)
                    self.assertEqual(len(calls), 1)
                    self.assertFalse(client.is_closed)
                    if stalled_body:
                        self.assertTrue(stream.cancelled)
                        self.assertTrue(stream.closed)
                    else:
                        self.assertTrue(cancelled.is_set())
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_native_and_chat_disconnects_cancel_header_wait_without_retry(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            with self.subTest(endpoint=endpoint):
                proxy.reset_rate_limit_state()
                await self.run_disconnected_request(chat_proxy.app, endpoint)

    async def test_native_and_chat_disconnects_cancel_buffered_body_and_close(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            for status in (200, 400):
                with self.subTest(endpoint=endpoint, status=status):
                    proxy.reset_rate_limit_state()
                    await self.run_disconnected_request(chat_proxy.app, endpoint, stalled_body=True, status=status)

    async def test_passthrough_disconnects_cancel_send_and_buffered_read_in_both_apps(self):
        for app in (proxy.app, chat_proxy.app):
            for stalled_body in (False, True):
                with self.subTest(app=app, stalled_body=stalled_body):
                    await self.run_disconnected_request(app, "/v1/embeddings", stalled_body=stalled_body)

    async def test_external_task_cancellation_joins_send_and_closes_buffered_response(self):
        for stalled_body in (False, True):
            with self.subTest(stalled_body=stalled_body):
                proxy.reset_rate_limit_state()
                await self.run_disconnected_request(chat_proxy.app, "/v1/responses", stalled_body=stalled_body, cancel_task=True)

    async def test_disconnect_does_not_start_another_attempt_during_transport_backoff(self):
        class Request:
            disconnected = False

            async def is_disconnected(self):
                return self.disconnected

        request = Request()
        calls = []

        async def respond(upstream_request):
            calls.append(upstream_request)
            request.disconnected = True
            raise httpx.ConnectError("")

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.dict(proxy.STATS, {"transport_retries": 0}):
                with self.assertRaises(proxy._ClientDisconnected):
                    await proxy._send_with_transport_retry(client, "POST", "https://upstream.invalid", {}, max_retries=2, request=request)
                self.assertEqual(proxy.STATS["transport_retries"], 0)
        self.assertEqual(len(calls), 1)

    async def test_disconnect_during_scheduled_backoff_cancels_next_attempt(self):
        class Request:
            disconnected = False

            async def is_disconnected(self):
                return self.disconnected

        request = Request()
        calls = []

        async def respond(upstream_request):
            calls.append(upstream_request)
            raise httpx.ConnectError("")

        async def wait(delay, downstream):
            self.assertIs(downstream, request)
            request.disconnected = True
            return False

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(proxy, "_sleep_unless_disconnected", wait):
                with self.assertRaises(proxy._ClientDisconnected):
                    await proxy._send_with_transport_retry(client, "POST", "https://upstream.invalid", {}, max_retries=2, request=request)
        self.assertEqual(len(calls), 1)

    async def test_response_arriving_during_disconnect_race_is_closed(self):
        stream = StalledStream()
        response = httpx.Response(200, stream=stream)
        release = asyncio.Event()
        ready = asyncio.Event()
        checks = 0

        async def operation():
            await release.wait()
            ready.set()
            return response

        async def is_disconnected(request):
            nonlocal checks
            checks += 1
            if checks == 1:
                return False
            release.set()
            await ready.wait()
            return True

        with patch.object(proxy, "_client_disconnected", is_disconnected):
            with self.assertRaises(proxy._ClientDisconnected):
                await proxy._run_unless_disconnected(operation, object())
        self.assertTrue(stream.closed)

    async def test_sse_cleanup_is_shielded_inside_cancelled_anyio_scope(self):
        stream = StalledStream()
        response = httpx.Response(200, stream=stream)
        queue = asyncio.Queue(maxsize=1)
        await queue.put(b"full")
        producer = asyncio.create_task(queue.put(b"blocked"))
        with anyio.CancelScope() as scope:
            scope.cancel()
            await proxy._close_sse_producer(response, producer)
        self.assertTrue(producer.done())
        self.assertTrue(producer.cancelled())
        self.assertTrue(stream.closed)

    async def test_anyio_scope_cancellation_joins_an_upstream_operation(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class Request:
            async def is_disconnected(self):
                return False

        async def operation():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                cancelled.set()

        async def cancel_when_started(scope):
            await started.wait()
            scope.cancel()

        with anyio.CancelScope() as scope:
            canceller = asyncio.create_task(cancel_when_started(scope))
            await proxy._run_unless_disconnected(operation, Request())
        await canceller
        self.assertTrue(cancelled.is_set())

    async def test_streaming_response_disconnect_closes_native_and_translated_streams(self):
        for generator in (proxy._stream_generator, chat_proxy._translate_responses_stream):
            with self.subTest(generator=generator):
                stream = StalledStream()
                response = httpx.Response(200, stream=stream)
                queue = asyncio.Queue(maxsize=1)
                await queue.put(b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n')
                producer = asyncio.create_task(queue.put(b"blocked"))
                kwargs = {"model": "gpt-6.1-sol"} if generator == chat_proxy._translate_responses_stream else {}
                iterator = generator(response, primed_chunks=[], remainder=b"", queue=queue, producer_task=producer, producer_exc=[], **kwargs)
                downstream = proxy._UpstreamStreamingResponse(iterator, upstream_response=response, producer_task=producer)
                disconnected = asyncio.Event()

                async def receive():
                    await disconnected.wait()
                    return {"type": "http.disconnect"}

                async def send(message):
                    if message["type"] == "http.response.body" and message.get("body"):
                        disconnected.set()
                        await asyncio.Event().wait()

                await asyncio.wait_for(downstream({"type": "http"}, receive, send), timeout=2)
                self.assertTrue(stream.closed)
                self.assertTrue(producer.done())

    async def test_send_failure_before_first_chunk_closes_unstarted_iterator_and_producer(self):
        stream = StalledStream()
        response = httpx.Response(200, stream=stream)
        queue = asyncio.Queue(maxsize=1)
        await queue.put(b"full")
        producer = asyncio.create_task(queue.put(b"blocked"))
        iterator = proxy._stream_generator(response, queue=queue, producer_task=producer)
        downstream = proxy._UpstreamStreamingResponse(iterator, upstream_response=response, producer_task=producer)

        async def receive():
            await asyncio.Event().wait()

        async def send(message):
            raise RuntimeError("downstream write failed")

        with self.assertRaises(Exception):
            await downstream({"type": "http"}, receive, send)
        self.assertTrue(stream.closed)
        self.assertTrue(producer.done())

    async def test_send_failure_after_chunk_preserves_failure_and_closes_upstream(self):
        stream = StalledStream()
        response = httpx.Response(200, stream=stream)
        queue = asyncio.Queue(maxsize=1)
        await queue.put(b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n')
        producer = asyncio.create_task(queue.put(b"blocked"))
        iterator = proxy._stream_generator(response, queue=queue, producer_task=producer)
        downstream = proxy._UpstreamStreamingResponse(iterator, upstream_response=response, producer_task=producer)
        sent = []

        async def receive():
            await asyncio.Event().wait()

        async def send(message):
            sent.append(message)
            if message["type"] == "http.response.body":
                raise RuntimeError("downstream write failed")

        with self.assertRaises(Exception) as caught:
            await downstream({"type": "http"}, receive, send)
        error = caught.exception
        while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
            error = error.exceptions[0]
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(str(error), "downstream write failed")
        self.assertIn(b"partial", sent[1]["body"])
        self.assertTrue(stream.closed)
        self.assertTrue(producer.done())

    async def test_cleanup_error_does_not_replace_original_read_failure(self):
        class FailingStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                raise httpx.ReadError("original failure")
                yield b""

            async def aclose(self):
                await asyncio.sleep(0)
                raise httpx.ReadError("cleanup Bearer cleanup-secret")

        response = httpx.Response(200, stream=FailingStream())
        with self.assertLogs(proxy.logger, level="WARNING") as logs:
            with self.assertRaisesRegex(httpx.ReadError, "original failure"):
                await proxy._read_and_close_response(response)
        self.assertIn("[Upstream Cleanup]", "\n".join(logs.output))
        self.assertNotIn("cleanup-secret", "\n".join(logs.output))


class TraceRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_causes_survive_cleanup_in_header_precommit_and_partial_failures(self):
        for endpoint in ("/v1/responses", "/v1/chat/completions"):
            for phase in ("headers", "precommit", "partial"):
                with self.subTest(endpoint=endpoint, phase=phase):
                    proxy.reset_rate_limit_state()
                    calls = []

                    async def fail(trace, event):
                        error = httpcore.ReadError("")
                        cause = anyio.BrokenResourceError()
                        cause.__cause__ = ConnectionResetError(10054, "reset Bearer diagnostic-token")
                        error.__cause__ = cause
                        await trace(event + ".started", {})
                        await trace(event + ".failed", {"exception": error})
                        suppressed_error(error)
                        raise httpx.ReadError("") from error

                    class FaultStream(httpx.AsyncByteStream):
                        closed = False

                        async def __aiter__(self):
                            if phase == "partial":
                                yield b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n'
                            await fail(calls[0].extensions["trace"], "http11.receive_response_body")

                        async def aclose(self):
                            self.closed = True

                    stream = FaultStream()

                    async def respond(request):
                        calls.append(request)
                        if phase == "headers":
                            await fail(request.extensions["trace"], "http11.receive_response_headers")
                        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

                    body = {"model": "gpt-6.1-sol", "input": "hi", "stream": True}
                    if "chat" in endpoint:
                        body = {"model": "gpt-6.1-sol", "messages": [{"role": "user", "content": "hi"}], "stream": True}
                    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as upstream:
                        with patch.object(proxy, "_SHARED_CLIENT", upstream), patch.object(proxy, "MAX_TRANSIENT_RETRIES", 0):
                            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=chat_proxy.app), base_url="http://local") as local:
                                result = await local.post(endpoint, json=body)
                    self.assertEqual(len(calls), 1)
                    self.assertEqual(result.status_code, 200 if phase == "partial" else 502)
                    self.assertIn("ConnectionResetError", result.text)
                    self.assertIn("10054", result.text)
                    self.assertNotIn("diagnostic-token", result.text)
                    if phase != "headers":
                        self.assertTrue(stream.closed)
                    if phase == "partial":
                        self.assertIn("partial", result.text)
                        self.assertIn("upstream_stream_interrupted", result.text)


class LoopbackFaultTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = None
        self.writers = set()
        self.handlers = set()

    async def asyncTearDown(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in list(self.writers):
            writer.close()
        tasks = list(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def start(self, handler):
        async def accept(reader, writer):
            task = asyncio.current_task()
            self.handlers.add(task)
            self.writers.add(writer)
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                length = 0
                for line in headers.split(b"\r\n")[1:]:
                    if line.lower().startswith(b"content-length:"):
                        length = int(line.split(b":", 1)[1])
                if length:
                    await reader.readexactly(length)
                path = headers.split(b" ", 2)[1]
                await handler(path, reader, writer)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (OSError, asyncio.CancelledError):
                    pass
                self.writers.discard(writer)
                self.handlers.discard(task)

        self.server = await asyncio.start_server(accept, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    async def reply(self, writer, body=b'{"status":"completed","output":[]}', *, declared_length=None):
        length = len(body) if declared_length is None else declared_length
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: " + str(length).encode() + b"\r\n\r\n" + body)
        await writer.drain()

    def client(self, max_connections=1):
        # Test traffic stays on loopback regardless of system proxy variables.
        return httpx.AsyncClient(trust_env=False, timeout=2, limits=httpx.Limits(max_connections=max_connections))

    async def test_disconnect_before_headers_retries_once_with_the_same_client(self):
        calls = []

        async def handler(path, reader, writer):
            calls.append(path)
            if len(calls) > 1:
                await self.reply(writer)
            # First attempt closes without sending any response headers.

        url = await self.start(handler)
        async with self.client() as client:
            response = await proxy._send_with_transport_retry(client, "POST", url, {}, content=b"{}", max_retries=2)
            self.assertEqual(response.json()["status"], "completed")
            self.assertTrue(response.is_closed)
            self.assertFalse(client.is_closed)
        self.assertEqual(len(calls), 2)

    async def test_truncated_body_releases_single_pool_slot_before_retry(self):
        calls = []

        async def handler(path, reader, writer):
            calls.append(path)
            await self.reply(writer, declared_length=500 if len(calls) == 1 else None)

        url = await self.start(handler)
        async with self.client() as client:
            response = await proxy._send_with_transport_retry(client, "POST", url, {}, content=b"{}", max_retries=2)
            self.assertEqual(response.json()["status"], "completed")
            self.assertTrue(response.is_closed)
        self.assertEqual(len(calls), 2)

    async def test_cancelled_header_wait_releases_pool_slot_for_concurrent_request(self):
        accepted = asyncio.Event()
        upstream_closed = asyncio.Event()
        disconnected = asyncio.Event()

        class Request:
            async def is_disconnected(self):
                return disconnected.is_set()

        async def handler(path, reader, writer):
            if path == b"/abandoned":
                accepted.set()
                await reader.read()
                upstream_closed.set()
            else:
                await self.reply(writer)

        url = await self.start(handler)
        async with self.client() as client:
            abandoned = asyncio.create_task(proxy._send_with_transport_retry(client, "POST", url + "/abandoned", {}, request=Request()))
            healthy = None
            try:
                await asyncio.wait_for(accepted.wait(), timeout=2)
                healthy = asyncio.create_task(client.get(url + "/healthy"))
                disconnected.set()
                with self.assertRaises(proxy._ClientDisconnected):
                    await asyncio.wait_for(abandoned, timeout=2)
                response = await asyncio.wait_for(healthy, timeout=2)
                self.assertEqual(response.status_code, 200)
                await asyncio.wait_for(upstream_closed.wait(), timeout=2)
                self.assertFalse(client.is_closed)
            finally:
                for task in (abandoned, healthy):
                    if task is not None:
                        task.cancel()
                await asyncio.gather(*(task for task in (abandoned, healthy) if task is not None), return_exceptions=True)

    async def test_retry_does_not_close_a_healthy_concurrent_stream(self):
        release = asyncio.Event()
        failures = 0
        first = b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n'
        terminal = b'data: {"type":"response.completed"}\n\n'

        async def handler(path, reader, writer):
            nonlocal failures
            if path == b"/stream":
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n" + first)
                await writer.drain()
                await release.wait()
                writer.write(terminal)
                await writer.drain()
            else:
                failures += 1
                await self.reply(writer, declared_length=500 if failures == 1 else None)

        url = await self.start(handler)
        async with self.client(max_connections=2) as client:
            response = await proxy._send_with_transport_retry(client, "GET", url + "/stream", {}, stream=True, max_retries=1)
            try:
                iterator = response.aiter_bytes()
                self.assertEqual(await iterator.__anext__(), first)
                recovered = await proxy._send_with_transport_retry(client, "POST", url + "/failure", {}, content=b"{}", max_retries=2)
                self.assertEqual(recovered.status_code, 200)
                self.assertFalse(response.is_closed)
                release.set()
                rest = b"".join([chunk async for chunk in iterator])
                self.assertEqual(rest, terminal)
                self.assertEqual(failures, 2)
            finally:
                release.set()
                await response.aclose()


if __name__ == "__main__":
    unittest.main()
