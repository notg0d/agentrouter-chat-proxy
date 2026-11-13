"""Automated unit and async integration tests for the AgentRouter OpenCode Proxy."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
import sys
import threading
import time
import unittest

import httpx

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import proxy  # noqa: E402


class _FakeSock:
    def __init__(self, fail_shutdown: bool = False):
        self.fail_shutdown = fail_shutdown
        self.closed = False

    def fileno(self) -> int:
        return 123

    def shutdown(self, how: int) -> None:
        if self.fail_shutdown:
            err = ConnectionResetError(10054, "An existing connection was forcibly closed by the remote host")
            err.winerror = 10054
            raise err

    def close(self) -> None:
        self.closed = True


class _FakeServer:
    def __init__(self) -> None:
        self.detach_calls = 0

    def _detach(self, transport=None) -> None:
        self.detach_calls += 1


class _FakeProtocol:
    def __init__(self, exc_to_raise: Exception | None = None) -> None:
        self.exc_to_raise = exc_to_raise
        self.lost_calls = 0

    def connection_lost(self, exc) -> None:
        self.lost_calls += 1
        if self.exc_to_raise is not None:
            raise self.exc_to_raise


class _DummyTransport:
    def __init__(self, sock, server, protocol) -> None:
        self._sock = sock
        self._server = server
        self._protocol = protocol
        self._called_connection_lost = False


class _QueuedMockTransport(httpx.AsyncBaseTransport):
    """Deterministic async mock transport returning queued scripted responses."""

    def __init__(self, responders):
        self.responders = list(responders)
        self.calls: list[dict] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body_raw = request.content or b""
        try:
            body_json = json.loads(body_raw.decode("utf-8"))
        except Exception:
            body_json = None
        self.calls.append({
            "method": request.method,
            "url": str(request.url),
            "body": body_json,
            "t": time.monotonic(),
        })
        item = self.responders.pop(0)
        if callable(item):
            res = item(len(self.calls), body_json)
            if asyncio.iscoroutine(res):
                return await res
            return res
        return item


class _ChunkedAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks_and_delays):
        self.chunks_and_delays = chunks_and_delays

    async def __aiter__(self):
        for chunk, delay_s in self.chunks_and_delays:
            if delay_s > 0:
                await asyncio.sleep(delay_s)
            yield chunk


def _sse_response(chunks_and_delays, headers=None, status_code=200) -> httpx.Response:
    hdrs = {"content-type": "text/event-stream", "date": "Wed, 11 Nov 2026 13:50:00 GMT"}
    if headers:
        hdrs.update(headers)
    return httpx.Response(
        status_code=status_code,
        headers=hdrs,
        stream=_ChunkedAsyncStream(chunks_and_delays),
    )


def _reset_proxy_for_test(mock_transport: _QueuedMockTransport, overrides: dict | None = None):
    proxy.reset_rate_limit_state()
    proxy.STATS["rate_limits_caught"] = 0
    proxy.STATS["stream_precommit_retries"] = 0
    proxy.STATS["successful_recoveries"] = 0
    proxy.STATS["stateless_fallbacks_applied"] = 0
    proxy.STATS["resource_mismatches_caught"] = 0
    proxy._SHARED_CLIENT = httpx.AsyncClient(transport=mock_transport, timeout=proxy.CLIENT_TIMEOUT)
    proxy.RATE_LIMIT_BASE_DELAY_S = 0.05
    proxy.RATE_LIMIT_MAX_DELAY_S = 0.25
    proxy.MAX_RATE_LIMIT_WAIT_S = 5.0
    proxy.MAX_TRANSIENT_RETRIES = 4
    proxy.RECOVERY_STAGGER_S = 0.05
    proxy.RETRY_DELAY_MS = 10
    proxy.SSE_PRIME_CREATED_GRACE_S = 0.3
    proxy.SSE_PRIME_FIRST_TIMEOUT_S = 2.0
    if overrides:
        for k, v in overrides.items():
            setattr(proxy, k, v)


class ProactorTeardownTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "win32", "Windows ProactorEventLoop test")
    def test_simulated_winerror_10054_completes_teardown(self):
        import asyncio.proactor_events as proactor_events

        cls = proactor_events._ProactorBasePipeTransport
        self.assertTrue(getattr(cls, "_agentrouter_teardown_guarded", False))

        sock = _FakeSock(fail_shutdown=True)
        server = _FakeServer()
        protocol = _FakeProtocol()
        tr = _DummyTransport(sock, server, protocol)

        cls._call_connection_lost(tr, None)
        self.assertEqual(protocol.lost_calls, 1)
        self.assertTrue(sock.closed)
        self.assertIsNone(tr._sock)
        self.assertEqual(server.detach_calls, 1)
        self.assertIsNone(tr._server)
        self.assertTrue(tr._called_connection_lost)

    @unittest.skipUnless(sys.platform == "win32", "Windows ProactorEventLoop test")
    def test_protocol_oserror_propagates_without_double_detach(self):
        import asyncio.proactor_events as proactor_events

        cls = proactor_events._ProactorBasePipeTransport
        sock = _FakeSock(fail_shutdown=False)
        server = _FakeServer()
        protocol = _FakeProtocol(exc_to_raise=OSError("protocol error"))
        tr = _DummyTransport(sock, server, protocol)

        with self.assertRaisesRegex(OSError, "protocol error"):
            cls._call_connection_lost(tr, None)

        self.assertTrue(sock.closed)
        self.assertEqual(server.detach_calls, 1)
        self.assertTrue(tr._called_connection_lost)

    @unittest.skipUnless(sys.platform == "win32", "Windows ProactorEventLoop test")
    def test_live_proactor_abrupt_tcp_reset_logs_no_errors(self):
        async def _stress():
            loop = asyncio.get_running_loop()
            self.assertIsInstance(loop, asyncio.ProactorEventLoop)
            loop_exceptions = []
            loop.set_exception_handler(lambda _loop, ctx: loop_exceptions.append(ctx))

            async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                try:
                    data = await reader.read(64)
                    if data:
                        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                        await writer.drain()
                except OSError:
                    pass
                finally:
                    writer.close()

            server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]

            def reset_clients() -> None:
                for _ in range(15):
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    s.connect(("127.0.0.1", port))
                    s.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    s.close()
                    time.sleep(0.01)

            t = threading.Thread(target=reset_clients, daemon=True)
            t.start()
            while t.is_alive():
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.15)

            server.close()
            await asyncio.wait_for(server.wait_closed(), timeout=3.0)
            self.assertEqual(loop_exceptions, [])

        asyncio.run(_stress())


class ProxyRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_sse_rate_limit_recovered_before_commit(self):
        fail_event_1 = (
            b'event: response.created\ndata: {"type":"response.created","response":{"status":"in_progress"}}\n\n'
            b'event: response.failed\ndata: {"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"Your requests to gpt-6.1-sol for gpt-6.1-sol in eastus2 have exceeded token rate limit."}}}\n\n'
        )
        fail_event_2 = (
            b'event: error\ndata: {"type":"error","error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"Please retry after 0.05 seconds."}}\n\n'
        )
        ok_event = (
            b'event: response.created\ndata: {"type":"response.created","response":{"status":"in_progress"}}\n\n'
            b'event: response.output_item.added\ndata: {"type":"response.output_item.added","item":{"id":"msg_1"}}\n\n'
            b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"OK"}\n\n'
            b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n'
        )
        mock = _QueuedMockTransport([
            _sse_response([(fail_event_1, 0.0)]),
            _sse_response([(fail_event_2, 0.0)]),
            _sse_response([(ok_event, 0.0)]),
        ])
        _reset_proxy_for_test(mock)

        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r = await client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "hi", "stream": True})

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 3)
        self.assertNotIn("rate_limit_exceeded", r.text)
        self.assertEqual(r.text.count("event: response.created"), 1)
        self.assertIn("event: response.completed", r.text)
        self.assertEqual(proxy.STATS["rate_limits_caught"], 2)
        self.assertEqual(proxy.STATS["stream_precommit_retries"], 2)
        self.assertEqual(proxy.STATS["successful_recoveries"], 1)

    async def test_sse_grace_commits_slow_thinking_without_stalling(self):
        mock = _QueuedMockTransport([
            _sse_response([
                (b'event: response.created\ndata: {"type":"response.created","response":{"status":"in_progress"}}\n\n', 0.0),
                (b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"thought"}\n\n', 0.35),
                (b'event: response.completed\ndata: {"type":"response.completed"}\n\n', 0.0),
            ])
        ])
        _reset_proxy_for_test(mock, {"SSE_PRIME_CREATED_GRACE_S": 0.12})

        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r = await client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "think", "stream": True})

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 1)
        self.assertIn("response.created", r.text)
        self.assertIn('"delta":"thought"', r.text)
        self.assertIn("response.completed", r.text)

    async def test_sse_resource_mismatch_triggers_stateless_fallback(self):
        mismatch_sse = (
            b'event: response.failed\n'
            b'data: {"type":"response.failed","response":{"status":"failed","error":{"code":"invalid_request","message":"The requested item was created under a different **** OpenAI resource."}}}\n\n'
        )
        ok_sse = (
            b'event: response.output_item.added\ndata: {"type":"response.output_item.added"}\n\n'
            b'event: response.completed\ndata: {"type":"response.completed"}\n\n'
        )
        mock = _QueuedMockTransport([
            _sse_response([(mismatch_sse, 0.0)]),
            _sse_response([(ok_sse, 0.0)]),
        ])
        _reset_proxy_for_test(mock)

        payload = {
            "model": "gpt-6.1-sol",
            "previous_response_id": "resp_old",
            "input": [{"type": "reasoning", "id": "rs_1"}, {"type": "message", "id": "msg_1", "role": "user", "content": "hi"}],
            "stream": True,
        }
        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r = await client.post("/v1/responses", json=payload)

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 2)
        self.assertNotIn("previous_response_id", mock.calls[1]["body"])
        self.assertEqual(len(mock.calls[1]["body"]["input"]), 1)
        self.assertNotIn("id", mock.calls[1]["body"]["input"][0])
        self.assertEqual(proxy.STATS["stateless_fallbacks_applied"], 1)

    async def test_sse_permanent_error_commits_without_retry(self):
        bad_sse = (
            b'event: error\ndata: {"type":"error","error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"Context too long"}}\n\n'
        )
        mock = _QueuedMockTransport([_sse_response([(bad_sse, 0.0)])])
        _reset_proxy_for_test(mock)

        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r = await client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "hi", "stream": True})

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 1)
        self.assertIn("context_length_exceeded", r.text)

    async def test_sse_rate_limit_exhaustion_returns_429_with_retry_after(self):
        rl_sse = (
            b'event: response.failed\ndata: {"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"Your requests to gpt-6.1-sol in eastus2 have exceeded token rate limit."}}}\n\n'
        )
        mock = _QueuedMockTransport([_sse_response([(rl_sse, 0.0)]) for _ in range(3)])
        _reset_proxy_for_test(mock, {"MAX_TRANSIENT_RETRIES": 2})

        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r = await client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "hi", "stream": True})

        self.assertEqual(r.status_code, 429)
        self.assertGreaterEqual(int(r.headers["retry-after"]), 1)
        self.assertEqual(r.json()["error"]["code"], "rate_limit_exceeded")
        self.assertEqual(len(mock.calls), 3)

    async def test_empty_done_stream_retried_and_recovered(self):
        mock = _QueuedMockTransport([
            _sse_response([(b"data: [DONE]\n\n", 0.0)]),
            _sse_response([(b'event: response.completed\ndata: {"type":"response.completed"}\n\n', 0.0)]),
        ])
        _reset_proxy_for_test(mock)

        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r = await client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "hi", "stream": True})

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 2)
        self.assertIn("response.completed", r.text)

    async def test_http_429_exhaustion_adds_retry_after(self):
        mock = _QueuedMockTransport([
            httpx.Response(429, json={"error": {"message": "too many requests"}}) for _ in range(3)
        ])
        _reset_proxy_for_test(mock, {"MAX_TRANSIENT_RETRIES": 2})

        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r = await client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "hi", "stream": True})

        self.assertEqual(r.status_code, 429)
        self.assertGreaterEqual(int(r.headers["retry-after"]), 1)
        self.assertEqual(len(mock.calls), 3)

    async def test_concurrent_subagents_stagger_after_rate_limit(self):
        rl_sse = (
            b'event: response.failed\ndata: {"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"Please retry after 0.05 seconds"}}}\n\n'
        )
        ok_sse = b'event: response.completed\ndata: {"type":"response.completed"}\n\n'

        def dynamic_responder(call_idx, _body):
            if call_idx == 1:
                return _sse_response([(rl_sse, 0.0)])
            return _sse_response([(ok_sse, 0.0)])

        mock = _QueuedMockTransport([dynamic_responder] * 4)
        _reset_proxy_for_test(mock, {"RECOVERY_STAGGER_S": 0.12, "RATE_LIMIT_BASE_DELAY_S": 0.08})

        transport = httpx.ASGITransport(app=proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client:
            r1, r2 = await asyncio.gather(
                client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "a", "stream": True}),
                client.post("/v1/responses", json={"model": "gpt-6.1-sol", "input": "b", "stream": True}),
            )

        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(mock.calls), 3)


class TestUpdaterAndHeadlessService(unittest.TestCase):
    def _run_git(self, cwd, *args):
        import subprocess
        proc = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        return proc.stdout.strip()

    def test_updater_check_apply_dirty_and_rollback(self):
        import tempfile
        from pathlib import Path
        import updater

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            remote = base / "remote.git"
            pusher = base / "pusher"
            work = base / "work"

            self._run_git(base, "init", "--bare", str(remote))
            self._run_git(base, "clone", str(remote), str(pusher))
            self._run_git(pusher, "config", "user.email", "test@example.com")
            self._run_git(pusher, "config", "user.name", "Test")
            (pusher / "proxy.py").write_text("V = 1\n", encoding="utf-8")
            (pusher / "requirements.txt").write_text("", encoding="utf-8")
            self._run_git(pusher, "add", "proxy.py", "requirements.txt")
            self._run_git(pusher, "commit", "-m", "init")
            branch = self._run_git(pusher, "symbolic-ref", "--short", "HEAD")
            self._run_git(pusher, "push", "-u", "origin", branch)

            self._run_git(base, "clone", str(remote), str(work))

            # Clean fast-forward pull
            (pusher / "proxy.py").write_text("V = 2\n", encoding="utf-8")
            self._run_git(pusher, "add", "proxy.py")
            self._run_git(pusher, "commit", "-m", "v2")
            self._run_git(pusher, "push", "origin", branch)

            chk = updater.check_for_update(work)
            self.assertEqual(chk["status"], "update_available")
            applied = updater.apply_update(work, python_exe=sys.executable)
            self.assertTrue(applied["applied"])
            self.assertEqual(applied["status"], "updated")

            # Dirty tracked edits block overwrite
            (pusher / "proxy.py").write_text("V = 3\n", encoding="utf-8")
            self._run_git(pusher, "add", "proxy.py")
            self._run_git(pusher, "commit", "-m", "v3")
            self._run_git(pusher, "push", "origin", branch)
            (work / "proxy.py").write_text("V = 99\n", encoding="utf-8")
            dirty = updater.apply_update(work, python_exe=sys.executable)
            self.assertFalse(dirty["applied"])
            self.assertEqual(dirty["status"], "dirty_worktree")
            self._run_git(work, "checkout", "--", "proxy.py")
            good = updater.apply_update(work, python_exe=sys.executable)
            self.assertTrue(good["applied"])
            good_sha = good["local_commit"]

            # Broken syntax rolls back cleanly
            (pusher / "proxy.py").write_text("def bad(:\n", encoding="utf-8")
            self._run_git(pusher, "add", "proxy.py")
            self._run_git(pusher, "commit", "-m", "bad")
            self._run_git(pusher, "push", "origin", branch)
            rolled = updater.apply_update(work, python_exe=sys.executable)
            self.assertFalse(rolled["applied"])
            self.assertEqual(rolled["status"], "rolled_back")
            self.assertEqual(self._run_git(work, "rev-parse", "HEAD"), good_sha)

    def test_service_rotating_logger_and_headless_flags(self):
        import tempfile
        from pathlib import Path
        import service

        if sys.platform == "win32":
            self.assertEqual(service._CREATE_NO_WINDOW, 0x08000000)
        parsed = service._parse_args(["--host", "0.0.0.0", "--port", "8799", "--max-log-kb", "128", "--no-auto-update"])
        self.assertEqual(parsed.host, "0.0.0.0")
        self.assertEqual(parsed.port, 8799)
        self.assertEqual(parsed.max_log_kb, 128)
        self.assertTrue(parsed.no_auto_update)
        self.assertEqual(service._probe_connect_host("0.0.0.0"), "127.0.0.1")
        self.assertEqual(service._probe_connect_host("::"), "::1")
        self.assertEqual(service._probe_connect_host("192.168.1.25"), "192.168.1.25")

        with tempfile.TemporaryDirectory() as tmp:
            log_file = Path(tmp) / "logs" / "proxy.log"
            logger = service.RotatingFileLogger(log_file, max_kb=64)
            logger.max_bytes = 200
            for i in range(20):
                logger.write("info", f"entry-{i}-" + ("x" * 20))
                logger._line_count = 0
            self.assertTrue(log_file.is_file())
            self.assertTrue(Path(f"{log_file}.1").is_file())

    def test_cli_host_and_port_flags_and_live_bind(self):
        import json
        import socket
        import subprocess
        import time
        import urllib.request
        from pathlib import Path
        import service

        self.assertTrue(proxy.is_loopback_host("127.0.0.1"))
        self.assertTrue(proxy.is_loopback_host("localhost"))
        self.assertTrue(proxy.is_loopback_host("::1"))
        self.assertFalse(proxy.is_loopback_host("0.0.0.0"))
        self.assertFalse(proxy.is_loopback_host("192.168.10.20"))

        default_args = proxy._parse_proxy_cli_args([])
        self.assertEqual(default_args.host, "127.0.0.1")

        custom_args = proxy._parse_proxy_cli_args(["--host", "0.0.0.0", "--port", "9878"])
        self.assertEqual(custom_args.host, "0.0.0.0")
        self.assertEqual(custom_args.port, 9878)

        uv_host, uv_port = proxy._extract_uvicorn_bind_overrides(
            ["uvicorn", "proxy:app", "--host", "0.0.0.0", "--port", "9012"],
            "127.0.0.1",
            8788,
        )
        self.assertEqual((uv_host, uv_port), ("0.0.0.0", 9012))
        self.assertEqual(
            proxy._extract_uvicorn_bind_overrides(["unittest", "--port", "1234"], "127.0.0.1", 8788),
            ("127.0.0.1", 8788),
        )

        root = Path(proxy.__file__).resolve().parent
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        proc = subprocess.Popen(
            [sys.executable, str(root / "proxy.py"), "--host", "0.0.0.0", "--port", str(port)],
            cwd=str(root),
            env={**os.environ, "PROXY_AUTH_TOKEN": "local-test-only"},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            health = None
            deadline = time.monotonic() + 12.0
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    out = proc.stdout.read() if proc.stdout else ""
                    self.fail(f"proxy.py exited early ({proc.returncode}): {out}")
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/_health", timeout=1.0) as resp:
                        health = json.loads(resp.read().decode("utf-8"))
                        break
                except Exception:
                    time.sleep(0.2)

            self.assertIsNotNone(health, "proxy.py --host 0.0.0.0 did not answer /_health in time")
            self.assertEqual(health["status"], "ok")
            self.assertEqual(health["listen"], f"0.0.0.0:{port}")
            self.assertTrue(service._probe_our_health(port, "0.0.0.0"))
            self.assertFalse(service._probe_our_health(port, "127.0.0.1"))
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3.0)
            if proc.stdout:
                proc.stdout.close()


if __name__ == "__main__":
    unittest.main()
