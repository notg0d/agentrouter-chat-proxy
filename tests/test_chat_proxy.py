"""Tests for the OpenCode Chat Completions proxy (chat_proxy.py)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import unittest

import httpx

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import chat_proxy  # noqa: E402
import proxy  # noqa: E402


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
    hdrs = {"content-type": "text/event-stream"}
    if headers:
        hdrs.update(headers)
    return httpx.Response(
        status_code=status_code,
        headers=hdrs,
        stream=_ChunkedAsyncStream(chunks_and_delays),
    )


def _reset_chat_proxy(mock_transport: _QueuedMockTransport, overrides: dict | None = None) -> None:
    proxy.reset_rate_limit_state()
    for key in (
        "requests_total",
        "chat_requests",
        "stream_requests",
        "nonstream_requests",
        "rate_limits_caught",
        "precommit_retries",
        "tool_call_streams",
    ):
        chat_proxy.STATS[key] = 0
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
        for key, value in overrides.items():
            setattr(proxy, key, value)


def _data_events(body: str) -> list[str]:
    """Returns the payload of every `data:` line in an SSE body, block by block."""
    events: list[str] = []
    for block in body.split("\n\n"):
        lines = [line[5:].strip() for line in block.splitlines() if line.startswith("data:")]
        if lines:
            events.append("\n".join(lines))
    return events


def _chunk_objects(body: str) -> list[dict]:
    return [json.loads(event) for event in _data_events(body) if event != "[DONE]"]


def _stream_pieces(body: str) -> tuple[str, str, list[dict]]:
    """Collects concatenated content, reasoning, and tool call deltas."""
    content = ""
    reasoning = ""
    tool_calls: list[dict] = []
    for chunk in _chunk_objects(body):
        for choice in chunk.get("choices") or []:
            delta = choice.get("delta") or {}
            content += delta.get("content") or ""
            reasoning += delta.get("reasoning_content") or ""
            for tool_call in delta.get("tool_calls") or []:
                tool_calls.append(tool_call)
    return content, reasoning, tool_calls


class ChatCompletionsTranslationTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_is_translated_to_stateless_responses_payload(self):
        mock = _QueuedMockTransport([
            httpx.Response(
                status_code=200,
                json={
                    "id": "resp_plain",
                    "status": "completed",
                    "output": [{
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "a.py"}],
                    }],
                    "usage": {"input_tokens": 4, "output_tokens": 2, "total_tokens": 6},
                },
            )
        ])
        _reset_chat_proxy(mock)

        payload = {
            "model": "gpt-6.1-sol",
            "messages": [
                {"role": "system", "content": "be terse"},
                {"role": "user", "content": "list files"},
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "bash", "arguments": "{\"cmd\":\"ls\"}"},
                    }],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "a.py"},
            ],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "bash",
                    "description": "run a command",
                    "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}},
                },
            }],
            "tool_choice": "auto",
            "max_tokens": 128,
            "temperature": 0.2,
            "stream": False,
        }

        transport = httpx.ASGITransport(app=chat_proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8789") as client:
            r = await client.post("/v1/chat/completions", json=payload)

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 1)
        self.assertTrue(mock.calls[0]["url"].endswith("/v1/responses"))

        sent = mock.calls[0]["body"]
        self.assertEqual(sent["instructions"], "be terse")
        self.assertNotIn("previous_response_id", sent)
        self.assertNotIn("messages", sent)
        self.assertEqual(sent["stream"], False)
        self.assertEqual(sent["max_output_tokens"], 128)
        self.assertEqual(sent["temperature"], 0.2)
        self.assertEqual(sent["tools"], [{
            "type": "function",
            "name": "bash",
            "description": "run a command",
            "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}},
        }])
        self.assertEqual(sent["tool_choice"], "auto")

        input_items = sent["input"]
        self.assertEqual(input_items[0], {"type": "message", "role": "user", "content": "list files"})
        self.assertEqual(input_items[1], {
            "type": "function_call",
            "call_id": "call_1",
            "name": "bash",
            "arguments": "{\"cmd\":\"ls\"}",
        })
        self.assertEqual(input_items[2], {
            "type": "function_call_output",
            "call_id": "call_1",
            "output": "a.py",
        })
        for item in input_items:
            self.assertNotIn("id", item)

        self.assertEqual(r.json()["choices"][0]["message"]["content"], "a.py")
        self.assertEqual(chat_proxy.STATS["nonstream_requests"], 1)

    async def test_nonstream_response_preserves_text_reasoning_and_tool_calls(self):
        mock = _QueuedMockTransport([
            httpx.Response(
                status_code=200,
                json={
                    "id": "resp_1",
                    "created_at": 1700000000,
                    "model": "gpt-6.1-sol",
                    "status": "completed",
                    "output": [
                        {
                            "type": "reasoning",
                            "id": "rs_1",
                            "summary": [{"type": "summary_text", "text": "thinking"}],
                        },
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "running ls"}],
                        },
                        {
                            "type": "function_call",
                            "id": "fc_1",
                            "call_id": "call_9",
                            "name": "bash",
                            "arguments": "{\"cmd\":\"ls\"}",
                        },
                    ],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "total_tokens": 15,
                        "output_tokens_details": {"reasoning_tokens": 2},
                    },
                },
            )
        ])
        _reset_chat_proxy(mock)

        transport = httpx.ASGITransport(app=chat_proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8789") as client:
            r = await client.post("/chat/completions", json={
                "model": "gpt-6.1-sol",
                "messages": [{"role": "user", "content": "list"}],
                "stream": False,
            })

        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["object"], "chat.completion")
        self.assertEqual(body["created"], 1700000000)
        choice = body["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        message = choice["message"]
        self.assertEqual(message["role"], "assistant")
        self.assertEqual(message["content"], "running ls")
        self.assertEqual(message["reasoning_content"], "thinking")
        self.assertEqual(message["tool_calls"], [{
            "id": "call_9",
            "type": "function",
            "function": {"name": "bash", "arguments": "{\"cmd\":\"ls\"}"},
        }])
        self.assertEqual(body["usage"], {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "completion_tokens_details": {"reasoning_tokens": 2},
        })

    async def test_stream_preserves_text_reasoning_and_tool_calls(self):
        upstream = (
            b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}\n\n'
            b'event: response.reasoning_summary_text.delta\ndata: {"type":"response.reasoning_summary_text.delta","item_id":"rs_1","delta":"think "}\n\n'
            b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","item_id":"msg_1","delta":"Hel"}\n\n'
            b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","item_id":"msg_1","delta":"lo"}\n\n'
            b'event: response.output_item.added\ndata: {"type":"response.output_item.added","output_index":2,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"bash","arguments":""}}\n\n'
            b'event: response.function_call_arguments.delta\ndata: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":2,"delta":"{\\"cmd\\":"}\n\n'
            b'event: response.function_call_arguments.delta\ndata: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":2,"delta":"\\"ls\\"}"}\n\n'
            b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7}}}\n\n'
        )
        mock = _QueuedMockTransport([_sse_response([(upstream, 0.0)])])
        _reset_chat_proxy(mock)

        transport = httpx.ASGITransport(app=chat_proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8789") as client:
            r = await client.post("/v1/chat/completions", json={
                "model": "gpt-6.1-sol",
                "messages": [{"role": "user", "content": "list"}],
                "stream": True,
            })

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 1)
        self.assertEqual(mock.calls[0]["body"]["stream"], True)
        self.assertTrue(r.headers["content-type"].startswith("text/event-stream"))

        body = r.text
        self.assertNotIn("event: response.", body)
        self.assertIn("data: [DONE]", body)

        chunks = _chunk_objects(body)
        self.assertEqual(chunks[0]["choices"][0]["delta"]["role"], "assistant")
        content, reasoning, tool_calls = _stream_pieces(body)
        self.assertEqual(content, "Hello")
        self.assertEqual(reasoning, "think ")

        announcements = [tc for tc in tool_calls if tc.get("id")]
        self.assertEqual(len(announcements), 1)
        self.assertEqual(announcements[0]["id"], "call_1")
        self.assertEqual(announcements[0]["type"], "function")
        self.assertEqual(announcements[0]["function"]["name"], "bash")
        arguments = "".join(tc.get("function", {}).get("arguments") or "" for tc in tool_calls)
        self.assertEqual(arguments, '{"cmd":"ls"}')
        self.assertEqual(len({tc["index"] for tc in tool_calls}), 1)

        final = chunks[-1]
        self.assertEqual(final["choices"][0]["finish_reason"], "tool_calls")
        self.assertEqual(final["usage"]["completion_tokens"], 4)
        self.assertEqual(chat_proxy.STATS["tool_call_streams"], 1)

    async def test_stream_rate_limit_retried_before_commit(self):
        limited = (
            b'event: response.failed\ndata: {"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"Please retry after 0.05 seconds"}}}\n\n'
        )
        ok = (
            b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"recovered"}\n\n'
            b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n'
        )
        mock = _QueuedMockTransport([
            _sse_response([(limited, 0.0)]),
            _sse_response([(ok, 0.0)]),
        ])
        _reset_chat_proxy(mock)

        transport = httpx.ASGITransport(app=chat_proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8789") as client:
            r = await client.post("/v1/chat/completions", json={
                "model": "gpt-6.1-sol",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            })

        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mock.calls), 2)
        self.assertNotIn("rate_limit_exceeded", r.text)
        content, _, _ = _stream_pieces(r.text)
        self.assertEqual(content, "recovered")
        self.assertEqual(chat_proxy.STATS["precommit_retries"], 1)
        self.assertEqual(chat_proxy.STATS["rate_limits_caught"], 1)

    async def test_streamed_reasoning_and_tool_calls_replayed_from_completed_only(self):
        upstream = (
            b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","output":['
            b'{"type":"reasoning","summary":[{"type":"summary_text","text":"hmm"}]},'
            b'{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]},'
            b'{"type":"function_call","id":"fc_2","call_id":"call_2","name":"read","arguments":"{\\"p\\":1}"}'
            b'],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}\n\n'
        )
        mock = _QueuedMockTransport([_sse_response([(upstream, 0.0)])])
        _reset_chat_proxy(mock)

        transport = httpx.ASGITransport(app=chat_proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8789") as client:
            r = await client.post("/v1/chat/completions", json={
                "model": "gpt-6.1-sol",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            })

        self.assertEqual(r.status_code, 200)
        content, reasoning, tool_calls = _stream_pieces(r.text)
        self.assertEqual(content, "done")
        self.assertEqual(reasoning, "hmm")
        self.assertEqual(tool_calls[0]["id"], "call_2")
        self.assertEqual(tool_calls[0]["function"]["name"], "read")
        self.assertEqual(
            "".join(tc.get("function", {}).get("arguments") or "" for tc in tool_calls),
            '{"p":1}',
        )
        self.assertEqual(_chunk_objects(r.text)[-1]["choices"][0]["finish_reason"], "tool_calls")

    async def test_upstream_http_error_is_forwarded(self):
        mock = _QueuedMockTransport([
            httpx.Response(400, json={"error": {"message": "bad tool schema", "type": "invalid_request_error"}}),
        ])
        _reset_chat_proxy(mock)

        transport = httpx.ASGITransport(app=chat_proxy.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8789") as client:
            r = await client.post("/v1/chat/completions", json={
                "model": "gpt-6.1-sol",
                "messages": [{"role": "user", "content": "hi"}],
            })

        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["error"]["message"], "bad tool schema")
        self.assertEqual(len(mock.calls), 1)


if __name__ == "__main__":
    unittest.main()
