"""
chat_proxy.py -- Local OpenAI Chat Completions compatibility proxy for OpenCode.

Purpose
-------
OpenCode providers can use either Chat Completions or Responses. This proxy
supports both on the same port. Chat Completions requests are translated to
AgentRouter's Responses endpoint:

  1. Intercepts `POST /v1/chat/completions` (and `/chat/completions`).
  2. Converts the request into a Responses payload built only from conversation
     history. Nothing server-minted (`previous_response_id`, item IDs, encrypted
     reasoning) is sent, so the cross-resource session-state errors fixed by the
     Codex proxy cannot occur in the first place.
  3. Converts the upstream Responses SSE stream back into Chat Completions
     chunks, preserving streamed text, reasoning (`reasoning_content`) and tool
     calls (`tool_calls` with incremental `function.arguments`).
  4. Reuses the Codex proxy's transport layer (resilient DNS, shared keep-alive
     pool, transport retries) and its pre-commit SSE priming, so early
     `rate_limit_exceeded` / `response.failed` / empty streams are retried
     before headers reach OpenCode.
   5. Routes native `/v1/responses` (and `/responses`) requests through the
      Codex proxy's recovery handler, NEVER through generic passthrough.
   6. Runs separately from the Codex proxy: port 8789 by default
     (`CHAT_PROXY_PORT` or `--port`).
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import json
import logging
import math
import os
import sys
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Set, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

import proxy

logger = logging.getLogger("agentrouter-chat-proxy")

UPSTREAM = proxy.UPSTREAM
DEFAULT_LISTEN_HOST = proxy.DEFAULT_LISTEN_HOST
PORT = int(os.environ.get("CHAT_PROXY_PORT", "8789"))
LISTEN_HOST, PORT = proxy._extract_uvicorn_bind_overrides(sys.argv, DEFAULT_LISTEN_HOST, PORT)

VERSION = "1.1.0"

STATS: Dict[str, Any] = {
    "requests_total": 0,
    "chat_requests": 0,
    "responses_requests": 0,
    "stream_requests": 0,
    "nonstream_requests": 0,
    "rate_limits_caught": 0,
    "precommit_retries": 0,
    "tool_call_streams": 0,
    "start_time": time.time(),
}


# ----------------------------------------------------------------------------
# Chat Completions request -> stateless Responses request
# ----------------------------------------------------------------------------
def _text_from_content(content: Any) -> str:
    """Flattens a Chat Completions content value (string or part list) to text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                part_type = part.get("type")
                if part_type in ("text", "input_text", "output_text"):
                    parts.append(str(part.get("text") or ""))
        return "".join(parts)
    return str(content)


def _content_to_responses(content: Any) -> Any:
    """Converts user content to Responses content, keeping text and images."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts: List[Dict[str, Any]] = []
    for part in content if isinstance(content, list) else [content]:
        if isinstance(part, str):
            parts.append({"type": "input_text", "text": part})
            continue
        if not isinstance(part, dict):
            continue
        part_type = str(part.get("type") or "")
        if part_type in ("text", "input_text", "output_text"):
            parts.append({"type": "input_text", "text": str(part.get("text") or "")})
        elif part_type in ("image_url", "input_image"):
            image = part.get("image_url")
            url = image.get("url") if isinstance(image, dict) else image
            if not url:
                continue
            converted: Dict[str, Any] = {"type": "input_image", "image_url": str(url)}
            if isinstance(image, dict) and image.get("detail"):
                converted["detail"] = image["detail"]
            parts.append(converted)
    if not parts:
        return ""
    if all(part.get("type") == "input_text" for part in parts):
        return "".join(str(part.get("text") or "") for part in parts)
    return parts


def _translate_tools(tools: Any) -> List[Dict[str, Any]]:
    converted: List[Dict[str, Any]] = []
    for tool in tools if isinstance(tools, list) else []:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            continue
        fn = tool.get("function") if isinstance(tool.get("function"), dict) else {}
        name = fn.get("name")
        if not name:
            continue
        item: Dict[str, Any] = {
            "type": "function",
            "name": name,
            "parameters": fn.get("parameters") or {"type": "object", "properties": {}},
        }
        if fn.get("description"):
            item["description"] = fn["description"]
        if fn.get("strict") is not None:
            item["strict"] = bool(fn["strict"])
        converted.append(item)
    return converted


def _translate_tool_choice(choice: Any) -> Optional[Any]:
    if isinstance(choice, str):
        lowered = choice.strip().lower()
        return lowered if lowered in ("auto", "none", "required") else None
    if isinstance(choice, dict):
        choice_type = choice.get("type")
        if choice_type == "function":
            fn = choice.get("function") if isinstance(choice.get("function"), dict) else {}
            name = fn.get("name") or choice.get("name")
            return {"type": "function", "name": name} if name else None
        if choice_type in ("auto", "none", "required"):
            return choice_type
    return None


def _translate_response_format(fmt: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(fmt, dict):
        return None
    fmt_type = fmt.get("type")
    if fmt_type == "json_schema":
        schema = fmt.get("json_schema") if isinstance(fmt.get("json_schema"), dict) else {}
        converted: Dict[str, Any] = {
            "type": "json_schema",
            "name": schema.get("name") or "response",
            "schema": schema.get("schema") or {},
        }
        if schema.get("strict") is not None:
            converted["strict"] = bool(schema["strict"])
        return converted
    if fmt_type in ("json_object", "text"):
        return {"type": fmt_type}
    return None


def _translate_reasoning(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Maps Chat Completions reasoning options onto Responses `reasoning`."""
    existing = payload.get("reasoning")
    if isinstance(existing, dict):
        return existing
    effort = payload.get("reasoning_effort")
    if not isinstance(effort, str) or not effort.strip():
        return None
    effort = effort.strip().lower()
    if effort in ("none", "off", "false"):
        return None
    if effort in ("xhigh", "max", "ultra"):
        effort = "high"
    if effort not in ("minimal", "low", "medium", "high"):
        return None
    return {"effort": effort, "summary": "auto"}


def chat_messages_to_responses(payload: Dict[str, Any]) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """Converts Chat Completions messages to Responses `instructions` + `input`."""
    instructions: List[str] = []
    items: List[Dict[str, Any]] = []

    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "user").lower()
        content = message.get("content")

        if role in ("system", "developer"):
            text = _text_from_content(content)
            if text:
                instructions.append(text)
            continue

        if role == "tool":
            call_id = message.get("tool_call_id") or message.get("call_id") or ""
            items.append({
                "type": "function_call_output",
                "call_id": call_id,
                "output": _text_from_content(content),
            })
            continue

        if role == "assistant":
            text = _text_from_content(content)
            if text:
                items.append({"type": "message", "role": "assistant", "content": text})
            for tool_call in message.get("tool_calls") or []:
                if not isinstance(tool_call, dict):
                    continue
                fn = tool_call.get("function") if isinstance(tool_call.get("function"), dict) else {}
                items.append({
                    "type": "function_call",
                    "call_id": tool_call.get("id") or "",
                    "name": fn.get("name") or "",
                    "arguments": fn.get("arguments") or "",
                })
            continue

        items.append({"type": "message", "role": "user", "content": _content_to_responses(content)})

    return ("\n\n".join(instructions) if instructions else None), items


def build_responses_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Builds a self-contained, stateless Responses request from a chat request."""
    instructions, items = chat_messages_to_responses(payload)
    out: Dict[str, Any] = {
        "model": payload.get("model"),
        "input": items,
        "stream": bool(payload.get("stream")),
    }
    if instructions:
        out["instructions"] = instructions

    for key in ("temperature", "top_p", "parallel_tool_calls", "metadata"):
        if payload.get(key) is not None:
            out[key] = payload[key]

    max_tokens = payload.get("max_completion_tokens")
    if not isinstance(max_tokens, int):
        max_tokens = payload.get("max_tokens")
    if isinstance(max_tokens, int) and max_tokens > 0:
        out["max_output_tokens"] = max_tokens

    tools = _translate_tools(payload.get("tools"))
    if tools:
        out["tools"] = tools
    tool_choice = _translate_tool_choice(payload.get("tool_choice"))
    if tool_choice is not None:
        out["tool_choice"] = tool_choice

    response_format = _translate_response_format(payload.get("response_format"))
    if response_format:
        out["text"] = {"format": response_format}

    reasoning = _translate_reasoning(payload)
    if reasoning:
        out["reasoning"] = reasoning

    return out


# ----------------------------------------------------------------------------
# Responses JSON -> Chat Completions completion
# ----------------------------------------------------------------------------
def _usage_from_response(usage: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(usage, dict):
        return None
    prompt_tokens = int(usage.get("input_tokens") or 0)
    completion_tokens = int(usage.get("output_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))
    out: Dict[str, Any] = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    }
    details = usage.get("output_tokens_details")
    if isinstance(details, dict) and details.get("reasoning_tokens") is not None:
        out["completion_tokens_details"] = {"reasoning_tokens": details["reasoning_tokens"]}
    return out


def _reasoning_text_from_item(item: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in ("summary", "content"):
        for part in item.get(key) or []:
            if isinstance(part, dict) and part.get("type") in ("summary_text", "reasoning_text", "text"):
                text = part.get("text")
                if text:
                    parts.append(str(text))
    return "".join(parts)


def responses_json_to_chat(obj: Dict[str, Any], fallback_model: str) -> Dict[str, Any]:
    """Converts a complete Responses object into a Chat Completions completion."""
    content_parts: List[str] = []
    reasoning_parts: List[str] = []
    tool_calls: List[Dict[str, Any]] = []
    status = str(obj.get("status") or "completed")

    for item in obj.get("output") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            for part in item.get("content") or []:
                if not isinstance(part, dict):
                    continue
                if part.get("type") in ("output_text", "text"):
                    content_parts.append(str(part.get("text") or ""))
                elif part.get("type") == "refusal":
                    content_parts.append(str(part.get("refusal") or ""))
        elif item_type == "reasoning":
            reasoning_parts.append(_reasoning_text_from_item(item))
        elif item_type == "function_call":
            tool_calls.append({
                "id": item.get("call_id") or item.get("id") or "",
                "type": "function",
                "function": {
                    "name": item.get("name") or "",
                    "arguments": item.get("arguments") or "",
                },
            })

    if not content_parts and isinstance(obj.get("output_text"), str):
        content_parts.append(obj["output_text"])

    message: Dict[str, Any] = {
        "role": "assistant",
        "content": "".join(content_parts) if content_parts else (None if tool_calls else ""),
    }
    reasoning = "".join(reasoning_parts)
    if reasoning:
        message["reasoning_content"] = reasoning
    if tool_calls:
        message["tool_calls"] = tool_calls

    finish_reason = "tool_calls" if tool_calls else "stop"
    incomplete = obj.get("incomplete_details") if isinstance(obj.get("incomplete_details"), dict) else {}
    if status == "incomplete" and incomplete.get("reason") == "max_output_tokens":
        finish_reason = "length"

    completion: Dict[str, Any] = {
        "id": f"chatcmpl-{obj.get('id') or 'completion'}",
        "object": "chat.completion",
        "created": int(obj.get("created_at") or time.time()),
        "model": obj.get("model") or fallback_model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
    }
    usage = _usage_from_response(obj.get("usage"))
    if usage:
        completion["usage"] = usage
    return completion


# ----------------------------------------------------------------------------
# Responses SSE -> Chat Completions chunks
# ----------------------------------------------------------------------------
@dataclass
class _ChatStreamState:
    chunk_id: str
    created: int
    model: str
    next_tool_index: int = 0
    index_by_id: Dict[str, int] = field(default_factory=dict)
    announced: Set[int] = field(default_factory=set)
    streamed_args: Dict[int, bool] = field(default_factory=dict)
    saw_text: bool = False
    saw_reasoning: bool = False
    saw_tool_call: bool = False
    finished: bool = False


def _chunk_bytes(
    state: _ChatStreamState,
    delta: Dict[str, Any],
    finish_reason: Optional[str] = None,
    usage: Optional[Dict[str, Any]] = None,
) -> bytes:
    payload: Dict[str, Any] = {
        "id": state.chunk_id,
        "object": "chat.completion.chunk",
        "created": state.created,
        "model": state.model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    if usage is not None:
        payload["usage"] = usage
    return b"data: " + json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n\n"


def _finish_chunks(
    state: _ChatStreamState,
    finish_reason: str,
    usage: Optional[Dict[str, Any]],
) -> List[bytes]:
    if state.finished:
        return []
    state.finished = True
    return [_chunk_bytes(state, {}, finish_reason, usage=usage), b"data: [DONE]\n\n"]


def _function_call_key(item: Dict[str, Any], obj: Optional[Dict[str, Any]] = None) -> str:
    return str(
        item.get("id")
        or (obj or {}).get("item_id")
        or item.get("call_id")
        or ""
    )


def _tool_index(state: _ChatStreamState, key: str) -> int:
    existing = state.index_by_id.get(key) if key else None
    if existing is not None:
        return existing
    index = state.next_tool_index
    state.next_tool_index += 1
    if key:
        state.index_by_id[key] = index
    return index


def _announce_function_call(
    state: _ChatStreamState,
    item: Dict[str, Any],
    obj: Optional[Dict[str, Any]] = None,
) -> List[bytes]:
    index = _tool_index(state, _function_call_key(item, obj))
    if index in state.announced:
        return []
    state.announced.add(index)
    state.saw_tool_call = True
    STATS["tool_call_streams"] += 1
    return [_chunk_bytes(state, {"tool_calls": [{
        "index": index,
        "id": item.get("call_id") or item.get("id") or f"call_{index}",
        "type": "function",
        "function": {"name": item.get("name") or "", "arguments": ""},
    }]})]


def _replay_completed_output(state: _ChatStreamState, response_obj: Dict[str, Any]) -> List[bytes]:
    """Emits output the stream never delivered as deltas (some upstreams only send `response.completed`)."""
    chunks: List[bytes] = []
    for item in response_obj.get("output") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")

        if item_type == "message" and not state.saw_text:
            texts = []
            for part in item.get("content") or []:
                if isinstance(part, dict) and part.get("type") in ("output_text", "text"):
                    texts.append(str(part.get("text") or ""))
            if texts:
                state.saw_text = True
                chunks.append(_chunk_bytes(state, {"content": "".join(texts)}))

        elif item_type == "reasoning" and not state.saw_reasoning:
            reasoning = _reasoning_text_from_item(item)
            if reasoning:
                state.saw_reasoning = True
                chunks.append(_chunk_bytes(state, {"reasoning_content": reasoning}))

        elif item_type == "function_call":
            key = _function_call_key(item)
            index = state.index_by_id.get(key) if key else None
            if index is None or index not in state.announced:
                chunks.extend(_announce_function_call(state, item))
                index = state.index_by_id.get(key) if key else None
            arguments = item.get("arguments")
            if index is not None and not state.streamed_args.get(index) and arguments:
                state.streamed_args[index] = True
                chunks.append(_chunk_bytes(state, {"tool_calls": [
                    {"index": index, "function": {"arguments": str(arguments)}}
                ]}))
    return chunks


def _handle_stream_event(
    state: _ChatStreamState,
    event_name: str,
    obj: Optional[Dict[str, Any]],
    data_str: str,
) -> Tuple[List[bytes], bool]:
    """Translates one Responses SSE event into zero or more chat completion chunks."""
    if state.finished:
        return [], False

    event = event_name.lower()
    obj = obj or {}

    if event in ("error", "response.failed"):
        code, message, error_obj = proxy._extract_error_fields(obj, data_str)
        error: Dict[str, Any] = {
            "message": message or "Upstream stream failed",
            "type": error_obj.get("type") or "proxy_error",
        }
        if code:
            error["code"] = code
        return [
            b"data: " + json.dumps({"error": error}).encode("utf-8") + b"\n\n",
            b"data: [DONE]\n\n",
        ], True

    if event == "response.output_item.added":
        item = obj.get("item") if isinstance(obj.get("item"), dict) else {}
        if item.get("type") == "function_call":
            return _announce_function_call(state, item, obj), False
        return [], False

    if event == "response.output_item.done":
        item = obj.get("item") if isinstance(obj.get("item"), dict) else {}
        if item.get("type") == "function_call":
            key = _function_call_key(item)
            index = state.index_by_id.get(key) if key else None
            arguments = item.get("arguments")
            if index is not None and not state.streamed_args.get(index) and arguments:
                state.streamed_args[index] = True
                return [_chunk_bytes(state, {"tool_calls": [
                    {"index": index, "function": {"arguments": str(arguments)}}
                ]})], False
        return [], False

    if event == "response.function_call_arguments.delta":
        delta = obj.get("delta")
        if not isinstance(delta, str) or not delta:
            return [], False
        key = str(obj.get("item_id") or "")
        index = state.index_by_id.get(key) if key else None
        if index is None or index not in state.announced:
            # No announced tool call for this item yet; `response.completed` replays it whole.
            return [], False
        state.streamed_args[index] = True
        return [_chunk_bytes(state, {"tool_calls": [
            {"index": index, "function": {"arguments": delta}}
        ]})], False

    if event == "response.output_text.delta":
        delta = obj.get("delta")
        if isinstance(delta, str) and delta:
            state.saw_text = True
            return [_chunk_bytes(state, {"content": delta})], False
        return [], False

    if event in ("response.reasoning_summary_text.delta", "response.reasoning_text.delta"):
        delta = obj.get("delta")
        if isinstance(delta, str) and delta:
            state.saw_reasoning = True
            return [_chunk_bytes(state, {"reasoning_content": delta})], False
        return [], False

    if event == "response.refusal.delta":
        delta = obj.get("delta")
        if isinstance(delta, str) and delta:
            state.saw_text = True
            return [_chunk_bytes(state, {"content": delta})], False
        return [], False

    if event in ("response.completed", "response.incomplete"):
        response_obj = obj.get("response") if isinstance(obj.get("response"), dict) else obj
        chunks = _replay_completed_output(state, response_obj)
        finish_reason = "tool_calls" if state.saw_tool_call else "stop"
        if event == "response.incomplete":
            details = response_obj.get("incomplete_details")
            if isinstance(details, dict) and details.get("reason") == "max_output_tokens":
                finish_reason = "length"
        chunks.extend(_finish_chunks(state, finish_reason, _usage_from_response(response_obj.get("usage"))))
        return chunks, True

    return [], False


def _handle_stream_frame(state: _ChatStreamState, raw_frame: bytes) -> Tuple[List[bytes], bool]:
    event_name, obj, data_str = proxy._parse_sse_frame(raw_frame)
    if not event_name and obj is None:
        if data_str == "[DONE]":
            return _finish_chunks(state, "tool_calls" if state.saw_tool_call else "stop", None), True
        return [], False
    return _handle_stream_event(state, event_name, obj, data_str)


async def _translate_responses_stream(
    upstream_response: httpx.Response,
    *,
    primed_chunks: Optional[List[bytes]],
    remainder: bytes,
    queue: Optional[asyncio.Queue],
    producer_task: Optional[asyncio.Task],
    model: str,
    producer_exc: Optional[List[Exception]] = None,
    ping_interval: float = 15.0,
) -> AsyncIterator[bytes]:
    """Streams Chat Completions chunks derived from the upstream Responses SSE stream."""
    if queue is None or producer_task is None:
        queue = asyncio.Queue(maxsize=32)
        producer_exc = []
        producer_task = asyncio.create_task(proxy._sse_producer(upstream_response, queue, producer_exc))

    state = _ChatStreamState(
        chunk_id=f"chatcmpl-{int(time.time() * 1000):x}",
        created=int(time.time()),
        model=model,
    )
    yield _chunk_bytes(state, {"role": "assistant", "content": ""})

    buf = bytearray(remainder or b"")

    def _consume_frames() -> Tuple[List[bytes], bool]:
        """Translates every complete SSE frame currently buffered."""
        nonlocal buf
        out: List[bytes] = []
        pos = 0
        done = False
        for match in proxy.SSE_DELIM_RE.finditer(buf):
            raw_frame = bytes(buf[pos:match.end()])
            pos = match.end()
            frame_chunks, done = _handle_stream_frame(state, raw_frame)
            out.extend(frame_chunks)
            if done:
                break
        if pos:
            del buf[:pos]
        return out, done

    try:
        for raw_frame in primed_chunks or ():
            chunks, finished = _handle_stream_frame(state, raw_frame)
            for chunk in chunks:
                yield chunk
            if finished:
                return

        # Frames already read during pre-commit priming must not wait for a new chunk.
        chunks, finished = _consume_frames()
        for chunk in chunks:
            yield chunk
        if finished:
            return

        while True:
            try:
                chunk = await asyncio.wait_for(queue.get(), timeout=ping_interval)
            except asyncio.TimeoutError:
                yield b": ping\n\n"
                continue
            if chunk is None:
                break

            buf.extend(chunk)
            chunks, finished = _consume_frames()
            for out_chunk in chunks:
                yield out_chunk
            if finished:
                return

        if buf and not state.finished:
            chunks, finished = _handle_stream_frame(state, bytes(buf))
            for out_chunk in chunks:
                yield out_chunk
            if finished:
                return

        if not state.finished:
            if producer_exc:
                logger.warning("[Stream Interrupted] %s", type(producer_exc[0]).__name__)
            # Never turn a dropped connection / timeout into a successful stop,
            # or replay an already-delivered tool call behind the client's back.
            yield b"data: " + json.dumps(proxy._stream_interrupted_error()).encode("utf-8") + b"\n\n"
            yield b"data: [DONE]\n\n"
    finally:
        await proxy._close_sse_producer(upstream_response, producer_task)


# ----------------------------------------------------------------------------
# FastAPI Application
# ----------------------------------------------------------------------------
@asynccontextmanager
async def _app_lifespan(_: FastAPI):
    if not proxy.is_loopback_host(LISTEN_HOST):
        logger.warning(
            "Non-loopback bind enabled (%s:%s) -- restrict access with a firewall "
            "and avoid exposing a shared fallback API key",
            LISTEN_HOST,
            PORT,
        )
    try:
        yield
    finally:
        if proxy._SHARED_CLIENT and not proxy._SHARED_CLIENT.is_closed:
            await proxy._SHARED_CLIENT.aclose()


app = FastAPI(
    title="agentrouter-chat-proxy",
    description="Local Chat Completions and Responses compatibility proxy for OpenCode",
    version=VERSION,
    lifespan=_app_lifespan,
)


@app.get("/_health")
async def health():
    return {
        "status": "ok",
        "proxy": "agentrouter-chat-proxy",
        "version": VERSION,
        "api": "chat.completions",
        "supported_apis": ["chat.completions", "responses"],
        "listen": f"{LISTEN_HOST}:{PORT}",
        "uptime_seconds": round(time.time() - STATS["start_time"], 1),
        "upstream": UPSTREAM,
        "stats": STATS,
        "responses_stats": proxy.STATS,
    }


@app.get("/_stats")
async def stats():
    return {**STATS, "responses_stats": proxy.STATS}


@app.post("/v1/responses")
@app.post("/responses")
async def handle_responses(request: Request):
    """Apply session recovery and shared retry handling to native Responses too."""
    STATS["requests_total"] += 1
    STATS["responses_requests"] += 1
    return await proxy.handle_responses(request)


def _chat_error(status_code: int, message: str, err_type: str = "proxy_error", code: str = "") -> JSONResponse:
    error: Dict[str, Any] = {"message": message, "type": err_type}
    if code:
        error["code"] = code
    return JSONResponse(status_code=status_code, content={"error": error})


@app.post("/v1/chat/completions")
@app.post("/chat/completions")
async def handle_chat_completions(request: Request):
    """Translates OpenCode Chat Completions traffic to stateless Responses requests."""
    STATS["requests_total"] += 1
    STATS["chat_requests"] += 1

    body_bytes = await request.body()
    try:
        payload = json.loads(body_bytes.decode("utf-8"))
    except Exception:
        return _chat_error(400, "Request body must be valid JSON", "invalid_request_error")
    if not isinstance(payload, dict):
        return _chat_error(400, "Request body must be a JSON object", "invalid_request_error")

    model = str(payload.get("model") or "")
    want_stream = bool(payload.get("stream"))
    STATS["stream_requests" if want_stream else "nonstream_requests"] += 1

    headers = proxy._filter_headers(dict(request.headers))
    headers["content-type"] = "application/json"

    upstream_body = json.dumps(build_responses_payload(payload)).encode("utf-8")
    upstream_url = f"{UPSTREAM}/v1/responses"

    retry_start = time.monotonic()
    transient_retries = 0

    while True:
        if not await proxy._wait_for_rate_limit_gate(
            request, deadline=retry_start + proxy.MAX_RATE_LIMIT_WAIT_S
        ):
            if await proxy._client_disconnected(request):
                return Response(status_code=499)
            return proxy._shared_cooldown_response()

        try:
            resp = await proxy._send_with_transport_retry(
                proxy.get_shared_client(),
                method="POST",
                url=upstream_url,
                headers=headers,
                content=upstream_body,
                stream=True,
                max_retries=1,  # Outer retry loop owns the budget; avoid nested retries.
            )
        except proxy.RETRYABLE_TRANSPORT_ERRORS as exc:
            out = await _schedule_retry(
                request, None, f"{type(exc).__name__}: {exc}",
                is_rate_limit=False, label="Transport", attempts=transient_retries,
                retry_start=retry_start,
            )
            if out is None:
                proxy.STATS["transport_retries"] += 1
                transient_retries += 1
                continue
            return out

        resp_headers = proxy._clean_response_headers(resp.headers)
        content_type = resp.headers.get("content-type", "").lower()

        if resp.status_code == 200 and "text/event-stream" in content_type:
            if not want_stream:
                await proxy._close_sse_producer(resp, None)
                logger.error("[Protocol] Upstream returned SSE for a non-streaming chat request")
                return _chat_error(502, "Upstream returned a stream for a non-streaming request")

            prime = await proxy._prime_responses_sse(resp, request)
            if prime.status == "commit":
                proxy._clear_rate_limit_cooldown()
                logger.info(
                    "[Stream] Translating Responses SSE -> Chat Completions chunks (model=%s)", model
                )
                return StreamingResponse(
                    _translate_responses_stream(
                        resp,
                        primed_chunks=prime.primed_chunks,
                        remainder=prime.remainder,
                        queue=prime.queue,
                        producer_task=prime.producer_task,
                        producer_exc=prime.producer_exc,
                        model=model,
                    ),
                    status_code=200,
                    headers={**resp_headers, "cache-control": "no-cache"},
                    media_type="text/event-stream",
                )

            if prime.status == "client_disconnected":
                return Response(status_code=499)

            STATS["precommit_retries"] += 1
            is_rate_limit = prime.status == "retry_rate_limit"
            out = await _schedule_retry(
                request,
                resp,
                prime.error_message,
                is_rate_limit=is_rate_limit,
                label="Rate Limit / SSE" if is_rate_limit else "Stream Pre-Commit",
                attempts=transient_retries,
                retry_start=retry_start,
            )
            if out is None:
                transient_retries += 1
                continue
            return out

        try:
            body = await proxy._read_and_close_response(resp)
        except proxy.RETRYABLE_TRANSPORT_ERRORS as exc:
            out = await _schedule_retry(
                request, resp, f"{type(exc).__name__}: {exc}",
                is_rate_limit=False, label="Response Read", attempts=transient_retries,
                retry_start=retry_start,
            )
            if out is None:
                proxy.STATS["transport_retries"] += 1
                transient_retries += 1
                continue
            return out
        body_text = body.decode("utf-8", errors="replace")

        if resp.status_code == 200:
            try:
                parsed = json.loads(body_text)
            except Exception:
                return _chat_error(502, "Upstream returned a non-JSON completion", "proxy_error")
            if not isinstance(parsed, dict):
                return _chat_error(502, "Upstream returned an unexpected completion payload", "proxy_error")

            if parsed.get("error") or parsed.get("status") == "failed":
                code, message, error_obj = proxy._extract_error_fields(parsed, "")
                is_rate_limit = proxy._is_rate_limit_error(code, message)
                if is_rate_limit or proxy._is_transient_error(code, message):
                    out = await _schedule_retry(
                        request,
                        resp,
                        message,
                        is_rate_limit=is_rate_limit,
                        label="Rate Limit / JSON" if is_rate_limit else "JSON Error",
                        attempts=transient_retries,
                        retry_start=retry_start,
                    )
                    if out is None:
                        transient_retries += 1
                        continue
                    return out
                return _chat_error(502, message or "Upstream request failed", code=code)

            proxy._clear_rate_limit_cooldown()
            return JSONResponse(
                content=responses_json_to_chat(parsed, model),
                status_code=200,
                headers=resp_headers,
            )

        is_rate_limit = resp.status_code == 429 or proxy._is_rate_limit_error("", body_text)
        if resp.status_code in proxy.TRANSIENT_HTTP_STATUSES or (resp.status_code == 400 and is_rate_limit):
            out = await _schedule_retry(
                request,
                resp,
                body_text,
                is_rate_limit=is_rate_limit,
                label=f"Gateway {resp.status_code}",
                attempts=transient_retries,
                retry_start=retry_start,
            )
            if out is None:
                transient_retries += 1
                continue
            return out

        logger.error("[Forwarding Error %d] %s", resp.status_code, body_text[:200])
        return Response(content=body, status_code=resp.status_code, headers=resp_headers)


async def _schedule_retry(
    request: Request,
    resp: Optional[httpx.Response],
    message: str,
    *,
    is_rate_limit: bool,
    label: str,
    attempts: int,
    retry_start: float,
) -> Optional[Response]:
    """
    Returns None when the caller should retry upstream, or the exhausted response.

    `attempts` counts retries already performed, so callers increment it on None.
    """
    remaining_budget = proxy.MAX_RATE_LIMIT_WAIT_S - (time.monotonic() - retry_start)
    if is_rate_limit:
        STATS["rate_limits_caught"] += 1
        streak = min(int(proxy._RATE_LIMIT_STATE["streak"]) + 1, 4)
        delay_s, hint_s = proxy._extract_retry_delay_s(resp.headers if resp is not None else {}, message, streak)
        retry_after = hint_s if hint_s is not None else max(
            1, math.ceil(delay_s)
        )
        # Keep the FULL provider cooldown, including when this caller gives up.
        proxy._record_rate_limit_cooldown(delay_s)
    else:
        delay_s = min(1.0 * (2 ** min(attempts, 3)) + 0.2, proxy.RATE_LIMIT_MAX_DELAY_S)
        retry_after = max(1, int(round(delay_s)))

    if attempts >= proxy.MAX_TRANSIENT_RETRIES or delay_s >= remaining_budget:
        if is_rate_limit:
            logger.error("[Forwarding Rate Limit 429] Exhausted retries: %s", (message or "")[:200])
            return JSONResponse(
                status_code=429,
                content={"error": {
                    "message": message or "Upstream rate limit exceeded",
                    "type": "rate_limit_error",
                    "code": "rate_limit_exceeded",
                }},
                headers={"retry-after": str(max(1, retry_after))},
            )
        logger.error("[Forwarding Upstream Error 502] %s", (message or "")[:200])
        return _chat_error(502, message or "Upstream stream failed before emitting output")

    wait_s = delay_s
    logger.warning(
        "[%s] Retrying (%d/%d) after %.1fs: %s",
        label, attempts + 1, proxy.MAX_TRANSIENT_RETRIES, wait_s, (message or "")[:140],
    )
    if not await proxy._sleep_unless_disconnected(wait_s, request):
        return Response(status_code=499)
    return None


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def passthrough_handler(request: Request, path: str):
    """Passthrough for model listings and any other API routes."""
    STATS["requests_total"] += 1

    headers = proxy._filter_headers(dict(request.headers))
    body = await request.body()

    clean_path = path.lstrip("/")
    target_url = f"{UPSTREAM}/{clean_path}" if clean_path.startswith("v1/") else f"{UPSTREAM}/v1/{clean_path}"

    try:
        resp = await proxy._send_with_transport_retry(
            proxy.get_shared_client(),
            method=request.method,
            url=target_url,
            headers=headers,
            params=dict(request.query_params),
            content=body,
            stream=False,
            max_retries=2,
        )
        resp_headers = {
            k: v for k, v in resp.headers.items()
            if k.lower() not in proxy.STRIP_RESPONSE_HEADERS
        }
        return Response(content=resp.content, status_code=resp.status_code, headers=resp_headers)
    except Exception as exc:
        logger.error("[Passthrough Error] %s -> %s: %s", request.method, target_url, str(exc))
        return _chat_error(502, f"Proxy passthrough error: {exc}")


# ----------------------------------------------------------------------------
# Main entrypoint
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    cli_args = proxy._parse_proxy_cli_args(default_host=LISTEN_HOST, default_port=PORT)
    LISTEN_HOST = cli_args.host
    PORT = cli_args.port
    os.environ["CHAT_PROXY_PORT"] = str(PORT)

    bind_scope = "loopback only" if proxy.is_loopback_host(LISTEN_HOST) else "non-loopback -- firewall required"
    url_host = f"[{LISTEN_HOST}]" if ":" in LISTEN_HOST and not LISTEN_HOST.startswith("[") else LISTEN_HOST
    print(f"[*] Starting agentrouter-chat-proxy on http://{url_host}:{PORT} ({bind_scope})")
    print(f"[*] Serving /v1/chat/completions and /v1/responses -> {UPSTREAM}/v1/responses")
    uvicorn.run(app, host=LISTEN_HOST, port=PORT, timeout_keep_alive=600, log_level="info")
