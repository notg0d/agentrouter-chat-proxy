"""Native Xingchi/Sub2API relay, isolated from AgentRouter API conversion."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from request_guard import XINGCHI_PREFIX, xingchi_upstream_path


def _upstream_base(raw: str) -> str:
    parts = urlsplit(raw.strip())
    if (parts.scheme not in ("http", "https") or not parts.hostname or parts.username is not None
            or parts.password is not None or parts.query or parts.fragment or parts.path.rstrip("/") not in ("", "/v1")):
        raise ValueError("XINGCHI_UPSTREAM must be an HTTP(S) server URL without credentials, query, or fragment; /v1 is optional")
    return f"{parts.scheme}://{parts.netloc}"


UPSTREAM = _upstream_base(os.environ.get("XINGCHI_UPSTREAM", "https://api.xingchiapi.com"))
logger = logging.getLogger("xingchi-proxy")


def _stream_error(path: str, code: str, message: str) -> bytes:
    if "v1beta/" in path:
        payload = {"error": {"code": 502, "message": message, "status": "UNAVAILABLE"}}
        event = b""
    elif path.endswith("/messages"):
        payload = {"type": "error", "error": {"type": "api_error", "message": message}}
        event = b"event: error\n"
    else:
        payload = {"error": {"type": "proxy_error", "code": code, "message": message}}
        event = b"event: error\n" if path.endswith("/responses") else b""
    return event + b"data: " + json.dumps(payload).encode("utf-8") + b"\n\n"


def _terminal_frame(backend, frame: bytes) -> tuple[bool, bool]:
    """Return (terminal_seen, stop_now); Gemini can send usage after finishReason."""
    event, obj, data = backend._parse_sse_frame(frame)
    if data == "[DONE]":
        return True, True
    if isinstance(obj, dict):
        if event in ("message_stop", "error", "response.completed", "response.incomplete", "response.failed"):
            return True, True
        if event in ("image_generation.completed", "image_edit.completed"):
            return True, False  # Multiple images can have separate completed events.
        if obj.get("error"):
            return True, True
        feedback = obj.get("promptFeedback")
        if isinstance(feedback, dict) and feedback.get("blockReason"):
            return True, False
        candidates = obj.get("candidates")
        if isinstance(candidates, list) and any(isinstance(item, dict) and item.get("finishReason") for item in candidates):
            return True, False
    return False, False


async def _relay_stream(backend, response: httpx.Response, path: str):
    """Forward bounded native frames without replaying output or inventing success."""
    frames = backend._SSEFrameBuffer()
    terminal_seen = False
    try:
        async for chunk in response.aiter_bytes():
            for frame in frames.feed(chunk):
                terminal, stop = _terminal_frame(backend, frame)
                terminal_seen = terminal_seen or terminal
                yield frame
                if stop:
                    return
        if frames.buf:
            frame = bytes(frames.buf)
            terminal, _ = _terminal_frame(backend, frame)
            terminal_seen = terminal_seen or terminal
            yield frame + b"\n\n"
        if not terminal_seen:
            yield _stream_error(path, "upstream_stream_interrupted", "Xingchi stream ended without a terminal event. Partial output was not replayed.")
    except backend.SSEFrameTooLarge:
        yield _stream_error(path, "upstream_sse_frame_too_large", "Xingchi SSE frame exceeds PROXY_MAX_SSE_FRAME_BYTES.")
    except Exception as exc:
        logger.warning("[Xingchi Stream Interrupted] %s", backend.describe_exception(exc))
        yield _stream_error(path, "upstream_stream_interrupted", "Xingchi stream was interrupted. Partial output was not replayed.")
    finally:
        await backend._close_upstream_response(response)


def register_routes(app: FastAPI, backend) -> None:
    """Install the same native relay on both proxy applications."""
    async def handle_xingchi(request: Request, path: str):
        backend.STATS["requests_total"] += 1
        upstream_path = xingchi_upstream_path(request.url.path)
        headers = backend._filter_headers(
            dict(request.headers), fallback_keys=("XINGCHI_API_KEY",), agentrouter_compat=False,
        )
        body = await request.body()
        response = None
        try:
            response = await backend._send_with_transport_retry(
                backend.get_shared_client(), method=request.method, url=UPSTREAM + upstream_path,
                headers=headers, content=body, params=request.query_params.multi_items(),
                stream=True, max_retries=1, request=request,
            )
            response_headers = backend._clean_response_headers(response.headers)
            nominated = {name.strip().lower() for name in response.headers.get("connection", "").split(",")}
            response_headers = {name: value for name, value in response_headers.items() if name.lower() not in nominated}
            if response.is_success and "text/event-stream" in response.headers.get("content-type", "").lower():
                return backend._UpstreamStreamingResponse(
                    _relay_stream(backend, response, upstream_path), upstream_response=response, producer_task=None,
                    status_code=response.status_code, headers=response_headers,
                )
            content = await backend._read_and_close_response(response, request)
            return Response(content=content, status_code=response.status_code, headers=response_headers)
        except backend._ClientDisconnected:
            logger.info("[Client Disconnected] Xingchi request cancelled")
            if response is not None:
                await backend._close_upstream_response(response)
            return Response(status_code=499)
        except asyncio.CancelledError:
            if response is not None:
                await backend._close_upstream_response(response)
            raise
        except Exception as exc:
            if response is not None:
                await backend._close_upstream_response(response)
            diagnostic = backend.describe_exception(exc)
            logger.error("[Xingchi Error] %s %s: %s", request.method, upstream_path, diagnostic)
            return JSONResponse(status_code=502, content={"error": {
                "type": "proxy_error", "code": "xingchi_upstream_error",
                "message": f"Xingchi upstream error: {diagnostic}",
            }})

    # Applications call this before registering their generic catch-all route.
    app.add_api_route(XINGCHI_PREFIX + "/{path:path}", handle_xingchi,
                      methods=["GET", "POST"], name="xingchi_relay")
