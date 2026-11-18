"""Native Xingchi/Sub2API relay, isolated from AgentRouter API conversion."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
MAX_429_RETRIES = 2
MAX_429_RECOVERY_S = 8.0


def _retry_delay(headers, retry: int) -> float:
    """Honor valid upstream hints without shortening them to fit our budget."""
    for name, scale in (("retry-after-ms", 0.001), ("retry-after", 1.0)):
        raw = headers.get(name)
        if raw is None:
            continue
        try:
            delay = float(raw) * scale
        except ValueError:
            if name != "retry-after":
                continue
            try:
                date = parsedate_to_datetime(raw)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                delay = max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                continue
        if math.isfinite(delay) and delay >= 0:
            return delay + random.uniform(0.1, 0.4)
    return min(2.0 ** retry, 8.0) + random.uniform(0.1, 0.4)


async def _wait_retry(backend, request: Request, delay: float) -> None:
    """Do not touch AgentRouter's cooldown or its rate-limit wait counters."""
    async def wait():
        await asyncio.sleep(delay)
    await backend._run_unless_disconnected(wait, request)


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


async def _relay_stream(backend, response: httpx.Response, path: str, label: str = "Xingchi"):
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
            yield _stream_error(path, "upstream_stream_interrupted", f"{label} stream ended without a terminal event. Partial output was not replayed.")
    except backend.SSEFrameTooLarge:
        yield _stream_error(path, "upstream_sse_frame_too_large", f"{label} SSE frame exceeds PROXY_MAX_SSE_FRAME_BYTES.")
    except Exception as exc:
        logger.warning("[%s Stream Interrupted] %s", label, backend.describe_exception(exc))
        yield _stream_error(path, "upstream_stream_interrupted", f"{label} stream was interrupted. Partial output was not replayed.")
    finally:
        await backend._close_upstream_response(response)


class _ProviderLogger(logging.LoggerAdapter):
    def process(self, message, kwargs):
        return message.replace("Xingchi", self.extra["label"]), kwargs


def register_routes(app: FastAPI, backend, *, provider: str = "xingchi", upstream=None,
                    prefix: str = XINGCHI_PREFIX, path_mapper=xingchi_upstream_path) -> None:
    """Install the same native relay on both proxy applications."""
    label = "FreeGG" if provider == "freegg" else "Xingchi"
    logger = _ProviderLogger(logging.getLogger("xingchi-proxy"), {"label": label})
    async def handle_xingchi(request: Request, path: str):
        backend.STATS["requests_total"] += 1
        upstream_path = path_mapper(request.url.path)
        headers = backend._filter_headers(
            dict(request.headers), fallback_keys=(provider.upper() + "_API_KEY",), agentrouter_compat=False,
        )
        body = await request.body()
        response = None
        retries = 0
        recovery_deadline = None
        try:
            while True:
                # A prior 429 is already closed. Never reuse it as this attempt's
                # response if sending fails, and never retry the relay iterator.
                response = None
                try:
                    response = await backend._send_with_transport_retry(
                        backend.get_shared_client(), method=request.method, url=(upstream() if upstream else UPSTREAM) + upstream_path,
                        headers=headers, content=body, params=request.query_params.multi_items(),
                        stream=True, max_retries=1, request=request,
                    )
                except backend.RETRYABLE_TRANSPORT_ERRORS as exc:
                    diagnostic = backend.describe_exception(exc)
                    if not backend._retryable_transport_error(exc):
                        logger.warning("[Xingchi Retry Stopped] Transport recovery disabled: %s", diagnostic)
                        raise
                    if recovery_deadline is None:
                        recovery_deadline = time.monotonic() + MAX_429_RECOVERY_S
                    delay = _retry_delay({}, retries)
                    if retries >= MAX_429_RETRIES or time.monotonic() + delay >= recovery_deadline:
                        logger.warning("[Xingchi Retry Exhausted] Transport failure; retries=%d: %s", retries, diagnostic)
                        raise
                    retries += 1
                    logger.warning("[Xingchi Retry %d/%d] Transport failure; waiting %.2fs: %s", retries, MAX_429_RETRIES, delay, diagnostic)
                    await _wait_retry(backend, request, delay)
                    if time.monotonic() >= recovery_deadline:
                        logger.warning("[Xingchi Retry Exhausted] Transport failure; reason=time budget: %s", diagnostic)
                        raise
                    continue
                if response.status_code != 429:
                    break
                if recovery_deadline is None:
                    recovery_deadline = time.monotonic() + MAX_429_RECOVERY_S
                content = await backend._read_and_close_response(response, request)
                try:
                    payload = json.loads(content)
                except (ValueError, UnicodeDecodeError):
                    payload = None
                code, message, _ = backend._extract_error_fields(payload if isinstance(payload, dict) else None, "")
                # Quota, authentication, and policy rejections can also use 429.
                permanent = (
                    backend.classify_upstream_error(429, code, message) == "permanent"
                    or code.lower() in {"api_key_required", "invalid_api_key", "unauthenticated", "permission_denied"}
                )
                delay = _retry_delay(response.headers, retries)
                remaining = recovery_deadline - time.monotonic()
                if permanent or retries >= MAX_429_RETRIES or delay >= remaining:
                    reason = "permanent rejection" if permanent else "retry limit" if retries >= MAX_429_RETRIES else "time budget"
                    logger.warning("[Xingchi Retry Exhausted] HTTP 429; retries=%d; reason=%s", retries, reason)
                    break
                retries += 1
                logger.warning("[Xingchi Retry %d/%d] HTTP 429; waiting %.2fs", retries, MAX_429_RETRIES, delay)
                await _wait_retry(backend, request, delay)
                if time.monotonic() >= recovery_deadline:
                    logger.warning("[Xingchi Retry Exhausted] HTTP 429; retries=%d; reason=time budget", retries - 1)
                    break

            if retries and response.status_code != 429:
                if response.is_success:
                    logger.info("[Xingchi Recovered] Upstream HTTP %d after %d retries; response completion not yet confirmed", response.status_code, retries)
                else:
                    logger.warning("[Xingchi Retry Stopped] HTTP %d after %d retries; not eligible for recovery", response.status_code, retries)
            response_headers = backend._clean_response_headers(response.headers)
            nominated = {name.strip().lower() for name in response.headers.get("connection", "").split(",")}
            response_headers = {name: value for name, value in response_headers.items() if name.lower() not in nominated}
            if response.is_success and "text/event-stream" in response.headers.get("content-type", "").lower():
                return backend._UpstreamStreamingResponse(
                    _relay_stream(backend, response, upstream_path, label), upstream_response=response, producer_task=None,
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
            logger.error("[%s Error] %s %s: %s", provider, request.method, upstream_path, diagnostic)
            return JSONResponse(status_code=502, content={"error": {
                "type": "proxy_error", "code": f"{provider}_upstream_error",
                "message": f"{provider} upstream error: {diagnostic}",
            }})

    # Applications call this before registering their generic catch-all route.
    app.add_api_route(prefix + "/{path:path}", handle_xingchi,
                      methods=["GET", "POST"], name=f"{provider}_relay")
