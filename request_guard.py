"""Small ASGI guard for local authentication, routes, and bounded uploads."""

from __future__ import annotations

import asyncio
import hmac
import json
import re
import time
from typing import Callable


# Only these API routes may reach generic upstream forwarding. Model requests
# handled locally still pass through the same method/auth/upload checks.
POST_PATHS = frozenset({
    "/v1/responses", "/responses", "/v1/responses/compact", "/responses/compact",
    "/v1/chat/completions", "/chat/completions", "/v1/completions", "/completions",
    "/v1/messages", "/messages", "/v1/messages/count_tokens", "/messages/count_tokens",
    "/v1/embeddings", "/embeddings", "/v1/moderations", "/moderations",
    "/v1/rerank", "/rerank", "/v1/edits", "/edits",
    "/v1/images/generations", "/v1/images/edits", "/v1/images/variations",
    "/v1/audio/transcriptions", "/v1/audio/translations", "/v1/audio/speech",
})


XINGCHI_PREFIX = "/xingchi"
FREEGG_PREFIX = "/freegg"
FREEGG_POST_PATHS = frozenset({
    "/v1/responses", "/v1/responses/compact", "/v1/chat/completions", "/v1/completions",
    "/v1/messages", "/v1/embeddings", "/v1/moderations", "/v1/rerank",
    "/v1/images/generations", "/v1/images/edits", "/v1/edits",
    "/v1/audio/transcriptions", "/v1/audio/translations", "/v1/audio/speech",
})


def freegg_upstream_path(path: str) -> str:
    """Map the isolated FreeGG namespace to native New API routes."""
    if not path.startswith(FREEGG_PREFIX + "/"):
        return ""
    path = path[len(FREEGG_PREFIX):]
    return path if path.startswith(("/v1/", "/v1beta/")) else "/v1" + path


def _freegg_allowed_methods(path: str) -> frozenset[str]:
    path = freegg_upstream_path(path)
    if path in ("/v1/models", "/v1beta/models", "/v1beta/openai/models"):
        return frozenset({"GET"})
    if path in FREEGG_POST_PATHS:
        return frozenset({"POST"})
    if path.startswith("/v1/models/") and _XINGCHI_MODEL_RE.fullmatch(path[len("/v1/models/"):]):
        return frozenset({"GET"})
    if path.startswith(("/v1beta/models/", "/v1/models/")):
        model, separator, action = path.split("/models/", 1)[1].partition(":")
        if separator and _XINGCHI_MODEL_RE.fullmatch(model) and action in {
            "generateContent", "streamGenerateContent", "embedContent", "batchEmbedContents",
        }:
            return frozenset({"POST"})
    return frozenset()


XINGCHI_POST_PATHS = frozenset({
    "/v1/responses", "/v1/responses/compact", "/v1/responses/input_tokens",
    "/v1/chat/completions", "/v1/messages", "/v1/messages/count_tokens",
    "/v1/embeddings", "/v1/images/generations", "/v1/images/edits",
    "/v1/images/generations/async", "/v1/images/edits/async",
    "/backend-api/codex/responses", "/backend-api/codex/responses/compact",
    "/backend-api/codex/responses/input_tokens",
    "/antigravity/v1/messages", "/antigravity/v1/messages/count_tokens",
})
XINGCHI_GET_PATHS = frozenset({
    "/v1/models", "/v1/usage", "/v1/sub2api/billing", "/backend-api/codex/models",
    "/antigravity/models", "/antigravity/v1/models", "/antigravity/v1/usage",
    "/v1beta/models", "/antigravity/v1beta/models",
})
# The local /xingchi/codex namespace is an OpenAI-compatible façade. Chat
# Completions are translated locally and sent to the Codex Responses endpoint.
XINGCHI_CODEX_CHAT_PATHS = frozenset({"/backend-api/codex/chat/completions"})
_XINGCHI_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_XINGCHI_GEMINI_ACTIONS = frozenset({"generateContent", "streamGenerateContent", "countTokens", "embedContent", "batchEmbedContents"})


def xingchi_upstream_path(path: str) -> str:
    """Map only the fixed Xingchi namespace; unversioned aliases use /v1."""
    if not path.startswith(XINGCHI_PREFIX + "/"):
        return ""
    codex_prefix = XINGCHI_PREFIX + "/codex/"
    if path.startswith(codex_prefix):
        # Map /xingchi/codex[/v1]/... to the upstream Sub2API-compatible
        # /backend-api/codex/... path without exposing an arbitrary proxy.
        relative = path[len(codex_prefix):]
        if relative.startswith("v1/"):
            relative = relative[3:]
        return "/backend-api/codex/" + relative
    upstream_path = path[len(XINGCHI_PREFIX):]
    if not upstream_path.startswith(("/v1/", "/v1beta/", "/antigravity/", "/backend-api/codex/")):
        upstream_path = "/v1" + upstream_path
    return upstream_path


def _xingchi_allowed_methods(path: str) -> frozenset[str]:
    upstream_path = xingchi_upstream_path(path)
    if path.startswith(XINGCHI_PREFIX + "/codex/") and upstream_path in XINGCHI_CODEX_CHAT_PATHS:
        return frozenset({"POST"})
    if upstream_path in XINGCHI_GET_PATHS:
        return frozenset({"GET"})
    if upstream_path in XINGCHI_POST_PATHS:
        return frozenset({"POST"})
    for prefix in ("/v1/models/", "/v1beta/models/", "/antigravity/v1beta/models/", "/v1/images/tasks/"):
        if not upstream_path.startswith(prefix):
            continue
        model, separator, action = upstream_path[len(prefix):].partition(":")
        if not _XINGCHI_MODEL_RE.fullmatch(model):
            return frozenset()
        if separator:
            if "v1beta" in prefix and action in _XINGCHI_GEMINI_ACTIONS:
                return frozenset({"POST"})
            return frozenset()
        return frozenset({"GET"})
    return frozenset()


def allowed_methods(path: str) -> frozenset[str]:
    if path.startswith(FREEGG_PREFIX + "/"):
        return _freegg_allowed_methods(path)
    if path.startswith(XINGCHI_PREFIX + "/"):
        return _xingchi_allowed_methods(path)
    if path in ("/_health", "/_stats", "/v1/models", "/models"):
        return frozenset({"GET"})
    for prefix in ("/v1/models/", "/models/"):
        if path.startswith(prefix) and path[len(prefix):] and "/" not in path[len(prefix):]:
            return frozenset({"GET"})
    if path in POST_PATHS:
        return frozenset({"POST"})
    return frozenset()


class ProxyRequestGuard:
    """Reject before upstream work; replay only a complete, size-bounded body.

    Settings are callables so both applications use the same live configuration.
    This is pure ASGI, not BaseHTTPMiddleware, to preserve SSE cancellation.
    """

    def __init__(
        self, app, *, token: Callable[[], str], max_body_bytes: Callable[[], int],
        upload_timeout_s: Callable[[], float], stats: dict,
    ):
        self.app = app
        self.token = token
        self.max_body_bytes = max_body_bytes
        self.upload_timeout_s = upload_timeout_s
        self.stats = stats

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def reject(status: int, code: str, message: str, extra_headers=()):
            self.stats["requests_rejected_total"] += 1
            body = json.dumps({"error": {
                "type": "invalid_request_error" if status in (400, 404, 405, 408, 413) else "authentication_error",
                "code": code, "message": message,
            }}).encode("utf-8")
            await send({"type": "http.response.start", "status": status, "headers": [
                (b"content-type", b"application/json"), (b"content-length", str(len(body)).encode()),
                (b"connection", b"close"), *extra_headers,
            ]})
            await send({"type": "http.response.body", "body": body})

        path = scope.get("path", "")
        methods = allowed_methods(path)
        if not methods:
            await reject(404, "not_found", "This proxy does not forward this API route")
            return

        # Health remains readable for the supervisor. All other routes,
        # including counters, require the separate local token when enabled.
        expected = self.token()
        if expected and path != "/_health":
            supplied = [v for k, v in scope.get("headers", ()) if k.lower() == b"x-proxy-token"]
            if len(supplied) != 1 or not hmac.compare_digest(supplied[0], expected.encode("utf-8")):
                await reject(401, "proxy_auth_required", "Supply the local proxy token in X-Proxy-Token")
                return

        if scope.get("method") not in methods:
            await reject(405, "method_not_allowed", "This method is not permitted on this API route",
                         ((b"allow", ", ".join(sorted(methods)).encode()),))
            return

        lengths = [v for k, v in scope.get("headers", ()) if k.lower() == b"content-length"]
        if lengths:
            if len(lengths) != 1 or not lengths[0].isdigit():
                await reject(400, "invalid_content_length", "Content-Length must be one non-negative integer")
                return
            try:
                length = int(lengths[0])
            except ValueError:
                await reject(400, "invalid_content_length", "Content-Length is too large to parse")
                return
            if length > self.max_body_bytes():
                await reject(413, "payload_too_large", "Request body exceeds PROXY_MAX_BODY_BYTES")
                return

        body = bytearray()
        deadline = time.monotonic() + self.upload_timeout_s()
        while True:
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                message = await asyncio.wait_for(receive(), timeout=remaining)
            except asyncio.TimeoutError:
                await reject(408, "request_upload_timeout", "Request upload exceeded PROXY_BODY_UPLOAD_TIMEOUT_S")
                return
            if message["type"] == "http.disconnect":
                return
            if message["type"] != "http.request":
                continue
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > self.max_body_bytes():
                await reject(413, "payload_too_large", "Request body exceeds PROXY_MAX_BODY_BYTES")
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        delivered = False

        async def replay_receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            # Keep the original disconnect channel alive for StreamingResponse.
            return await receive()

        await self.app(scope, replay_receive, send)
