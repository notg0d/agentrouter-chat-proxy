"""Small ASGI guard for local authentication, routes, and bounded uploads."""

from __future__ import annotations

import asyncio
import hmac
import json
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


def allowed_methods(path: str) -> frozenset[str]:
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
