"""Native FreeGG/New API relay with provider-specific routes and credentials."""

import os
import asyncio
import json

from fastapi import Request
from fastapi.responses import JSONResponse, Response

import xingchi_proxy
from request_guard import FREEGG_PREFIX, freegg_upstream_path


def _upstream_base(raw: str) -> str:
    try:
        return xingchi_proxy._upstream_base(raw)
    except ValueError as exc:
        raise ValueError(str(exc).replace("XINGCHI_UPSTREAM", "FREEGG_UPSTREAM")) from None


UPSTREAM = _upstream_base(os.environ.get("FREEGG_UPSTREAM", "https://freegg.app"))


def register_routes(app, backend) -> None:
    """Install FreeGG's Responses-compatible relay and native API routes."""
    async def handle_chat_completions(request: Request):
        """Translate client Chat Completions to FreeGG's supported Responses API."""
        backend.STATS["requests_total"] += 1
        import chat_proxy

        payload, error = await backend.read_json_payload(request)
        if error is not None:
            return error
        messages = payload.get("messages")
        if not isinstance(messages, list) or not messages or any(not isinstance(item, dict) for item in messages):
            return JSONResponse(status_code=400, content={"error": {
                "type": "invalid_request_error", "message": "messages must be a non-empty array of objects",
            }})
        if any(item.get("role") not in ("system", "developer", "user", "assistant", "tool") for item in messages):
            return JSONResponse(status_code=400, content={"error": {
                "type": "invalid_request_error", "message": "Each message must have a supported role",
            }})

        headers = backend._filter_headers(
            dict(request.headers), fallback_keys=("FREEGG_API_KEY",), agentrouter_compat=False,
        )
        headers["content-type"] = "application/json"
        body = json.dumps(chat_proxy.build_responses_payload(payload), ensure_ascii=False).encode("utf-8")
        upstream_response = None
        try:
            upstream_response = await backend._send_with_transport_retry(
                backend.get_shared_client(), method="POST", url=UPSTREAM + "/v1/responses",
                headers=headers, content=body, params=request.query_params.multi_items(),
                stream=bool(payload.get("stream")), max_retries=2, request=request,
            )
            response_headers = backend._clean_response_headers(upstream_response.headers)
            if upstream_response.is_success and "text/event-stream" in upstream_response.headers.get("content-type", "").lower():
                queue = asyncio.Queue(maxsize=32)
                producer_exc = []
                producer_task = asyncio.create_task(backend._sse_producer(upstream_response, queue, producer_exc))
                return backend._UpstreamStreamingResponse(
                    chat_proxy._translate_responses_stream(
                        upstream_response, primed_chunks=None, remainder=b"", queue=queue,
                        producer_task=producer_task, producer_exc=producer_exc, model=payload["model"],
                    ), upstream_response=upstream_response, producer_task=producer_task,
                    status_code=upstream_response.status_code,
                    headers={**response_headers, "cache-control": "no-cache"}, media_type="text/event-stream",
                )

            result_body = await backend._read_and_close_response(upstream_response, request)
            if not upstream_response.is_success:
                return Response(content=result_body, status_code=upstream_response.status_code, headers=response_headers)
            try:
                parsed = json.loads(result_body)
            except (ValueError, UnicodeDecodeError):
                return JSONResponse(status_code=502, content={"error": {
                    "type": "proxy_error", "message": "FreeGG returned a non-JSON Responses completion",
                }})
            if not isinstance(parsed, dict):
                return JSONResponse(status_code=502, content={"error": {
                    "type": "proxy_error", "message": "FreeGG returned an unexpected Responses payload",
                }})
            if parsed.get("error") or parsed.get("status") == "failed":
                code, message, error_obj = backend._extract_error_fields(parsed, "")
                return JSONResponse(status_code=502, content={"error": {
                    "type": error_obj.get("type") or "proxy_error", "code": code,
                    "message": message or "FreeGG Responses request failed",
                }})
            return JSONResponse(
                content=chat_proxy.responses_json_to_chat(parsed, payload["model"]),
                status_code=upstream_response.status_code, headers=response_headers,
            )
        except backend._ClientDisconnected:
            return Response(status_code=499)
        except Exception as exc:
            if upstream_response is not None:
                await backend._close_upstream_response(upstream_response)
            diagnostic = backend.describe_exception(exc)
            return JSONResponse(status_code=502, content={"error": {
                "type": "proxy_error", "code": "freegg_upstream_error",
                "message": f"FreeGG upstream error: {diagnostic}",
            }})

    # Register exact endpoints before the provider catch-all relay.
    app.add_api_route(FREEGG_PREFIX + "/v1/chat/completions", handle_chat_completions,
                      methods=["POST"], name="freegg_chat_completions_v1")
    app.add_api_route(FREEGG_PREFIX + "/chat/completions", handle_chat_completions,
                      methods=["POST"], name="freegg_chat_completions")
    xingchi_proxy.register_routes(
        app, backend, provider="freegg", upstream=lambda: UPSTREAM,
        prefix=FREEGG_PREFIX, path_mapper=freegg_upstream_path,
    )
