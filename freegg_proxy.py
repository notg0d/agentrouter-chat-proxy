"""Native FreeGG/New API relay with provider-specific routes and credentials."""

import os
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
    xingchi_proxy.register_chat_conversion(
        app, backend, prefix=FREEGG_PREFIX, upstream=UPSTREAM, provider="freegg",
    )
    xingchi_proxy.register_routes(
        app, backend, provider="freegg", upstream=lambda: UPSTREAM,
        prefix=FREEGG_PREFIX, path_mapper=freegg_upstream_path,
    )
