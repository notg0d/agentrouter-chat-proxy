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
    """Use the bounded native relay, without AgentRouter conversion or keys."""
    xingchi_proxy.register_routes(
        app, backend, provider="freegg", upstream=lambda: UPSTREAM,
        prefix=FREEGG_PREFIX, path_mapper=freegg_upstream_path,
    )
