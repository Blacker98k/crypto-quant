"""Runtime proxy helpers."""

from __future__ import annotations

import os


def binance_proxy_url() -> str:
    return os.getenv("CQ_BINANCE_PROXY", "").strip()


__all__ = ["binance_proxy_url"]
