from __future__ import annotations

from core.common.proxy import binance_proxy_url


def test_binance_proxy_url_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("CQ_BINANCE_PROXY", "http://proxy.local:1234")

    assert binance_proxy_url() == "http://proxy.local:1234"


def test_binance_proxy_url_defaults_to_empty(monkeypatch) -> None:
    monkeypatch.delenv("CQ_BINANCE_PROXY", raising=False)

    assert binance_proxy_url() == ""
