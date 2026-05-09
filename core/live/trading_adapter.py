"""Private Binance Spot trading adapter for small-live execution.

This module contains the exchange-ordering surface, but it does not decide when
live trading should start. Callers must pass the readiness gate before creating
or using this adapter.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from core.data.exchange.binance_spot import BinanceSpotAdapter
from core.execution.order_types import CancelResult, OrderHandle, OrderIntent

API_KEY_ENV_VAR = "BINANCE_SPOT_API_KEY"
API_SECRET_ENV_VAR = "BINANCE_SPOT_API_SECRET"


@dataclass(frozen=True, slots=True)
class BinanceSpotCredentials:
    """Environment-loaded Binance Spot credentials."""

    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str],
        *,
        api_key_var: str = API_KEY_ENV_VAR,
        api_secret_var: str = API_SECRET_ENV_VAR,
    ) -> BinanceSpotCredentials:
        api_key = env.get(api_key_var, "").strip()
        api_secret = env.get(api_secret_var, "").strip()
        if not api_key or not api_secret:
            raise ValueError("missing Binance spot credentials")
        return cls(api_key=api_key, api_secret=api_secret)


class BinanceSpotTradingAdapter(BinanceSpotAdapter):
    """Binance Spot private-order adapter backed by ccxt."""

    def __init__(
        self,
        *,
        credentials: BinanceSpotCredentials | None = None,
        proxy: str = "",
        timeout_ms: int = 30_000,
        client: Any | None = None,
    ) -> None:
        super().__init__(proxy=proxy, timeout_ms=timeout_ms, client=client)
        self._credentials = credentials

    async def place_order(self, intent: OrderIntent, *, now_ms: int) -> OrderHandle:
        """Submit an exchange order and return a normalized handle."""
        await self._ensure_markets_loaded()
        client = self._client()
        ccxt_symbol = self._to_ccxt_symbol(intent.symbol)
        params: dict[str, Any] = {}
        if intent.client_order_id:
            params["newClientOrderId"] = intent.client_order_id
        if intent.time_in_force and intent.order_type == "limit":
            params["timeInForce"] = intent.time_in_force
        if intent.stop_price is not None:
            params["stopPrice"] = intent.stop_price

        raw = await self._call(
            client.create_order,
            ccxt_symbol,
            _to_ccxt_order_type(intent.order_type),
            intent.side,
            intent.quantity,
            intent.price,
            params,
        )
        return _handle_from_order(raw, fallback_client_id=intent.client_order_id, now_ms=now_ms)

    async def cancel_order(
        self,
        symbol: str,
        *,
        client_order_id: str,
        exchange_order_id: str | None = None,
    ) -> CancelResult:
        """Cancel a live exchange order by exchange ID or client order ID."""
        await self._ensure_markets_loaded()
        params = {"origClientOrderId": client_order_id}
        raw = await self._call(
            self._client().cancel_order,
            exchange_order_id,
            self._to_ccxt_symbol(symbol),
            params,
        )
        status = str(raw.get("status") or "").lower()
        return CancelResult(
            client_order_id=client_order_id,
            success=status in {"canceled", "cancelled", "closed"},
            reason=None if status in {"canceled", "cancelled", "closed"} else status or None,
        )

    async def fetch_order(
        self,
        symbol: str,
        *,
        client_order_id: str,
        exchange_order_id: str | None = None,
    ) -> OrderHandle:
        """Fetch a live exchange order by exchange ID or client order ID."""
        await self._ensure_markets_loaded()
        raw = await self._call(
            self._client().fetch_order,
            exchange_order_id,
            self._to_ccxt_symbol(symbol),
            {"origClientOrderId": client_order_id},
        )
        return _handle_from_order(raw, fallback_client_id=client_order_id, now_ms=0)

    async def fetch_balance(self) -> dict[str, Any]:
        """Fetch the private Spot account balance."""
        await self._ensure_markets_loaded()
        raw = await self._call(self._client().fetch_balance)
        if not isinstance(raw, dict):
            return {}
        return raw

    def _build_client(self) -> Any:
        if self._credentials is None:
            raise RuntimeError("Binance spot credentials are required for private trading")

        import ccxt  # type: ignore[import-untyped]

        options: dict[str, Any] = {
            "apiKey": self._credentials.api_key,
            "secret": self._credentials.api_secret,
            "enableRateLimit": True,
            "timeout": self.timeout_ms,
        }
        if self.proxy:
            options["proxies"] = {"http": self.proxy, "https": self.proxy}
        return ccxt.binance(options)


def _to_ccxt_order_type(order_type: str) -> str:
    if order_type == "stop_limit":
        return "STOP_LOSS_LIMIT"
    if order_type == "stop":
        return "STOP_LOSS"
    if order_type == "take_profit":
        return "TAKE_PROFIT_LIMIT"
    return order_type


def _handle_from_order(raw: Mapping[str, Any], *, fallback_client_id: str, now_ms: int) -> OrderHandle:
    status = _normalize_order_status(str(raw.get("status") or ""))
    submitted_at = int(raw.get("timestamp") or now_ms)
    return OrderHandle(
        client_order_id=str(raw.get("clientOrderId") or raw.get("client_order_id") or fallback_client_id),
        exchange_order_id=str(raw.get("id")) if raw.get("id") is not None else None,
        status=status,
        submitted_at=submitted_at,
    )


def _normalize_order_status(
    status: str,
) -> Literal["accepted", "canceled", "filled", "partial", "rejected"]:
    lowered = status.lower()
    if lowered in {"closed", "filled"}:
        return "filled"
    if lowered in {"canceled", "cancelled"}:
        return "canceled"
    if lowered in {"rejected", "expired"}:
        return "rejected"
    if lowered in {"partially_filled", "partial"}:
        return "partial"
    return "accepted"


__all__ = [
    "API_KEY_ENV_VAR",
    "API_SECRET_ENV_VAR",
    "BinanceSpotCredentials",
    "BinanceSpotTradingAdapter",
]
