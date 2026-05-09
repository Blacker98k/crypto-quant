"""Manual small-live order CLI implementation."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import fields
from pathlib import Path
from typing import Any

import yaml

from core.execution.order_types import OrderIntent
from core.live.executor import LiveTradingAdapter, SmallLiveExecutor
from core.live.small_live import PaperStatus, SmallLiveConfig, evaluate_small_live_readiness
from core.live.trading_adapter import BinanceSpotCredentials, BinanceSpotTradingAdapter

LIVE_ORDER_CONFIRM_VALUE = "I_UNDERSTAND_LIVE_ORDER_RISK"


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    adapter_factory: Callable[[Mapping[str, str]], LiveTradingAdapter] | None = None,
) -> int:
    args = _parser().parse_args(argv)
    runtime_env = env if env is not None else os.environ

    config = _load_dataclass(SmallLiveConfig, Path(args.config), loader=_load_yaml)
    paper_status = _load_dataclass(PaperStatus, Path(args.paper_status_json), loader=_load_json)
    readiness = evaluate_small_live_readiness(config, paper_status, env=runtime_env)
    intent = _intent_from_args(args)

    blockers = list(readiness.blockers)
    if not args.dry_run and args.confirm_live_order != LIVE_ORDER_CONFIRM_VALUE:
        blockers.append("missing_live_order_confirmation")

    if blockers or args.dry_run:
        print(
            json.dumps(
                {
                    "ready": readiness.ready,
                    "dry_run": bool(args.dry_run),
                    "would_submit": bool(args.dry_run and not blockers),
                    "blockers": blockers,
                    "warnings": readiness.warnings,
                    "order": _redacted_order_payload(intent),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if args.dry_run and not blockers else 2

    adapter = (
        adapter_factory(runtime_env)
        if adapter_factory is not None
        else BinanceSpotTradingAdapter(credentials=BinanceSpotCredentials.from_env(runtime_env))
    )
    executor = SmallLiveExecutor(adapter=adapter, config=config, readiness=readiness)
    result = asyncio.run(executor.submit_order(intent, now_ms=_now_ms()))
    print(
        json.dumps(
            {
                "ready": True,
                "dry_run": False,
                "submitted": True,
                "entry": {
                    "client_order_id": result.entry.client_order_id,
                    "exchange_order_id": result.entry.exchange_order_id,
                    "status": result.entry.status,
                },
                "stop_loss": (
                    {
                        "client_order_id": result.stop_loss.client_order_id,
                        "exchange_order_id": result.stop_loss.exchange_order_id,
                        "status": result.stop_loss.status,
                    }
                    if result.stop_loss is not None
                    else None
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--paper-status-json", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", choices=("buy", "sell"), required=True)
    parser.add_argument("--order-type", choices=("market", "limit"), default="market")
    parser.add_argument("--quantity", type=float, required=True)
    parser.add_argument("--price", type=float)
    parser.add_argument("--stop-loss-price", type=float)
    parser.add_argument("--client-order-id", required=True)
    parser.add_argument("--purpose", choices=("entry", "exit"), default="entry")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-live-order", default="")
    return parser


def _intent_from_args(args: argparse.Namespace) -> OrderIntent:
    return OrderIntent(
        signal_id=0,
        strategy="manual_small_live",
        strategy_version="manual",
        symbol=str(args.symbol),
        side=args.side,
        order_type=args.order_type,
        quantity=float(args.quantity),
        price=args.price,
        stop_loss_price=args.stop_loss_price,
        purpose=args.purpose,
        reduce_only=args.purpose == "exit",
        client_order_id=str(args.client_order_id),
    )


def _redacted_order_payload(intent: OrderIntent) -> dict[str, object]:
    return {
        "symbol": intent.symbol,
        "side": intent.side,
        "order_type": intent.order_type,
        "purpose": intent.purpose,
        "client_order_id": intent.client_order_id,
        "has_stop_loss": intent.stop_loss_price is not None,
    }


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise SystemExit(f"config must be a mapping: {path}")
    return data


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise SystemExit(f"paper status must be an object: {path}")
    return data


def _load_dataclass(cls: type, path: Path, *, loader: Callable[[Path], dict[str, Any]]) -> Any:
    data = loader(path)
    allowed = {field.name for field in fields(cls)}
    kwargs = {key: value for key, value in data.items() if key in allowed}
    if "allowed_symbols" in kwargs and isinstance(kwargs["allowed_symbols"], list):
        kwargs["allowed_symbols"] = tuple(str(item) for item in kwargs["allowed_symbols"])
    return cls(**kwargs)


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


__all__ = ["LIVE_ORDER_CONFIRM_VALUE", "main"]
