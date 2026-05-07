"""Paper 撮合引擎测试——订单验证 / 市价/限价/止损撮合 / 费率+滑点 / 幂等。"""

from __future__ import annotations

import pytest

from core.common.exceptions import IdempotencyConflict, InvalidOrderIntent, InvalidStopLoss
from core.execution.order_types import CancelResult, OrderHandle, OrderIntent
from core.execution.paper_engine import PaperMatchingEngine


def _intent(**kw) -> OrderIntent:
    defaults = dict(
        signal_id=1,
        strategy="s1",
        strategy_version="dev",
        symbol="BTCUSDT",
        side="buy",
        order_type="market",
        quantity=0.1,
        client_order_id="test-cid-001",
    )
    defaults.update(kw)
    return OrderIntent(**defaults)


def _make_engine(sqlite_repo, price=50000.0):
    return PaperMatchingEngine(sqlite_repo, get_price=lambda s: price)


@pytest.fixture(autouse=True)
def _stub_symbol_and_signal(sqlite_repo):
    """每个测试前插入桩 symbol 和 signal，满足 FK 约束。"""
    import time

    now = int(time.time() * 1000)
    # 插入桩 symbol（幂等：先查后插）
    existing = sqlite_repo.get_symbol("BTCUSDT")
    if existing is None:
        sqlite_repo._conn.execute(
            "INSERT INTO symbols (exchange, symbol, type, base, quote, tick_size, lot_size, min_notional, listed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("binance", "BTCUSDT", "perp", "BTC", "USDT", 0.01, 0.001, 10.0, 1500000000000),
        )
        sqlite_repo._conn.commit()
    # 插入桩 signal
    sqlite_repo._conn.execute(
        "INSERT OR IGNORE INTO signals (id, strategy, strategy_version, config_hash, universe_version, "
        "run_id, symbol_id, side, stop_price, confidence, suggested_size, rationale, captured_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, "s1", "dev", "hash", "v1", "run-1", 1, "long", 49000.0, 0.8, 0.1, "{}", now, now + 60000),
    )
    sqlite_repo._conn.commit()


# ─── OrderIntent 验证 ──────────────────────────────────────────────────────────


class TestOrderIntentValidation:
    def test_client_order_id_required(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(client_order_id="", symbol="")
        with pytest.raises(InvalidOrderIntent, match="client_order_id"):
            engine.place_order(intent, 1_700_000_000_000)

    def test_symbol_required(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(symbol="")
        with pytest.raises(InvalidOrderIntent, match="symbol"):
            engine.place_order(intent, 1_700_000_000_000)

    def test_quantity_must_be_positive(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(quantity=0)
        with pytest.raises(InvalidOrderIntent, match="quantity"):
            engine.place_order(intent, 1_700_000_000_000)

    def test_limit_order_requires_price(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(order_type="limit", price=None)
        with pytest.raises(InvalidOrderIntent, match="price"):
            engine.place_order(intent, 1_700_000_000_000)

    def test_market_order_rejects_price(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(order_type="market", price=50000.0)
        with pytest.raises(InvalidOrderIntent, match="market"):
            engine.place_order(intent, 1_700_000_000_000)

    def test_stop_order_requires_stop_price(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(order_type="stop", stop_price=None)
        with pytest.raises(InvalidOrderIntent, match="stop_price"):
            engine.place_order(intent, 1_700_000_000_000)

    def test_invalid_stop_loss_direction_long(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(order_type="limit", price=50000.0, side="buy", stop_loss_price=51000.0)
        with pytest.raises(InvalidStopLoss, match="多头"):
            engine.place_order(intent, 1_700_000_000_000)

    def test_invalid_stop_loss_direction_short(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        intent = _intent(order_type="limit", price=50000.0, side="sell", stop_loss_price=49000.0)
        with pytest.raises(InvalidStopLoss, match="空头"):
            engine.place_order(intent, 1_700_000_000_000)


# ─── 市价单撮合 ────────────────────────────────────────────────────────────────


class TestMarketOrder:
    def test_immediate_fill(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        handle = engine.place_order(_intent(), now)
        assert handle.status == "filled"
        assert handle.client_order_id == "test-cid-001"

    def test_fill_persisted_to_db(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="db-test"), now)
        row = sqlite_repo.get_order("db-test")
        assert row is not None
        assert row["status"] == "filled"
        assert row["filled_qty"] == 0.1

    def test_fill_record_created(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="fill-test"), now)
        order_row = sqlite_repo.get_order("fill-test")
        fills = sqlite_repo.get_fills(order_row["id"])
        assert len(fills) == 1
        assert fills[0]["quantity"] == 0.1

    def test_exchange_order_id_matches_persisted_order(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        handle = engine.place_order(_intent(client_order_id="eid-test"), now)
        order_row = sqlite_repo.get_order("eid-test")
        assert order_row["exchange_order_id"] == handle.exchange_order_id

    def test_buy_slippage_applied(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="slip-buy", side="buy"), now)
        order_row = sqlite_repo.get_order("slip-buy")
        # buy slippage: price * (1 + 0.0001) = 50005
        assert order_row["avg_fill_price"] == pytest.approx(50005.0, rel=1e-6)

    def test_sell_slippage_applied(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="slip-sell", side="sell"), now)
        order_row = sqlite_repo.get_order("slip-sell")
        # sell slippage: price * (1 - 0.0001) = 49995
        assert order_row["avg_fill_price"] == pytest.approx(49995.0, rel=1e-6)

    def test_taker_fee_recorded(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="fee-test", quantity=1.0), now)
        order_row = sqlite_repo.get_order("fee-test")
        fills = sqlite_repo.get_fills(order_row["id"])
        # taker fee: 1.0 * 50005 * 0.0004 = 20.002
        assert fills[0]["fee"] == pytest.approx(20.002, rel=1e-5)
        assert fills[0]["is_maker"] == 0

    def test_no_price_stays_accepted(self, sqlite_repo):
        engine = PaperMatchingEngine(sqlite_repo, get_price=lambda s: None)
        now = 1_700_000_000_000
        handle = engine.place_order(_intent(client_order_id="no-price"), now)
        assert handle.status == "accepted"


# ─── 限价单撮合 ────────────────────────────────────────────────────────────────


class TestLimitOrder:
    def test_buy_limit_fills_when_price_below(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=49900.0)
        now = 1_700_000_000_000
        handle = engine.place_order(
            _intent(client_order_id="lim-buy", order_type="limit", price=50000.0, side="buy"),
            now,
        )
        assert handle.status == "filled"

    def test_buy_limit_not_filled_when_price_above(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50100.0)
        now = 1_700_000_000_000
        handle = engine.place_order(
            _intent(client_order_id="lim-buy2", order_type="limit", price=50000.0, side="buy"),
            now,
        )
        assert handle.status == "accepted"

    def test_sell_limit_fills_when_price_above(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50100.0)
        now = 1_700_000_000_000
        handle = engine.place_order(
            _intent(client_order_id="lim-sell", order_type="limit", price=50000.0, side="sell"),
            now,
        )
        assert handle.status == "filled"

    def test_limit_order_maker_fee(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=49900.0)
        now = 1_700_000_000_000
        engine.place_order(
            _intent(client_order_id="maker-fee", order_type="limit", price=50000.0, quantity=1.0),
            now,
        )
        order_row = sqlite_repo.get_order("maker-fee")
        fills = sqlite_repo.get_fills(order_row["id"])
        assert fills[0]["is_maker"] == 1
        # maker fee: 1.0 * 50000 * 0.0002 = 10.0
        assert fills[0]["fee"] == pytest.approx(10.0, rel=1e-5)


# ─── 止损单 ────────────────────────────────────────────────────────────────────


class TestStopOrder:
    def test_stop_order_parked(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        handle = engine.place_order(
            _intent(client_order_id="stop-1", order_type="stop", stop_price=51000.0, side="buy"),
            now,
        )
        assert handle.status == "accepted"

    def test_stop_order_triggers_on_check(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(
            _intent(client_order_id="stop-trig", order_type="stop", stop_price=51000.0, side="buy"),
            now,
        )
        # 更新价格触及止损线
        engine2 = _make_engine(sqlite_repo, price=51200.0)
        fills = engine2.check_pending_orders(now + 3600000)
        assert len(fills) == 1


# ─── 撤单 ──────────────────────────────────────────────────────────────────────


class TestCancelOrder:
    def test_cancel_pending_order(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=60000.0)
        now = 1_700_000_000_000
        engine.place_order(
            _intent(client_order_id="cancel-me", order_type="limit", price=50000.0),
            now,
        )
        result = engine.cancel_order("cancel-me", now)
        assert result.success is True

    def test_cancel_filled_order_fails(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="filled-1"), now)
        result = engine.cancel_order("filled-1", now)
        assert result.success is False

    def test_cancel_unknown_order(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        result = engine.cancel_order("no-such-id", 1_700_000_000_000)
        assert result.success is False


# ─── 幂等性 ────────────────────────────────────────────────────────────────────


class TestIdempotency:
    def test_duplicate_cid_raises(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="dup-id"), now)
        with pytest.raises(IdempotencyConflict):
            engine.place_order(_intent(client_order_id="dup-id"), now)


# ─── 查询 ──────────────────────────────────────────────────────────────────────


class TestGetOrder:
    def test_get_existing_order(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="query-me"), now)
        row = engine.get_order("query-me")
        assert row is not None
        assert row["client_order_id"] == "query-me"

    def test_get_unknown_order(self, sqlite_repo):
        engine = _make_engine(sqlite_repo)
        assert engine.get_order("no-such") is None


# ─── 待处理订单批量检查 ────────────────────────────────────────────────────────


class TestCheckPending:
    def test_limit_triggered_on_price_move(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=51000.0)
        now = 1_700_000_000_000
        engine.place_order(
            _intent(client_order_id="lim-wait", order_type="limit", price=50000.0, side="buy"),
            now,
        )
        # 价格下移至限价以下
        engine2 = _make_engine(sqlite_repo, price=49900.0)
        fills = engine2.check_pending_orders(now + 3600000)
        assert len(fills) == 1

    def test_multiple_pending_orders(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=51000.0)
        now = 1_700_000_000_000
        engine.place_order(
            _intent(client_order_id="l1", order_type="limit", price=50000.0, side="buy"), now
        )
        engine.place_order(
            _intent(client_order_id="l2", order_type="limit", price=50000.0, side="buy"), now
        )
        engine2 = _make_engine(sqlite_repo, price=49900.0)
        fills = engine2.check_pending_orders(now + 3600000)
        assert len(fills) == 2

    def test_empty_when_no_open_orders(self, sqlite_repo):
        engine = _make_engine(sqlite_repo, price=50000.0)
        fills = engine.check_pending_orders(1_700_000_000_000)
        assert fills == []

    def test_pending_order_uses_symbol_id_to_fetch_price(self, sqlite_repo):
        sqlite_repo.upsert_symbols([
            {
                "exchange": "binance",
                "symbol": "ETHUSDT",
                "type": "perp",
                "base": "ETH",
                "quote": "USDT",
                "tick_size": 0.01,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1_500_000_000_000,
            }
        ])
        now = 1_700_000_000_000
        engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: 3100.0)
        engine.place_order(
            _intent(
                client_order_id="eth-limit",
                symbol="ETHUSDT",
                order_type="limit",
                price=3000.0,
                side="buy",
            ),
            now,
        )

        seen = []

        def get_price(symbol):
            seen.append(symbol)
            return 2900.0 if symbol == "ETHUSDT" else None

        fills = PaperMatchingEngine(sqlite_repo, get_price=get_price).check_pending_orders(
            now + 3600000
        )

        assert seen == ["ETHUSDT"]
        assert len(fills) == 1


# ─── OrderHandle / CancelResult 数据类 ──────────────────────────────────────────


class TestOrderHandle:
    def test_to_order_snapshot(self):
        handle = OrderHandle(
            client_order_id="cid",
            exchange_order_id="eid",
            status="filled",
            submitted_at=1_700_000_000_000,
        )
        order = handle.to_order()
        assert order.client_order_id == "cid"
        assert order.status == "filled"


class TestCancelResult:
    def test_success_result(self):
        r = CancelResult("cid", True)
        assert r.success is True
        assert r.reason is None

    def test_failure_result(self):
        r = CancelResult("cid", False, "already filled")
        assert r.success is False
        assert r.reason == "already filled"


# ─── 自定义费率/滑点 ──────────────────────────────────────────────────────────


class TestCustomFeeModel:
    def test_custom_taker_fee(self, sqlite_repo):
        engine = PaperMatchingEngine(sqlite_repo, get_price=lambda s: 50000.0, taker_fee=0.001)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="custom-fee", quantity=1.0), now)
        order_row = sqlite_repo.get_order("custom-fee")
        fills = sqlite_repo.get_fills(order_row["id"])
        # 1.0 * 50005 * 0.001 = 50.005
        assert fills[0]["fee"] == pytest.approx(50.005, rel=1e-5)

    def test_custom_slippage(self, sqlite_repo):
        engine = PaperMatchingEngine(sqlite_repo, get_price=lambda s: 50000.0, slippage=0.002)
        now = 1_700_000_000_000
        engine.place_order(_intent(client_order_id="slip-custom", side="buy"), now)
        order_row = sqlite_repo.get_order("slip-custom")
        # buy with 0.2% slippage: 50000 * 1.002 = 50100
        assert order_row["avg_fill_price"] == pytest.approx(50100.0, rel=1e-6)
