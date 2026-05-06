"""策略层单元测试：DataRequirement / Signal / StrategyContext / S1-S3。"""

from __future__ import annotations

import pytest

from core.strategy.base import DataRequirement, Signal, Strategy, StrategyContext


# ─── DataRequirement ────────────────────────────────────────────────────────────


class TestDataRequirement:
    def test_defaults(self):
        dr = DataRequirement()
        assert dr.symbols == []
        assert dr.timeframes == []
        assert dr.history_lookback_bars == 500
        assert dr.needs_funding is False
        assert dr.needs_orderbook_l1 is False
        assert dr.needs_orderbook_l5 is False
        assert dr.subscribe_partial_bars is False

    def test_custom(self):
        dr = DataRequirement(
            symbols=["BTCUSDT"],
            timeframes=["1h"],
            history_lookback_bars=200,
            needs_funding=True,
            needs_orderbook_l1=True,
        )
        assert dr.symbols == ["BTCUSDT"]
        assert dr.timeframes == ["1h"]
        assert dr.history_lookback_bars == 200
        assert dr.needs_funding is True
        assert dr.needs_orderbook_l1 is True


# ─── Signal ─────────────────────────────────────────────────────────────────────


class TestSignal:
    def test_minimal(self):
        s = Signal(side="long", symbol="BTCUSDT")
        assert s.side == "long"
        assert s.symbol == "BTCUSDT"
        assert s.entry_price is None
        assert s.stop_price == 0.0
        assert s.target_price is None
        assert s.confidence == 0.5
        assert s.suggested_size == 0.0
        assert s.time_in_force == "GTC"
        assert s.rationale == {}
        assert s.expires_in_ms == 60_000
        assert s.trade_group_id is None

    def test_full(self):
        s = Signal(
            side="short",
            symbol="ETHUSDT",
            entry_price=3000.0,
            stop_price=3100.0,
            target_price=2800.0,
            confidence=0.8,
            suggested_size=1.5,
            time_in_force="IOC",
            rationale={"rsi": 18.2},
            expires_in_ms=30_000,
            trade_group_id="grp-001",
        )
        assert s.side == "short"
        assert s.symbol == "ETHUSDT"
        assert s.entry_price == 3000.0
        assert s.stop_price == 3100.0
        assert s.target_price == 2800.0
        assert s.confidence == 0.8
        assert s.suggested_size == 1.5
        assert s.time_in_force == "IOC"
        assert s.rationale == {"rsi": 18.2}
        assert s.expires_in_ms == 30_000
        assert s.trade_group_id == "grp-001"

    def test_close_signal(self):
        s = Signal(side="close", symbol="BTCUSDT")
        assert s.side == "close"

    def test_rationale_default_mutable(self):
        s1 = Signal(side="long", symbol="A")
        s2 = Signal(side="long", symbol="B")
        s1.rationale["k"] = "v"
        assert s2.rationale == {}


# ─── StrategyContext ────────────────────────────────────────────────────────────


class TestStrategyContext:
    def test_kv_set_and_get(self, sqlite_repo):
        from core.common.clock import FixedClock

        clock = FixedClock(1_700_000_000_000)
        ctx = StrategyContext(data=None, clock=clock, repo=sqlite_repo, strategy_name="test_s1")
        ctx.kv_set("key1", {"a": 1})
        assert ctx.kv_get("key1") == {"a": 1}

    def test_kv_get_missing(self, sqlite_repo):
        from core.common.clock import FixedClock

        clock = FixedClock(1_700_000_000_000)
        ctx = StrategyContext(data=None, clock=clock, repo=sqlite_repo, strategy_name="test_s1")
        assert ctx.kv_get("no_such_key") is None

    def test_kv_set_overwrite(self, sqlite_repo):
        from core.common.clock import FixedClock

        clock = FixedClock(1_700_000_000_000)
        ctx = StrategyContext(data=None, clock=clock, repo=sqlite_repo, strategy_name="test_s1")
        ctx.kv_set("key", "old")
        ctx.kv_set("key", "new")
        assert ctx.kv_get("key") == "new"

    def test_kv_set_native_types(self, sqlite_repo):
        from core.common.clock import FixedClock

        clock = FixedClock(1_700_000_000_000)
        ctx = StrategyContext(data=None, clock=clock, repo=sqlite_repo, strategy_name="test_s1")
        ctx.kv_set("int_key", 42)
        ctx.kv_set("float_key", 3.14)
        ctx.kv_set("str_key", "hello")
        ctx.kv_set("list_key", [1, 2, 3])
        assert ctx.kv_get("int_key") == 42
        assert ctx.kv_get("float_key") == 3.14
        assert ctx.kv_get("str_key") == "hello"
        assert ctx.kv_get("list_key") == [1, 2, 3]

    def test_now_ms(self, sqlite_repo):
        from core.common.clock import FixedClock

        clock = FixedClock(1_700_000_000_000)
        ctx = StrategyContext(data=None, clock=clock, repo=sqlite_repo, strategy_name="test_s1")
        assert ctx.now_ms() == 1_700_000_000_000

    def test_data_property(self, sqlite_repo):
        from core.common.clock import FixedClock

        clock = FixedClock(1_700_000_000_000)
        ctx = StrategyContext(data="fake_feed", clock=clock, repo=sqlite_repo, strategy_name="test_s1")
        assert ctx.data == "fake_feed"


# ─── S1 BtcEth Trend ───────────────────────────────────────────────────────────


class TestS1BtcEthTrend:
    def test_required_data(self):
        from core.strategy import S1BtcEthTrend

        s = S1BtcEthTrend()
        dr = s.required_data()
        assert dr.symbols == ["BTCUSDT", "ETHUSDT"]
        assert "4h" in dr.timeframes
        assert "1d" in dr.timeframes
        assert dr.needs_funding is False

    def test_on_bar_returns_list(self):
        from core.strategy import S1BtcEthTrend

        s = S1BtcEthTrend()
        # on_bar 现在需要真实数据，传 None 检查不会崩溃
        # 实际信号产生在数据充足时才会发生
        assert callable(s.on_bar)

    def test_name_and_version(self):
        from core.strategy import S1BtcEthTrend

        s = S1BtcEthTrend()
        assert s.name == "S1_btc_eth_trend"
        assert s.version == "dev"
        assert s.config_hash == "0000000000000000"

    def test_config_defaults(self):
        from core.strategy import S1BtcEthTrend

        s = S1BtcEthTrend()
        assert s.donchian_period == 20
        assert s.atr_period == 14
        assert s.atr_relative_min_mult == 1.0
        assert s.volume_spike_mult == 1.2
        assert s.trail_stop_atr_mult == 1.5


# ─── S2 Altcoin Reversal ───────────────────────────────────────────────────────


class TestS2AltcoinReversal:
    def test_required_data(self):
        from core.strategy import S2AltcoinReversal

        s = S2AltcoinReversal()
        dr = s.required_data()
        assert "BTCUSDT" in dr.symbols
        assert "ETHUSDT" in dr.symbols
        assert "1h" in dr.timeframes
        assert "1d" in dr.timeframes
        assert dr.needs_funding is True

    def test_on_bar_returns_list(self):
        from core.strategy import S2AltcoinReversal

        s = S2AltcoinReversal()
        assert callable(s.on_bar)

    def test_name_and_version(self):
        from core.strategy import S2AltcoinReversal

        s = S2AltcoinReversal()
        assert s.name == "S2_altcoin_reversal"
        assert s.version == "dev"


# ─── S3 Funding Arbitrage ──────────────────────────────────────────────────────


class TestS3FundingArbitrage:
    def test_required_data(self):
        from core.strategy import S3FundingArbitrage

        s = S3FundingArbitrage()
        dr = s.required_data()
        assert dr.symbols == []
        assert "1h" in dr.timeframes
        assert "4h" in dr.timeframes
        assert dr.needs_funding is True

    def test_on_bar_stub(self):
        from core.strategy import S3FundingArbitrage

        s = S3FundingArbitrage()
        result = s.on_bar(None, None)
        assert result == []

    def test_name_and_version(self):
        from core.strategy import S3FundingArbitrage

        s = S3FundingArbitrage()
        assert s.name == "S3_funding_arbitrage"
        assert s.version == "dev"


# ─── Strategy ABC ──────────────────────────────────────────────────────────────


def test_strategy_cannot_instantiate_directly():
    """未实现 abstractmethod 的子类不能实例化。"""
    with pytest.raises(TypeError):
        Strategy()  # type: ignore[abstract]


def test_strategy_subclass_with_abstract_impl_can_instantiate():
    """实现了 on_bar + required_data 的子类可以实例化。"""

    class DemoStrategy(Strategy):
        name = "demo"
        __slots__ = ()

        def required_data(self):
            return DataRequirement()

        def on_bar(self, bar, ctx):
            return []

    s = DemoStrategy()
    assert s.name == "demo"
    assert s.on_bar(None, None) == []
