"""测试 Symbol 命名规范化：normalize / normalize_with_type / display / 过滤 / 提取。

覆盖 ``core/data/symbol.py`` 中所有公开 API。
"""

from __future__ import annotations

from core.data.symbol import Symbol, display_symbol, normalize_symbol

# ─── normalize ────────────────────────────────────────────────────────────


def test_normalize_slash_format() -> None:
    """BTC/USDT → BTCUSDT。"""
    assert Symbol.normalize("BTC/USDT") == "BTCUSDT"


def test_normalize_lowercase() -> None:
    """btcusdt → BTCUSDT（大小写不敏感）。"""
    assert Symbol.normalize("btcusdt") == "BTCUSDT"


def test_normalize_okx_swap_style() -> None:
    """BTC-USDT-SWAP → BTCUSDT（OKX 永续风格，丢掉后缀）。"""
    assert Symbol.normalize("BTC-USDT-SWAP") == "BTCUSDT"


def test_normalize_perp_suffix() -> None:
    """BTCUSDT.PERP → BTCUSDT（部分平台后缀）。"""
    assert Symbol.normalize("BTCUSDT.PERP") == "BTCUSDT"


def test_normalize_eth_usdt() -> None:
    """ETH/USDT → ETHUSDT。"""
    assert Symbol.normalize("ETH/USDT") == "ETHUSDT"


def test_normalize_with_underscore_delimiter() -> None:
    """BTC_USDT → BTCUSDT（下划线分隔符）。"""
    assert Symbol.normalize("BTC_USDT") == "BTCUSDT"


def test_normalize_with_colon_delimiter() -> None:
    """BTC:USDT → BTCUSDT（冒号分隔符）。"""
    assert Symbol.normalize("BTC:USDT") == "BTCUSDT"


def test_normalize_with_extra_whitespace() -> None:
    """带前后空格也能正确归一化。"""
    assert Symbol.normalize("  BTC/USDT  ") == "BTCUSDT"


def test_normalize_already_standard() -> None:
    """已经是标准格式的 symbol 不变。"""
    assert Symbol.normalize("BTCUSDT") == "BTCUSDT"


# ─── normalize_with_type ─────────────────────────────────────────────────


def test_normalize_with_type_okx_swap() -> None:
    """BTC-USDT-SWAP with_type=True → ("BTCUSDT", "perp")。"""
    assert Symbol.normalize_with_type("BTC-USDT-SWAP", with_type=True) == ("BTCUSDT", "perp")


def test_normalize_with_type_perp_suffix() -> None:
    """BTCUSDT.PERP with_type=True → ("BTCUSDT", "perp")。"""
    assert Symbol.normalize_with_type("BTCUSDT.PERP", with_type=True) == ("BTCUSDT", "perp")


def test_normalize_with_type_spot_default() -> None:
    """BTCUSDT with_type=True → ("BTCUSDT", "spot")（无明确 type 后缀时默认 spot）。"""
    assert Symbol.normalize_with_type("BTCUSDT", with_type=True) == ("BTCUSDT", "spot")


def test_normalize_with_type_slash_spot() -> None:
    """BTC/USDT with_type=True → ("BTCUSDT", "spot")。"""
    assert Symbol.normalize_with_type("BTC/USDT", with_type=True) == ("BTCUSDT", "spot")


def test_normalize_with_type_false_returns_str() -> None:
    """with_type=False 时只返回字符串（默认行为）。"""
    result = Symbol.normalize_with_type("BTC-USDT-SWAP", with_type=False)
    assert result == "BTCUSDT"
    assert isinstance(result, str)


def test_normalize_with_type_no_keyword_returns_str() -> None:
    """不传 with_type 时只返回字符串。"""
    result = Symbol.normalize_with_type("BTC-USDT-SWAP")
    assert result == "BTCUSDT"
    assert isinstance(result, str)


def test_normalize_with_type_spot_explicit_suffix() -> None:
    """BTC-USDT-SPOT with_type=True → type 推断为 spot，但 -SPOT 后缀留在 symbol 中（normalize 不剥 -SPOT）。"""
    assert Symbol.normalize_with_type("BTC-USDT-SPOT", with_type=True) == ("BTCUSDTSPOT", "spot")


# ─── display ──────────────────────────────────────────────────────────────


def test_display_btc_usdt() -> None:
    """BTCUSDT → BTC/USDT。"""
    assert Symbol.display("BTCUSDT") == "BTC/USDT"


def test_display_eth_usdt() -> None:
    """ETHUSDT → ETH/USDT。"""
    assert Symbol.display("ETHUSDT") == "ETH/USDT"


def test_display_usdc_quote() -> None:
    """BTCUSDC → BTC/USDC。"""
    assert Symbol.display("BTCUSDC") == "BTC/USDC"


def test_display_btc_quote() -> None:
    """ETHBTC → ETH/BTC（以 BTC 为计价单位）。"""
    assert Symbol.display("ETHBTC") == "ETH/BTC"


def test_display_eth_quote() -> None:
    """BTCETH → BTC/ETH（以 ETH 为计价单位）。"""
    # 注意：BTCETH 结尾是 ETH，不是 USDT 等更常见的 quote，但 ETH 在支持列表中
    # 由于 USDT 在前，BTCETH 不会匹配 USDT；匹配到 ETH
    assert Symbol.display("BTCETH") == "BTC/ETH"


def test_display_bnb_quote() -> None:
    """ADABNB → ADA/BNB。"""
    assert Symbol.display("ADABNB") == "ADA/BNB"


def test_display_fallback_no_known_quote() -> None:
    """找不到已知 quote 则返回原样。"""
    # 故意构造一个不以已知 quote 结尾的 symbol
    assert Symbol.display("XYZ") == "XYZ"


def test_display_already_slash_format() -> None:
    """已经是 BTC/USDT 格式也正确处理。"""
    assert Symbol.display("BTC/USDT") == "BTC/USDT"


# ─── is_stablecoin / is_platform_coin ─────────────────────────────────────


def test_is_stablecoin_usdc_usdt_true() -> None:
    """USDCUSDT → True（base 是 USDC 稳定币）。"""
    assert Symbol.is_stablecoin("USDCUSDT") is True


def test_is_stablecoin_dai_true() -> None:
    """DAIUSDT → True（base 是 DAI 稳定币）。"""
    assert Symbol.is_stablecoin("DAIUSDT") is True


def test_is_stablecoin_btc_false() -> None:
    """BTCUSDT → False（BTC 不是稳定币）。"""
    assert Symbol.is_stablecoin("BTCUSDT") is False


def test_is_stablecoin_eth_false() -> None:
    """ETHUSDT → False（ETH 不是稳定币）。"""
    assert Symbol.is_stablecoin("ETHUSDT") is False


def test_is_stablecoin_bnb_true() -> None:
    """BNBUSDT → True（BNB 是平台币，视为稳定币排除）。"""
    assert Symbol.is_stablecoin("BNBUSDT") is True


def test_is_stablecoin_okb_true() -> None:
    """OKBUSDT → True（OKB 是平台币）。"""
    assert Symbol.is_stablecoin("OKBUSDT") is True


# ─── base_asset / quote_asset ─────────────────────────────────────────────


def test_base_asset_btc_usdt() -> None:
    """base_asset("BTCUSDT") → "BTC"。"""
    assert Symbol.base_asset("BTCUSDT") == "BTC"


def test_base_asset_eth_usdt() -> None:
    """base_asset("ETHUSDT") → "ETH"。"""
    assert Symbol.base_asset("ETHUSDT") == "ETH"


def test_base_asset_sol_usdc() -> None:
    """base_asset("SOLUSDC") → "SOL"（quote 是 USDC）。"""
    assert Symbol.base_asset("SOLUSDC") == "SOL"


def test_base_asset_unknown_quote_returns_full() -> None:
    """base_asset("XYZ") 找不到 quote 返回全串。"""
    assert Symbol.base_asset("XYZ") == "XYZ"


def test_quote_asset_btc_usdt() -> None:
    """quote_asset("BTCUSDT") → "USDT"。"""
    assert Symbol.quote_asset("BTCUSDT") == "USDT"


def test_quote_asset_eth_btc() -> None:
    """quote_asset("ETHBTC") → "BTC"。"""
    assert Symbol.quote_asset("ETHBTC") == "BTC"


def test_quote_asset_sol_usdc() -> None:
    """quote_asset("SOLUSDC") → "USDC"。"""
    assert Symbol.quote_asset("SOLUSDC") == "USDC"


def test_quote_asset_fallback_usdt() -> None:
    """quote_asset 找不到已知 quote 默认返回 "USDT"。"""
    assert Symbol.quote_asset("XYZ123") == "USDT"


# ─── 模块级快捷函数 ──────────────────────────────────────────────────────


def test_normalize_symbol_module_level() -> None:
    """normalize_symbol 等价于 Symbol.normalize。"""
    assert normalize_symbol("BTC/USDT") == "BTCUSDT"
    assert normalize_symbol("eth-usdt-swap") == "ETHUSDT"


def test_display_symbol_module_level() -> None:
    """display_symbol 等价于 Symbol.display。"""
    assert display_symbol("BTCUSDT") == "BTC/USDT"
    assert display_symbol("ETHUSDT") == "ETH/USDT"
