"""Symbol 命名规范化工具。"""

from __future__ import annotations

_KNOWN_QUOTES = ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH", "BNB")
_STABLE_OR_PLATFORM_BASES = {"USDC", "USDT", "DAI", "BUSD", "FDUSD", "TUSD", "BNB", "OKB"}


class Symbol:
    """对外部交易所 symbol 做项目内规范化。"""

    @staticmethod
    def normalize(raw: str) -> str:
        """归一化为无分隔符大写格式，如 ``BTC/USDT`` -> ``BTCUSDT``。"""
        s = raw.strip().upper()
        s = s.replace("/", "").replace("_", "").replace(":", "")
        if s.endswith("-SWAP"):
            s = s[: -len("-SWAP")]
        s = s.replace("-", "")
        if s.endswith(".PERP"):
            s = s[: -len(".PERP")]
        return s

    @staticmethod
    def normalize_with_type(raw: str, *, with_type: bool = False) -> str | tuple[str, str]:
        """归一化 symbol，并可同时推断 spot/perp。"""
        upper = raw.strip().upper()
        stype = "perp" if upper.endswith("-SWAP") or upper.endswith(".PERP") else "spot"
        symbol = Symbol.normalize(upper)
        if with_type:
            return symbol, stype
        return symbol

    @staticmethod
    def display(symbol: str) -> str:
        """把内部格式展示为 ``BASE/QUOTE``。"""
        s = Symbol.normalize(symbol)
        for quote in _KNOWN_QUOTES:
            if s.endswith(quote) and len(s) > len(quote):
                return f"{s[:-len(quote)]}/{quote}"
        return s

    @staticmethod
    def base_asset(symbol: str) -> str:
        """提取 base asset。"""
        s = Symbol.normalize(symbol)
        for quote in _KNOWN_QUOTES:
            if s.endswith(quote) and len(s) > len(quote):
                return s[:-len(quote)]
        return s

    @staticmethod
    def quote_asset(symbol: str) -> str:
        """提取 quote asset；未知时按 USDT 兜底。"""
        s = Symbol.normalize(symbol)
        for quote in _KNOWN_QUOTES:
            if s.endswith(quote) and len(s) > len(quote):
                return quote
        return "USDT"

    @staticmethod
    def is_stablecoin(symbol: str) -> bool:
        """项目币池过滤用：稳定币与平台币视为不可交易 base。"""
        return Symbol.base_asset(symbol) in _STABLE_OR_PLATFORM_BASES


def normalize_symbol(raw: str) -> str:
    """模块级快捷函数。"""
    return Symbol.normalize(raw)


def display_symbol(symbol: str) -> str:
    """模块级快捷函数。"""
    return Symbol.display(symbol)


__all__ = ["Symbol", "display_symbol", "normalize_symbol"]
