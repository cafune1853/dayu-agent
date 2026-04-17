"""市场解析器定义。"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal


_HK_SUFFIX_TICKER_PATTERN = re.compile(r"^\d{3,5}(?:\.HK|HK)$")


@dataclass(frozen=True)
class MarketProfile:
    """市场画像。"""

    ticker: str
    market: Literal["US", "HK", "CN"]


class MarketResolver:
    """市场解析器占位实现。"""

    @classmethod
    def resolve(cls, ticker: str) -> MarketProfile:
        """解析 ticker 的市场属性。

        Args:
            ticker: 股票代码。

        Returns:
            市场画像。

        Raises:
            ValueError: ticker 为空时抛出。
        """

        code = ticker.strip().upper()
        if not code:
            raise ValueError("ticker 不能为空")
        # 仅将“数字代码 + HK 后缀”识别为港股，避免将 BHK 等美股误判为 HK。
        if _HK_SUFFIX_TICKER_PATTERN.fullmatch(code):
            return MarketProfile(ticker=code, market="HK")
        # 港股常见裸码为 4 位数字（如 0300）。
        if code.isdigit() and len(code) == 4:
            return MarketProfile(ticker=code, market="HK")
        # A 股代码固定 6 位，且常见前缀为 0/3/6。
        if code.isdigit() and len(code) == 6 and code.startswith(("0", "3", "6")):
            return MarketProfile(ticker=code, market="CN")
        return MarketProfile(ticker=code, market="US")
