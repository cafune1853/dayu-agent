"""MarketResolver 测试。"""

from __future__ import annotations

import pytest

from dayu.fins.resolver.market_resolver import MarketResolver


def test_market_resolver_resolves_cn_ticker() -> None:
    """验证 6 位 A 股代码归类为 CN。

    Args:
        无。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    profile = MarketResolver.resolve("000333")
    assert profile.ticker == "000333"
    assert profile.market == "CN"


def test_market_resolver_resolves_hk_ticker() -> None:
    """验证 4 位纯数字代码归类为 HK。

    Args:
        无。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    profile = MarketResolver.resolve("0300")
    assert profile.ticker == "0300"
    assert profile.market == "HK"


def test_market_resolver_resolves_hk_ticker_with_suffix() -> None:
    """验证 HK 后缀代码归类为 HK。

    Args:
        无。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    profile = MarketResolver.resolve("0300.hk")
    assert profile.ticker == "0300.HK"
    assert profile.market == "HK"


def test_market_resolver_resolves_hk_ticker_with_suffix_without_dot() -> None:
    """验证 HK 后缀代码（无点号）归类为 HK。

    Args:
        无。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    profile = MarketResolver.resolve("0300hk")
    assert profile.ticker == "0300HK"
    assert profile.market == "HK"


def test_market_resolver_defaults_to_us() -> None:
    """验证默认分支归类为 US domestic。

    Args:
        无。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    profile = MarketResolver.resolve("aapl")
    assert profile.ticker == "AAPL"
    assert profile.market == "US"


def test_market_resolver_does_not_misclassify_hk_suffix_like_us_ticker() -> None:
    """验证以 HK 结尾的字母 ticker 不会误判为港股。

    Args:
        无。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    profile = MarketResolver.resolve("BHK")
    assert profile.ticker == "BHK"
    assert profile.market == "US"


def test_market_resolver_rejects_empty_ticker() -> None:
    """验证空 ticker 抛错。

    Args:
        无。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    with pytest.raises(ValueError, match="ticker 不能为空"):
        MarketResolver.resolve("   ")
