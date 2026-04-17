"""Host 内部共享的轻量规范化工具。"""

from __future__ import annotations


def _coerce_string_tuple(values: object) -> tuple[str, ...]:
    """将任意列表值规范化为字符串元组。

    Args:
        values: 原始值。

    Returns:
        过滤空白后的字符串元组；非列表输入返回空元组。

    Raises:
        无。
    """

    if not isinstance(values, list):
        return ()
    normalized: list[str] = []
    for item in values:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return tuple(normalized)


__all__ = ["_coerce_string_tuple"]