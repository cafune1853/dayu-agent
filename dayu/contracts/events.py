"""应用层公共事件契约。

该模块只定义 UI / Service / Host 共享的数据模型，
不负责把 Engine 事件映射成应用层事件，避免 ``contracts -> engine`` 反向依赖。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Protocol, runtime_checkable


class AppEventType(Enum):
    """应用层事件类型。"""

    CONTENT_DELTA = "content_delta"
    REASONING_DELTA = "reasoning_delta"
    FINAL_ANSWER = "final_answer"
    CANCELLED = "cancelled"
    TOOL_EVENT = "tool_event"
    WARNING = "warning"
    ERROR = "error"
    METADATA = "metadata"
    DONE = "done"


@runtime_checkable
class PublishedRunEventProtocol(Protocol):
    """Host 事件总线可发布的稳定事件包络。"""

    @property
    def type(self) -> object:
        """返回事件类型对象。"""

        ...

    @property
    def payload(self) -> object:
        """返回事件负载对象。"""

        ...


@dataclass
class AppEvent:
    """应用层标准事件。

    Attributes:
        type: 事件类型。
        payload: 事件负载。
        meta: 额外元数据。
    """

    type: AppEventType
    payload: Any
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AppResult:
    """应用层一次执行结果。

    Attributes:
        content: 最终文本内容。
        errors: 错误列表。
        warnings: 告警列表。
        degraded: 是否降级。
        filtered: 是否为受过滤完成态。
    """

    content: str
    errors: list[str]
    warnings: list[str]
    degraded: bool = False
    filtered: bool = False
__all__ = [
    "AppEvent",
    "AppEventType",
    "AppResult",
    "PublishedRunEventProtocol",
]
