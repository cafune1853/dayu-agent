"""Service → Host 宿主执行边界数据类型。

定义 Service 层提交宿主执行时使用的稳定数据契约：
- ``HostedRunSpec``：描述一次宿主执行的 run 规格。
- ``HostedRunContext``：宿主执行传递给业务 handler 的上下文。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dayu.contracts.cancellation import CancellationToken


@dataclass(frozen=True)
class HostedRunSpec:
    """宿主执行所需的 run 描述。

    Attributes:
        operation_name: 操作名称，用于 run registry 标识。
        session_id: 关联的 Host session ID。
        scene_name: 关联的 scene 名称。
        metadata: 非结构化元数据。
        concurrency_lane: 并发 lane 名称。
        timeout_ms: 超时毫秒数。
        publish_events: 是否发布事件到 event bus。
        error_summary_limit: 错误摘要字符上限。
    """

    operation_name: str
    session_id: str | None = None
    scene_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    concurrency_lane: str | None = None
    timeout_ms: int | None = None
    publish_events: bool = True
    error_summary_limit: int = 500


@dataclass(frozen=True)
class HostedRunContext:
    """宿主执行传递给业务 handler 的上下文。

    Attributes:
        run_id: 当前 Host run ID。
        cancellation_token: 取消令牌。
    """

    run_id: str
    cancellation_token: CancellationToken


__all__ = [
    "HostedRunContext",
    "HostedRunSpec",
]
