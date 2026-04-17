"""Dayu 公共契约层。

该包导出跨层稳定契约，但不在包导入时急切加载所有子模块，
避免 `contracts <-> execution` 这类包级循环导入。
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dayu.contracts.cancellation import CancelledError, CancellationToken
    from dayu.contracts.agent_execution import (
        AcceptedExecutionSpec,
        AgentCreateArgs,
        AgentInput,
        ExecutionContract,
        ExecutionHostPolicy,
        ExecutionMessageInputs,
        ExecutionPermissions,
        ScenePreparationSpec,
    )
    from dayu.contracts.agent_types import AgentMessage, AgentRuntimeLimits, AgentTraceIdentity
    from dayu.contracts.events import AppEvent, AppEventType, AppResult
    from dayu.contracts.execution_metadata import ExecutionDeliveryContext
    from dayu.contracts.fins import FinsCommand, FinsEvent, FinsEventType, FinsResult
    from dayu.contracts.model_config import ModelConfig, RunnerParams
    from dayu.contracts.protocols import (
        DupCallSpecProtocol,
        ToolExecutor,
        ToolTraceRecorder,
        ToolTraceRecorderFactory,
    )
    from dayu.contracts.reply_outbox import ReplyOutboxRecord, ReplyOutboxState, ReplyOutboxSubmitRequest
    from dayu.contracts.run import RunRecord, RunState
    from dayu.contracts.session import SessionRecord, SessionSource, SessionState


_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "CancelledError": ("dayu.contracts.cancellation", "CancelledError"),
    "CancellationToken": ("dayu.contracts.cancellation", "CancellationToken"),
    "AcceptedExecutionSpec": ("dayu.contracts.agent_execution", "AcceptedExecutionSpec"),
    "AgentCreateArgs": ("dayu.contracts.agent_execution", "AgentCreateArgs"),
    "AgentInput": ("dayu.contracts.agent_execution", "AgentInput"),
    "ExecutionContract": ("dayu.contracts.agent_execution", "ExecutionContract"),
    "ExecutionHostPolicy": ("dayu.contracts.agent_execution", "ExecutionHostPolicy"),
    "ExecutionMessageInputs": ("dayu.contracts.agent_execution", "ExecutionMessageInputs"),
    "ExecutionPermissions": ("dayu.contracts.agent_execution", "ExecutionPermissions"),
    "ScenePreparationSpec": ("dayu.contracts.agent_execution", "ScenePreparationSpec"),
    "AgentMessage": ("dayu.contracts.agent_types", "AgentMessage"),
    "AgentRuntimeLimits": ("dayu.contracts.agent_types", "AgentRuntimeLimits"),
    "AgentTraceIdentity": ("dayu.contracts.agent_types", "AgentTraceIdentity"),
    "ExecutionDeliveryContext": ("dayu.contracts.execution_metadata", "ExecutionDeliveryContext"),
    "ModelConfig": ("dayu.contracts.model_config", "ModelConfig"),
    "RunnerParams": ("dayu.contracts.model_config", "RunnerParams"),
    "DupCallSpecProtocol": ("dayu.contracts.protocols", "DupCallSpecProtocol"),
    "ToolExecutor": ("dayu.contracts.protocols", "ToolExecutor"),
    "ToolTraceRecorder": ("dayu.contracts.protocols", "ToolTraceRecorder"),
    "ToolTraceRecorderFactory": ("dayu.contracts.protocols", "ToolTraceRecorderFactory"),
    "AppEvent": ("dayu.contracts.events", "AppEvent"),
    "AppEventType": ("dayu.contracts.events", "AppEventType"),
    "AppResult": ("dayu.contracts.events", "AppResult"),
    "FinsCommand": ("dayu.contracts.fins", "FinsCommand"),
    "FinsEvent": ("dayu.contracts.fins", "FinsEvent"),
    "FinsEventType": ("dayu.contracts.fins", "FinsEventType"),
    "FinsResult": ("dayu.contracts.fins", "FinsResult"),
    "ReplyOutboxRecord": ("dayu.contracts.reply_outbox", "ReplyOutboxRecord"),
    "ReplyOutboxState": ("dayu.contracts.reply_outbox", "ReplyOutboxState"),
    "ReplyOutboxSubmitRequest": ("dayu.contracts.reply_outbox", "ReplyOutboxSubmitRequest"),
    "RunRecord": ("dayu.contracts.run", "RunRecord"),
    "RunState": ("dayu.contracts.run", "RunState"),
    "SessionRecord": ("dayu.contracts.session", "SessionRecord"),
    "SessionSource": ("dayu.contracts.session", "SessionSource"),
    "SessionState": ("dayu.contracts.session", "SessionState"),
}


def __getattr__(name: str) -> object:
    """按需加载公共契约导出。

    Args:
        name: 导出名称。

    Returns:
        对应的导出对象。

    Raises:
        AttributeError: 名称不存在时抛出。
    """

    export = _EXPORT_MAP.get(name)
    if export is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = export
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """返回模块可见导出列表。

    Args:
        无。

    Returns:
        排序后的名称列表。

    Raises:
        无。
    """

    return sorted(list(globals().keys()) + list(__all__))

__all__ = [
    "CancelledError",
    "CancellationToken",
    "AgentCreateArgs",
    "AgentInput",
    "AgentMessage",
    "AgentRuntimeLimits",
    "AgentTraceIdentity",
    "AcceptedExecutionSpec",
    "ExecutionDeliveryContext",
    "ModelConfig",
    "RunnerParams",
    "DupCallSpecProtocol",
    "ToolExecutor",
    "ToolTraceRecorder",
    "ToolTraceRecorderFactory",
    "AppEvent",
    "AppEventType",
    "AppResult",
    "ExecutionContract",
    "ExecutionHostPolicy",
    "ExecutionMessageInputs",
    "ExecutionPermissions",
    "ScenePreparationSpec",
    "FinsCommand",
    "FinsEvent",
    "FinsEventType",
    "FinsResult",
    "ReplyOutboxRecord",
    "ReplyOutboxState",
    "ReplyOutboxSubmitRequest",
    # Run / Session 数据模型
    "RunRecord",
    "RunState",
    "SessionRecord",
    "SessionSource",
    "SessionState",
]
