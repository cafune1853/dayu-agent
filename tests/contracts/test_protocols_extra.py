"""`contracts.protocols` 额外覆盖测试。"""

from __future__ import annotations

import pytest

from dayu.contracts.cancellation import CancellationToken
from dayu.contracts.protocols import ToolExecutionContext


@pytest.mark.unit
def test_tool_execution_context_from_value_normalizes_mapping_inputs() -> None:
    """上下文收敛应支持映射输入、字符串数值和取消令牌。"""

    token = CancellationToken()
    context = ToolExecutionContext.from_value(
        {
            "run_id": " run_1 ",
            "iteration_id": " 2 ",
            "tool_call_id": " tool_1 ",
            "index_in_iteration": "3",
            "timeout": "4.5",
            "cancellation_token": token,
        }
    )

    assert context is not None
    assert context.run_id == "run_1"
    assert context.iteration_id == "2"
    assert context.tool_call_id == "tool_1"
    assert context.index_in_iteration == 3
    assert context.timeout_seconds == 4.5
    assert context.cancellation_token is token


@pytest.mark.unit
def test_tool_execution_context_handles_passthrough_defaults_and_lookup_errors() -> None:
    """上下文对象应支持透传、默认值和键访问错误分支。"""

    original = ToolExecutionContext(run_id="run_1", index_in_iteration=1)

    assert ToolExecutionContext.from_value(None) is None
    assert ToolExecutionContext.from_value(original) is original
    assert original.get("run_id") == "run_1"
    assert original.get("missing", "fallback") == "fallback"
    assert original["index_in_iteration"] == 1
    assert original["timeout"] is None

    with pytest.raises(KeyError):
        _ = original["missing"]


@pytest.mark.unit
def test_tool_execution_context_normalizes_invalid_numeric_inputs_to_safe_defaults() -> None:
    """非法数值输入应回退到安全默认值。"""

    context = ToolExecutionContext.from_value(
        {
            "index_in_iteration": "bad-index",
            "timeout_seconds": "bad-timeout",
            "run_id": 123,
            "iteration_id": "   ",
        }
    )

    assert context is not None
    assert context.index_in_iteration == 0
    assert context.timeout_seconds is None
    assert context.run_id == "123"
    assert context.iteration_id is None