"""Engine 内置 toolset adapter。"""

from __future__ import annotations

from typing import cast

from dayu.contracts.toolset_config import (
    ToolsetConfigSnapshot,
    coerce_toolset_config_float,
    coerce_toolset_config_int,
)
from dayu.contracts.toolset_registrar import ToolsetRegistrationContext
from dayu.engine.tool_registry import ToolRegistry
from dayu.engine.tools.doc_tools import register_doc_tools
from dayu.engine.tools.utils_tools import register_utils_builtin_tools
from dayu.engine.tools.web_tools import register_web_tools
from dayu.execution.doc_access import build_effective_doc_allowed_paths
from dayu.execution.doc_limits import DocToolLimits
from dayu.execution.web_limits import WebToolsConfig


def _build_doc_tool_limits(snapshot: ToolsetConfigSnapshot | None) -> DocToolLimits:
    """从通用 toolset 快照恢复文档工具限制。"""

    payload = snapshot.payload if snapshot is not None else {}
    defaults = DocToolLimits()
    return DocToolLimits(
        list_files_max=coerce_toolset_config_int(
            payload.get("list_files_max"),
            field_name="doc.list_files_max",
            default=defaults.list_files_max,
        ),
        get_sections_max=coerce_toolset_config_int(
            payload.get("get_sections_max"),
            field_name="doc.get_sections_max",
            default=defaults.get_sections_max,
        ),
        search_files_max_results=coerce_toolset_config_int(
            payload.get("search_files_max_results"),
            field_name="doc.search_files_max_results",
            default=defaults.search_files_max_results,
        ),
        read_file_max_chars=coerce_toolset_config_int(
            payload.get("read_file_max_chars"),
            field_name="doc.read_file_max_chars",
            default=defaults.read_file_max_chars,
        ),
        read_file_section_max_chars=coerce_toolset_config_int(
            payload.get("read_file_section_max_chars"),
            field_name="doc.read_file_section_max_chars",
            default=defaults.read_file_section_max_chars,
        ),
    )


def _build_web_tools_config(snapshot: ToolsetConfigSnapshot | None) -> WebToolsConfig:
    """从通用 toolset 快照恢复联网工具配置。"""

    payload = snapshot.payload if snapshot is not None else {}
    defaults = WebToolsConfig()
    return WebToolsConfig(
        provider=str(payload.get("provider", defaults.provider)),
        request_timeout_seconds=coerce_toolset_config_float(
            payload.get("request_timeout_seconds"),
            field_name="web.request_timeout_seconds",
            default=defaults.request_timeout_seconds,
        ),
        max_search_results=coerce_toolset_config_int(
            payload.get("max_search_results"),
            field_name="web.max_search_results",
            default=defaults.max_search_results,
        ),
        fetch_truncate_chars=coerce_toolset_config_int(
            payload.get("fetch_truncate_chars"),
            field_name="web.fetch_truncate_chars",
            default=defaults.fetch_truncate_chars,
        ),
        allow_private_network_url=bool(
            payload.get("allow_private_network_url", defaults.allow_private_network_url)
        ),
        playwright_channel=str(payload.get("playwright_channel", defaults.playwright_channel)),
        playwright_storage_state_dir=str(
            payload.get("playwright_storage_state_dir", defaults.playwright_storage_state_dir)
        ),
    )


def register_utils_toolset(context: ToolsetRegistrationContext) -> int:
    """注册 utils toolset。

    Args:
        context: toolset 注册上下文。

    Returns:
        实际注册的工具数量。

    Raises:
        无。
    """

    return register_utils_builtin_tools(
        cast(ToolRegistry, context.registry),
        timeout_budget=context.tool_timeout_seconds,
    )


def register_doc_toolset(context: ToolsetRegistrationContext) -> int:
    """注册 doc toolset。

    Args:
        context: toolset 注册上下文。

    Returns:
        实际注册的工具数量。

    Raises:
        无。
    """

    before_count = len(context.registry.tools)
    doc_tool_limits = _build_doc_tool_limits(context.toolset_config)
    register_doc_tools(
        cast(ToolRegistry, context.registry),
        limits=doc_tool_limits,
        allowed_paths=list(
            build_effective_doc_allowed_paths(
                workspace=context.workspace,
                doc_permissions=context.execution_permissions.doc,
            )
        ),
        allow_file_write=bool(context.execution_permissions.doc.allow_file_write),
        allowed_write_paths=list(context.execution_permissions.doc.allowed_write_paths),
        timeout_budget=context.tool_timeout_seconds,
    )
    return len(context.registry.tools) - before_count


def register_web_toolset(context: ToolsetRegistrationContext) -> int:
    """注册 web toolset。

    Args:
        context: toolset 注册上下文。

    Returns:
        实际注册的工具数量。

    Raises:
        ValueError: 当 web provider 已启用但缺少 web 配置时抛出。
    """

    web_tools_config = _build_web_tools_config(context.toolset_config)
    web_provider = str(getattr(web_tools_config, "provider", "") or "").strip().lower()
    if web_provider in {"", "off"}:
        return 0
    before_count = len(context.registry.tools)
    register_web_tools(
        cast(ToolRegistry, context.registry),
        provider=web_tools_config.provider,
        request_timeout_seconds=web_tools_config.request_timeout_seconds,
        max_search_results=web_tools_config.max_search_results,
        fetch_truncate_chars=web_tools_config.fetch_truncate_chars,
        allow_private_network_url=bool(context.execution_permissions.web.allow_private_network_url),
        playwright_channel=web_tools_config.playwright_channel,
        playwright_storage_state_dir=web_tools_config.playwright_storage_state_dir,
        timeout_budget=context.tool_timeout_seconds,
    )
    return len(context.registry.tools) - before_count


__all__ = [
    "register_doc_toolset",
    "register_utils_toolset",
    "register_web_toolset",
]