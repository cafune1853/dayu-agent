"""Fins toolset adapter。"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import cast

from dayu.contracts.toolset_config import ToolsetConfigSnapshot, coerce_toolset_config_int
from dayu.contracts.toolset_registrar import ToolsetRegistrationContext
from dayu.engine.tool_registry import ToolRegistry
from dayu.fins.service_runtime import DefaultFinsRuntime
from dayu.fins.tools.fins_limits import FinsToolLimits
from dayu.fins.tools.fins_tools import register_fins_ingestion_tools, register_fins_read_tools


@lru_cache(maxsize=8)
def _get_cached_fins_runtime(workspace_root: str) -> DefaultFinsRuntime:
    """按工作区缓存 Fins runtime。

    Args:
        workspace_root: 工作区根目录绝对路径。

    Returns:
        缓存后的 Fins runtime。

    Raises:
        无。
    """

    return DefaultFinsRuntime.create(workspace_root=Path(workspace_root))


def _build_fins_tool_limits(snapshot: ToolsetConfigSnapshot | None) -> FinsToolLimits:
    """从通用 toolset 快照恢复财报工具限制。"""

    payload = snapshot.payload if snapshot is not None else {}
    defaults = FinsToolLimits()
    return FinsToolLimits(
        processor_cache_max_entries=coerce_toolset_config_int(
            payload.get("processor_cache_max_entries"),
            field_name="fins.processor_cache_max_entries",
            default=defaults.processor_cache_max_entries,
        ),
        list_documents_max_items=coerce_toolset_config_int(
            payload.get("list_documents_max_items"),
            field_name="fins.list_documents_max_items",
            default=defaults.list_documents_max_items,
        ),
        get_document_sections_max_items=coerce_toolset_config_int(
            payload.get("get_document_sections_max_items"),
            field_name="fins.get_document_sections_max_items",
            default=defaults.get_document_sections_max_items,
        ),
        search_document_max_items=coerce_toolset_config_int(
            payload.get("search_document_max_items"),
            field_name="fins.search_document_max_items",
            default=defaults.search_document_max_items,
        ),
        list_tables_max_items=coerce_toolset_config_int(
            payload.get("list_tables_max_items"),
            field_name="fins.list_tables_max_items",
            default=defaults.list_tables_max_items,
        ),
        read_section_max_chars=coerce_toolset_config_int(
            payload.get("read_section_max_chars"),
            field_name="fins.read_section_max_chars",
            default=defaults.read_section_max_chars,
        ),
        get_page_content_max_chars=coerce_toolset_config_int(
            payload.get("get_page_content_max_chars"),
            field_name="fins.get_page_content_max_chars",
            default=defaults.get_page_content_max_chars,
        ),
        get_table_max_items=coerce_toolset_config_int(
            payload.get("get_table_max_items"),
            field_name="fins.get_table_max_items",
            default=defaults.get_table_max_items,
        ),
        get_financial_statement_max_items=coerce_toolset_config_int(
            payload.get("get_financial_statement_max_items"),
            field_name="fins.get_financial_statement_max_items",
            default=defaults.get_financial_statement_max_items,
        ),
        query_xbrl_facts_max_items=coerce_toolset_config_int(
            payload.get("query_xbrl_facts_max_items"),
            field_name="fins.query_xbrl_facts_max_items",
            default=defaults.query_xbrl_facts_max_items,
        ),
    )


def register_fins_read_toolset(context: ToolsetRegistrationContext) -> int:
    """注册 fins 读取 toolset。

    Args:
        context: toolset 注册上下文。

    Returns:
        实际注册的工具数量。

    Raises:
        无。
    """

    runtime = _get_cached_fins_runtime(str(context.workspace.workspace_dir.resolve()))
    fins_tool_limits = _build_fins_tool_limits(context.toolset_config)
    before_count = len(context.registry.tools)
    register_fins_read_tools(
        cast(ToolRegistry, context.registry),
        service=runtime.get_tool_service(
            processor_cache_max_entries=fins_tool_limits.processor_cache_max_entries
        ),
        limits=fins_tool_limits,
        timeout_budget=context.tool_timeout_seconds,
    )
    return len(context.registry.tools) - before_count


def register_fins_ingestion_toolset(context: ToolsetRegistrationContext) -> int:
    """注册 fins ingestion toolset。

    Args:
        context: toolset 注册上下文。

    Returns:
        实际注册的工具数量。

    Raises:
        无。
    """

    runtime = _get_cached_fins_runtime(str(context.workspace.workspace_dir.resolve()))
    before_count = len(context.registry.tools)
    register_fins_ingestion_tools(
        cast(ToolRegistry, context.registry),
        service_factory=runtime.build_ingestion_service_factory(),
        manager_key=runtime.get_ingestion_manager_key(),
        timeout_budget=context.tool_timeout_seconds,
    )
    return len(context.registry.tools) - before_count


__all__ = [
    "register_fins_ingestion_toolset",
    "register_fins_read_toolset",
]