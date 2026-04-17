"""工具 decorator 与 schema 构建辅助。

该模块负责：
- 生成 ``ToolSchema``。
- 将运行时元数据（tags / truncate / dup_call / file_path_params）挂到函数对象上。
- 供 ``ToolRegistry.register()`` 在注册时统一读取。
"""

import copy
from dataclasses import dataclass
from typing import AbstractSet, Any, Callable, Dict, Optional, ParamSpec, Protocol, TypeVar, Union, cast

from ..tool_contracts import DupCallSpec, ToolFunctionSchema, ToolSchema, ToolTruncateSpec
from ..exceptions import ConfigError

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


def _resolve_enum_values(enum_spec: Any, registry: Any) -> Optional[list]:
    """
    Resolve enum values for a parameter.

    Args:
        enum_spec: A list of enum values or a callable returning such list.
        registry: ToolRegistry instance, required when enum_spec is callable.

    Returns:
        A list of enum values or None.
    """
    if enum_spec is None:
        return None
    if callable(enum_spec):
        if registry is None:
            raise ConfigError("tool_schema", None, "enum resolver requires registry")
        enum_values = enum_spec(registry)
    else:
        enum_values = enum_spec

    if enum_values is None:
        return None
    if not isinstance(enum_values, list):
        raise ConfigError("tool_schema", None, "enum values must be a list")
    return enum_values


def build_tool_schema(
    *,
    name: str,
    description: str,
    parameters: Dict[str, Any],
    enums: Optional[Dict[str, Any]] = None,
    registry: Any = None
) -> ToolSchema:
    """
    Build a ToolSchema with optional enum injection and truncation metadata.

    Args:
        name: Tool name.
        description: Tool description for the LLM.
        parameters: JSON Schema dict for parameters.
        enums: Optional mapping of field -> enum values or callable(registry) -> list.
        registry: ToolRegistry instance for dynamic enum resolution.
        truncate: Optional ToolTruncateSpec or dict for truncation settings.

    Returns:
        ToolSchema instance with internal truncation metadata.
    """
    if not isinstance(parameters, dict):
        raise ConfigError("tool_schema", None, "parameters must be a dict")

    params_copy = copy.deepcopy(parameters)
    properties = params_copy.get("properties")
    if not isinstance(properties, dict):
        raise ConfigError("tool_schema", None, "parameters.properties must be a dict")

    if enums:
        for field_name, enum_spec in enums.items():
            if field_name not in properties:
                raise ConfigError("tool_schema", None, f"enum field not found in parameters: {field_name}")
            enum_values = _resolve_enum_values(enum_spec, registry)
            if enum_values:
                properties[field_name]["enum"] = enum_values
            else:
                properties[field_name].pop("enum", None)

    return ToolSchema(
        function=ToolFunctionSchema(
            name=name,
            description=description,
            parameters=params_copy,
        )
    )


@dataclass
class ToolExtra:
    """附加工具元数据（不参与 OpenAI schema）。"""

    __file_path_params__: list[str]
    __truncate__: ToolTruncateSpec
    __dup_call__: Optional[DupCallSpec]
    __execution_context_param_name__: str | None


class DecoratedToolCallable(Protocol[P, R]):
    """带工具元数据的可调用对象协议。"""

    __tool_name__: str
    __tool_schema__: ToolSchema
    __tool_tags__: set[str]
    __tool_extra__: ToolExtra

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """执行工具函数。"""

        ...


def tool(
    registry: Any,
    *,
    name: str,
    description: str,
    parameters: Union[Dict[str, Any], Callable[[Any], Dict[str, Any]]],
    enums: Optional[Dict[str, Any]] = None,
    tags: Optional[AbstractSet[str]] = None,
    truncate: Optional[Union[ToolTruncateSpec, Dict[str, Any]]] = None,
    dup_call: Optional[Union[DupCallSpec, Dict[str, Any]]] = None,
    file_path_params: Optional[list[str]] = None,
    execution_context_param_name: str | None = None,
) -> Callable[[Callable[P, R]], DecoratedToolCallable[P, R]]:
    """
    Decorator for tool functions.

    This decorator resolves parameters, injects enums, builds ToolSchema,
    and attaches metadata to the function for later registration.
    
    Args:
        registry: ToolRegistry instance
        name: Tool name
        description: Tool description for LLM
        parameters: JSON Schema dict or callable returning it
        enums: Optional field -> enum values mapping
        tags: Optional tags for tool grouping
        truncate: Optional truncation specification
        dup_call: Optional duplicate-call specification
        file_path_params: Optional list of parameter names that should be validated
                         as file paths (e.g., ["file_path", "directory"])
        execution_context_param_name: 工具函数中接收 execution context 的显式参数名。
    """

    def wrap(func: Callable[P, R]) -> DecoratedToolCallable[P, R]:
        resolved_parameters = parameters(registry) if callable(parameters) else parameters
        schema = build_tool_schema(
            name=name,
            description=description,
            parameters=resolved_parameters,
            enums=enums,
            registry=registry
        )
        if truncate is None:
            truncate_spec = ToolTruncateSpec()
        elif isinstance(truncate, ToolTruncateSpec):
            truncate_spec = truncate
        elif isinstance(truncate, dict):
            truncate_spec = ToolTruncateSpec(**truncate)
        else:
            raise ConfigError("tool_schema", None, "truncate must be ToolTruncateSpec or dict")

        if dup_call is None:
            dup_call_spec = None
        elif isinstance(dup_call, DupCallSpec):
            dup_call_spec = dup_call
        elif isinstance(dup_call, dict):
            dup_call_spec = DupCallSpec(**dup_call)
        else:
            raise ConfigError("tool_schema", None, "dup_call must be DupCallSpec or dict")

        decorated_func = cast(DecoratedToolCallable[P, R], func)
        decorated_func.__tool_name__ = name
        decorated_func.__tool_schema__ = schema
        decorated_func.__tool_tags__ = set(tags) if tags is not None else set()
        decorated_func.__tool_extra__ = ToolExtra(
            __file_path_params__=file_path_params or [],
            __truncate__=truncate_spec,
            __dup_call__=dup_call_spec,
            __execution_context_param_name__=(
                str(execution_context_param_name).strip()
                if execution_context_param_name is not None and str(execution_context_param_name).strip()
                else None
            ),
        )
        return decorated_func

    return wrap
