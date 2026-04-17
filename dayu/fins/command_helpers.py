"""财报命令辅助函数。

该模块提供与 UI 无关的参数规范化和命令校验能力，供 application 层复用。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Optional, Sequence

from dayu.fins.cli import (
    _coerce_document_ids_input,
    _coerce_forms_input,
    _generate_upload_filings_script,
    _prepare_cli_args,
    _to_paths,
    _validate_upload_filing_args,
    _validate_upload_material_args,
)


def coerce_forms_input(value: Optional[Sequence[str] | str]) -> Optional[str]:
    """标准化 `forms` 参数。"""

    return _coerce_forms_input(value)


def coerce_document_ids_input(
    value: Optional[Sequence[str] | str],
) -> Optional[list[str]]:
    """标准化 `document_ids` 参数。"""

    return _coerce_document_ids_input(value)


def to_paths(values: Optional[list[str]]) -> list[Path]:
    """将字符串路径列表转换为 `Path` 列表。"""

    return _to_paths(values)


def validate_upload_filing_args(args: argparse.Namespace) -> None:
    """校验 `upload_filing` 参数组合。"""

    _validate_upload_filing_args(args)


def validate_upload_material_args(args: argparse.Namespace) -> None:
    """校验 `upload_material` 参数组合。"""

    _validate_upload_material_args(args)


def prepare_cli_args(args: argparse.Namespace) -> None:
    """执行 fins CLI 参数预处理。

    Args:
        args: argparse 参数对象。

    Returns:
        无。

    Raises:
        ValueError: 参数非法时抛出。
    """

    _prepare_cli_args(args)


def generate_upload_filings_script(args: argparse.Namespace) -> dict[str, Any]:
    """生成批量上传脚本。"""

    return _generate_upload_filings_script(args)


__all__ = [
    "coerce_document_ids_input",
    "coerce_forms_input",
    "to_paths",
    "prepare_cli_args",
    "validate_upload_filing_args",
    "validate_upload_material_args",
    "generate_upload_filings_script",
]
