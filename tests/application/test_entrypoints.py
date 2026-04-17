"""入口模块覆盖测试。"""

from __future__ import annotations

from pathlib import Path
import runpy
import sys
from types import ModuleType
from typing import Any, cast

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _install_main_module(monkeypatch: pytest.MonkeyPatch, module_name: str, return_code: int) -> None:
    """安装可控的 `main` 模块测试桩。"""

    if "." in module_name:
        package_name, attr_name = module_name.rsplit(".", 1)
        package_module = ModuleType(package_name)
        cast(Any, package_module).__path__ = []
        monkeypatch.setitem(sys.modules, package_name, package_module)
    else:
        package_name = ""
        attr_name = module_name

    module = ModuleType(module_name)

    def _main() -> int:
        """返回预设退出码。"""

        return return_code

    cast(Any, module).main = _main
    monkeypatch.setitem(sys.modules, module_name, module)
    if package_name:
        cast(Any, sys.modules[package_name]).__dict__[attr_name] = module


@pytest.mark.unit
@pytest.mark.parametrize(
    ("entrypoint_path", "dependency_module", "return_code"),
    [
        (_REPO_ROOT / "dayu/__main__.py", "dayu.cli.main", 11),
        (_REPO_ROOT / "dayu/cli/__main__.py", "dayu.cli.main", 12),
        (_REPO_ROOT / "dayu/wechat/__main__.py", "dayu.wechat.main", 13),
    ],
)
def test_module_entrypoints_raise_system_exit(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint_path: Path,
    dependency_module: str,
    return_code: int,
) -> None:
    """包入口应把 `main()` 返回值透传为 `SystemExit.code`。"""

    _install_main_module(monkeypatch, dependency_module, return_code)

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(str(entrypoint_path), run_name="__main__")

    assert exc_info.value.code == return_code


@pytest.mark.unit
def test_tool_limits_reexports_stable_limit_types() -> None:
    """稳定导出模块应直接重导出 doc/fins limits 类型。"""

    from dayu.execution.doc_limits import DocToolLimits
    from dayu.fins.tools.fins_limits import FinsToolLimits
    from dayu.tool_limits import __all__, DocToolLimits as ExportedDocToolLimits, FinsToolLimits as ExportedFinsToolLimits

    assert ExportedDocToolLimits is DocToolLimits
    assert ExportedFinsToolLimits is FinsToolLimits
    assert __all__ == ["DocToolLimits", "FinsToolLimits"]