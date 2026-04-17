"""startup preparation public 函数。

本模块只负责把启动期原始路径、配置与资源收敛成稳定依赖。
涉及 Service / Host 内部装配细节的 preparation，应由对应层对外暴露公开 API。
"""

from __future__ import annotations

from pathlib import Path

from dayu.execution.options import (
    ExecutionOptions,
    ResolvedExecutionOptions,
    build_base_execution_options,
    merge_execution_options,
)
from dayu.fins.service_runtime import DefaultFinsRuntime
from dayu.startup.config_file_resolver import ConfigFileResolver
from dayu.startup.config_loader import ConfigLoader
from dayu.startup.model_catalog import ConfigLoaderModelCatalog
from dayu.startup.paths import StartupPaths, resolve_startup_paths
from dayu.startup.prompt_assets import FilePromptAssetStore
from dayu.startup.workspace import WorkspaceResources


def prepare_startup_paths(
    *,
    workspace_root: Path,
    config_root: Path | None = None,
) -> StartupPaths:
    """准备启动期路径。"""

    return resolve_startup_paths(workspace_root=workspace_root, config_root=config_root)


def prepare_config_file_resolver(*, config_root: Path) -> ConfigFileResolver:
    """准备配置文件解析器。"""

    return ConfigFileResolver(config_root)


def prepare_config_loader(*, resolver: ConfigFileResolver) -> ConfigLoader:
    """准备配置加载器。"""

    return ConfigLoader(resolver)


def prepare_prompt_asset_store(*, resolver: ConfigFileResolver) -> FilePromptAssetStore:
    """准备 prompt 资产仓储。"""

    return FilePromptAssetStore(resolver)


def prepare_workspace_resources(
    *,
    paths: StartupPaths,
    config_loader: ConfigLoader,
    prompt_asset_store: FilePromptAssetStore,
) -> WorkspaceResources:
    """准备工作区稳定资源。"""

    return WorkspaceResources(
        workspace_dir=paths.workspace_root,
        config_root=paths.config_root,
        output_dir=paths.output_dir,
        config_loader=config_loader,
        prompt_asset_store=prompt_asset_store,
    )


def prepare_model_catalog(*, config_loader: ConfigLoader) -> ConfigLoaderModelCatalog:
    """准备模型目录。"""

    return ConfigLoaderModelCatalog(config_loader)


def prepare_default_execution_options(
    *,
    workspace_root: Path,
    config_loader: ConfigLoader,
    execution_options: ExecutionOptions | None = None,
) -> ResolvedExecutionOptions:
    """准备启动期默认执行选项。"""

    base_execution_options = build_base_execution_options(
        workspace_dir=workspace_root,
        run_config=config_loader.load_run_config(),
    )
    return merge_execution_options(
        base_options=base_execution_options,
        workspace_dir=workspace_root,
        execution_options=execution_options,
    )

def prepare_fins_runtime(*, workspace_root: Path) -> DefaultFinsRuntime:
    """准备金融领域运行时。"""

    return DefaultFinsRuntime.create(workspace_root=workspace_root)


__all__ = [
    "prepare_config_loader",
    "prepare_default_execution_options",
    "prepare_fins_runtime",
    "prepare_model_catalog",
    "prepare_prompt_asset_store",
    "prepare_startup_paths",
    "prepare_workspace_resources",
]
