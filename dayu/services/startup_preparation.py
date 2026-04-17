"""Service 层对启动期暴露的 preparation API。

本模块是 Service 层的 public surface，供 UI 启动期装配调用。
它负责把稳定输入收敛成 Service 请求期需要的公开依赖，
但不把内部 reader / preparer 的实现细节泄漏给 `startup/` 或 UI。
"""

from __future__ import annotations

from pathlib import Path

from dayu.contracts.infrastructure import ModelCatalogProtocol, PromptAssetStoreProtocol
from dayu.execution.options import ResolvedExecutionOptions
from dayu.services.conversation_policy_reader import ConversationPolicyReader
from dayu.services.scene_definition_reader import SceneDefinitionReader
from dayu.services.scene_execution_acceptance import SceneExecutionAcceptancePreparer


def prepare_scene_execution_acceptance_preparer(
    *,
    workspace_root: Path,
    default_execution_options: ResolvedExecutionOptions,
    model_catalog: ModelCatalogProtocol,
    prompt_asset_store: PromptAssetStoreProtocol,
) -> SceneExecutionAcceptancePreparer:
    """准备 Service 侧 scene 执行接受准备器。

    Args:
        workspace_root: 当前工作区根目录。
        default_execution_options: 启动期已解析的默认执行选项。
        model_catalog: 启动期模型目录对象。
        prompt_asset_store: prompt 资产仓储对象。

    Returns:
        已完成内部 reader 装配的 `SceneExecutionAcceptancePreparer`。

    Raises:
        无。
    """

    scene_definition_reader = SceneDefinitionReader(prompt_asset_store)
    conversation_policy_reader = ConversationPolicyReader()
    return SceneExecutionAcceptancePreparer(
        workspace_dir=workspace_root,
        base_execution_options=default_execution_options,
        model_catalog=model_catalog,
        scene_definition_reader=scene_definition_reader,
        conversation_policy_reader=conversation_policy_reader,
    )


__all__ = [
    "prepare_scene_execution_acceptance_preparer",
]
