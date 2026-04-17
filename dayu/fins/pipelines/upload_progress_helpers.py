"""上传进度事件辅助函数。

该模块负责 upload stream 的文件级进度事件处理，目标是：
1. 在 Docling Convert 前先输出原文件上传回显；
2. 过滤后续重复的 original `file_uploaded` 事件，避免日志重复。
"""

from __future__ import annotations

import mimetypes
from pathlib import Path

from .docling_upload_service import UploadFileEventPayload
from .upload_filing_events import UploadFilingEventType
from .upload_material_events import UploadMaterialEventType

_UPLOAD_FILE_TO_FILING_EVENT_TYPE: dict[str, UploadFilingEventType] = {
    "conversion_started": UploadFilingEventType.CONVERSION_STARTED,
    "file_uploaded": UploadFilingEventType.FILE_UPLOADED,
    "file_skipped": UploadFilingEventType.FILE_SKIPPED,
    "file_failed": UploadFilingEventType.FILE_FAILED,
}

_UPLOAD_FILE_TO_MATERIAL_EVENT_TYPE: dict[str, UploadMaterialEventType] = {
    "conversion_started": UploadMaterialEventType.CONVERSION_STARTED,
    "file_uploaded": UploadMaterialEventType.FILE_UPLOADED,
    "file_skipped": UploadMaterialEventType.FILE_SKIPPED,
    "file_failed": UploadMaterialEventType.FILE_FAILED,
}


def build_original_file_uploaded_events(files: list[Path]) -> list[UploadFileEventPayload]:
    """构建原文件预回显事件列表。

    Args:
        files: 用户输入的原始文件路径列表。

    Returns:
        仅包含 `source=original` 的 `file_uploaded` 事件列表，顺序与输入一致。

    Raises:
        无。
    """

    events: list[UploadFileEventPayload] = []
    for file_path in files:
        payload: dict[str, object] = {"source": "original"}
        try:
            payload["size"] = file_path.stat().st_size
        except OSError:
            # 文件状态读取失败时不阻断主流程，让后续上传逻辑给出最终错误。
            pass
        content_type = mimetypes.guess_type(str(file_path))[0]
        if content_type:
            payload["content_type"] = content_type
        events.append(
            UploadFileEventPayload(
                event_type="file_uploaded",
                name=file_path.name,
                payload=payload,
            )
        )
    return events


def build_conversion_started_events(files: list[Path]) -> list[UploadFileEventPayload]:
    """构建 Docling 转换开始事件列表。

    Args:
        files: 用户输入的原始文件路径列表。

    Returns:
        `conversion_started` 事件列表，顺序与输入一致。

    Raises:
        无。
    """

    events: list[UploadFileEventPayload] = []
    for file_path in files:
        events.append(
            UploadFileEventPayload(
                event_type="conversion_started",
                name=file_path.name,
                payload={
                    "source": "docling",
                    "message": "正在 convert",
                },
            )
        )
    return events


def should_emit_upload_file_event(file_event: UploadFileEventPayload) -> bool:
    """判断上传结果事件是否需要继续向外透传。

    Args:
        file_event: 上传服务返回的文件级事件。

    Returns:
        `True` 表示应透传；`False` 表示应过滤。

    Raises:
        无。
    """

    if file_event.event_type != "file_uploaded":
        return True
    source = str(file_event.payload.get("source", "")).strip().lower()
    return source != "original"


def map_upload_file_event_to_filing_event_type(
    file_event: UploadFileEventPayload,
) -> UploadFilingEventType:
    """将上传文件事件映射为财报上传事件类型。

    Args:
        file_event: 上传服务返回的文件级事件。

    Returns:
        `upload_filing_stream` 对外事件类型。

    Raises:
        ValueError: 事件类型未知时抛出。
    """

    event_type = _UPLOAD_FILE_TO_FILING_EVENT_TYPE.get(file_event.event_type)
    if event_type is None:
        raise ValueError(f"未知上传文件事件类型: {file_event.event_type}")
    return event_type


def map_upload_file_event_to_material_event_type(
    file_event: UploadFileEventPayload,
) -> UploadMaterialEventType:
    """将上传文件事件映射为材料上传事件类型。

    Args:
        file_event: 上传服务返回的文件级事件。

    Returns:
        `upload_material_stream` 对外事件类型。

    Raises:
        ValueError: 事件类型未知时抛出。
    """

    event_type = _UPLOAD_FILE_TO_MATERIAL_EVENT_TYPE.get(file_event.event_type)
    if event_type is None:
        raise ValueError(f"未知上传文件事件类型: {file_event.event_type}")
    return event_type
