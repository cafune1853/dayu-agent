"""CnPipeline 占位行为测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from dayu.fins.domain.enums import SourceKind
from dayu.fins.pipelines.download_events import DownloadEventType
from dayu.fins.pipelines.upload_filing_events import UploadFilingEventType
from dayu.fins.pipelines.upload_material_events import UploadMaterialEventType
from dayu.fins.pipelines.cn_pipeline import CnPipeline
from dayu.fins.processors.registry import build_fins_processor_registry


def test_download_returns_not_implemented_status(tmp_path: Path) -> None:
    """验证 `download` 返回未实现状态。

    Args:
        tmp_path: 临时目录。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    pipeline = CnPipeline(
        workspace_root=tmp_path,
        processor_registry=build_fins_processor_registry(),
    )

    result = pipeline.download(
        ticker="000001",
        form_type="ANNUAL",
        start_date="2025-01-01",
        end_date="2025-12-31",
        overwrite=True,
    )

    assert result["pipeline"] == "cn"
    assert result["action"] == "download"
    assert result["status"] == "not_implemented"
    assert result["message"] == "CnPipeline.download 尚未实现"
    assert result["ticker"] == "000001"


@pytest.mark.asyncio
async def test_download_stream_emits_not_implemented_result(tmp_path: Path) -> None:
    """验证 `download_stream` 结束事件返回未实现结果。

    Args:
        tmp_path: 临时目录。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    pipeline = CnPipeline(
        workspace_root=tmp_path,
        processor_registry=build_fins_processor_registry(),
    )

    events = [
        event
        async for event in pipeline.download_stream(
            ticker="000001",
            form_type="ANNUAL",
            start_date="2025-01-01",
            end_date="2025-12-31",
            overwrite=False,
        )
    ]

    assert len(events) == 2
    assert events[0].event_type == DownloadEventType.PIPELINE_STARTED
    assert events[1].event_type == DownloadEventType.PIPELINE_COMPLETED
    assert events[1].payload["result"]["status"] == "not_implemented"
    assert events[1].payload["result"]["message"] == "CnPipeline.download_stream 尚未实现"


@pytest.mark.asyncio
async def test_upload_filing_stream_uploads_files_with_docling(tmp_path: Path) -> None:
    """验证 `upload_filing_stream` 可完成上传并生成 docling 主文件。

    Args:
        tmp_path: 临时目录。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    pipeline = CnPipeline(
        workspace_root=tmp_path,
        processor_registry=build_fins_processor_registry(),
    )
    pipeline._upload_service._convert_with_docling = lambda path: {  # type: ignore[attr-defined]
        "name": path.name,
        "format": "docling",
    }
    sample_file = tmp_path / "sample.pdf"
    sample_file.write_text("demo", encoding="utf-8")

    events = [
        event
        async for event in pipeline.upload_filing_stream(
            ticker="000001",
            action="create",
            files=[sample_file],
            fiscal_year=2025,
            fiscal_period="FY",
            amended=False,
            filing_date="2025-01-01",
            report_date="2024-12-31",
            company_id="000001",
            company_name="平安银行",
            overwrite=False,
        )
    ]

    assert len(events) == 5
    assert events[0].event_type == UploadFilingEventType.UPLOAD_STARTED
    assert events[1].event_type == UploadFilingEventType.FILE_UPLOADED
    assert events[1].payload["name"] == "sample.pdf"
    assert events[1].payload["source"] == "original"
    assert events[2].event_type == UploadFilingEventType.CONVERSION_STARTED
    assert events[2].payload["name"] == "sample.pdf"
    assert events[2].payload["message"] == "正在 convert"
    assert events[3].event_type == UploadFilingEventType.FILE_UPLOADED
    assert events[3].payload["name"] == "sample_docling.json"
    assert events[3].payload["source"] == "docling"
    assert events[4].event_type == UploadFilingEventType.UPLOAD_COMPLETED
    result = events[4].payload["result"]
    assert result["status"] == "ok"
    assert str(result["document_id"]).startswith("fil_cn_")
    assert str(result["internal_document_id"]).startswith("cn_")
    company_meta = pipeline._company_repository.get_company_meta("000001")  # type: ignore[attr-defined]
    assert company_meta.company_id == "000001"
    assert company_meta.company_name == "平安银行"
    meta = pipeline._source_repository.get_source_meta("000001", result["document_id"], SourceKind.FILING)  # type: ignore[attr-defined]
    assert str(meta["primary_document"]).endswith("_docling.json")


@pytest.mark.asyncio
async def test_upload_material_stream_uploads_files_with_docling(tmp_path: Path) -> None:
    """验证 `upload_material_stream` 可完成上传并生成 docling 主文件。

    Args:
        tmp_path: 临时目录。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    pipeline = CnPipeline(
        workspace_root=tmp_path,
        processor_registry=build_fins_processor_registry(),
    )
    pipeline._upload_service._convert_with_docling = lambda path: {  # type: ignore[attr-defined]
        "name": path.name,
        "format": "docling",
    }
    sample_file = tmp_path / "material.pdf"
    sample_file.write_text("demo", encoding="utf-8")

    events = [
        event
        async for event in pipeline.upload_material_stream(
            ticker="000001",
            action="create",
            form_type="MATERIAL_OTHER",
            material_name="Deck",
            files=[sample_file],
            filing_date="2025-01-01",
            report_date="2024-12-31",
            company_id="000001",
            company_name="平安银行",
            overwrite=False,
        )
    ]

    assert len(events) == 5
    assert events[0].event_type == UploadMaterialEventType.UPLOAD_STARTED
    assert events[1].event_type == UploadMaterialEventType.FILE_UPLOADED
    assert events[1].payload["name"] == "material.pdf"
    assert events[1].payload["source"] == "original"
    assert events[2].event_type == UploadMaterialEventType.CONVERSION_STARTED
    assert events[2].payload["name"] == "material.pdf"
    assert events[2].payload["message"] == "正在 convert"
    assert events[3].event_type == UploadMaterialEventType.FILE_UPLOADED
    assert events[3].payload["name"] == "material_docling.json"
    assert events[3].payload["source"] == "docling"
    assert events[4].event_type == UploadMaterialEventType.UPLOAD_COMPLETED
    result = events[4].payload["result"]
    assert result["status"] == "ok"
    assert str(result["document_id"]).startswith("mat_")
    company_meta = pipeline._company_repository.get_company_meta("000001")  # type: ignore[attr-defined]
    assert company_meta.company_id == "000001"
    assert company_meta.company_name == "平安银行"
    meta = pipeline._source_repository.get_source_meta("000001", result["document_id"], SourceKind.MATERIAL)  # type: ignore[attr-defined]
    assert str(meta["primary_document"]).endswith("_docling.json")
