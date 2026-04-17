"""SecPipeline upload_material_stream 测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from dayu.fins.domain.enums import SourceKind
from dayu.fins.pipelines.upload_material_events import UploadMaterialEventType
from dayu.fins.pipelines.sec_pipeline import SecPipeline
from dayu.fins.processors.registry import build_fins_processor_registry


@pytest.mark.asyncio
async def test_upload_material_stream_uploads_docling_files(tmp_path: Path) -> None:
    """验证 `upload_material_stream` 可完成上传并生成 docling 主文件。

    Args:
        tmp_path: 临时目录。

    Returns:
        无。

    Raises:
        AssertionError: 断言失败时抛出。
    """

    pipeline = SecPipeline(
        workspace_root=tmp_path,
        processor_registry=build_fins_processor_registry(),
    )
    pipeline._upload_service._convert_with_docling = lambda path: {  # type: ignore[attr-defined]
        "name": path.name,
        "format": "docling",
    }
    material_file = tmp_path / "material.pdf"
    material_file.write_text("demo material", encoding="utf-8")

    events = [
        event
        async for event in pipeline.upload_material_stream(
            ticker="AAPL",
            action="create",
            form_type="MATERIAL_OTHER",
            material_name="Deck",
            files=[material_file],
            filing_date="2025-05-01",
            report_date="2025-03-31",
            company_id="320193",
            company_name="Apple Inc.",
            ticker_aliases=["AAPL", "APC"],
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
    assert result["action"] == "upload_material"
    assert result["ticker"] == "AAPL"
    assert result["status"] == "ok"
    assert str(result["document_id"]).startswith("mat_")
    company_meta = pipeline._company_repository.get_company_meta("AAPL")  # type: ignore[attr-defined]
    assert company_meta.company_id == "320193"
    assert company_meta.company_name == "Apple Inc."
    assert company_meta.ticker_aliases == ["AAPL", "APC"]
    meta = pipeline._source_repository.get_source_meta("AAPL", result["document_id"], SourceKind.MATERIAL)  # type: ignore[attr-defined]
    assert str(meta["primary_document"]).endswith("_docling.json")
