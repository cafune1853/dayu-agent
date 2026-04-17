"""SecPipeline upload_filing_stream 测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from dayu.fins.domain.enums import SourceKind
from dayu.fins.pipelines.upload_filing_events import UploadFilingEventType
from dayu.fins.pipelines.sec_pipeline import SecPipeline
from dayu.fins.processors.registry import build_fins_processor_registry


@pytest.mark.asyncio
async def test_upload_filing_stream_returns_not_implemented_result(tmp_path: Path) -> None:
    """验证 `upload_filing_stream` 返回 started/completed 且结果为未实现。

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
    filing_file = tmp_path / "filing.txt"
    filing_file.write_text("demo filing", encoding="utf-8")

    events = [
        event
        async for event in pipeline.upload_filing_stream(
            ticker="AAPL",
            action="create",
            files=[filing_file],
            fiscal_year=2025,
            fiscal_period="Q1",
            amended=False,
            filing_date="2025-05-01",
            report_date="2025-03-31",
            company_id="320193",
            company_name="Apple Inc.",
            ticker_aliases=["AAPL", "APC"],
            overwrite=False,
        )
    ]

    assert len(events) == 2
    assert events[0].event_type == UploadFilingEventType.UPLOAD_STARTED
    assert events[1].event_type == UploadFilingEventType.UPLOAD_COMPLETED
    result = events[1].payload["result"]
    assert result["action"] == "upload_filing"
    assert result["filing_action"] == "create"
    assert result["status"] == "not_implemented"
    assert result["message"] == "SecPipeline.upload_filing_stream 尚未实现"
    company_meta = pipeline._company_repository.get_company_meta("AAPL")  # type: ignore[attr-defined]
    assert company_meta.ticker_aliases == ["AAPL", "APC"]
