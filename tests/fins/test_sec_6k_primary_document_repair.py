"""6-K active source 主文件修复测试。"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest

from dayu.fins.domain.document_models import SourceDocumentUpsertRequest
from dayu.fins.domain.enums import SourceKind
from dayu.fins.pipelines import sec_6k_primary_document_repair as repair_module
from tests.fins.storage_testkit import FsStorageTestContext, build_fs_storage_test_context


def _create_active_6k_filing(
    context: FsStorageTestContext,
    *,
    ticker: str,
    document_id: str,
    primary_document: str,
    file_payloads: dict[str, bytes],
) -> None:
    """创建带多个 HTML 文件的 active 6-K。

    Args:
        context: 测试仓储上下文。
        ticker: 股票代码。
        document_id: 文档 ID。
        primary_document: 当前主文件名。
        file_payloads: 文件名到文件内容的映射。

    Returns:
        无。

    Raises:
        OSError: 仓储写入失败时抛出。
    """

    context.source_repository.create_source_document(
        SourceDocumentUpsertRequest(
            ticker=ticker,
            document_id=document_id,
            internal_document_id=document_id.replace("fil_", ""),
            form_type="6-K",
            primary_document=primary_document,
            meta={
                "form_type": "6-K",
                "report_date": "2025-09-30",
                "is_deleted": False,
            },
        ),
        source_kind=SourceKind.FILING,
    )
    handle = context.source_repository.get_source_handle(ticker, document_id, SourceKind.FILING)
    file_metas = []
    for filename, payload in file_payloads.items():
        file_metas.append(
            context.blob_repository.store_file(
                handle=handle,
                filename=filename,
                data=BytesIO(payload),
                content_type="text/html",
            )
        )
    context.source_repository.update_source_document(
        SourceDocumentUpsertRequest(
            ticker=ticker,
            document_id=document_id,
            internal_document_id=document_id.replace("fil_", ""),
            form_type="6-K",
            primary_document=primary_document,
            meta={
                "form_type": "6-K",
                "report_date": "2025-09-30",
                "is_deleted": False,
            },
            files=file_metas,
        ),
        source_kind=SourceKind.FILING,
    )


@pytest.mark.unit
def test_repair_active_6k_primary_document_promotes_parseable_attachment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 cover primary 失败且附件可提取核心报表时会修正主文件。"""

    context = build_fs_storage_test_context(tmp_path)
    _create_active_6k_filing(
        context,
        ticker="ALVO",
        document_id="fil_alvo_q1",
        primary_document="form6-k.htm",
        file_payloads={
            "form6-k.htm": b"FORM 6-K cover page",
            "ex99-1.htm": b"EX-99.1 press release",
        },
    )

    assessment_by_filename = {
        "form6-k.htm": repair_module.SixKPrimaryCandidateAssessment(
            filename="form6-k.htm",
            income_row_count=0,
            balance_sheet_row_count=0,
            filename_priority=3,
        ),
        "ex99-1.htm": repair_module.SixKPrimaryCandidateAssessment(
            filename="ex99-1.htm",
            income_row_count=18,
            balance_sheet_row_count=32,
            filename_priority=0,
        ),
    }

    def _fake_assess_active_6k_candidate(
        *,
        source_repository: object,
        ticker: str,
        document_id: str,
        filename: str,
        primary_document: str,
    ) -> repair_module.SixKPrimaryCandidateAssessment:
        """返回固定候选评估结果。"""

        del source_repository, ticker, document_id, primary_document
        return assessment_by_filename[filename]

    monkeypatch.setattr(
        repair_module,
        "_assess_active_6k_candidate",
        _fake_assess_active_6k_candidate,
    )
    reprocess_calls: list[tuple[str, str]] = []

    outcome = repair_module.reconcile_active_6k_primary_document(
        source_repository=context.source_repository,
        ticker="ALVO",
        document_id="fil_alvo_q1",
        mark_processed_reprocess_required=lambda ticker, document_id: reprocess_calls.append(
            (ticker, document_id)
        ),
    )

    assert outcome is not None
    assert outcome.previous_primary_document == "form6-k.htm"
    assert outcome.selected_primary_document == "ex99-1.htm"
    updated_meta = context.source_repository.get_source_meta("ALVO", "fil_alvo_q1", SourceKind.FILING)
    assert updated_meta["primary_document"] == "ex99-1.htm"
    assert reprocess_calls == [("ALVO", "fil_alvo_q1")]


@pytest.mark.unit
def test_reconcile_active_6k_primary_document_updates_non_cover_primary_when_sibling_is_better(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证当前主文件不是 cover 时也会按处理器真源重选主文件。"""

    context = build_fs_storage_test_context(tmp_path)
    _create_active_6k_filing(
        context,
        ticker="BABA",
        document_id="fil_baba_q4",
        primary_document="ex99-2.htm",
        file_payloads={
            "ex99-1.htm": b"EX-99.1 quarterly results",
            "ex99-2.htm": b"EX-99.2 supplementary materials",
        },
    )

    assessment_by_filename = {
        "ex99-1.htm": repair_module.SixKPrimaryCandidateAssessment(
            filename="ex99-1.htm",
            income_row_count=15,
            balance_sheet_row_count=26,
            filename_priority=0,
        ),
        "ex99-2.htm": repair_module.SixKPrimaryCandidateAssessment(
            filename="ex99-2.htm",
            income_row_count=0,
            balance_sheet_row_count=0,
            filename_priority=1,
        ),
    }

    def _fake_assess_active_6k_candidate(
        *,
        source_repository: object,
        ticker: str,
        document_id: str,
        filename: str,
        primary_document: str,
    ) -> repair_module.SixKPrimaryCandidateAssessment:
        """返回固定候选评估结果。"""

        del source_repository, ticker, document_id, primary_document
        return assessment_by_filename[filename]

    monkeypatch.setattr(
        repair_module,
        "_assess_active_6k_candidate",
        _fake_assess_active_6k_candidate,
    )

    outcome = repair_module.reconcile_active_6k_primary_document(
        source_repository=context.source_repository,
        ticker="BABA",
        document_id="fil_baba_q4",
    )

    assert outcome is not None
    assert outcome.previous_primary_document == "ex99-2.htm"
    assert outcome.selected_primary_document == "ex99-1.htm"
    meta = context.source_repository.get_source_meta("BABA", "fil_baba_q4", SourceKind.FILING)
    assert meta["primary_document"] == "ex99-1.htm"
