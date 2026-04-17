"""SEC company meta 与 ticker alias 真源。"""

from __future__ import annotations

from typing import Any, Optional

from dayu.fins.domain.document_models import CompanyMeta, now_iso8601
from dayu.fins.storage import CompanyMetaRepositoryProtocol


def normalize_sec_ticker_aliases(
    *,
    primary_ticker: str,
    raw_aliases: Optional[list[Any]],
) -> list[str]:
    """标准化 SEC 返回的 ticker alias 列表。"""

    normalized_primary = str(primary_ticker).strip().upper()
    if not normalized_primary:
        raise ValueError("primary_ticker 不能为空")
    normalized_aliases: list[str] = []
    for raw_alias in [normalized_primary, *(raw_aliases or [])]:
        normalized_alias = str(raw_alias).strip().upper()
        if not normalized_alias:
            continue
        if normalized_alias in normalized_aliases:
            continue
        normalized_aliases.append(normalized_alias)
    return normalized_aliases


def extract_sec_ticker_aliases(
    *,
    submissions: dict[str, Any],
    primary_ticker: str,
) -> list[str]:
    """从 SEC submissions 中提取 ticker alias。"""

    raw_aliases = submissions.get("tickers")
    alias_list = raw_aliases if isinstance(raw_aliases, list) else None
    return normalize_sec_ticker_aliases(
        primary_ticker=primary_ticker,
        raw_aliases=alias_list,
    )


def merge_ticker_aliases(
    *,
    primary_ticker: str,
    alias_groups: list[Optional[list[str]]],
) -> list[str]:
    """按顺序合并多组 ticker alias。"""

    merged_aliases: list[str] = [str(primary_ticker).strip().upper()]
    if not merged_aliases[0]:
        raise ValueError("primary_ticker 不能为空")
    for alias_group in alias_groups:
        for alias in alias_group or []:
            normalized_alias = str(alias).strip().upper()
            if not normalized_alias or normalized_alias in merged_aliases:
                continue
            merged_aliases.append(normalized_alias)
    return merged_aliases


def upsert_company_meta(
    *,
    repository: CompanyMetaRepositoryProtocol,
    ticker: str,
    company_id: str,
    company_name: str,
    ticker_aliases: Optional[list[str]] = None,
) -> None:
    """写入 SEC 公司级元数据。"""

    repository.upsert_company_meta(
        CompanyMeta(
            company_id=company_id,
            company_name=company_name or ticker,
            ticker=ticker,
            market="US",
            resolver_version="market_resolver_v1",
            updated_at=now_iso8601(),
            ticker_aliases=normalize_sec_ticker_aliases(
                primary_ticker=ticker,
                raw_aliases=ticker_aliases,
            ),
        )
    )


__all__ = [
    "extract_sec_ticker_aliases",
    "merge_ticker_aliases",
    "normalize_sec_ticker_aliases",
    "upsert_company_meta",
]