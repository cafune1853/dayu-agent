#!/usr/bin/env python3
"""分析 workspace/portfolio 下所有 tickers 的下载 warning。

规则与 SecPipeline._warn_insufficient_filings / _should_warn_missing_sc13 保持一致：

Warning 类型一：SC 13D/G 缺失
  - 所有 issuer 均包含 SC 13D/G 默认表单窗口
  - filing_manifest 中无任何 SC 13D / 13D/A / 13G / 13G/A 记录时触发

Warning 类型二：年报（10-K + 20-F 合并）有效落盘数不足
  - 最低期望合计：5 份（10-K 与 20-F 合并统计，公司使用哪种因类型而异）

Warning 类型三：季报（10-Q + 6-K 合并）有效落盘数不足
  - 最低期望合计：3 份
  - 6-K 额外区分是否有 rejection_registry 过滤记录

Warning 类型四：DEF 14A 落盘数不足
  - 最低期望：2 份（仅当存在 10-K 时才检查，外资公司无需提交 DEF 14A）

Warning 类型五：强制 XBRL 缺失
  - 10-K / 10-K/A / 10-Q / 10-Q/A：始终强制提交 XBRL（SEC 自 2009 年起要求）
  - 20-F / 20-F/A：自 filing_date >= 2022-01-01 起强制（SEC iXBRL 要求全面生效）
  - 检测方式：filing 目录下是否存在 *_htm.xml 文件（XBRL instance document）

有效落盘判定：manifest entry 中 ingest_complete=True 且 is_deleted=False。
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from typing import Any


# ── 常量（与 sec_pipeline.py 保持一致） ──────────────────────────────────────

# SC 13 系列 form_type 集合（SC 13D/G 别名展开后的全集）
_SC13_FORMS = frozenset({"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"})

# 年报组（10-K + 20-F 合并检查，统一阈值）
_ANNUAL_FORMS: frozenset[str] = frozenset({"10-K", "10-K/A", "20-F", "20-F/A"})
# 季报组（10-Q + 6-K 合并检查，统一阈值）
_QUARTERLY_FORMS: frozenset[str] = frozenset({"10-Q", "10-Q/A", "6-K"})

_MIN_ANNUAL_COUNT: int = 5
_MIN_QUARTERLY_COUNT: int = 3
_MIN_DEF14A_COUNT: int = 2

# XBRL 强制要求相关常量
# 10-K / 10-K/A / 10-Q / 10-Q/A：始终强制
_XBRL_ALWAYS_REQUIRED_FORMS: frozenset[str] = frozenset({"10-K", "10-K/A", "10-Q", "10-Q/A"})
# 20-F / 20-F/A：自此 filing_date 起强制（iXBRL 要求全面生效）
_XBRL_CONDITIONAL_FORMS: frozenset[str] = frozenset({"20-F", "20-F/A"})
_XBRL_20F_CUTOFF_DATE: str = "2022-01-01"
# 所有可能触发 XBRL 检查的 form 集合（用于 forms_filter 判断）
_XBRL_CHECKED_FORMS: frozenset[str] = _XBRL_ALWAYS_REQUIRED_FORMS | _XBRL_CONDITIONAL_FORMS
# XBRL taxonomy 支撑文件的识别标记（与 _fallback_instance_files 中的排除条件保持一致）
# 这些 token 存在于 taxonomy 文件名中，不存在于 instance document 文件名中
_XBRL_TAXONOMY_TOKENS: tuple[str, ...] = ("_pre.xml", "_cal.xml", "_def.xml", "_lab.xml")


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> Any | None:
    """读取 JSON 文件，失败返回 None。

    Args:
        path: JSON 文件路径。

    Returns:
        解析后的对象，或 None（文件不存在 / 解析失败时）。
    """
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _is_valid_filing(entry: dict[str, Any]) -> bool:
    """判断 manifest entry 是否为有效落盘（等价于 status in {downloaded, skipped}）。

    Args:
        entry: filing_manifest.json 中的单条 document 记录。

    Returns:
        True 表示有效落盘。
    """
    return bool(entry.get("ingest_complete")) and not entry.get("is_deleted", False)


def _count_valid_by_form(
    documents: list[dict[str, Any]],
) -> dict[str, int]:
    """统计各 form_type 的有效落盘数量。

    Args:
        documents: filing_manifest.json["documents"] 列表。

    Returns:
        form_type → 有效落盘数 的映射。
    """
    counts: dict[str, int] = defaultdict(int)
    for entry in documents:
        if _is_valid_filing(entry):
            form = entry.get("form_type", "")
            if form:
                counts[form] += 1
    return dict(counts)


def _count_rejected_6k(rejection_registry: dict[str, Any]) -> int:
    """统计 rejection_registry 中被过滤的 6-K 数量。

    Args:
        rejection_registry: _download_rejections.json 内容。

    Returns:
        被拒绝的 6-K 记录数。
    """
    return sum(
        1
        for entry in rejection_registry.values()
        if isinstance(entry, dict) and entry.get("form_type") == "6-K"
    )


def _has_xbrl_instance(filing_dir: Path) -> bool:
    """检查 filing 目录是否包含 XBRL instance 文件。

    与 sec_processor._discover_xbrl_files 的发现逻辑保持一致：
    1. *_htm.xml  — inline XBRL（iXBRL，2020 年起强制）。
    2. *_ins.xml  — 传统 XBRL instance 格式。
    3. 回退：任何不包含 taxonomy 标记（_pre.xml/_cal.xml/_def.xml/_lab.xml）
       的 .xml 文件。

    排除逻辑采用子串匹配（token in filename），与 _fallback_instance_files 一致。

    Args:
        filing_dir: filing 本地目录路径。

    Returns:
        True 表示存在 XBRL instance 文件。
    """
    for f in filing_dir.iterdir():
        if not f.is_file() or f.suffix != ".xml":
            continue
        lowered = f.name.lower()
        # iXBRL 或传统 instance 格式，直接认定
        if lowered.endswith("_htm.xml") or lowered.endswith("_ins.xml"):
            return True
        # 回退：排除 taxonomy 支撑文件后剩余的即为 instance
        if not any(token in lowered for token in _XBRL_TAXONOMY_TOKENS):
            return True
    return False


def _find_xbrl_missing_filings(
    ticker_dir: Path,
    documents: list[dict[str, Any]],
) -> list[str]:
    """找出强制 XBRL 却缺少 XBRL instance 文件的 filing document_id 列表。

    检查逻辑（优先级由高到低）：
    1. manifest entry 含 has_xbrl 字段（新版 pipeline 写入）：
       - True  → 有 XBRL，跳过
       - False → 缺失，加入结果
       - None  → 字段不存在或未知，回退到文件系统扫描（兼容旧 manifest）
    2. 文件系统扫描（调用 _has_xbrl_instance，与 discover_xbrl_files 逻辑一致）

    表单限定规则：
    - 10-K / 10-K/A / 10-Q / 10-Q/A：始终强制
    - 20-F / 20-F/A：filing_date >= _XBRL_20F_CUTOFF_DATE 时强制
    - 仅检查有效落盘（ingest_complete=True 且 is_deleted=False）
    - 若 filing 目录不存在且 manifest 无 has_xbrl，则跳过（由其他检查处理）

    Args:
        ticker_dir: ticker 根目录（如 workspace/portfolio/AAPL）。
        documents: filing_manifest.json["documents"] 列表。

    Returns:
        缺少 XBRL instance 文件的 document_id 列表，按 filing_date 升序。
    """
    missing: list[tuple[str, str]] = []  # (filing_date, document_id)
    for entry in documents:
        if not _is_valid_filing(entry):
            continue
        form = entry.get("form_type", "")
        doc_id = entry.get("document_id", "")
        filing_date = entry.get("filing_date") or ""
        if not doc_id:
            continue

        # 判断是否强制 XBRL
        if form in _XBRL_ALWAYS_REQUIRED_FORMS:
            xbrl_required = True
        elif form in _XBRL_CONDITIONAL_FORMS:
            xbrl_required = bool(filing_date) and filing_date >= _XBRL_20F_CUTOFF_DATE
        else:
            xbrl_required = False

        if not xbrl_required:
            continue

        # 优先读 manifest 中的 has_xbrl 字段，None 时回退到文件系统扫描
        has_xbrl = entry.get("has_xbrl")
        if has_xbrl is True:
            continue
        if has_xbrl is False:
            missing.append((filing_date, doc_id))
            continue

        # has_xbrl 未知（旧 manifest）：回退到文件系统扫描
        filing_dir = ticker_dir / "filings" / doc_id
        if not filing_dir.exists():
            continue  # 目录缺失交由其他检查处理，此处跳过
        if not _has_xbrl_instance(filing_dir):
            missing.append((filing_date, doc_id))

    missing.sort()  # 按 filing_date 升序
    return [doc_id for _, doc_id in missing]


# ── 单 ticker 分析 ─────────────────────────────────────────────────────────────

def _analyze_ticker(
    ticker_dir: Path,
    forms_filter: frozenset[str] | None = None,
) -> dict[str, Any]:
    """分析单个 ticker 目录，返回 warning 信息。

    Args:
        ticker_dir: ticker 根目录（如 workspace/portfolio/AAPL）。
        forms_filter: 需要检查的 form 集合；None 表示检查全部。

    Returns:
        分析结果字典，包含以下字段：
            ticker (str)
            total_valid (int)
            counts_by_form (dict)
            warnings (list[str])
            errors (list[str])  — 配置文件读取错误等
    """
    ticker = ticker_dir.name
    result: dict[str, Any] = {
        "ticker": ticker,
        "total_valid": 0,
        "counts_by_form": {},
        "warnings": [],
        "errors": [],
    }

    # 读取 filing_manifest.json
    manifest = _load_json(ticker_dir / "filings" / "filing_manifest.json")
    if manifest is None:
        result["errors"].append("filing_manifest.json 缺失或无法解析")
        return result
    documents: list[dict[str, Any]] = manifest.get("documents", [])
    counts = _count_valid_by_form(documents)
    result["counts_by_form"] = counts
    result["total_valid"] = sum(counts.values())

    # 读取 rejection_registry（不存在视为空）
    rejection_path = ticker_dir / "filings" / "_download_rejections.json"
    rejection_registry: dict[str, Any] = _load_json(rejection_path) or {}

    warnings: list[str] = []

    # ── Warning 类型一：SC 13D/G 缺失 ──────────────────────────────────────
    # 所有 ticker 默认均包含 SC 13D/G 下载，故统一检查
    check_sc13 = forms_filter is None or bool(_SC13_FORMS & forms_filter)
    has_sc13 = any(form in _SC13_FORMS for form in counts)
    if check_sc13 and not has_sc13:
        warnings.append(
            "SC 13D/G 缺失：未在 issuer 的 submissions/browse-edgar 中发现 SC 13D/G；"
            "13D/G 往往由申报人提交，需要申报人 CIK 维度或反查补齐。"
        )

    # ── Warning 类型二：年报落盘数不足（10-K + 20-F 合并计算）──────────────
    check_annual = forms_filter is None or bool(_ANNUAL_FORMS & forms_filter)
    if check_annual:
        annual_count = sum(counts.get(f, 0) for f in _ANNUAL_FORMS)
        if annual_count == 0:
            # 全无年报：比"低于期望"更严重，单独细分
            warnings.append(
                "年报（10-K/20-F）全无落盘：未发现任何 10-K/10-K·A/20-F/20-F·A；"
                "请检查 SEC submissions 数据或确认该公司已在 SEC 登记。"
            )
        elif annual_count < _MIN_ANNUAL_COUNT:
            warnings.append(
                f"年报（10-K/20-F）有效落盘数 {annual_count} 低于期望 {_MIN_ANNUAL_COUNT}；"
                "请检查 SEC submissions 数据或适当扩大回溯窗口。"
            )

    # ── Warning 类型三：季报落盘数不足（10-Q + 6-K 合并计算）──────────────
    check_quarterly = forms_filter is None or bool(_QUARTERLY_FORMS & forms_filter)
    if check_quarterly:
        quarterly_count = sum(counts.get(f, 0) for f in _QUARTERLY_FORMS)
        if quarterly_count == 0:
            # 全无季报：比"低于期望"更严重，单独细分
            warnings.append(
                "季报（10-Q/6-K）全无落盘：未发现任何 10-Q/10-Q·A/6-K；"
                "该公司可能不报告季报，或需检查 SEC submissions 数据。"
            )
        elif quarterly_count < _MIN_QUARTERLY_COUNT:
            rejected_6k = _count_rejected_6k(rejection_registry)
            if rejected_6k > 0:
                warnings.append(
                    f"季报（10-Q/6-K）有效落盘数 {quarterly_count} 低于期望 {_MIN_QUARTERLY_COUNT}；"
                    f"另有 {rejected_6k} 份 6-K 被分类过滤（rejection_registry）。"
                    "请检查 _classify_6k_text 是否漏判季报。"
                )
            else:
                warnings.append(
                    f"季报（10-Q/6-K）有效落盘数 {quarterly_count} 低于期望 {_MIN_QUARTERLY_COUNT}；"
                    "请检查 SEC submissions 数据或适当扩大回溯窗口。"
                )

    # ── Warning 类型四：DEF 14A 落盘数不足（仅 10-K 公司适用）──────────────
    has_10k = sum(counts.get(f, 0) for f in {"10-K", "10-K/A"}) > 0
    check_def14a = forms_filter is None or "DEF 14A" in forms_filter
    if has_10k and check_def14a:
        def14a_count = counts.get("DEF 14A", 0)
        if def14a_count < _MIN_DEF14A_COUNT:
            warnings.append(
                f"DEF 14A 有效落盘数 {def14a_count} 低于期望 {_MIN_DEF14A_COUNT}；"
                "请检查 SEC submissions 数据或适当扩大回溯窗口。"
            )

    # ── Warning 类型五：强制 XBRL 缺失 ────────────────────────────────────
    # 10-K/10-Q 始终；20-F 自 _XBRL_20F_CUTOFF_DATE 起
    check_xbrl = forms_filter is None or bool(_XBRL_CHECKED_FORMS & forms_filter)
    if check_xbrl:
        xbrl_missing = _find_xbrl_missing_filings(ticker_dir, documents)
        if xbrl_missing:
            # 最多展示 5 个 doc_id，超出省略
            sample = xbrl_missing[:5]
            suffix = f" …（共 {len(xbrl_missing)} 份）" if len(xbrl_missing) > 5 else ""
            warnings.append(
                f"强制 XBRL 缺失：{len(xbrl_missing)} 份年报/季报无 XBRL instance 文件"
                f"（10-K/10-Q 始终；20-F 自 {_XBRL_20F_CUTOFF_DATE} 起）："
                f"{', '.join(sample)}{suffix}"
            )

    result["warnings"] = warnings
    return result


# ── 主流程 ────────────────────────────────────────────────────────────────────

def _iter_ticker_dirs(portfolio_dir: Path) -> list[Path]:
    """遍历 portfolio 目录，返回有效 ticker 子目录列表（排除隐藏目录）。

    Args:
        portfolio_dir: workspace/portfolio 路径。

    Returns:
        按字母排序的 ticker 目录列表。
    """
    dirs = sorted(
        d for d in portfolio_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )
    return dirs


def _render_report(
    results: list[dict[str, Any]],
    forms_filter: frozenset[str] | None = None,
) -> str:
    """将分析结果渲染为 Markdown 报告。

    Args:
        results: 所有 ticker 的分析结果列表。
        forms_filter: 需要检查的 form 集合；None 表示检查全部。

    Returns:
        Markdown 格式的报告字符串。
    """
    total = len(results)
    error_tickers = [r for r in results if r["errors"]]
    warn_tickers = [r for r in results if r["warnings"]]
    clean_count = total - len(error_tickers) - len(warn_tickers)

    # 各 warning 类型的不足 tickers（按 warn_tickers 顺序收集）
    annual_insuf_tickers = [r["ticker"] for r in warn_tickers if any("年报" in w for w in r["warnings"])]
    annual_zero_tickers = [r["ticker"] for r in warn_tickers if any("年报（10-K/20-F）全无落盘" in w for w in r["warnings"])]
    quarterly_insuf_tickers = [r["ticker"] for r in warn_tickers if any("季报" in w for w in r["warnings"])]
    quarterly_zero_tickers = [r["ticker"] for r in warn_tickers if any("季报（10-Q/6-K）全无落盘" in w for w in r["warnings"])]
    def14a_insuf_tickers = [r["ticker"] for r in warn_tickers if any("DEF 14A" in w for w in r["warnings"])]
    xbrl_missing_tickers = [r["ticker"] for r in warn_tickers if any("强制 XBRL 缺失" in w for w in r["warnings"])]

    lines: list[str] = []
    lines.append("# Download Warning 总览\n")
    lines.append(f"**扫描目录**：`workspace/portfolio/`  ")
    lines.append(f"**总 tickers**：{total}  ")
    lines.append(f"**有 warning**：{len(warn_tickers)}  ")
    lines.append(f"**有错误（配置缺失）**：{len(error_tickers)}  ")
    lines.append(f"**无问题**：{clean_count}\n")

    # Warning 类型分布
    lines.append("## Warning 类型统计\n")
    lines.append("| Warning 类型 | Ticker 数 | Tickers |")
    lines.append("|---|---|---|")
    sc13_missing_tickers = [r["ticker"] for r in warn_tickers if any("SC 13D/G" in w for w in r["warnings"])]
    if sc13_missing_tickers:
        lines.append(f"| SC 13D/G 缺失 | {len(sc13_missing_tickers)} | {', '.join(sc13_missing_tickers)} |")
    if annual_insuf_tickers:
        lines.append(f"| 年报（10-K/20-F）落盘数不足（期望≥{_MIN_ANNUAL_COUNT}）| {len(annual_insuf_tickers)} | {', '.join(annual_insuf_tickers)} |")
    if annual_zero_tickers:
        lines.append(f"| ↳ 年报全无（= 0）| {len(annual_zero_tickers)} | {', '.join(annual_zero_tickers)} |")
    if quarterly_insuf_tickers:
        lines.append(f"| 季报（10-Q/6-K）落盘数不足（期望≥{_MIN_QUARTERLY_COUNT}）| {len(quarterly_insuf_tickers)} | {', '.join(quarterly_insuf_tickers)} |")
    if quarterly_zero_tickers:
        lines.append(f"| ↳ 季报全无（= 0）| {len(quarterly_zero_tickers)} | {', '.join(quarterly_zero_tickers)} |")
    if def14a_insuf_tickers:
        lines.append(f"| DEF 14A 落盘数不足（期望≥{_MIN_DEF14A_COUNT}）| {len(def14a_insuf_tickers)} | {', '.join(def14a_insuf_tickers)} |")
    if xbrl_missing_tickers:
        lines.append(f"| 强制 XBRL 缺失（10-K/10-Q 始终；20-F 自 {_XBRL_20F_CUTOFF_DATE}）| {len(xbrl_missing_tickers)} | {', '.join(xbrl_missing_tickers)} |")
    lines.append("")

    # 配置错误 tickers
    if error_tickers:
        lines.append("## 配置错误（meta.json / filing_manifest.json 缺失）\n")
        for r in error_tickers:
            lines.append(f"- **{r['ticker']}**：{'; '.join(r['errors'])}")
        lines.append("")

    # 有 warning 的 tickers 明细
    if warn_tickers:
        lines.append("## Warning 明细\n")
        lines.append("| Ticker | 年报落盘 | 季报落盘 | Warning |")
        lines.append("|---|---|---|---|")
        for r in warn_tickers:
            ticker = r["ticker"]
            counts = r["counts_by_form"]
            # 年报：展示各 form 明细（只展示有数量的）
            annual_parts = [f"{f}:{counts[f]}" for f in sorted(_ANNUAL_FORMS) if counts.get(f, 0) > 0]
            annual_str = ", ".join(annual_parts) if annual_parts else "0"
            # 季报：展示各 form 明细
            quarterly_parts = [f"{f}:{counts[f]}" for f in sorted(_QUARTERLY_FORMS) if counts.get(f, 0) > 0]
            quarterly_str = ", ".join(quarterly_parts) if quarterly_parts else "0"
            for w in r["warnings"]:
                short_w = w[:120] + "…" if len(w) > 120 else w
                lines.append(f"| {ticker} | {annual_str} | {quarterly_str} | {short_w} |")
        lines.append("")

    # 附录：无 warning 的 tickers（可折叠）
    clean_tickers = [r["ticker"] for r in results if not r["warnings"] and not r["errors"]]
    if clean_tickers:
        lines.append("<details>")
        lines.append(f"<summary>无 warning 的 {len(clean_tickers)} 个 tickers</summary>\n")
        # 每行8个
        for i in range(0, len(clean_tickers), 8):
            lines.append("  " + "  ".join(clean_tickers[i:i+8]))
        lines.append("</details>")

    return "\n".join(lines) + "\n"


def main() -> None:
    """脚本入口：扫描 portfolio 目录并输出 warning 报告。"""
    import argparse

    parser = argparse.ArgumentParser(description="分析 workspace/portfolio 下所有 tickers 的下载 warning。")
    parser.add_argument(
        "--forms",
        type=str,
        default=None,
        metavar="FORMS",
        help="只检查指定 forms（逗号分隔，如 '10-K,10-Q,SC 13G'）；不指定则检查全部。",
    )
    args = parser.parse_args()

    forms_filter: frozenset[str] | None = None
    if args.forms:
        forms_filter = frozenset(f.strip() for f in args.forms.split(",") if f.strip())

    script_dir = Path(__file__).resolve().parent
    portfolio_dir = script_dir.parent / "workspace" / "portfolio"

    if not portfolio_dir.is_dir():
        print(f"错误：portfolio 目录不存在：{portfolio_dir}", file=sys.stderr)
        sys.exit(1)

    ticker_dirs = _iter_ticker_dirs(portfolio_dir)
    if not ticker_dirs:
        print("未找到任何 ticker 目录。", file=sys.stderr)
        sys.exit(1)

    print(f"正在分析 {len(ticker_dirs)} 个 tickers…", file=sys.stderr)

    results = [_analyze_ticker(d, forms_filter=forms_filter) for d in ticker_dirs]

    report = _render_report(results, forms_filter=forms_filter)
    print(report)


if __name__ == "__main__":
    main()
