from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mapping_core import build_mapping
from settlement_adapters import REGISTRY, detect_platform, normalize_settlement, summarize_normalization


DEFAULT_SOURCE_ROOT = Path(r"\\172.16.10.120\소설사업부\판무팀_ssot\100_계산서_매출등록_자료")


def main() -> None:
    args = parse_args()
    root = Path(args.root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = discover_files(root, args.period)
    file_rows: list[dict[str, Any]] = []
    platform_feeds: dict[str, list[pd.DataFrame]] = defaultdict(list)

    for path in files:
        rel = str(path.relative_to(root))
        platform = detect_platform(rel) or Path(rel).parts[0]
        record = {
            "platform": platform,
            "relative_path": rel,
            "period": args.period,
            "parse_status": "",
            "error": "",
        }
        try:
            result = normalize_settlement(path, platform=platform, source_name=rel)
            summary = summarize_normalization(result)
            record.update(
                {
                    "file_status": summary["file_status"],
                    "parser_contract": summary["parser_contract"],
                    "parsed_rows": summary["parsed_rows"],
                    "title_present_rows": summary["title_present_rows"],
                    "default_feed_rows": summary["default_feed_rows"],
                    "amount_rule_status": summary["amount_rule_status"],
                    "s2_amount_policy_locked": summary["s2_amount_policy_locked"],
                    "s2_gate": summary["s2_gate"],
                    "sheet_audits": " ; ".join(
                        f"{audit.sheet}:{audit.status}:h{audit.header_row}:r{audit.parsed_rows}"
                        for audit in result.sheet_audits
                    ),
                    "parse_status": "parsed",
                }
            )
            if not result.default_feed_rows.empty:
                platform_feeds[platform].append(result.default_feed_rows)
        except Exception as exc:
            spec = REGISTRY.get(platform)
            record.update(
                {
                    "file_status": "",
                    "parser_contract": spec.parser_contract if spec else "",
                    "parsed_rows": 0,
                    "title_present_rows": 0,
                    "default_feed_rows": 0,
                    "amount_rule_status": spec.amount_rule_status if spec else "",
                    "s2_amount_policy_locked": False,
                    "s2_gate": spec.s2_gate if spec else "",
                    "sheet_audits": "",
                    "parse_status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        file_rows.append(record)

    platform_rows = build_platform_rows(file_rows, platform_feeds, args.mapping_sample_rows)
    amount_rows = build_amount_rows(platform_rows)
    summary = build_summary(args.period, file_rows, platform_rows, amount_rows)

    prefix = args.output_prefix or f"{args.period.replace('-', '_')}_adapter_materialization"
    write_csv(out_dir / f"{prefix}_files.csv", file_rows)
    write_csv(out_dir / f"{prefix}_platforms.csv", platform_rows)
    write_csv(out_dir / f"{prefix}_amount_policy_audit.csv", amount_rows)
    (out_dir / f"{prefix}_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_dir / f"{prefix}_report.md").write_text(
        render_report(args.period, summary, platform_rows, amount_rows),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit settlement adapter materialization for a period.")
    parser.add_argument("--root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--period", required=True, help="YYYY-MM")
    parser.add_argument("--out-dir", default="doc/2026-05-07")
    parser.add_argument("--output-prefix", default="")
    parser.add_argument("--mapping-sample-rows", type=int, default=1000)
    return parser.parse_args()


def discover_files(root: Path, period: str) -> list[Path]:
    result: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".xlsx", ".xlsm"}:
            continue
        if path.name.startswith("~$") or "정산상세" not in path.name:
            continue
        if path_period(path, root) == period:
            result.append(path)
    return sorted(result, key=lambda p: str(p))


def path_period(path: Path, root: Path) -> str:
    rel = str(path.relative_to(root))
    normalized = rel.replace(" ", "")
    match = re.search(r"(20\d{2})년(\d{1,2})월", normalized)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    match = re.search(r"(20\d{2})[-_.]?(\d{2})", normalized)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    month_match = re.search(r"\\(\d{1,2})월\\", rel)
    if month_match:
        year = "2025" if "25년" in rel else "2026"
        return f"{year}-{int(month_match.group(1)):02d}"
    return ""


def build_platform_rows(
    file_rows: list[dict[str, Any]],
    platform_feeds: dict[str, list[pd.DataFrame]],
    sample_rows: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in file_rows:
        grouped[row["platform"]].append(row)

    platform_rows: list[dict[str, Any]] = []
    for platform in sorted(REGISTRY):
        spec = REGISTRY[platform]
        rows = grouped.get(platform, [])
        parsed_rows = sum(int(row.get("parsed_rows") or 0) for row in rows)
        default_rows = sum(int(row.get("default_feed_rows") or 0) for row in rows)
        errors = [row["error"] for row in rows if row.get("parse_status") == "error"]

        if not rows:
            materialization_status = "no_period_source"
        elif errors and default_rows == 0:
            materialization_status = "parse_failed"
        elif spec.blocks_default_feed:
            materialization_status = "materialized_but_blocked_from_default_feed"
        elif default_rows > 0:
            materialization_status = "materialized_default_feed"
        else:
            materialization_status = "parsed_no_default_feed"

        smoke = mapping_smoke(platform_feeds.get(platform, []), sample_rows)
        platform_rows.append(
            {
                "platform": platform,
                "period_file_count": len(rows),
                "parsed_rows_all_files": parsed_rows,
                "default_feed_rows": default_rows,
                "materialization_status": materialization_status,
                "mapping_engine_smoke_status": smoke["status"],
                "mapping_smoke_rows": smoke["rows"],
                "mapping_smoke_matched": smoke["matched"],
                "mapping_smoke_review_rows": smoke["review_rows"],
                "clean_title_blank_in_smoke": smoke["blank_keys"],
                "amount_rule_status": spec.amount_rule_status,
                "s2_amount_policy_locked": spec.s2_amount_policy_locked,
                "s2_projection_status": s2_projection_status(spec),
                "parser_contract": spec.parser_contract,
                "error": " | ".join(errors[:3]),
            }
        )
    return platform_rows


def mapping_smoke(frames: list[pd.DataFrame], sample_rows: int) -> dict[str, Any]:
    if not frames:
        return {"status": "skipped_no_default_feed", "rows": 0, "matched": 0, "review_rows": 0, "blank_keys": 0}
    feed = pd.concat(frames, ignore_index=True)
    if feed.empty:
        return {"status": "skipped_no_default_feed", "rows": 0, "matched": 0, "review_rows": 0, "blank_keys": 0}

    sample = feed.head(sample_rows).copy()
    titles = sample["상품명"].fillna("").astype(str)
    unique_titles = pd.Series(titles.drop_duplicates().tolist())
    s2 = pd.DataFrame(
        {
            "콘텐츠명": unique_titles,
            "판매채널콘텐츠ID": [f"SYN-{idx + 1}" for idx in range(len(unique_titles))],
        }
    )
    master = pd.DataFrame(
        {
            "콘텐츠명": unique_titles.map(lambda value: f"<{value}>_SMOKE_0_0_확정"),
            "콘텐츠ID": [f"CID-{idx + 1}" for idx in range(len(unique_titles))],
        }
    )
    result = build_mapping(s2, sample.rename(columns={"상품명": "작품명"}), master)
    summary = dict(zip(result.summary["항목"], result.summary["값"]))
    review_rows = int(summary.get("검토필요 행 수", 0))
    return {
        "status": "pass_after_substitution" if review_rows == 0 else "pass_with_review",
        "rows": len(sample),
        "matched": int(summary.get("S2 matched", 0)),
        "review_rows": review_rows,
        "blank_keys": int(summary.get("정산서 빈 정제키 행 수", 0)),
    }


def build_amount_rows(platform_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    amount_rows: list[dict[str, Any]] = []
    for row in platform_rows:
        spec = REGISTRY[row["platform"]]
        amount_rows.append(
            {
                "platform": spec.platform,
                "period_file_count": row["period_file_count"],
                "default_feed_rows": row["default_feed_rows"],
                "amount_rule_status": spec.amount_rule_status,
                "s2_amount_policy_locked": spec.s2_amount_policy_locked,
                "s2_projection_status": row["s2_projection_status"],
                "sale_amount_from": spec.sale_amount_from,
                "settlement_base_amount_from": spec.settlement_amount_from,
                "offset_amount_from": spec.offset_amount_from,
                "s2_gate": spec.s2_gate,
            }
        )
    return amount_rows


def s2_projection_status(spec) -> str:
    if spec.blocks_default_feed:
        return "s2_projection_blocked_non_s2_source"
    if spec.s2_amount_policy_locked:
        return "s2_projection_candidate_after_policy"
    return "s2_projection_blocked_by_amount_or_policy"


def build_summary(
    period: str,
    file_rows: list[dict[str, Any]],
    platform_rows: list[dict[str, Any]],
    amount_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "period": period,
        "platform_total": len(REGISTRY),
        "period_file_count": len(file_rows),
        "platforms_with_period_source": sum(1 for row in platform_rows if int(row["period_file_count"]) > 0),
        "platform_materialization_status_counts": counts(platform_rows, "materialization_status"),
        "mapping_engine_smoke_status_counts": counts(platform_rows, "mapping_engine_smoke_status"),
        "s2_projection_status_counts": counts(amount_rows, "s2_projection_status"),
        "parse_error_count": sum(1 for row in file_rows if row.get("parse_status") == "error"),
    }


def counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, ""))
        result[value] = result.get(value, 0) + 1
    return dict(sorted(result.items()))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def render_report(
    period: str,
    summary: dict[str, Any],
    platform_rows: list[dict[str, Any]],
    amount_rows: list[dict[str, Any]],
) -> str:
    lines = [
        f"# {period} adapter materialization audit",
        "",
        "## Summary",
        f"- Platform total: {summary['platform_total']}",
        f"- Platforms with period source: {summary['platforms_with_period_source']}",
        f"- Period files tested: {summary['period_file_count']}",
        f"- Parse errors: {summary['parse_error_count']}",
        f"- Materialization statuses: `{summary['platform_materialization_status_counts']}`",
        f"- Mapping smoke statuses: `{summary['mapping_engine_smoke_status_counts']}`",
        f"- S2 projection statuses: `{summary['s2_projection_status_counts']}`",
        "",
        "## Platform Results",
        "| platform | files | default_feed_rows | materialization | mapping_smoke | amount_rule | s2_projection |",
        "|---|---:|---:|---|---|---|---|",
    ]
    for row in platform_rows:
        lines.append(
            f"| {row['platform']} | {row['period_file_count']} | {row['default_feed_rows']} | "
            f"{row['materialization_status']} | {row['mapping_engine_smoke_status']} | "
            f"{row['amount_rule_status']} | {row['s2_projection_status']} |"
        )

    locked = [row for row in amount_rows if row["s2_amount_policy_locked"]]
    blocked = [row for row in amount_rows if not row["s2_amount_policy_locked"]]
    lines.extend(
        [
            "",
            "## Amount Policy Gate",
            f"- Candidate after policy lock: {len(locked)}",
            f"- Still blocked or non-S2: {len(blocked)}",
            "- This audit does not emit final S2 4-column upload rows for blocked amount policies.",
            "",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    main()
