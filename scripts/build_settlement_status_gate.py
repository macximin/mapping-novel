from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from settlement_status_gate import (  # noqa: E402
    COL_CONTENT_SHAPE,
    COL_DEPARTMENT,
    COL_STATUS,
    DEFAULT_CORE_NOVEL_DEPARTMENTS,
    build_status_table,
    summary_dict,
)

DEFAULT_JUDGEMENT_ROOT = ROOT / "판매채널-지급정산 불일치" / "05_불일치_판정"
DEFAULT_IPS = ROOT / "data" / "kidari_contents.xlsx"
DEFAULT_OUTPUT = ROOT / "data" / "settlement_status_gate_latest.csv"
DEFAULT_SUMMARY = ROOT / "data" / "settlement_status_gate_latest_summary.json"


def main() -> None:
    args = parse_args()
    judgement_path = Path(args.judgement_csv) if args.judgement_csv else latest_judgement_csv(Path(args.judgement_root))
    ips_path = Path(args.ips) if args.ips else None
    output_path = Path(args.output)
    summary_path = Path(args.summary)
    as_of = args.as_of or infer_as_of(judgement_path) or date.today().isoformat()

    judgement = pd.read_csv(judgement_path, dtype=object)
    ips = pd.read_excel(ips_path, dtype=object) if ips_path and ips_path.exists() else None

    full_status = build_status_table(judgement, ips, as_of=as_of)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    full_status.to_csv(output_path, index=False, encoding="utf-8-sig")

    core_status = build_status_table(
        judgement,
        ips,
        departments=DEFAULT_CORE_NOVEL_DEPARTMENTS,
        content_shape="소설",
        as_of=as_of,
    )

    summary: dict[str, Any] = {
        "as_of": as_of,
        "judgement_csv": str(judgement_path),
        "ips": str(ips_path) if ips_path else "",
        "output": str(output_path),
        "overall": summary_dict(full_status),
        "core_novel_departments": {
            "departments": list(DEFAULT_CORE_NOVEL_DEPARTMENTS),
            "content_shape": "소설",
            **summary_dict(core_status),
            "status_by_department": status_by_department(core_status, departments=DEFAULT_CORE_NOVEL_DEPARTMENTS),
        },
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.core_output_dir:
        core_dir = Path(args.core_output_dir)
        core_dir.mkdir(parents=True, exist_ok=True)
        hold = core_status[core_status[COL_STATUS].str.startswith("HOLD_", na=False)].copy()
        mixed = core_status[core_status[COL_STATUS].eq("HOLD_MIXED_CONTENT_RISK")].copy()
        hold.to_csv(core_dir / "novel_core_dept_scope_ab_checklist.csv", index=False, encoding="utf-8-sig")
        mixed.to_csv(core_dir / "novel_core_dept_scope_mixed.csv", index=False, encoding="utf-8-sig")
        (core_dir / "novel_core_dept_scope_summary.json").write_text(
            json.dumps(summary["core_novel_departments"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    print(json.dumps(summary, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the dynamic settlement-status gate table.")
    parser.add_argument("--judgement-csv", default="")
    parser.add_argument("--judgement-root", default=str(DEFAULT_JUDGEMENT_ROOT))
    parser.add_argument("--ips", default=str(DEFAULT_IPS))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--summary", default=str(DEFAULT_SUMMARY))
    parser.add_argument("--as-of", default="")
    parser.add_argument("--core-output-dir", default="")
    return parser.parse_args()


def latest_judgement_csv(root: Path) -> Path:
    candidates = sorted(root.glob("*/판매채널콘텐츠_vs_지급정산관리_전체판정.csv"))
    if not candidates:
        raise SystemExit(f"판정 CSV를 찾을 수 없습니다: {root}")
    return max(candidates, key=lambda path: (path.parent.name, path.stat().st_mtime))


def infer_as_of(path: Path) -> str:
    parent = path.parent.name
    if len(parent) == 8 and parent.isdigit():
        return f"{parent[:4]}-{parent[4:6]}-{parent[6:]}"
    return ""


def status_by_department(frame: pd.DataFrame, departments: tuple[str, ...] | None = None) -> dict[str, dict[str, int]]:
    if frame.empty or COL_DEPARTMENT not in frame.columns:
        return {department: {} for department in departments or ()}
    statuses = sorted(str(status) for status in frame[COL_STATUS].dropna().unique())
    grouped = (
        frame.pivot_table(index=COL_DEPARTMENT, columns=COL_STATUS, values=COL_CONTENT_SHAPE, aggfunc="count", fill_value=0)
        .astype(int)
        .sort_index()
    )
    result = {
        str(department): {str(status): int(count) for status, count in row.items()}
        for department, row in grouped.to_dict(orient="index").items()
    }
    for department, counts in result.items():
        for status in statuses:
            counts.setdefault(status, 0)
    for department in departments or ():
        result.setdefault(department, {status: 0 for status in statuses})
    return result


if __name__ == "__main__":
    main()
