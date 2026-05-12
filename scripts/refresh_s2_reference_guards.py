from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from s2_reference_guards import (
    DEFAULT_BILLING_LOOKUP,
    DEFAULT_MISSING_LOOKUP,
    normalize_billing_rows,
    normalize_missing_rows,
    write_billing_lookup,
    write_missing_lookup,
)
from scripts.refresh_kiss_payment_settlement import create_authenticated_session, load_env


FULL_REPLACE_START_DATE = date(1900, 1, 1)
DEFAULT_PAGE_SIZE = 1_000_000
DEFAULT_CONTENT_STYLE_CODE = "102"
DEFAULT_S2_LOOKUP = ROOT / "data" / "kiss_payment_settlement_s2_lookup.csv"


def main() -> None:
    args = parse_args()
    today = date.fromisoformat(args.today) if args.today else date.today()
    load_env(Path(args.env_file))
    session = create_authenticated_session()
    try:
        missing_rows = fetch_missing_rows(
            session,
            content_style_code=args.content_style_code,
            platform_code=args.platform_code,
            page_size=args.page_size,
        )
        missing_comparisons = fetch_missing_platform_comparisons(
            session,
            base_rows=missing_rows,
            platform_codes=args.compare_platform_code,
            content_style_code=args.content_style_code,
            page_size=args.page_size,
        )
        billing_rows, billing_total, billing_pages = fetch_billing_rows(
            session,
            end_date=today,
            page_size=args.page_size,
        )
    finally:
        session.close()

    missing = normalize_missing_rows(missing_rows)
    billing = normalize_billing_rows(billing_rows)
    missing_path = write_missing_lookup(missing, args.missing_lookup)
    billing_path = write_billing_lookup(billing, args.billing_lookup)
    summary_path = Path(args.summary) if args.summary else ROOT / "doc" / today.isoformat() / "s2_reference_guards_refresh_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "created_at": today.isoformat(),
        "content_style_code": args.content_style_code,
        "missing_platform_code": args.platform_code,
        "missing_rows_raw": len(missing_rows),
        "missing_lookup_rows": len(missing),
        "missing_sales_channel_counts": value_counts(missing, "판매채널명"),
        "missing_content_shape_counts": value_counts(missing, "콘텐츠형태"),
        "missing_settlement_start_counts": value_counts(missing, "정산시작여부"),
        "missing_overlap_with_s2_lookup": missing_overlap_count(missing, Path(args.s2_lookup)),
        "missing_platform_comparisons": missing_comparisons,
        "billing_api_total_rows": billing_total,
        "billing_rows_raw": len(billing_rows),
        "billing_pages": billing_pages,
        "billing_lookup_rows": len(billing),
        "billing_sales_channel_counts": value_counts(billing, "판매채널명"),
        "missing_lookup": str(missing_path),
        "billing_lookup": str(billing_path),
        "s2_lookup": str(args.s2_lookup),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"content_style_code={args.content_style_code}")
    print(f"missing_platform_code={args.platform_code or '<blank>'}")
    print(f"missing_rows_raw={len(missing_rows)}")
    print(f"missing_lookup_rows={len(missing)}")
    print(f"missing_overlap_with_s2_lookup={summary['missing_overlap_with_s2_lookup']}")
    print(f"missing_platform_comparisons={len(missing_comparisons)}")
    print(f"billing_api_total_rows={billing_total}")
    print(f"billing_rows_raw={len(billing_rows)}")
    print(f"billing_pages={billing_pages}")
    print(f"billing_lookup_rows={len(billing)}")
    print(f"missing_lookup={missing_path}")
    print(f"billing_lookup={billing_path}")
    print(f"summary={summary_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh S2 missing-settlement and billing-settlement guard lookups.")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--today", default="")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--content-style-code", default=DEFAULT_CONTENT_STYLE_CODE)
    parser.add_argument("--platform-code", default="", help="S2 plfmCd. Blank fetches all platforms when the API supports it.")
    parser.add_argument(
        "--compare-platform-code",
        action="append",
        default=[],
        help="Fetch an additional S2 plfmCd for audit only. Can be passed multiple times.",
    )
    parser.add_argument("--missing-lookup", default=str(DEFAULT_MISSING_LOOKUP))
    parser.add_argument("--billing-lookup", default=str(DEFAULT_BILLING_LOOKUP))
    parser.add_argument("--s2-lookup", default=str(DEFAULT_S2_LOOKUP))
    parser.add_argument("--summary", default="")
    return parser.parse_args()


def fetch_missing_rows(
    session: requests.Session,
    *,
    content_style_code: str,
    platform_code: str,
    page_size: int,
) -> list[dict[str, Any]]:
    base = session.headers["X-KISS-API-BASE-URL"]
    response = session.get(
        f"{base}/stm/stm",
        params={
            "plfmCd": platform_code,
            "ctnsStleCd": content_style_code,
            "pageNum": 1,
            "pageSize": page_size,
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else payload
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("list"), list):
        return data["list"]
    raise RuntimeError(f"Unexpected missing-settlement response: {type(data).__name__}")


def fetch_missing_platform_comparisons(
    session: requests.Session,
    *,
    base_rows: list[dict[str, Any]],
    platform_codes: list[str],
    content_style_code: str,
    page_size: int,
) -> list[dict[str, Any]]:
    base = normalize_missing_rows(base_rows)
    base_ids = set(base["판매채널콘텐츠ID"].map(str))
    comparisons: list[dict[str, Any]] = []
    seen: set[str] = set()
    for platform_code in platform_codes:
        platform_code = str(platform_code).strip()
        if not platform_code or platform_code in seen:
            continue
        seen.add(platform_code)
        rows = fetch_missing_rows(
            session,
            content_style_code=content_style_code,
            platform_code=platform_code,
            page_size=page_size,
        )
        frame = normalize_missing_rows(rows)
        ids = set(frame["판매채널콘텐츠ID"].map(str))
        comparisons.append(
            {
                "platform_code": platform_code,
                "raw_rows": len(rows),
                "lookup_rows": len(frame),
                "same_ids_as_base": ids == base_ids,
                "ids_only_in_base": len(base_ids - ids),
                "ids_only_in_platform": len(ids - base_ids),
                "sales_channel_counts": value_counts(frame, "판매채널명"),
                "content_shape_counts": value_counts(frame, "콘텐츠형태"),
                "settlement_start_counts": value_counts(frame, "정산시작여부"),
            }
        )
    return comparisons


def value_counts(frame: pd.DataFrame, column: str, *, limit: int = 20) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    counter = Counter(str(value).strip() for value in frame[column].fillna("") if str(value).strip())
    return {key: int(value) for key, value in counter.most_common(limit)}


def missing_overlap_count(missing: pd.DataFrame, s2_lookup_path: Path) -> int:
    if missing.empty or "판매채널콘텐츠ID" not in missing.columns or not s2_lookup_path.exists():
        return 0
    s2 = pd.read_csv(s2_lookup_path, dtype=object)
    if "판매채널콘텐츠ID" not in s2.columns:
        return 0
    missing_ids = set(missing["판매채널콘텐츠ID"].fillna("").map(str))
    s2_ids = set(s2["판매채널콘텐츠ID"].fillna("").map(str))
    return len({value for value in missing_ids & s2_ids if value})


def fetch_billing_rows(
    session: requests.Session,
    *,
    end_date: date,
    page_size: int,
) -> tuple[list[dict[str, Any]], int, int]:
    rows: list[dict[str, Any]] = []
    total_rows = 0
    page_num = 1
    fetched_pages = 0
    while True:
        total_rows, page_rows = fetch_billing_page(session, end_date=end_date, page_num=page_num, page_size=page_size)
        rows.extend(page_rows)
        fetched_pages += 1
        print(f"[billing page {page_num}] fetched={len(page_rows)} total_accumulated={len(rows)} / total={total_rows}")
        if not page_rows or len(rows) >= total_rows:
            break
        page_num += 1
    return rows, total_rows, fetched_pages


def fetch_billing_page(
    session: requests.Session,
    *,
    end_date: date,
    page_num: int,
    page_size: int,
) -> tuple[int, list[dict[str, Any]]]:
    base = session.headers["X-KISS-API-BASE-URL"]
    response = session.get(
        f"{base}/mst/setl/req-setl",
        params={
            "bgnDt": FULL_REPLACE_START_DATE.isoformat(),
            "endDt": end_date.isoformat(),
            "reqSetlId": "",
            "schnSeCd": "",
            "uperSchnCd": "",
            "schnId": "",
            "bcncNm": "",
            "reqSetlStsCd": "",
            "cnfmStsCd": "",
            "chgerNm": "",
            "chrgDeptNm": "",
            "reprsntCtnsNm": "",
            "pageNum": page_num,
            "pageSize": page_size,
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(data, dict) or not isinstance(data.get("list"), list) or data.get("total") is None:
        raise RuntimeError(f"Unexpected billing-settlement response: {type(data).__name__}")
    return int(data["total"]), data["list"]


if __name__ == "__main__":
    main()
