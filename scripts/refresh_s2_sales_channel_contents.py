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

from cleaning_rules import text
from matching_rules import PLATFORM_EXACT_CHANNELS, SPECIAL_EXACT_CHANNELS
from s2_reference_guards import (
    DEFAULT_SERVICE_CONTENT_LOOKUP,
    normalize_service_content_rows,
    write_service_content_lookup,
)
from scripts.refresh_kiss_payment_settlement import create_authenticated_session, load_env


def main() -> None:
    args = parse_args()
    today = date.fromisoformat(args.today) if args.today else date.today()
    load_env(Path(args.env_file))
    session = create_authenticated_session()
    try:
        channel_catalog = fetch_channel_catalog(session)
        targets, target_audit = build_targets(channel_catalog)
        rows, fetch_audit = fetch_all_service_contents(
            session,
            targets,
            content_style_code=args.content_style_code,
        )
    finally:
        session.close()

    lookup = normalize_service_content_rows(rows)
    lookup_path = write_service_content_lookup(lookup, args.output)

    summary_path = Path(args.summary) if args.summary else ROOT / "doc" / today.isoformat() / "s2_sales_channel_contents_refresh_summary.json"
    audit_path = Path(args.audit) if args.audit else ROOT / "doc" / today.isoformat() / "s2_sales_channel_contents_refresh_audit.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    audit_frame = pd.DataFrame([*target_audit, *fetch_audit])
    audit_frame.to_csv(audit_path, index=False, encoding="utf-8-sig")
    summary = {
        "created_at": today.isoformat(),
        "content_style_code": args.content_style_code,
        "target_channels": len(targets),
        "target_missing_channels": sum(1 for row in target_audit if row["상태"] == "missing_channel_catalog"),
        "fetch_failures": sum(1 for row in fetch_audit if row["상태"] == "failed"),
        "raw_rows": len(rows),
        "lookup_rows": len(lookup),
        "sales_channel_counts": value_counts(lookup, "판매채널명"),
        "content_shape_counts": value_counts(lookup, "콘텐츠형태"),
        "output": str(lookup_path),
        "audit": str(audit_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"target_channels={len(targets)}")
    print(f"target_missing_channels={summary['target_missing_channels']}")
    print(f"fetch_failures={summary['fetch_failures']}")
    print(f"raw_rows={len(rows)}")
    print(f"lookup_rows={len(lookup)}")
    print(f"output={lookup_path}")
    print(f"audit={audit_path}")
    print(f"summary={summary_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh S2 sales-channel content lookup for no-match evidence only.")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--today", default="")
    parser.add_argument("--content-style-code", default="102")
    parser.add_argument("--output", default=str(DEFAULT_SERVICE_CONTENT_LOOKUP))
    parser.add_argument("--summary", default="")
    parser.add_argument("--audit", default="")
    return parser.parse_args()


def fetch_channel_catalog(session: requests.Session) -> pd.DataFrame:
    base = session.headers["X-KISS-API-BASE-URL"]
    response = session.get(f"{base}/ssmgmt/cmm/schn", timeout=60)
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected channel catalog response: {type(rows).__name__}")
    return pd.DataFrame(rows)


def build_targets(channel_catalog: pd.DataFrame) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    rules = {**PLATFORM_EXACT_CHANNELS, **SPECIAL_EXACT_CHANNELS}
    catalog = channel_catalog.copy()
    if "schnNm" not in catalog.columns:
        raise RuntimeError("S2 channel catalog missing schnNm")
    catalog["_schnNm"] = catalog["schnNm"].map(text)

    targets: dict[tuple[str, str, str], dict[str, str]] = {}
    audit: list[dict[str, Any]] = []
    for platform, channels in sorted(rules.items()):
        for channel in channels:
            matched = catalog[catalog["_schnNm"].eq(text(channel))]
            if matched.empty:
                audit.append({"단계": "target", "플랫폼": platform, "판매채널명": channel, "상태": "missing_channel_catalog"})
                continue
            for _, row in matched.iterrows():
                target = {
                    "platform": text(platform),
                    "schn_id": id_text(row.get("schnId")),
                    "schn_nm": text(row.get("schnNm")),
                    "bcnc_cd": text(row.get("bcncCd")),
                    "bcnc_nm": text(row.get("bcncNm")),
                    "ctns_stle_cd_nm": text(row.get("ctnsStleCdNm")),
                }
                key = (target["platform"], target["schn_id"], target["bcnc_cd"])
                targets[key] = target
                audit.append({"단계": "target", "플랫폼": platform, "판매채널명": channel, "상태": "targeted"})
    return sorted(targets.values(), key=lambda row: (row["platform"], row["schn_nm"], row["schn_id"])), audit


def fetch_all_service_contents(
    session: requests.Session,
    targets: list[dict[str, str]],
    *,
    content_style_code: str = "102",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for idx, target in enumerate(targets, start=1):
        try:
            fetched = fetch_service_contents(session, target, content_style_code=content_style_code)
            status = "success"
            error = ""
        except Exception as exc:
            fetched = []
            status = "failed"
            error = str(exc)
        for row in fetched:
            copied = dict(row)
            copied.update(
                {
                    "platform": target["platform"],
                    "schnId": target["schn_id"],
                    "schnNm": target["schn_nm"],
                    "bcncCd": target["bcnc_cd"],
                    "bcncNm": target["bcnc_nm"],
                    "ctnsStleCdNm": text(row.get("ctnsStleCdNm")) or target["ctns_stle_cd_nm"],
                }
            )
            rows.append(copied)
        audit.append(
            {
                "단계": "fetch",
                "순번": idx,
                "플랫폼": target["platform"],
                "판매채널명": target["schn_nm"],
                "판매채널ID": target["schn_id"],
                "거래처코드": target["bcnc_cd"],
                "상태": status,
                "행수": len(fetched),
                "오류": error,
            }
        )
        print(f"[{idx}/{len(targets)}] {target['platform']} / {target['schn_nm']}: {status} rows={len(fetched)}")
    return rows, audit


def fetch_service_contents(
    session: requests.Session,
    target: dict[str, str],
    *,
    content_style_code: str = "102",
) -> list[dict[str, Any]]:
    if not target["bcnc_cd"] or not target["schn_id"]:
        raise RuntimeError("bcncCd/schnId is required")
    base = session.headers["X-KISS-API-BASE-URL"]
    response = session.get(
        f"{base}/sale/ext/ext-salm/schn-ctns",
        params={
            "bcncCd": target["bcnc_cd"],
            "schnIds": target["schn_id"],
            "ctnsStleCd": content_style_code,
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected sales-channel content response: {type(rows).__name__}")
    return rows


def value_counts(frame: pd.DataFrame, column: str, *, limit: int = 20) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    counter = Counter(str(value).strip() for value in frame[column].fillna("") if str(value).strip())
    return {key: int(value) for key, value in counter.most_common(limit)}


def id_text(value: Any) -> str:
    raw = text(value)
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


if __name__ == "__main__":
    main()
