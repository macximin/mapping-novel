from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kiss_refresh_history import now_iso, record_refresh_run
from kiss_payment_settlement import import_payment_settlement_export, save_summary


def main() -> None:
    args = parse_args()
    started_at = now_iso()
    try:
        result = import_payment_settlement_export(
            args.input,
            cache_path=args.cache,
            s2_lookup_path=args.s2_lookup,
        )
        save_summary(args.summary, result)
        history_id = record_refresh_run(
            args.history_db,
            started_at=started_at,
            finished_at=now_iso(),
            status="success",
            source="manual_upload",
            mode="manual_upload",
            source_rows=result.source_rows,
            cache_rows_before=result.cache_rows_before,
            cache_rows_after=result.cache_rows_after,
            s2_lookup_rows=result.s2_lookup_rows,
            sales_channel_content_id_unique=result.summary.get("sales_channel_content_id_unique"),
            content_id_unique=result.summary.get("content_id_unique"),
            summary_json_path=args.summary,
            cache_path=result.output_cache,
            s2_lookup_path=result.output_s2_lookup,
            source_file_path=args.input,
            script=Path(__file__).name,
        )
        print(f"source_rows={result.source_rows}")
        print(f"cache_rows_before={result.cache_rows_before}")
        print(f"cache_rows_after={result.cache_rows_after}")
        print(f"s2_lookup_rows={result.s2_lookup_rows}")
        print(f"cache={result.output_cache}")
        print(f"s2_lookup={result.output_s2_lookup}")
        print(f"summary={args.summary}")
        print(f"history_db={args.history_db}")
        print(f"history_id={history_id}")
    except Exception as exc:
        record_refresh_run(
            args.history_db,
            started_at=started_at,
            finished_at=now_iso(),
            status="failed",
            source="manual_upload",
            mode="manual_upload",
            summary_json_path=args.summary,
            cache_path=args.cache,
            s2_lookup_path=args.s2_lookup,
            source_file_path=args.input,
            error_message=exc,
            script=Path(__file__).name,
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import KISS 지급 정산 관리 목록 export into a local cache.")
    parser.add_argument("input", help="Downloaded KISS 지급_정산_관리_목록 xlsx")
    parser.add_argument("--cache", default=str(ROOT / "data" / "kiss_payment_settlement_cache.csv"))
    parser.add_argument("--s2-lookup", default=str(ROOT / "data" / "kiss_payment_settlement_s2_lookup.csv"))
    parser.add_argument("--summary", default=str(ROOT / "doc" / "2026-05-07" / "kiss_payment_settlement_refresh_summary.json"))
    parser.add_argument("--history-db", default=str(ROOT / "data" / "kiss_refresh_history.sqlite"))
    return parser.parse_args()


if __name__ == "__main__":
    main()
