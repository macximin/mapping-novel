from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kiss_refresh_history import latest_refresh_runs, record_refresh_run


class KissRefreshHistoryTest(unittest.TestCase):
    def test_records_success_and_failure_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "history.sqlite"

            success_id = record_refresh_run(
                db_path,
                started_at="2026-05-07T10:00:00+09:00",
                finished_at="2026-05-07T10:01:00+09:00",
                status="success",
                source="kiss_api",
                mode="rolling-3m",
                search_start_date="2026-02-07",
                search_end_date="2026-05-07",
                api_total_rows=7536,
                fetched_rows=7536,
                fetched_pages=8,
                cache_rows_before=1000,
                cache_rows_after=7536,
                s2_lookup_rows=6617,
                sales_channel_content_id_unique=6617,
                summary_json_path="doc/2026-05-07/kiss_payment_settlement_refresh_summary.json",
                cache_path="data/kiss_payment_settlement_cache.csv",
                s2_lookup_path="data/kiss_payment_settlement_s2_lookup.csv",
                script="refresh_kiss_payment_settlement.py",
            )
            failure_id = record_refresh_run(
                db_path,
                status="failed",
                source="manual_upload",
                mode="manual_upload",
                error_message="line1\nline2 " + ("x" * 1200),
                script="import_kiss_payment_settlement.py",
            )

            self.assertEqual(success_id, 1)
            self.assertEqual(failure_id, 2)

            rows = latest_refresh_runs(db_path, limit=10)
            self.assertEqual([row["id"] for row in rows], [2, 1])
            self.assertEqual(rows[0]["status"], "failed")
            self.assertEqual(rows[0]["source"], "manual_upload")
            self.assertLessEqual(len(rows[0]["error_message"]), 1000)
            self.assertNotIn("\n", rows[0]["error_message"])
            self.assertEqual(rows[1]["s2_lookup_rows"], 6617)
            self.assertEqual(rows[1]["summary_json_path"], "doc/2026-05-07/kiss_payment_settlement_refresh_summary.json")


if __name__ == "__main__":
    unittest.main()
