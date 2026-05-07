from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from kiss_refresh_history import latest_refresh_runs, latest_s2_refresh_changes, record_refresh_run, record_s2_refresh_changes


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
                s2_change_added=7,
                s2_change_deleted=3,
                s2_change_modified=2,
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
            self.assertEqual(rows[1]["s2_change_added"], 7)
            self.assertEqual(rows[1]["s2_change_deleted"], 3)
            self.assertEqual(rows[1]["s2_change_modified"], 2)
            self.assertEqual(rows[1]["summary_json_path"], "doc/2026-05-07/kiss_payment_settlement_refresh_summary.json")

    def test_records_s2_change_detail_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "history.sqlite"
            run_id = record_refresh_run(
                db_path,
                status="success",
                source="s2_api",
                mode="full-replace",
                s2_change_added=1,
                s2_change_deleted=0,
                s2_change_modified=1,
            )
            changes = pd.DataFrame(
                [
                    {
                        "변경유형": "modified",
                        "판매채널콘텐츠ID": "301",
                        "변경필드": "콘텐츠명 | S2마스터ID | 작가정보",
                        "이전_콘텐츠명": "이전 제목",
                        "신규_콘텐츠명": "새 제목",
                        "이전_S2마스터ID": "101",
                        "신규_S2마스터ID": "102",
                        "이전_콘텐츠ID": "401",
                        "신규_콘텐츠ID": "401",
                        "이전_작가정보": "이전 작가",
                        "신규_작가정보": "새 작가",
                    },
                    {
                        "변경유형": "added",
                        "판매채널콘텐츠ID": "777",
                        "변경필드": "신규",
                        "신규_콘텐츠명": "신규 작품",
                    },
                ]
            )

            inserted = record_s2_refresh_changes(db_path, run_id, changes)
            rows = latest_s2_refresh_changes(db_path, refresh_run_id=run_id, limit=10)

            self.assertEqual(inserted, 2)
            self.assertEqual([row["change_type"] for row in rows], ["modified", "added"])
            self.assertEqual(rows[0]["sales_channel_content_id"], "301")
            self.assertEqual(rows[0]["old_s2_master_id"], "101")
            self.assertEqual(rows[0]["new_author_info"], "새 작가")


if __name__ == "__main__":
    unittest.main()
