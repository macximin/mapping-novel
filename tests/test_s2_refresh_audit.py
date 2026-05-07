from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from kiss_payment_settlement import import_payment_settlement_frame, payment_settlement_frame_from_api_rows


def s2_source_row(
    sales_channel_content_id: str,
    *,
    master_id: str,
    detail_id: str,
    content_id: str,
    title: str,
    author: str,
    created_at: str,
) -> dict[str, str]:
    return {
        "pymtSetlId": master_id,
        "pymtSetlDtlId": detail_id,
        "schnCtnsId": sales_channel_content_id,
        "ctnsId": content_id,
        "ctnsNm": title,
        "schnNm": "테스트 채널",
        "ctnsStleCdNm": "소설",
        "cnfmStsCdNm": "승인",
        "pymtSetlStsCdNm": "운영중",
        "cretDtm": created_at,
        "작가명": author,
    }


class S2RefreshAuditTest(unittest.TestCase):
    def test_full_replace_records_added_deleted_modified_and_drops_stale_rows(self) -> None:
        existing = payment_settlement_frame_from_api_rows(
            [
                s2_source_row(
                    "301",
                    master_id="101",
                    detail_id="201",
                    content_id="401",
                    title="오래된 제목",
                    author="이전 작가",
                    created_at="2026-05-07 11:00:00",
                ),
                s2_source_row(
                    "999",
                    master_id="199",
                    detail_id="299",
                    content_id="499",
                    title="삭제될 작품",
                    author="삭제 작가",
                    created_at="2026-05-07 12:00:00",
                ),
            ]
        )
        incoming = payment_settlement_frame_from_api_rows(
            [
                s2_source_row(
                    "301",
                    master_id="102",
                    detail_id="202",
                    content_id="402",
                    title="새 제목",
                    author="새 작가",
                    created_at="2026-05-08 11:00:00",
                ),
                s2_source_row(
                    "777",
                    master_id="177",
                    detail_id="277",
                    content_id="477",
                    title="신규 작품",
                    author="신규 작가",
                    created_at="2026-05-08 12:00:00",
                ),
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "s2_cache.csv"
            lookup_path = Path(tmp) / "s2_lookup.csv"
            existing.to_csv(cache_path, index=False, encoding="utf-8-sig")

            with self.assertRaisesRegex(ValueError, "전체 교체"):
                import_payment_settlement_frame(
                    incoming,
                    cache_path=cache_path,
                    s2_lookup_path=lookup_path,
                    merge_existing=True,
                )

            result = import_payment_settlement_frame(
                incoming,
                cache_path=cache_path,
                s2_lookup_path=lookup_path,
            )

            self.assertEqual(result.cache_rows_before, 2)
            self.assertEqual(result.cache_rows_after, 2)
            self.assertEqual(result.s2_change_added, 1)
            self.assertEqual(result.s2_change_deleted, 1)
            self.assertEqual(result.s2_change_modified, 1)

            cache_text = cache_path.read_text(encoding="utf-8-sig")
            lookup_text = lookup_path.read_text(encoding="utf-8-sig")
            self.assertNotIn("삭제될 작품", cache_text)
            self.assertNotIn("삭제될 작품", lookup_text)
            self.assertIn("신규 작품", lookup_text)
            self.assertIn("새 제목", lookup_text)

            changes = result.s2_change_rows.set_index(["변경유형", "판매채널콘텐츠ID"])
            modified = changes.loc[("modified", "301")]
            self.assertIn("콘텐츠명", modified["변경필드"])
            self.assertIn("S2마스터ID", modified["변경필드"])
            self.assertIn("콘텐츠ID", modified["변경필드"])
            self.assertIn("작가정보", modified["변경필드"])
            self.assertEqual(modified["이전_콘텐츠명"], "오래된 제목")
            self.assertEqual(modified["신규_콘텐츠명"], "새 제목")
            self.assertEqual(modified["이전_S2마스터ID"], "101")
            self.assertEqual(modified["신규_S2마스터ID"], "102")
            self.assertEqual(modified["이전_작가정보"], "이전 작가")
            self.assertEqual(modified["신규_작가정보"], "새 작가")

    def test_first_full_replace_records_incoming_rows_as_added(self) -> None:
        incoming = payment_settlement_frame_from_api_rows(
            [
                s2_source_row(
                    "301",
                    master_id="101",
                    detail_id="201",
                    content_id="401",
                    title="첫 작품",
                    author="첫 작가",
                    created_at="2026-05-07 11:00:00",
                )
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            result = import_payment_settlement_frame(
                incoming,
                cache_path=Path(tmp) / "s2_cache.csv",
                s2_lookup_path=Path(tmp) / "s2_lookup.csv",
            )

            self.assertEqual(result.cache_rows_before, 0)
            self.assertEqual(result.s2_change_added, 1)
            self.assertEqual(result.s2_change_deleted, 0)
            self.assertEqual(result.s2_change_modified, 0)
            self.assertEqual(result.s2_change_rows.loc[0, "판매채널콘텐츠ID"], "301")

    def test_identical_full_replace_records_no_changes(self) -> None:
        incoming = payment_settlement_frame_from_api_rows(
            [
                s2_source_row(
                    "301",
                    master_id="101",
                    detail_id="201",
                    content_id="401",
                    title="동일 작품",
                    author="동일 작가",
                    created_at="2026-05-07 11:00:00",
                )
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "s2_cache.csv"
            lookup_path = Path(tmp) / "s2_lookup.csv"
            incoming.to_csv(cache_path, index=False, encoding="utf-8-sig")

            result = import_payment_settlement_frame(
                pd.DataFrame(incoming),
                cache_path=cache_path,
                s2_lookup_path=lookup_path,
            )

            self.assertEqual(result.s2_change_added, 0)
            self.assertEqual(result.s2_change_deleted, 0)
            self.assertEqual(result.s2_change_modified, 0)
            self.assertTrue(result.s2_change_rows.empty)


if __name__ == "__main__":
    unittest.main()
