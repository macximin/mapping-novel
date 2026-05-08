from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

from kiss_payment_settlement import (
    cache_part_paths,
    import_payment_settlement_frame,
    load_payment_settlement_cache,
    load_payment_settlement_list,
    payment_settlement_frame_from_api_rows,
    summarize_payment_settlement,
    to_s2_lookup,
)


SAMPLE = Path(__file__).resolve().parents[1] / "temp" / "지급_정산_관리_목록 (2).xlsx"


class KissPaymentSettlementTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not SAMPLE.exists():
            raise unittest.SkipTest(f"샘플 지급 정산 관리 목록이 없습니다: {SAMPLE}")

    def test_loads_kiss_export_with_ooxml_fallback(self) -> None:
        frame = load_payment_settlement_list(SAMPLE)

        self.assertGreater(len(frame), 900)
        self.assertIn("지급정산마스터 등록 일자", frame.columns)
        self.assertIn("판매채널콘텐츠ID", frame.columns)

        summary = summarize_payment_settlement(frame)
        self.assertEqual(summary["rows"], len(frame))
        self.assertEqual(summary["content_shape_counts"].get("소설"), len(frame))

    def test_file_like_upload_can_be_converted_to_s2_lookup(self) -> None:
        uploaded = io.BytesIO(SAMPLE.read_bytes())
        frame = load_payment_settlement_list(uploaded)
        lookup = to_s2_lookup(frame)

        self.assertIn("콘텐츠명", lookup.columns)
        self.assertIn("판매채널콘텐츠ID", lookup.columns)
        self.assertGreater(len(lookup), 900)
        self.assertEqual(len(lookup), lookup["판매채널콘텐츠ID"].nunique())

    def test_api_downloader_raw_sheet_can_be_converted_to_s2_lookup(self) -> None:
        workbook = Workbook()
        first = workbook.active
        first.title = "정산다운로드"
        first.append(["콘텐츠명", "승인상태"])
        first.append(["테스트 작품", "승인"])

        raw = workbook.create_sheet("원본데이터")
        raw.append(
            [
                "pymtSetlId",
                "pymtSetlDtlId",
                "schnCtnsId",
                "ctnsId",
                "ctnsNm",
                "schnNm",
                "ctnsStleCdNm",
                "cnfmStsCdNm",
                "pymtSetlStsCdNm",
                "cretDtm",
            ]
        )
        raw.append(
            [
                "100",
                "200",
                "300",
                "400",
                "테스트 작품",
                "테스트 채널",
                "소설",
                "승인",
                "운영중",
                "2026-05-07 10:00:00",
            ]
        )

        uploaded = io.BytesIO()
        workbook.save(uploaded)
        uploaded.seek(0)

        frame = load_payment_settlement_list(uploaded)
        lookup = to_s2_lookup(frame)

        self.assertEqual(frame.loc[0, "판매채널콘텐츠ID"], "300")
        self.assertEqual(frame.loc[0, "지급정산상세ID"], "200")
        self.assertEqual(lookup.loc[0, "판매채널콘텐츠ID"], "300")
        self.assertEqual(lookup.loc[0, "콘텐츠ID"], "400")

    def test_api_rows_can_be_converted_to_s2_lookup(self) -> None:
        frame = payment_settlement_frame_from_api_rows(
            [
                {
                    "pymtSetlId": "101",
                    "pymtSetlDtlId": "201",
                    "schnCtnsId": "301",
                    "ctnsId": "401",
                    "ctnsNm": "API 테스트 작품",
                    "schnNm": "테스트 채널",
                    "ctnsStleCdNm": "소설",
                    "cnfmStsCdNm": "승인",
                    "pymtSetlStsCdNm": "운영중",
                    "cretDtm": "2026-05-07 11:00:00",
                }
            ]
        )
        lookup = to_s2_lookup(frame)

        self.assertEqual(lookup.loc[0, "판매채널콘텐츠ID"], "301")
        self.assertEqual(lookup.loc[0, "콘텐츠ID"], "401")

    def test_disabled_marker_rows_are_removed_from_import_outputs(self) -> None:
        def row(title: str, sales_channel_content_id: str, author: str = "홍길동") -> dict[str, object]:
            return {
                "승인상태": "승인",
                "지급정산상태": "운영중",
                "판매채널명": "테스트 채널",
                "콘텐츠형태": "소설",
                "콘텐츠명": title,
                "작가명": author,
                "지급정산마스터 등록 일자": "2026-05-08 11:00:00",
                "지급정산마스터ID": f"M-{sales_channel_content_id}",
                "지급정산상세ID": f"D-{sales_channel_content_id}",
                "콘텐츠ID": f"C-{sales_channel_content_id}",
                "판매채널콘텐츠ID": sales_channel_content_id,
            }

        incoming = pd.DataFrame(
            [
                row("정상 작품", "301"),
                row("[사용안함]_삭제 작품", "302"),
                row("(사용안함)_삭제 작품", "303"),
                row("[사용금지]_삭제 작품", "304"),
                row("(사용금지)_삭제 작품", "305"),
                row("작가 칸 표식 작품", "306", "(사용금지)"),
                row("[사용안함]_[정산정보없음]_이중 차단 작품", "307"),
                row("작가 칸 이중 표식 작품", "308", "[사용안함]_[정산정보없음]"),
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "s2_cache.csv"
            lookup_path = Path(tmp) / "s2_lookup.csv"

            result = import_payment_settlement_frame(
                incoming,
                cache_path=cache_path,
                s2_lookup_path=lookup_path,
                merge_existing=False,
            )
            cached = pd.read_csv(cache_path, dtype=object)
            lookup = pd.read_csv(lookup_path, dtype=object)

            self.assertEqual(result.source_rows, 1)
            self.assertEqual(result.cache_rows_after, 1)
            self.assertEqual(result.s2_lookup_rows, 1)
            self.assertEqual(cached["판매채널콘텐츠ID"].tolist(), ["301"])
            self.assertEqual(lookup["판매채널콘텐츠ID"].tolist(), ["301"])
            self.assertNotIn("사용안함", cache_path.read_text(encoding="utf-8-sig"))
            self.assertNotIn("사용금지", lookup_path.read_text(encoding="utf-8-sig"))
            self.assertNotIn("정산정보없음", cache_path.read_text(encoding="utf-8-sig"))

    def test_cache_can_be_written_as_parts_and_read_back_for_refresh_audit(self) -> None:
        def row(title: str, sales_channel_content_id: str) -> dict[str, object]:
            return {
                "승인상태": "승인",
                "지급정산상태": "운영중",
                "판매채널명": "테스트 채널",
                "콘텐츠형태": "소설",
                "콘텐츠명": title,
                "작가명": "홍길동",
                "지급정산마스터 등록 일자": "2026-05-08 11:00:00",
                "지급정산마스터ID": f"M-{sales_channel_content_id}",
                "지급정산상세ID": f"D-{sales_channel_content_id}",
                "콘텐츠ID": f"C-{sales_channel_content_id}",
                "판매채널콘텐츠ID": sales_channel_content_id,
            }

        incoming = pd.DataFrame([row("작품 A", "401"), row("작품 B", "402"), row("작품 C", "403")])

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "s2_cache.csv"
            lookup_path = Path(tmp) / "s2_lookup.csv"

            first = import_payment_settlement_frame(
                incoming,
                cache_path=cache_path,
                s2_lookup_path=lookup_path,
                merge_existing=False,
                cache_part_rows=2,
            )
            parts = cache_part_paths(cache_path)
            cached = load_payment_settlement_cache(cache_path)

            self.assertFalse(cache_path.exists())
            self.assertEqual([part.name for part in parts], ["s2_cache_part_001.csv", "s2_cache_part_002.csv"])
            self.assertEqual([part.name for part in first.output_cache_parts], [part.name for part in parts])
            assert cached is not None
            self.assertEqual(cached["판매채널콘텐츠ID"].tolist(), ["401", "402", "403"])

            updated = incoming.copy()
            updated.loc[1, "콘텐츠명"] = "작품 B 개정"
            second = import_payment_settlement_frame(
                updated,
                cache_path=cache_path,
                s2_lookup_path=lookup_path,
                merge_existing=False,
                cache_part_rows=2,
            )

            self.assertEqual(second.cache_rows_before, 3)
            self.assertEqual(second.s2_change_modified, 1)

    def test_confirmed_master_suffix_variants_are_simplified_in_s2_lookup(self) -> None:
        def row(title: str, sales_channel_content_id: str) -> dict[str, object]:
            return {
                "승인상태": "승인",
                "지급정산상태": "운영중",
                "판매채널명": "테스트 채널",
                "콘텐츠형태": "소설",
                "콘텐츠명": title,
                "작가명": "말리브해적",
                "지급정산마스터 등록 일자": "2026-05-08 11:00:00",
                "지급정산마스터ID": f"M-{sales_channel_content_id}",
                "지급정산상세ID": f"D-{sales_channel_content_id}",
                "콘텐츠ID": f"C-{sales_channel_content_id}",
                "판매채널콘텐츠ID": sales_channel_content_id,
            }

        frame = pd.DataFrame(
            [
                row("재벌가 차남은 먼치킨_말리브해적_1003258_472_확정", "401"),
                row("작품A_작가_1003258_미연결_확정", "402"),
                row("작품B_작가_1003258_선인세없음_확정", "403"),
            ]
        )

        lookup = to_s2_lookup(frame)

        self.assertEqual(lookup["콘텐츠명"].tolist(), ["재벌가 차남은 먼치킨", "작품A", "작품B"])

    def test_summary_counts_sales_channel_content_conflicts(self) -> None:
        frame = payment_settlement_frame_from_api_rows(
            [
                {
                    "pymtSetlId": "101",
                    "pymtSetlDtlId": "201",
                    "schnCtnsId": "301",
                    "ctnsId": "401",
                    "ctnsNm": "이전 제목",
                    "schnNm": "테스트 채널",
                    "ctnsStleCdNm": "소설",
                    "cnfmStsCdNm": "승인",
                    "pymtSetlStsCdNm": "운영중",
                    "cretDtm": "2026-05-07 11:00:00",
                },
                {
                    "pymtSetlId": "102",
                    "pymtSetlDtlId": "202",
                    "schnCtnsId": "301",
                    "ctnsId": "402",
                    "ctnsNm": "새 제목",
                    "schnNm": "테스트 채널",
                    "ctnsStleCdNm": "소설",
                    "cnfmStsCdNm": "승인",
                    "pymtSetlStsCdNm": "운영중",
                    "cretDtm": "2026-05-08 11:00:00",
                },
            ]
        )

        summary = summarize_payment_settlement(frame)

        self.assertEqual(summary["sales_channel_content_id_duplicate_keys"], 1)
        self.assertEqual(summary["sales_channel_content_id_multiple_titles"], 1)
        self.assertEqual(summary["sales_channel_content_id_multiple_master_ids"], 1)
        self.assertEqual(summary["sales_channel_content_id_multiple_content_ids"], 1)

    def test_replace_refresh_drops_stale_existing_rows(self) -> None:
        existing = payment_settlement_frame_from_api_rows(
            [
                {
                    "pymtSetlId": "101",
                    "pymtSetlDtlId": "201",
                    "schnCtnsId": "301",
                    "ctnsId": "401",
                    "ctnsNm": "오래된 제목",
                    "schnNm": "테스트 채널",
                    "ctnsStleCdNm": "소설",
                    "cnfmStsCdNm": "승인",
                    "pymtSetlStsCdNm": "운영중",
                    "cretDtm": "2026-05-07 11:00:00",
                }
            ]
        )
        incoming = payment_settlement_frame_from_api_rows(
            [
                {
                    "pymtSetlId": "102",
                    "pymtSetlDtlId": "202",
                    "schnCtnsId": "301",
                    "ctnsId": "401",
                    "ctnsNm": "새 제목",
                    "schnNm": "테스트 채널",
                    "ctnsStleCdNm": "소설",
                    "cnfmStsCdNm": "승인",
                    "pymtSetlStsCdNm": "운영중",
                    "cretDtm": "2026-05-08 11:00:00",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "s2_cache.csv"
            lookup_path = Path(tmp) / "s2_lookup.csv"
            existing.to_csv(cache_path, index=False, encoding="utf-8-sig")

            result = import_payment_settlement_frame(
                incoming,
                cache_path=cache_path,
                s2_lookup_path=lookup_path,
                merge_existing=False,
            )

            self.assertEqual(result.cache_rows_before, 1)
            self.assertEqual(result.cache_rows_after, 1)
            self.assertEqual(cache_path.read_text(encoding="utf-8-sig").count("오래된 제목"), 0)
            self.assertIn("새 제목", lookup_path.read_text(encoding="utf-8-sig"))


if __name__ == "__main__":
    unittest.main()
