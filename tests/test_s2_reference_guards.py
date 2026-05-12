from __future__ import annotations

import unittest

import pandas as pd

from mapping_core import MATCH_NONE, MATCH_OK, build_mapping
from s2_reference_guards import (
    S2ReferenceGuards,
    annotate_mapping_result,
    apply_missing_exclusions,
    normalize_billing_rows,
    normalize_missing_rows,
    normalize_service_content_rows,
)


class S2ReferenceGuardsTest(unittest.TestCase):
    def test_missing_lookup_normalizes_api_rows(self) -> None:
        frame = normalize_missing_rows(
            [
                {
                    "schnCtnsId": 841228,
                    "ctnsId": 109416,
                    "ctnsNm": "[정산정보없음]_나태한 연기 천재",
                    "schnId": 10,
                    "schnNm": "원스토어(소설)",
                    "ctnsStleCdNm": "소설",
                }
            ]
        )

        self.assertEqual(frame.loc[0, "판매채널콘텐츠ID"], "841228")
        self.assertEqual(frame.loc[0, "콘텐츠ID"], "109416")
        self.assertEqual(frame.loc[0, "정제_콘텐츠명"], "나태한연기천재")
        self.assertEqual(frame.loc[0, "제외사유"], "S2 정산정보 누락 건 등재")

    def test_service_content_lookup_normalizes_api_rows(self) -> None:
        frame = normalize_service_content_rows(
            [
                {
                    "platform": "원스토어",
                    "schnId": 10,
                    "schnNm": "원스토어(소설)",
                    "bcncCd": "B-1",
                    "bcncNm": "거래처",
                    "schnCtnsId": 841228,
                    "ctnsId": 109416,
                    "ctnsNm": "나태한 연기 천재",
                    "ctnsStleCdNm": "소설",
                }
            ]
        )

        self.assertEqual(frame.loc[0, "판매채널콘텐츠ID"], "841228")
        self.assertEqual(frame.loc[0, "콘텐츠ID"], "109416")
        self.assertEqual(frame.loc[0, "판매채널명"], "원스토어(소설)")
        self.assertEqual(frame.loc[0, "정제_콘텐츠명"], "나태한연기천재")

    def test_missing_exclusion_drops_matching_sales_channel_content_id(self) -> None:
        guards = S2ReferenceGuards(
            missing=normalize_missing_rows(
                [
                    {
                        "schnCtnsId": "DROP-1",
                        "ctnsId": "CID-1",
                        "ctnsNm": "누락 작품",
                        "schnNm": "원스토어(소설)",
                    }
                ]
            ),
            billing=normalize_billing_rows([]),
        )
        s2 = pd.DataFrame(
            {
                "콘텐츠명": ["누락 작품", "정상 작품"],
                "판매채널콘텐츠ID": ["DROP-1", "KEEP-1"],
                "판매채널명": ["원스토어(소설)", "원스토어(소설)"],
            }
        )

        result = apply_missing_exclusions(s2, guards)

        self.assertEqual(result.frame["판매채널콘텐츠ID"].tolist(), ["KEEP-1"])
        self.assertEqual(result.excluded_count, 1)
        self.assertEqual(result.excluded_rows.loc[0, "S2_제외사유"], "S2 정산정보 누락 건 등재")

    def test_no_match_rows_are_annotated_with_missing_and_billing_candidates(self) -> None:
        guards = S2ReferenceGuards(
            missing=normalize_missing_rows(
                [
                    {
                        "schnCtnsId": "MISS-1",
                        "ctnsId": "CID-MISS",
                        "ctnsNm": "청구 후보 작품",
                        "schnNm": "카카오페이지(선투자)",
                    }
                ]
            ),
            billing=normalize_billing_rows(
                [
                    {
                        "reqSetlId": "REQ-1",
                        "cntrId": "CNTR-1",
                        "schnId": "S-1",
                        "schnNm": "카카오페이지(선투자)",
                        "reprsntCtnsNm": "청구 후보 작품",
                    }
                ]
            ),
        )
        s2 = pd.DataFrame({"콘텐츠명": ["다른 작품"], "판매채널콘텐츠ID": ["S2-1"], "판매채널명": ["카카오페이지(선투자)"]})
        settlement = pd.DataFrame({"작품명": ["청구 후보 작품"]})

        mapping = build_mapping(s2, settlement, None)
        annotated = annotate_mapping_result(mapping, guards, sales_channel="카카오페이지(선투자)")
        row = annotated.rows.iloc[0]

        self.assertEqual(row["S2_매칭상태"], MATCH_NONE)
        self.assertEqual(row["S2_정산정보누락_후보수"], "1")
        self.assertEqual(row["S2_정산정보누락_판매채널콘텐츠ID목록"], "MISS-1")
        self.assertEqual(row["청구정산_후보수"], "1")
        self.assertEqual(row["청구정산마스터ID목록"], "REQ-1")
        self.assertIn("S2 정산정보 누락 건 등재", row["검토필요사유"])
        self.assertIn("청구정산 후보", row["검토필요사유"])
        self.assertIn("해당채널 판매채널콘텐츠 있음 / 지급정산 없음", row["S2_미매핑상세사유"])
        self.assertIn("청구정산 후보 있음", row["S2_미매핑상세사유"])
        self.assertIn("MISS-1", row["S2_미매핑근거"])
        self.assertIn("REQ-1", row["S2_미매핑근거"])

    def test_matched_rows_are_not_reclassified_by_title_only_guard_hits(self) -> None:
        guards = S2ReferenceGuards(
            missing=normalize_missing_rows(
                [
                    {
                        "schnCtnsId": "MISS-1",
                        "ctnsId": "CID-MISS",
                        "ctnsNm": "같은 제목",
                        "schnNm": "원스토어(소설)",
                    }
                ]
            ),
            billing=normalize_billing_rows(
                [
                    {
                        "reqSetlId": "REQ-1",
                        "cntrId": "CNTR-1",
                        "schnNm": "원스토어(소설)",
                        "reprsntCtnsNm": "같은 제목",
                    }
                ]
            ),
        )
        s2 = pd.DataFrame({"콘텐츠명": ["같은 제목"], "판매채널콘텐츠ID": ["S2-OK"], "판매채널명": ["원스토어(소설)"]})
        settlement = pd.DataFrame({"작품명": ["같은 제목"]})

        mapping = build_mapping(s2, settlement, None)
        annotated = annotate_mapping_result(mapping, guards, sales_channel="원스토어(소설)")
        row = annotated.rows.iloc[0]

        self.assertEqual(row["S2_매칭상태"], MATCH_OK)
        self.assertEqual(row["S2_정산정보누락_후보수"], "0")
        self.assertEqual(row["청구정산_후보수"], "0")
        self.assertEqual(row["검토필요(Y/N)"], "N")
        self.assertEqual(row["S2_미매핑상세사유"], "")

    def test_no_match_rows_are_annotated_with_other_channel_payment_candidates(self) -> None:
        guards = S2ReferenceGuards(missing=normalize_missing_rows([]), billing=normalize_billing_rows([]))
        s2_filtered = pd.DataFrame(columns=["콘텐츠명", "판매채널콘텐츠ID", "판매채널명"])
        s2_all = pd.DataFrame(
            {
                "콘텐츠명": ["타채널 작품"],
                "판매채널콘텐츠ID": ["OTHER-S2"],
                "콘텐츠ID": ["CID-OTHER"],
                "판매채널명": ["다른채널"],
            }
        )
        settlement = pd.DataFrame({"작품명": ["타채널 작품"]})

        mapping = build_mapping(s2_filtered, settlement, None)
        annotated = annotate_mapping_result(
            mapping,
            guards,
            sales_channel="현재채널",
            s2_all_frame=s2_all,
        )
        row = annotated.rows.iloc[0]

        self.assertEqual(row["S2_매칭상태"], MATCH_NONE)
        self.assertEqual(row["S2_미매핑상세사유"], "해당채널 지급정산 없음 / 타채널 지급정산 존재")
        self.assertIn("다른채널", row["S2_미매핑근거"])
        self.assertIn("CID-OTHER", row["S2_미매핑근거"])
        self.assertIn("해당 판매채널 지급정산 생성/보강", row["S2_권장조치"])

    def test_no_match_rows_are_annotated_with_master_candidates(self) -> None:
        guards = S2ReferenceGuards(missing=normalize_missing_rows([]), billing=normalize_billing_rows([]))
        s2 = pd.DataFrame(columns=["콘텐츠명", "판매채널콘텐츠ID", "판매채널명"])
        settlement = pd.DataFrame({"작품명": ["콘텐츠마스터 작품"]})
        master = pd.DataFrame(
            {
                "콘텐츠명": ["콘텐츠마스터 작품"],
                "콘텐츠ID": ["MASTER-1"],
                "담당부서": ["소설팀"],
                "담당자명": ["담당자"],
            }
        )

        mapping = build_mapping(s2, settlement, None)
        annotated = annotate_mapping_result(mapping, guards, sales_channel="현재채널", master_df=master)
        row = annotated.rows.iloc[0]

        self.assertEqual(row["S2_미매핑상세사유"], "콘텐츠마스터 있음 / S2 지급정산 없음")
        self.assertIn("MASTER-1", row["S2_미매핑근거"])
        self.assertIn("판매채널콘텐츠/지급정산 생성", row["S2_권장조치"])

    def test_no_match_rows_are_annotated_with_service_content_candidates(self) -> None:
        guards = S2ReferenceGuards(
            missing=normalize_missing_rows([]),
            billing=normalize_billing_rows([]),
            service_contents=normalize_service_content_rows(
                [
                    {
                        "schnCtnsId": "SVC-1",
                        "ctnsId": "CID-SVC",
                        "ctnsNm": "서비스 작품",
                        "schnNm": "현재채널",
                    }
                ]
            ),
        )
        s2 = pd.DataFrame(columns=["콘텐츠명", "판매채널콘텐츠ID", "판매채널명"])
        settlement = pd.DataFrame({"작품명": ["서비스 작품"]})

        mapping = build_mapping(s2, settlement, None)
        annotated = annotate_mapping_result(mapping, guards, sales_channel="현재채널")
        row = annotated.rows.iloc[0]

        self.assertEqual(row["S2_매칭상태"], MATCH_NONE)
        self.assertEqual(row["S2_판매채널콘텐츠_후보수"], "1")
        self.assertEqual(row["S2_판매채널콘텐츠_판매채널콘텐츠ID목록"], "SVC-1")
        self.assertEqual(row["S2_미매핑상세사유"], "같은채널 판매채널콘텐츠 있음 / 지급정산 없음")
        self.assertIn("CID-SVC", row["S2_미매핑근거"])
        self.assertIn("지급정산 생성/연결", row["S2_권장조치"])

    def test_no_match_rows_without_evidence_are_annotated_as_no_key_candidate(self) -> None:
        guards = S2ReferenceGuards(missing=normalize_missing_rows([]), billing=normalize_billing_rows([]))
        s2 = pd.DataFrame(columns=["콘텐츠명", "판매채널콘텐츠ID", "판매채널명"])
        settlement = pd.DataFrame({"작품명": ["새 작품"]})

        mapping = build_mapping(s2, settlement, None)
        annotated = annotate_mapping_result(mapping, guards, sales_channel="현재채널")
        row = annotated.rows.iloc[0]

        self.assertEqual(row["S2_미매핑상세사유"], "S2/콘텐츠마스터 정제키 후보 없음")
        self.assertIn("정제키=새작품", row["S2_미매핑근거"])
        self.assertIn("신규 작품 등록", row["S2_권장조치"])


if __name__ == "__main__":
    unittest.main()
