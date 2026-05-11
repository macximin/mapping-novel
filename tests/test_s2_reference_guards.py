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


if __name__ == "__main__":
    unittest.main()
