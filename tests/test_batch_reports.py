from __future__ import annotations

import unittest

import pandas as pd

from batch_reports import build_combined_mapping_report_frame, build_pd_work_order_report_frame
from mapping_core import MappingResult


class BatchReportsTest(unittest.TestCase):
    def test_combined_report_sorts_s2_match_status_text_descending(self) -> None:
        rows = pd.DataFrame(
            [
                {"정산서_콘텐츠명": "정상", "정제_상품명": "정상", "S2_매칭상태": "matched"},
                {"정산서_콘텐츠명": "공백", "정제_상품명": "", "S2_매칭상태": "blank_key"},
                {"정산서_콘텐츠명": "없음", "정제_상품명": "없음", "S2_매칭상태": "no_match"},
            ]
        )
        result = {
            "status": "success",
            "source_name": "fixture.xlsx",
            "s2_sales_channel": "네이버_연재",
            "platform": "네이버",
            "mapping": MappingResult(
                rows=rows,
                summary=pd.DataFrame(),
                review_rows=pd.DataFrame(),
                duplicate_candidates=pd.DataFrame(),
                input_validation=pd.DataFrame(),
            ),
        }

        combined = build_combined_mapping_report_frame([result])

        self.assertEqual(combined["S2_매칭상태"].tolist(), ["no_match", "matched", "blank_key"])

    def test_work_order_report_groups_review_rows_for_pd_sheet(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "정산서_원본행번호": 1,
                    "정산서원본_source_row": 10,
                    "정산서_콘텐츠명": "없는 작품",
                    "정제_상품명": "없는작품",
                    "S2_매칭상태": "no_match",
                    "S2_후보수": "0",
                    "S2_미매핑상세사유": "해당채널 지급정산 없음 / 타채널 지급정산 존재",
                    "S2_미매핑근거": "타채널 지급정산: 판매채널명=다른채널, 콘텐츠ID=CID-1",
                    "S2_권장조치": "해당 판매채널 지급정산 생성/보강 여부 판단",
                    "검토필요사유": "S2 미매핑",
                    "검토필요(Y/N)": "Y",
                },
                {
                    "정산서_원본행번호": 2,
                    "정산서원본_source_row": 20,
                    "정산서_콘텐츠명": "없는 작품",
                    "정제_상품명": "없는작품",
                    "S2_매칭상태": "no_match",
                    "S2_후보수": "0",
                    "S2_미매핑상세사유": "해당채널 지급정산 없음 / 타채널 지급정산 존재",
                    "S2_미매핑근거": "타채널 지급정산: 판매채널명=다른채널, 콘텐츠ID=CID-1",
                    "S2_권장조치": "해당 판매채널 지급정산 생성/보강 여부 판단",
                    "검토필요사유": "S2 미매핑",
                    "검토필요(Y/N)": "Y",
                },
                {
                    "정산서_원본행번호": 3,
                    "정산서원본_source_row": 30,
                    "정산서_콘텐츠명": "정상 작품",
                    "정제_상품명": "정상작품",
                    "S2_매칭상태": "matched",
                    "S2_판매채널콘텐츠ID": "S2-1",
                    "S2_후보수": "1",
                    "검토필요사유": "",
                    "검토필요(Y/N)": "N",
                },
            ]
        )
        result = {
            "status": "success",
            "source_name": "네이버_fixture.xlsx",
            "s2_sales_channel": "네이버_연재",
            "platform": "네이버",
            "mapping": MappingResult(
                rows=rows,
                summary=pd.DataFrame(),
                review_rows=rows[rows["검토필요(Y/N)"].eq("Y")],
                duplicate_candidates=pd.DataFrame(),
                input_validation=pd.DataFrame(),
            ),
        }

        combined = build_combined_mapping_report_frame([result])
        work_order = build_pd_work_order_report_frame([result])

        self.assertEqual(len(combined), 3)
        self.assertEqual(len(work_order), 1)
        self.assertEqual(work_order.loc[0, "정산서 행 수"], 2)
        self.assertEqual(work_order.loc[0, "정제_상품명"], "없는작품")
        self.assertEqual(work_order.loc[0, "S2 검색어"], "없는작품")
        self.assertIn("S2 판매채널", work_order.columns)
        self.assertIn("담당PD", work_order.columns)
        self.assertIn("S2_미매핑상세사유", combined.columns)
        self.assertIn("S2_미매핑근거", work_order.columns)
        self.assertIn("해당 판매채널 지급정산 생성/보강 여부 판단", work_order.loc[0, "권장액션"])
        self.assertEqual(work_order.loc[0, "원본행번호목록"], "1 | 2")
        self.assertEqual(work_order.loc[0, "엑셀행번호목록"], "10 | 20")


if __name__ == "__main__":
    unittest.main()
