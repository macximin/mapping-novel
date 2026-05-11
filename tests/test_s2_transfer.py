from __future__ import annotations

import unittest

import pandas as pd

from s2_transfer import build_s2_transfer, export_s2_transfer


class S2TransferTest(unittest.TestCase):
    def test_blocks_when_amount_policy_is_not_locked(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "정산서_콘텐츠명": "작품",
                    "S2_매칭상태": "matched",
                    "S2_판매채널콘텐츠ID": "123",
                    "정산서원본_판매금액_후보": 1000,
                    "정산서원본_정산기준액_후보": 700,
                    "정산서원본_상계금액_후보": 300,
                }
            ]
        )

        result = build_s2_transfer(rows, amount_policy_locked=False, s2_gate="금액 정책 미확정")

        self.assertFalse(result.exportable)
        self.assertEqual(len(result.rows), 0)
        self.assertEqual(len(result.blocked_rows), 1)
        self.assertIn("금액 정책 미확정", result.blocked_rows.loc[0, "차단사유"])

    def test_exports_only_when_all_hard_gates_pass(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "정산서_콘텐츠명": "작품",
                    "S2_매칭상태": "matched",
                    "S2_판매채널콘텐츠ID": "123",
                    "정산서원본_판매금액_후보": "1,000",
                    "정산서원본_정산기준액_후보": "700",
                    "정산서원본_상계금액_후보": "300",
                }
            ]
        )

        result = build_s2_transfer(rows, amount_policy_locked=True, s2_gate="confirmed")

        self.assertTrue(result.exportable)
        self.assertEqual(result.rows.to_dict("records"), [{"판매채널콘텐츠ID": "123", "판매금액": 1000, "정산기준액": 700, "상계금액": 300}])
        self.assertGreater(len(export_s2_transfer(result)), 1000)

    def test_blocks_partial_export_when_any_row_fails(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "정산서_콘텐츠명": "정상",
                    "S2_매칭상태": "matched",
                    "S2_판매채널콘텐츠ID": "123",
                    "정산서원본_판매금액_후보": 1000,
                    "정산서원본_정산기준액_후보": 700,
                    "정산서원본_상계금액_후보": 300,
                },
                {
                    "정산서_콘텐츠명": "미매칭",
                    "S2_매칭상태": "no_match",
                    "S2_판매채널콘텐츠ID": "",
                    "정산서원본_판매금액_후보": 1000,
                    "정산서원본_정산기준액_후보": 700,
                    "정산서원본_상계금액_후보": 300,
                },
            ]
        )

        result = build_s2_transfer(rows, amount_policy_locked=True, s2_gate="confirmed")

        self.assertFalse(result.exportable)
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(len(result.blocked_rows), 1)
        self.assertIn("matched가 아닙니다", result.blocked_rows.loc[0, "차단사유"])

    def test_blocks_duplicate_s2_candidates_even_after_auto_selection(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "정산서_콘텐츠명": "중복 후보 작품",
                    "S2_매칭상태": "matched",
                    "S2_판매채널콘텐츠ID": "123",
                    "S2_후보수": "2",
                    "정산서원본_판매금액_후보": 1000,
                    "정산서원본_정산기준액_후보": 700,
                    "정산서원본_상계금액_후보": 300,
                }
            ]
        )

        result = build_s2_transfer(rows, amount_policy_locked=True, s2_gate="confirmed")

        self.assertFalse(result.exportable)
        self.assertEqual(len(result.rows), 0)
        self.assertEqual(len(result.blocked_rows), 1)
        self.assertIn("S2 중복 후보", result.blocked_rows.loc[0, "차단사유"])

    def test_blocks_rows_with_s2_split_reason(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "정산서_콘텐츠명": "청구 후보 작품",
                    "S2_매칭상태": "matched",
                    "S2_판매채널콘텐츠ID": "123",
                    "S2_후보수": "1",
                    "S2_분리사유": "청구정산 후보",
                    "정산서원본_판매금액_후보": 1000,
                    "정산서원본_정산기준액_후보": 700,
                    "정산서원본_상계금액_후보": 300,
                }
            ]
        )

        result = build_s2_transfer(rows, amount_policy_locked=True, s2_gate="confirmed")

        self.assertFalse(result.exportable)
        self.assertEqual(len(result.rows), 0)
        self.assertEqual(len(result.blocked_rows), 1)
        self.assertIn("S2 분리사유", result.blocked_rows.loc[0, "차단사유"])


if __name__ == "__main__":
    unittest.main()
