from __future__ import annotations

import unittest

import pandas as pd

from cleaning_rules import CleaningPolicy


class CleaningRulesTest(unittest.TestCase):
    def test_policy_extracts_and_cleans_master_title(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.extract_master_work_title("<그 남자의 비밀>_홍길동_100_200_확정"), "그 남자의 비밀")
        self.assertEqual(policy.clean_master_title("너_그리고_나_홍길동_100_200_확정"), "너그리고나")

    def test_policy_extracts_confirmed_master_suffix_variants(self) -> None:
        policy = CleaningPolicy()
        cases = {
            "재벌가 차남은 먼치킨_말리브해적_1003258_472_확정": "재벌가 차남은 먼치킨",
            "재벌가 차남은 먼치킨_말리브해적_1003258_미연결_확정": "재벌가 차남은 먼치킨",
            "재벌가 차남은 먼치킨_말리브해적_1003258_선인세없음_확정": "재벌가 차남은 먼치킨",
            "너_그리고_나_홍길동_1001234_2002345_확정": "너_그리고_나",
        }

        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(policy.extract_confirmed_master_title(raw), expected)
                self.assertEqual(policy.extract_master_work_title(raw), expected)

    def test_default_disabled_markers_drop_rows_before_mapping(self) -> None:
        policy = CleaningPolicy()
        frame = pd.DataFrame(
            {
                "콘텐츠명": [
                    "정상 작품",
                    "[사용안함] 삭제 작품",
                    "(사용안함) 삭제 작품",
                    "[사용금지] 삭제 작품",
                    "(사용금지) 삭제 작품",
                    "[정산정보없음] 삭제 작품",
                    "(정산정보없음) 삭제 작품",
                    "[사용안함]_[정산정보없음] 이중 차단 작품",
                    "[정산정보없음]_[사용안함] 역순 이중 차단 작품",
                    "(사용안함)_(정산정보없음) 괄호 이중 차단 작품",
                ]
            }
        )

        filtered = policy.drop_disabled_rows(frame)

        self.assertEqual(filtered["콘텐츠명"].tolist(), ["정상 작품"])

    def test_policy_can_override_disabled_markers(self) -> None:
        policy = CleaningPolicy(disabled_row_markers=("DROP",))
        frame = pd.DataFrame({"콘텐츠명": ["정상 작품", "DROP 삭제 작품", "[사용안함]_기본표식"]})

        filtered = policy.drop_disabled_rows(frame)

        self.assertEqual(filtered["콘텐츠명"].tolist(), ["정상 작품", "[사용안함]_기본표식"])


if __name__ == "__main__":
    unittest.main()
