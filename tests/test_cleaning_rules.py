from __future__ import annotations

import unittest

import pandas as pd

from cleaning_rules import CleaningPolicy


class CleaningRulesTest(unittest.TestCase):
    def test_policy_extracts_and_cleans_master_title(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.extract_master_work_title("<그 남자의 비밀>_홍길동_100_200_확정"), "그 남자의 비밀")
        self.assertEqual(policy.clean_master_title("너_그리고_나_홍길동_100_200_확정"), "너그리고나")

    def test_policy_can_override_disabled_markers(self) -> None:
        policy = CleaningPolicy(disabled_row_markers=("DROP",))
        frame = pd.DataFrame({"콘텐츠명": ["정상 작품", "DROP 삭제 작품", "[사용안함]_기본표식"]})

        filtered = policy.drop_disabled_rows(frame)

        self.assertEqual(filtered["콘텐츠명"].tolist(), ["정상 작품", "[사용안함]_기본표식"])


if __name__ == "__main__":
    unittest.main()
