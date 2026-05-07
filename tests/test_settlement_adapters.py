from __future__ import annotations

import csv
import os
import unittest
from pathlib import Path

from settlement_adapters import REGISTRY, adapter_blocking_messages, normalize_settlement, summarize_normalization


DEFAULT_SOURCE_ROOT = Path(r"\\172.16.10.120\소설사업부\판무팀_ssot\100_계산서_매출등록_자료")
DOC_DIR = Path(__file__).resolve().parents[1] / "doc" / "2026-05-07"


def source_root() -> Path:
    return Path(os.environ.get("SETTLEMENT_SOURCE_ROOT", str(DEFAULT_SOURCE_ROOT)))


class SettlementAdapterRegistryTest(unittest.TestCase):
    def test_registry_covers_all_survey_platforms(self) -> None:
        self.assertEqual(len(REGISTRY), 37)

        blocked = [spec.platform for spec in REGISTRY.values() if spec.blocks_default_feed]
        self.assertEqual(sorted(blocked), ["보인&국립장애인도서관", "알라딘 종이책"])

    def test_amount_policy_lock_is_explicit(self) -> None:
        locked = [spec.platform for spec in REGISTRY.values() if spec.s2_amount_policy_locked]

        self.assertIn("북큐브", locked)
        self.assertIn("피우리(누온)", locked)
        self.assertNotIn("무툰", locked)
        self.assertNotIn("카카오", locked)

        self.assertGreaterEqual(len(locked), 10)
        self.assertLess(len(locked), len(REGISTRY) - 2)


class SettlementAdapterFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = source_root()
        if not cls.root.exists():
            raise unittest.SkipTest(f"정산상세 원본 루트가 없습니다: {cls.root}")

    def test_latest_fixture_files_normalize_to_expected_data_rows(self) -> None:
        manifest = DOC_DIR / "latest_origin_substitution_materialization_test_files.csv"
        with manifest.open(encoding="utf-8-sig", newline="") as handle:
            fixtures = list(csv.DictReader(handle))

        self.assertEqual(len(fixtures), 53)
        for fixture in fixtures:
            with self.subTest(platform=fixture["platform"], relative_path=fixture["relative_path"]):
                path = self.root / fixture["relative_path"]
                result = normalize_settlement(path, platform=fixture["platform"], source_name=fixture["relative_path"])
                summary = summarize_normalization(result)

                expected_rows = int(fixture["parsed_rows"] or 0)
                if fixture["platform"] == "네이버" and fixture["counted_in_default_feed"] == "True":
                    # 기존 survey parsed_rows는 네이버 합계 행을 포함했다. 운영 어댑터는 feed에서 합계 행을 제외한다.
                    expected_rows -= 1

                self.assertEqual(summary["parsed_rows"], expected_rows)
                self.assertEqual(summary["title_present_rows"], expected_rows)

                if fixture["counted_in_default_feed"] == "True":
                    self.assertEqual(summary["default_feed_rows"], expected_rows)
                    self.assertEqual(adapter_blocking_messages(result), [])
                else:
                    self.assertEqual(summary["default_feed_rows"], 0)
                    self.assertTrue(adapter_blocking_messages(result))

                if expected_rows:
                    bad_titles = result.rows["상품명"].astype(str).str.strip().isin(["합계", "총 합계", "총 액"])
                    self.assertFalse(bad_titles.any())


if __name__ == "__main__":
    unittest.main()
