from __future__ import annotations

import unittest

import pandas as pd

from matching_rules import detect_s2_sales_channel, filter_s2_by_platform, filter_s2_by_sales_channel


class MatchingRulesTest(unittest.TestCase):
    def test_filters_s2_rows_to_platform_sales_channel(self) -> None:
        s2 = pd.DataFrame(
            {
                "콘텐츠명": ["같은 작품", "같은 작품", "다른 작품"],
                "판매채널콘텐츠ID": ["G-1", "O-1", "N-1"],
                "판매채널명": ["구글(소설)", "원스토어(소설)", "네이버_장르"],
            }
        )

        result = filter_s2_by_platform(s2, platform="구글", source_name="2026년 2월 구글 정산상세.xlsx")

        self.assertTrue(result.active)
        self.assertEqual(result.before_rows, 3)
        self.assertEqual(result.after_rows, 1)
        self.assertEqual(result.frame["판매채널콘텐츠ID"].tolist(), ["G-1"])
        self.assertEqual(result.matched_channels, ("구글(소설)",))

    def test_naver_genre_rule_excludes_ad_channel(self) -> None:
        s2 = pd.DataFrame(
            {
                "콘텐츠명": ["장르 작품", "장르 작품", "연재 작품"],
                "판매채널콘텐츠ID": ["NG-1", "AD-1", "NR-1"],
                "판매채널명": ["네이버_장르", "네이버_장르(광고수익)", "네이버_연재"],
            }
        )

        result = filter_s2_by_platform(s2, platform="네이버", source_name="2026년 2월 네이버 장르 정산상세.xlsx")

        self.assertEqual(result.frame["판매채널콘텐츠ID"].tolist(), ["NG-1"])
        self.assertEqual(result.rule_label, "네이버 장르")

    def test_unknown_platform_keeps_rows_without_active_filter(self) -> None:
        s2 = pd.DataFrame({"콘텐츠명": ["작품"], "판매채널명": ["구글(소설)"]})

        result = filter_s2_by_platform(s2, platform="미감지", source_name="file.xlsx")

        self.assertFalse(result.active)
        self.assertEqual(result.after_rows, 1)
        self.assertEqual(len(result.frame), 1)

    def test_missing_channel_column_keeps_rows_without_active_filter(self) -> None:
        s2 = pd.DataFrame({"콘텐츠명": ["작품"]})

        result = filter_s2_by_platform(s2, platform="구글", source_name="file.xlsx")

        self.assertFalse(result.active)
        self.assertEqual(result.after_rows, 1)
        self.assertIn("판매채널명", result.reason)

    def test_detects_real_s2_sales_channel_from_filename(self) -> None:
        detection = detect_s2_sales_channel("2026년 2월 카카오페이지(소설) 정산상세.xlsx")

        self.assertIsNotNone(detection)
        assert detection is not None
        self.assertEqual(detection.sales_channel, "카카오페이지(소설)")
        self.assertEqual(detection.platform, "카카오")

    def test_rejects_broad_platform_name_without_s2_sales_channel(self) -> None:
        self.assertIsNone(detect_s2_sales_channel("2026년 2월 카카오 정산상세.xlsx"))

    def test_detects_longest_sales_channel_name_first(self) -> None:
        detection = detect_s2_sales_channel("2026년 2월 네이버_장르(광고수익) 정산상세.xlsx")

        self.assertIsNotNone(detection)
        assert detection is not None
        self.assertEqual(detection.sales_channel, "네이버_장르(광고수익)")

    def test_detects_explicit_ssot_segment_before_longer_later_tokens(self) -> None:
        detection = detect_s2_sales_channel(
            "2026-02__토스(소설)__토스(구루컴퍼니)__2026년 2월 토스 정산상세.xlsx"
        )

        self.assertIsNotNone(detection)
        assert detection is not None
        self.assertEqual(detection.sales_channel, "토스(소설)")
        self.assertEqual(detection.platform, "토스(구루컴퍼니)")

    def test_filters_s2_rows_by_exact_sales_channel(self) -> None:
        s2 = pd.DataFrame(
            {
                "콘텐츠명": ["같은 작품", "같은 작품", "다른 작품"],
                "판매채널콘텐츠ID": ["K-NOVEL", "K-SHORT", "G-1"],
                "판매채널명": ["카카오페이지(소설)", "카카오페이지(숏툰)", "구글(소설)"],
            }
        )

        result = filter_s2_by_sales_channel(
            s2,
            sales_channel="카카오페이지(소설)",
            source_name="2026년 2월 카카오페이지(소설) 정산상세.xlsx",
        )

        self.assertTrue(result.active)
        self.assertEqual(result.frame["판매채널콘텐츠ID"].tolist(), ["K-NOVEL"])
        self.assertEqual(result.matched_channels, ("카카오페이지(소설)",))


if __name__ == "__main__":
    unittest.main()
