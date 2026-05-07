from __future__ import annotations

import unittest
from datetime import date

from scripts.refresh_kiss_payment_settlement import KISSRefreshError, resolve_query_window


class S2RefreshWindowTest(unittest.TestCase):
    def test_full_replace_window_uses_blank_dates_for_unbounded_query(self) -> None:
        window = resolve_query_window(
            "full-replace",
            today=date(2026, 5, 7),
            start_date="",
            end_date="",
        )

        self.assertEqual(window.start_date, "")
        self.assertEqual(window.end_date, "")

    def test_initial_alias_uses_full_replace_window(self) -> None:
        window = resolve_query_window(
            "initial",
            today=date(2026, 5, 7),
            start_date="",
            end_date="",
        )

        self.assertEqual(window.start_date, "")
        self.assertEqual(window.end_date, "")

    def test_custom_window_only_allows_full_replace_fallback_range(self) -> None:
        window = resolve_query_window(
            "custom",
            today=date(2026, 5, 7),
            start_date="1900-01-01",
            end_date="2026-05-07",
        )

        self.assertEqual(window.start_date, "1900-01-01")
        self.assertEqual(window.end_date, "2026-05-07")

    def test_custom_window_rejects_partial_range(self) -> None:
        with self.assertRaisesRegex(KISSRefreshError, "전체 교체"):
            resolve_query_window(
                "custom",
                today=date(2026, 5, 7),
                start_date="2026-02-07",
                end_date="2026-05-07",
            )

    def test_rolling_window_is_rejected(self) -> None:
        with self.assertRaisesRegex(KISSRefreshError, "전체 교체"):
            resolve_query_window(
                "rolling-3m",
                today=date(2026, 5, 7),
                start_date="",
                end_date="",
            )


if __name__ == "__main__":
    unittest.main()
