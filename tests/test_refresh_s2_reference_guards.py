from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.refresh_s2_reference_guards import (
    fetch_missing_platform_comparisons,
    fetch_missing_rows,
    missing_overlap_count,
    value_counts,
)
from s2_reference_guards import normalize_missing_rows


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.headers = {"X-KISS-API-BASE-URL": "https://kiss-api.example"}
        self.calls: list[dict] = []

    def get(self, url: str, *, params: dict, timeout: int) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        platform_code = params.get("plfmCd")
        rows = [
            {"schnCtnsId": "BASE-1", "ctnsId": "CID-1", "ctnsNm": "기준 작품", "schnNm": "채널A"},
        ]
        if platform_code == "DIST":
            rows = [
                {"schnCtnsId": "BASE-1", "ctnsId": "CID-1", "ctnsNm": "기준 작품", "schnNm": "채널A"},
                {"schnCtnsId": "DIST-1", "ctnsId": "CID-2", "ctnsNm": "유통 작품", "schnNm": "채널B"},
            ]
        return FakeResponse({"data": {"list": rows}})


class RefreshS2ReferenceGuardsTest(unittest.TestCase):
    def test_fetch_missing_rows_sends_platform_and_content_style_params(self) -> None:
        session = FakeSession()

        rows = fetch_missing_rows(session, content_style_code="102", platform_code="DIST", page_size=1000)

        self.assertEqual(len(rows), 2)
        self.assertEqual(session.calls[0]["url"], "https://kiss-api.example/stm/stm")
        self.assertEqual(session.calls[0]["params"]["plfmCd"], "DIST")
        self.assertEqual(session.calls[0]["params"]["ctnsStleCd"], "102")
        self.assertEqual(session.calls[0]["params"]["pageSize"], 1000)

    def test_fetch_missing_platform_comparisons_counts_delta(self) -> None:
        session = FakeSession()
        base_rows = fetch_missing_rows(session, content_style_code="102", platform_code="", page_size=1000)

        comparisons = fetch_missing_platform_comparisons(
            session,
            base_rows=base_rows,
            platform_codes=["DIST"],
            content_style_code="102",
            page_size=1000,
        )

        self.assertEqual(comparisons[0]["platform_code"], "DIST")
        self.assertEqual(comparisons[0]["ids_only_in_base"], 0)
        self.assertEqual(comparisons[0]["ids_only_in_platform"], 1)
        self.assertFalse(comparisons[0]["same_ids_as_base"])

    def test_missing_summary_helpers_count_and_overlap(self) -> None:
        missing = normalize_missing_rows(
            [
                {"schnCtnsId": "A", "ctnsNm": "작품", "schnNm": "채널", "ctnsStleCdNm": "소설", "setlBgnYn": "N"},
                {"schnCtnsId": "B", "ctnsNm": "작품2", "schnNm": "채널", "ctnsStleCdNm": "소설", "setlBgnYn": "Y"},
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            lookup_path = Path(temp_dir) / "s2.csv"
            pd.DataFrame({"판매채널콘텐츠ID": ["A", "C"]}).to_csv(lookup_path, index=False)

            self.assertEqual(value_counts(missing, "판매채널명"), {"채널": 2})
            self.assertEqual(missing_overlap_count(missing, lookup_path), 1)


if __name__ == "__main__":
    unittest.main()
