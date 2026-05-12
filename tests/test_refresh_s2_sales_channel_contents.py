from __future__ import annotations

import unittest

import pandas as pd

from scripts.refresh_s2_sales_channel_contents import build_targets, fetch_service_contents


class FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.headers = {"X-KISS-API-BASE-URL": "https://kiss-api.example"}
        self.calls: list[dict] = []

    def get(self, url: str, *, params: dict, timeout: int) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse({"data": [{"schnCtnsId": "SVC-1", "ctnsId": "CID-1", "ctnsNm": "작품"}]})


class RefreshS2SalesChannelContentsTest(unittest.TestCase):
    def test_build_targets_records_missing_catalog_channels_without_failing(self) -> None:
        catalog = pd.DataFrame(
            [
                {
                    "schnId": "S-1",
                    "schnNm": "원스토어(소설)",
                    "bcncCd": "B-1",
                    "bcncNm": "거래처",
                    "ctnsStleCdNm": "소설",
                }
            ]
        )

        targets, audit = build_targets(catalog)

        self.assertTrue(any(target["schn_nm"] == "원스토어(소설)" for target in targets))
        self.assertTrue(any(row["상태"] == "missing_channel_catalog" for row in audit))

    def test_fetch_service_contents_sends_content_style_filter(self) -> None:
        session = FakeSession()
        target = {"bcnc_cd": "B-1", "schn_id": "S-1"}

        rows = fetch_service_contents(session, target, content_style_code="102")

        self.assertEqual(rows[0]["schnCtnsId"], "SVC-1")
        self.assertEqual(session.calls[0]["url"], "https://kiss-api.example/sale/ext/ext-salm/schn-ctns")
        self.assertEqual(session.calls[0]["params"]["bcncCd"], "B-1")
        self.assertEqual(session.calls[0]["params"]["schnIds"], "S-1")
        self.assertEqual(session.calls[0]["params"]["ctnsStleCd"], "102")


if __name__ == "__main__":
    unittest.main()
