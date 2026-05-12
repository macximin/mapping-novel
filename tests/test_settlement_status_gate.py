import unittest

import pandas as pd

from settlement_status_gate import (
    COL_CONTENT_ID,
    COL_CONTENT_SHAPE,
    COL_CONTENT_TITLE,
    COL_DEPARTMENT,
    COL_DISABLED_MARKER,
    COL_MIXED_RISK,
    COL_PAYMENT_EXISTS,
    COL_SALES_CHANNEL_CONTENT_ID,
    COL_STATUS,
    STATUS_BLOCK_DISABLED_MARKER,
    STATUS_HOLD_MIXED_CONTENT_RISK,
    STATUS_HOLD_NO_PAYMENT_SETTLEMENT,
    STATUS_OK_PAYMENT_LOOKUP_SOURCE,
    STATUS_OK_PAYMENT_SETTLEMENT_EXISTS,
    STATUS_REVIEW_UNKNOWN_STATUS,
    apply_settlement_status_gate,
    build_status_table,
    find_blocked_title_hits,
    find_only_blocked_candidate_alerts,
    summary_dict,
)


class SettlementStatusGateTest(unittest.TestCase):
    def test_build_status_table_classifies_payment_and_hold_rows(self) -> None:
        table = build_status_table(sample_judgement(), sample_ips(), as_of="2026-05-09")
        status_by_id = dict(zip(table[COL_SALES_CHANNEL_CONTENT_ID], table[COL_STATUS]))
        mixed_by_id = dict(zip(table[COL_SALES_CHANNEL_CONTENT_ID], table[COL_MIXED_RISK]))

        self.assertEqual(status_by_id["S-OK"], STATUS_OK_PAYMENT_SETTLEMENT_EXISTS)
        self.assertEqual(status_by_id["S-MIXED"], STATUS_HOLD_MIXED_CONTENT_RISK)
        self.assertEqual(status_by_id["S-HOLD"], STATUS_HOLD_NO_PAYMENT_SETTLEMENT)
        self.assertEqual(status_by_id["S-DISABLED"], STATUS_BLOCK_DISABLED_MARKER)
        self.assertEqual(mixed_by_id["S-MIXED"], "Y")
        self.assertEqual(mixed_by_id["S-HOLD"], "N")

    def test_build_status_table_can_filter_core_departments_and_shape(self) -> None:
        table = build_status_table(
            sample_judgement(),
            sample_ips(),
            departments=("소설편집팀", "소설유통팀"),
            content_shape="소설",
        )

        self.assertEqual(set(table[COL_DEPARTMENT]), {"소설편집팀", "소설유통팀"})
        self.assertEqual(set(table[COL_CONTENT_SHAPE]), {"소설"})
        self.assertNotIn("S-DISABLED", set(table[COL_SALES_CHANNEL_CONTENT_ID]))

    def test_apply_gate_blocks_hold_and_unknown_rows_in_strict_mode(self) -> None:
        status_table = build_status_table(sample_judgement(), sample_ips())
        s2 = pd.DataFrame(
            {
                COL_SALES_CHANNEL_CONTENT_ID: ["S-OK", "S-MIXED", "S-UNKNOWN"],
                COL_CONTENT_TITLE: ["정상 작품", "혼합 작품", "알 수 없는 작품"],
            }
        )

        result = apply_settlement_status_gate(s2, status_table, source_kind="manual_s2", mode="strict")

        self.assertEqual(result.allowed[COL_SALES_CHANNEL_CONTENT_ID].tolist(), ["S-OK"])
        self.assertEqual(result.blocked[COL_STATUS].tolist(), [STATUS_HOLD_MIXED_CONTENT_RISK, STATUS_REVIEW_UNKNOWN_STATUS])
        self.assertTrue(result.warnings)

    def test_payment_lookup_unknown_id_is_allowed_as_lookup_source(self) -> None:
        status_table = build_status_table(sample_judgement(), sample_ips())
        s2 = pd.DataFrame({COL_SALES_CHANNEL_CONTENT_ID: ["S-UNKNOWN"], COL_CONTENT_TITLE: ["새 지급정산 작품"]})

        result = apply_settlement_status_gate(s2, status_table, source_kind="payment_lookup", mode="payment_lookup_safe")

        self.assertEqual(result.allowed.loc[0, COL_STATUS], STATUS_OK_PAYMENT_LOOKUP_SOURCE)
        self.assertTrue(result.blocked.empty)

    def test_payment_lookup_safe_mode_does_not_allow_unknown_manual_s2_ids(self) -> None:
        status_table = build_status_table(sample_judgement(), sample_ips())
        s2 = pd.DataFrame({COL_SALES_CHANNEL_CONTENT_ID: ["S-UNKNOWN"], COL_CONTENT_TITLE: ["수동 S2 작품"]})

        result = apply_settlement_status_gate(s2, status_table, source_kind="manual_s2", mode="payment_lookup_safe")

        self.assertTrue(result.allowed.empty)
        self.assertEqual(result.blocked.loc[0, COL_STATUS], STATUS_REVIEW_UNKNOWN_STATUS)

    def test_blank_content_id_does_not_create_false_mixed_risk(self) -> None:
        judgement = pd.DataFrame(
            [
                {
                    COL_SALES_CHANNEL_CONTENT_ID: "S-BLANK-OK",
                    COL_CONTENT_ID: "",
                    COL_CONTENT_TITLE: "공란 정상",
                    COL_PAYMENT_EXISTS: "Y",
                    COL_DISABLED_MARKER: "N",
                },
                {
                    COL_SALES_CHANNEL_CONTENT_ID: "S-BLANK-HOLD",
                    COL_CONTENT_ID: "",
                    COL_CONTENT_TITLE: "공란 보류",
                    COL_PAYMENT_EXISTS: "N",
                    COL_DISABLED_MARKER: "N",
                },
            ]
        )

        table = build_status_table(judgement)

        self.assertEqual(table.loc[table[COL_SALES_CHANNEL_CONTENT_ID].eq("S-BLANK-HOLD"), COL_STATUS].item(), STATUS_HOLD_NO_PAYMENT_SETTLEMENT)

    def test_audit_only_mode_keeps_blocked_rows_in_allowed_output(self) -> None:
        status_table = build_status_table(sample_judgement(), sample_ips())
        s2 = pd.DataFrame({COL_SALES_CHANNEL_CONTENT_ID: ["S-MIXED"], COL_CONTENT_TITLE: ["혼합 작품"]})

        result = apply_settlement_status_gate(s2, status_table, source_kind="manual_s2", mode="audit_only")

        self.assertEqual(len(result.allowed), 1)
        self.assertEqual(len(result.blocked), 1)
        self.assertIn("audit_only", result.warnings[0])

    def test_blocked_title_hits_and_only_blocked_alerts_explain_no_match(self) -> None:
        allowed = pd.DataFrame({COL_SALES_CHANNEL_CONTENT_ID: ["S-OK"], COL_CONTENT_TITLE: ["정상 작품"]})
        blocked = pd.DataFrame(
            {
                COL_SALES_CHANNEL_CONTENT_ID: ["S-MIXED"],
                COL_CONTENT_TITLE: ["혼합 작품"],
                COL_STATUS: [STATUS_HOLD_MIXED_CONTENT_RISK],
            }
        )
        settlement = pd.DataFrame({"작품명": ["혼합 작품", "정상 작품"]})

        hits = find_blocked_title_hits(settlement, blocked)
        alerts = find_only_blocked_candidate_alerts(settlement, allowed, blocked)

        self.assertEqual(len(hits), 1)
        self.assertEqual(alerts["settlement_콘텐츠명"].tolist(), ["혼합 작품"])
        self.assertEqual(alerts.loc[0, "차단후보수"], 1)

    def test_summary_dict_counts_ab_and_mixed_scope(self) -> None:
        table = build_status_table(sample_judgement(), sample_ips())
        summary = summary_dict(table)

        self.assertEqual(summary["missing_payment_settlement"], 3)
        self.assertEqual(summary["ab_checklist"], 2)
        self.assertEqual(summary["mixed_risk"], 1)
        self.assertEqual(summary["mixed_content_ids"], 1)


def sample_judgement() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                COL_SALES_CHANNEL_CONTENT_ID: "S-OK",
                COL_CONTENT_ID: "C-1",
                COL_CONTENT_TITLE: "정상 작품",
                COL_CONTENT_SHAPE: "소설",
                COL_PAYMENT_EXISTS: "Y",
                COL_DISABLED_MARKER: "N",
            },
            {
                COL_SALES_CHANNEL_CONTENT_ID: "S-MIXED",
                COL_CONTENT_ID: "C-1",
                COL_CONTENT_TITLE: "혼합 작품",
                COL_CONTENT_SHAPE: "소설",
                COL_PAYMENT_EXISTS: "N",
                COL_DISABLED_MARKER: "N",
            },
            {
                COL_SALES_CHANNEL_CONTENT_ID: "S-HOLD",
                COL_CONTENT_ID: "C-2",
                COL_CONTENT_TITLE: "보류 작품",
                COL_CONTENT_SHAPE: "소설",
                COL_PAYMENT_EXISTS: "N",
                COL_DISABLED_MARKER: "N",
            },
            {
                COL_SALES_CHANNEL_CONTENT_ID: "S-DISABLED",
                COL_CONTENT_ID: "C-3",
                COL_CONTENT_TITLE: "차단 작품",
                COL_CONTENT_SHAPE: "웹툰",
                COL_PAYMENT_EXISTS: "N",
                COL_DISABLED_MARKER: "Y",
            },
        ]
    )


def sample_ips() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {COL_CONTENT_ID: "C-1", COL_DEPARTMENT: "소설편집팀"},
            {COL_CONTENT_ID: "C-2", COL_DEPARTMENT: "소설유통팀"},
            {COL_CONTENT_ID: "C-3", COL_DEPARTMENT: "웹툰팀"},
        ]
    )


if __name__ == "__main__":
    unittest.main()
