from __future__ import annotations

import unittest
from datetime import datetime, timezone

from clickup_notifications import (
    build_clickup_config,
    build_s2_refresh_task_payload,
    create_s2_refresh_request_task,
    normalize_clickup_secret_values,
)


class ClickUpNotificationsTest(unittest.TestCase):
    def test_section_secrets_are_normalized(self) -> None:
        values = normalize_clickup_secret_values(
            {
                "clickup": {
                    "api_token": "token-123",
                    "list_id": "901817301594",
                    "assignee_ids": "306885786",
                    "app_url": "https://example.test/app",
                }
            }
        )

        config = build_clickup_config(values)

        self.assertTrue(config.is_configured)
        self.assertEqual(config.token, "token-123")
        self.assertEqual(config.list_id, "901817301594")
        self.assertEqual(config.assignee_ids, (306885786,))
        self.assertEqual(config.app_url, "https://example.test/app")

    def test_payload_contains_current_s2_state(self) -> None:
        config = build_clickup_config(
            {
                "CLICKUP_API_TOKEN": "token-123",
                "CLICKUP_LIST_ID": "901817301594",
                "CLICKUP_APP_URL": "https://example.test/app",
            }
        )

        payload = build_s2_refresh_task_payload(
            config=config,
            updated_at="2026-05-12 13:14",
            usage_label="확인 필요",
            s2_rows=124755,
            s2_id_rows=124755,
            missing_guard_rows=2327,
            billing_guard_rows=869,
            service_content_rows=96298,
            requested_at=datetime(2026, 5, 13, 1, 30, tzinfo=timezone.utc),
            assignee_ids=(306885786,),
        )

        self.assertEqual(payload["name"], "S2 최신화 요청 - 2026-05-13 10:30")
        self.assertEqual(payload["assignees"], [306885786])
        self.assertTrue(payload["notify_all"])
        self.assertIn("현재 S2 기준 행: 124,755", payload["markdown_content"])
        self.assertIn("누락 guard: 2,327", payload["markdown_content"])
        self.assertIn("https://example.test/app", payload["markdown_content"])

    def test_create_task_auto_assigns_token_owner(self) -> None:
        config = build_clickup_config({"CLICKUP_API_TOKEN": "token-123", "CLICKUP_LIST_ID": "901817301594"})
        session = FakeSession()

        result = create_s2_refresh_request_task(
            config,
            updated_at="2026-05-12 13:14",
            usage_label="확인 필요",
            s2_rows=124755,
            s2_id_rows=124755,
            missing_guard_rows=2327,
            billing_guard_rows=869,
            service_content_rows=96298,
            requested_at=datetime(2026, 5, 13, 1, 30, tzinfo=timezone.utc),
            session=session,  # type: ignore[arg-type]
        )

        self.assertEqual(result.task_id, "task-1")
        self.assertEqual(result.url, "https://app.clickup.com/t/task-1")
        self.assertEqual(session.post_payloads[0]["assignees"], [306885786])
        self.assertTrue(session.post_payloads[0]["notify_all"])


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = ""
        self.reason = ""

    def json(self) -> dict[str, object]:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.post_payloads: list[dict[str, object]] = []

    def request(self, method: str, url: str, **kwargs: object) -> FakeResponse:
        if method == "GET" and url.endswith("/user"):
            return FakeResponse(200, {"user": {"id": 306885786}})
        if method == "POST" and "/list/901817301594/task" in url:
            payload = kwargs["json"]
            assert isinstance(payload, dict)
            self.post_payloads.append(payload)
            return FakeResponse(200, {"id": "task-1", "url": "https://app.clickup.com/t/task-1"})
        return FakeResponse(404, {"err": "not found"})


if __name__ == "__main__":
    unittest.main()
