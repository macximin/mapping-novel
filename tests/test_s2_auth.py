from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from s2_auth import apply_env_file, has_s2_credentials, normalize_s2_login_values, normalize_s2_secret_values, read_env_file
from scripts import refresh_kiss_payment_settlement as refresh_script


class S2AuthTest(unittest.TestCase):
    def test_login_values_require_id_and_password(self) -> None:
        self.assertEqual(normalize_s2_login_values(" user ", " password "), {"S2_ID": "user", "S2_PW": "password"})
        self.assertEqual(normalize_s2_login_values("user", ""), {})
        self.assertEqual(normalize_s2_login_values("", "password"), {})

    def test_streamlit_root_secret_values_are_credentials(self) -> None:
        values = normalize_s2_secret_values(
            {
                "S2_ID": "cloud-user",
                "S2_PW": "cloud-password",
                "S2_API_BASE_URL": "https://s2.example.test",
            }
        )

        self.assertTrue(has_s2_credentials(values))
        self.assertEqual(values["S2_ID"], "cloud-user")
        self.assertEqual(values["S2_PW"], "cloud-password")
        self.assertEqual(values["S2_API_BASE_URL"], "https://s2.example.test")

    def test_streamlit_root_access_token_is_credential(self) -> None:
        values = normalize_s2_secret_values(
            {
                "S2_ACCESS_TOKEN": "cloud-token",
                "S2_API_BASE_URL": "https://s2.example.test",
            }
        )

        self.assertTrue(has_s2_credentials(values))
        self.assertEqual(values["S2_ACCESS_TOKEN"], "cloud-token")
        self.assertEqual(values["S2_API_BASE_URL"], "https://s2.example.test")

    def test_streamlit_section_secret_aliases_are_credentials(self) -> None:
        values = normalize_s2_secret_values(
            {
                "s2": {
                    "id": "section-user",
                    "pw": "section-password",
                    "api_base_url": "https://section.example.test",
                }
            }
        )

        self.assertTrue(has_s2_credentials(values))
        self.assertEqual(values["S2_ID"], "section-user")
        self.assertEqual(values["S2_PW"], "section-password")
        self.assertEqual(values["S2_API_BASE_URL"], "https://section.example.test")

    def test_streamlit_section_token_alias_is_credential(self) -> None:
        values = normalize_s2_secret_values(
            {
                "s2": {
                    "token": "section-token",
                    "api_base_url": "https://section.example.test",
                }
            }
        )

        self.assertTrue(has_s2_credentials(values))
        self.assertEqual(values["S2_ACCESS_TOKEN"], "section-token")
        self.assertEqual(values["S2_API_BASE_URL"], "https://section.example.test")

    def test_missing_env_file_is_allowed_when_env_has_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"S2_ID": "env-user", "S2_PW": "env-password"}, clear=True):
                applied = apply_env_file(Path(tmp) / "missing.env")

                self.assertFalse(applied)
                self.assertEqual(os.environ["S2_ID"], "env-user")

    def test_env_file_fills_missing_values_without_overwriting_cloud_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("S2_ID=local-user\nS2_PW=local-password\n", encoding="utf-8")

            self.assertEqual(read_env_file(env_path)["S2_ID"], "local-user")
            with patch.dict(os.environ, {"S2_ID": "cloud-user"}, clear=True):
                applied = apply_env_file(env_path, overwrite=False)

                self.assertTrue(applied)
                self.assertEqual(os.environ["S2_ID"], "cloud-user")
                self.assertEqual(os.environ["S2_PW"], "local-password")

    def test_authenticated_session_uses_environment_without_env_file(self) -> None:
        fake_session = FakeSession()
        with patch.dict(
            os.environ,
            {
                "S2_ID": "env-user",
                "S2_PW": "env-password",
                "S2_API_BASE_URL": "https://s2-api.example.test",
            },
            clear=True,
        ):
            with patch.object(refresh_script.requests, "Session", return_value=fake_session):
                session = refresh_script.create_authenticated_session()

        self.assertIs(session, fake_session)
        self.assertEqual(fake_session.post_url, "https://s2-api.example.test/user/login")
        self.assertEqual(fake_session.post_json, {"username": "env-user", "password": "env-password", "cprCd": "1000"})
        self.assertEqual(fake_session.headers["Authorization"], "Bearer header.payload.signature")

    def test_authenticated_session_skips_login_when_access_token_exists(self) -> None:
        fake_session = FakeSession()
        with patch.dict(
            os.environ,
            {
                "S2_ACCESS_TOKEN": "env-token",
                "S2_API_BASE_URL": "https://s2-api.example.test",
            },
            clear=True,
        ):
            with patch.object(refresh_script.requests, "Session", return_value=fake_session):
                session = refresh_script.create_authenticated_session()

        self.assertIs(session, fake_session)
        self.assertEqual(fake_session.post_url, "")
        self.assertEqual(fake_session.headers["Authorization"], "Bearer env-token")
        self.assertEqual(fake_session.headers["X-KISS-API-BASE-URL"], "https://s2-api.example.test")

    def test_access_token_allows_existing_bearer_prefix(self) -> None:
        self.assertEqual(refresh_script.bearer_authorization_value("Bearer existing-token"), "Bearer existing-token")


class FakeResponse:
    ok = True
    text = ""
    encoding = "utf-8"

    def json(self) -> dict[str, str]:
        return {"token": "header.payload.signature"}


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.post_url = ""
        self.post_json: dict[str, str] = {}

    def post(self, url: str, *, json: dict[str, str], timeout: int) -> FakeResponse:
        self.post_url = url
        self.post_json = json
        return FakeResponse()


if __name__ == "__main__":
    unittest.main()
