from __future__ import annotations

import os
from pathlib import Path
from typing import Any


S2_USERNAME_KEYS = ("KLD_LOGIN_ID", "S2_ID", "IPS_ID", "KISS_ID", "KIPM_ID")
S2_PASSWORD_KEYS = ("KLD_LOGIN_PW", "S2_PW", "IPS_PW", "KISS_PW", "KIPM_PW")
S2_API_BASE_URL_KEYS = ("S2_API_BASE_URL", "KISS_API_BASE_URL")
S2_ACCESS_TOKEN_KEYS = ("S2_ACCESS_TOKEN", "S2_TOKEN", "KISS_ACCESS_TOKEN", "KISS_TOKEN", "KIPM_ACCESS_TOKEN")
S2_SECRET_SECTIONS = ("s2", "S2", "ips", "IPS", "auth", "credentials")
SECTION_USERNAME_KEYS = ("id", "user", "username", "login_id", "login")
SECTION_PASSWORD_KEYS = ("pw", "password", "login_pw")
SECTION_API_BASE_URL_KEYS = ("api_base_url", "base_url", "url")
SECTION_ACCESS_TOKEN_KEYS = ("access_token", "token", "bearer_token")

S2_AUTH_ERROR_MESSAGE = (
    "S2/IPS 접속 정보가 없습니다. 앱 사이드바에 S2 ID/PW를 입력하거나 "
    "로컬 .env, 환경변수, 또는 Streamlit Secrets에 ID/PW 또는 access token을 설정하세요."
)
S2_AUTH_FAILURE_HINT = (
    "S2 로그인 실패: S2 ID/PW가 틀렸거나 S2 API가 인증을 거부했습니다. "
    "입력값을 확인하세요."
)
S2_AUTH_FAILURE_TOKENS = (
    "s2 로그인 실패",
    "s2 인증 실패",
    "인증 토큰을 찾지 못했습니다",
    "http 401",
    "http 403",
    "unauthorized",
    "forbidden",
    "invalid credentials",
    "invalid token",
)


def read_env_file(path: str | Path) -> dict[str, str]:
    target = Path(path)
    if not target.exists():
        return {}

    values: dict[str, str] = {}
    for line in target.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        if key:
            values[key] = value.strip().strip("'\"")
    return values


def apply_env_file(path: str | Path, *, overwrite: bool = False) -> bool:
    values = read_env_file(path)
    for key, value in values.items():
        if overwrite or not os.environ.get(key):
            os.environ[key] = value
    return bool(values)


def first_config_value(config: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = config.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def first_env_value(*keys: str) -> str:
    return first_config_value(dict(os.environ), tuple(keys))


def has_s2_credentials(config: dict[str, Any]) -> bool:
    if first_config_value(config, S2_ACCESS_TOKEN_KEYS):
        return True
    return bool(first_config_value(config, S2_USERNAME_KEYS) and first_config_value(config, S2_PASSWORD_KEYS))


def looks_like_s2_auth_failure(raw_text: object) -> bool:
    normalized = str(raw_text or "").lower()
    return any(token in normalized for token in S2_AUTH_FAILURE_TOKENS)


def normalize_s2_login_values(username: object, password: object) -> dict[str, str]:
    username_text = str(username or "").strip()
    password_text = str(password or "").strip()
    if not username_text or not password_text:
        return {}
    return {"S2_ID": username_text, "S2_PW": password_text}


def normalize_s2_secret_values(secrets: object) -> dict[str, str]:
    values: dict[str, str] = {}

    _copy_exact_keys(values, secrets, S2_USERNAME_KEYS + S2_PASSWORD_KEYS + S2_API_BASE_URL_KEYS + S2_ACCESS_TOKEN_KEYS)
    _copy_section_alias(values, secrets)
    return values


def _copy_section_alias(values: dict[str, str], secrets: object) -> None:
    for section_name in S2_SECRET_SECTIONS:
        section = _mapping_value(secrets, section_name)
        if section is None:
            continue
        _copy_exact_keys(values, section, S2_USERNAME_KEYS + S2_PASSWORD_KEYS + S2_API_BASE_URL_KEYS + S2_ACCESS_TOKEN_KEYS)
        _copy_alias(values, "S2_ID", section, SECTION_USERNAME_KEYS)
        _copy_alias(values, "S2_PW", section, SECTION_PASSWORD_KEYS)
        _copy_alias(values, "S2_API_BASE_URL", section, SECTION_API_BASE_URL_KEYS)
        _copy_alias(values, "S2_ACCESS_TOKEN", section, SECTION_ACCESS_TOKEN_KEYS)


def _copy_exact_keys(values: dict[str, str], source: object, keys: tuple[str, ...]) -> None:
    for key in keys:
        value = _scalar_value(source, key)
        if value:
            values[key] = value


def _copy_alias(values: dict[str, str], target_key: str, source: object, keys: tuple[str, ...]) -> None:
    for key in keys:
        value = _scalar_value(source, key)
        if value:
            values[target_key] = value
            return


def _mapping_value(source: object, key: str) -> object | None:
    if not hasattr(source, "__getitem__"):
        return None
    try:
        value = source[key]  # type: ignore[index]
    except (KeyError, TypeError, AttributeError):
        return None
    return value if _looks_like_mapping(value) else None


def _scalar_value(source: object, key: str) -> str:
    if not hasattr(source, "__getitem__"):
        return ""
    try:
        value = source[key]  # type: ignore[index]
    except (KeyError, TypeError, AttributeError):
        return ""
    if value is None or _looks_like_mapping(value):
        return ""
    return str(value).strip()


def _looks_like_mapping(value: object) -> bool:
    return hasattr(value, "__getitem__") and hasattr(value, "keys")
