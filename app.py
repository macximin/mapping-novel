from __future__ import annotations

import calendar
import html
import io
import json
import os
import subprocess
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from batch_reports import build_combined_mapping_report_frame, build_pd_work_order_report_frame
from kiss_refresh_history import latest_refresh_runs, latest_s2_refresh_changes
from kiss_payment_settlement import load_payment_settlement_list, summarize_payment_settlement, to_s2_lookup
from cleaning_rules import drop_disabled_rows, text
from mapping_core import build_mapping, export_mapping, read_first_sheet
from matching_rules import (
    detect_s2_sales_channel,
    filter_s2_by_sales_channel,
    platform_for_s2_sales_channel,
    s2_filter_validation_rows,
    s2_sales_channel_to_platform,
)
from settlement_adapters import (
    adapter_audit_dataframe,
    adapter_blocking_messages,
    adapter_warning_messages,
    normalize_settlement,
    summarize_normalization,
)
from s2_transfer import build_s2_transfer, export_s2_transfer
from s2_reference_guards import (
    S2GuardFilterResult,
    S2ReferenceGuards,
    annotate_mapping_result,
    apply_missing_exclusions,
    load_s2_reference_guards,
)
from s2_auth import (
    S2_AUTH_ERROR_MESSAGE,
    S2_AUTH_FAILURE_HINT,
    S2_NETWORK_FAILURE_HINT,
    has_s2_credentials,
    looks_like_s2_auth_failure,
    looks_like_s2_network_failure,
    normalize_s2_login_values,
    normalize_s2_secret_values,
    read_env_file,
)


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
S2_SOURCE_LOOKUP = DATA_DIR / "kiss_payment_settlement_s2_lookup.csv"
S2_MISSING_LOOKUP = DATA_DIR / "s2_payment_missing_lookup.csv"
S2_BILLING_LOOKUP = DATA_DIR / "s2_billing_settlement_lookup.csv"
S2_SERVICE_CONTENTS_LOOKUP = DATA_DIR / "s2_sales_channel_content_lookup.csv"
S2_HISTORY_DB = DATA_DIR / "kiss_refresh_history.sqlite"
S2_BASELINE_SUMMARY_NAME = "kiss_payment_settlement_refresh_summary.json"
S2_REFRESH_SCRIPT = ROOT / "scripts" / "refresh_kiss_payment_settlement.py"
S2_GUARD_REFRESH_SCRIPT = ROOT / "scripts" / "refresh_s2_reference_guards.py"
S2_SERVICE_CONTENT_REFRESH_SCRIPT = ROOT / "scripts" / "refresh_s2_sales_channel_contents.py"
S2_ENV_FILE = ROOT / ".env"
S2_REFRESH_START_DATE = date(1900, 1, 1)
S2_FAST_PAGE_SIZE = "1000000"
S2_NOVEL_CONTENT_STYLE_CODE = "102"
S2_PAYMENT_MANAGEMENT_URL = "https://kiss.kld.kr/mst/stmi/pymt-setl"
S2_DAILY_REFRESH_TIME_LABEL = "매일 10:00"
KST = timezone(timedelta(hours=9))
AUTO_PLATFORM_OPTION = "엑셀 파일명으로 자동감지"
S2_SESSION_USERNAME_KEY = "s2_session_username"
S2_SESSION_PASSWORD_KEY = "s2_session_password"
S2_REMEMBER_ID_KEY = "s2_remember_id"
S2_ID_MEMORY_COMPONENT_KEY = "s2_id_memory"
S2_ID_MEMORY_CLEAR_COUNTER_KEY = "s2_id_memory_clear_counter"
S2_ID_MEMORY_STORAGE_KEY = "mapping_novel_s2_id"
SETTLEMENT_UPLOAD_RESET_COUNTER_KEY = "settlement_upload_reset_counter"
MAPPING_RESULT_STATE_KEY = "mapping_result_state"
S2_CHANNEL_SCHEMA_EXAMPLES = (
    "네이버_장르(광고수익)",
    "네이버_장르",
    "네이버_일반",
    "카카오페이지(소설)",
    "카카오페이지(선투자)",
    "블라이스_인앱결제",
    "블라이스_일반결제",
)
S2_CHANNEL_FILENAME_GUIDE = (
    "{S2정산플랫폼} 정보가 반드시 필요합니다. "
    "파일명에 S2에서 사용하는 판매채널명을 넣어 주세요. "
    f"예: {', '.join(S2_CHANNEL_SCHEMA_EXAMPLES)}. "
    "전체 목록은 아래 '판매채널명 스키마'를 펼쳐 확인하세요. "
    "예를 들어 '카카오 정산상세.xlsx'는 S2 스키마와 일치하지 않는 고맥락 워딩이라 사용할 수 없습니다."
)


S2_ID_MEMORY_COMPONENT = (
    st.components.v2.component(
        "mapping_novel_s2_id_memory",
        html="<div aria-hidden='true'></div>",
        js="""
            export default function(component) {
                const { data, setStateValue, parentElement } = component;
                parentElement.style.display = "none";

                const storageKey = data.storageKey;
                const clearSignal = Number(data.clearSignal || 0);
                const remember = data.remember !== false;
                const username = (data.username || "").trim();
                const clearSignalKey = `${storageKey}.lastClearSignal`;
                const lastClearSignal = Number(window.localStorage.getItem(clearSignalKey) || 0);

                if (clearSignal > 0 && clearSignal !== lastClearSignal) {
                    window.localStorage.setItem(clearSignalKey, String(clearSignal));
                    window.localStorage.removeItem(storageKey);
                }

                if (!remember) {
                    window.localStorage.removeItem(storageKey);
                } else if (username) {
                    window.localStorage.setItem(storageKey, username);
                }

                setStateValue("saved_id", window.localStorage.getItem(storageKey) || "");
            }
        """,
    )
    if hasattr(st.components, "v2")
    else None
)


def cache_metrics(path: Path) -> dict[str, int]:
    if not path.exists():
        return {"rows": 0, "sales_channel_content_id_nonblank": 0}
    frame = pd.read_csv(path, dtype=object)
    metrics = {"rows": len(frame), "sales_channel_content_id_nonblank": 0}
    if "판매채널콘텐츠ID" in frame.columns:
        metrics["sales_channel_content_id_nonblank"] = int(frame["판매채널콘텐츠ID"].map(str).str.strip().ne("").sum())
    return metrics


def lookup_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(pd.read_csv(path, dtype=object))


def streamlit_s2_secret_values() -> dict[str, str]:
    try:
        return normalize_s2_secret_values(st.secrets)
    except (FileNotFoundError, KeyError, RuntimeError):
        return {}


def session_s2_login_values() -> dict[str, str]:
    return normalize_s2_login_values(
        st.session_state.get(S2_SESSION_USERNAME_KEY),
        st.session_state.get(S2_SESSION_PASSWORD_KEY),
    )


def remember_s2_id_default() -> None:
    st.session_state.setdefault(S2_REMEMBER_ID_KEY, True)
    st.session_state.setdefault(S2_ID_MEMORY_CLEAR_COUNTER_KEY, 0)


def sync_browser_s2_id_memory() -> None:
    remember_s2_id_default()
    saved_id = s2_id_memory_state().get("saved_id", "")
    if saved_id and not text(st.session_state.get(S2_SESSION_USERNAME_KEY)):
        st.session_state[S2_SESSION_USERNAME_KEY] = saved_id

    if S2_ID_MEMORY_COMPONENT is None:
        return

    S2_ID_MEMORY_COMPONENT(
        data={
            "storageKey": S2_ID_MEMORY_STORAGE_KEY,
            "remember": bool(st.session_state.get(S2_REMEMBER_ID_KEY)),
            "username": text(st.session_state.get(S2_SESSION_USERNAME_KEY)),
            "clearSignal": safe_int(st.session_state.get(S2_ID_MEMORY_CLEAR_COUNTER_KEY)),
        },
        default={"saved_id": saved_id},
        key=S2_ID_MEMORY_COMPONENT_KEY,
        on_saved_id_change=lambda: None,
        height=0,
    )


def s2_id_memory_state() -> dict[str, str]:
    state = st.session_state.get(S2_ID_MEMORY_COMPONENT_KEY, {})
    if isinstance(state, dict):
        return {str(key): text(value) for key, value in state.items()}
    return {}


def clear_browser_s2_id_memory() -> None:
    st.session_state[S2_ID_MEMORY_CLEAR_COUNTER_KEY] = safe_int(
        st.session_state.get(S2_ID_MEMORY_CLEAR_COUNTER_KEY)
    ) + 1
    st.session_state.pop(S2_SESSION_USERNAME_KEY, None)


def s2_runtime_auth_config() -> dict[str, str]:
    config: dict[str, str] = {}
    config.update(read_env_file(S2_ENV_FILE))
    config.update(dict(os.environ))
    config.update(streamlit_s2_secret_values())
    config.update(session_s2_login_values())
    return config


def s2_refresh_environment() -> dict[str, str]:
    runtime_env = os.environ.copy()
    runtime_env.update(streamlit_s2_secret_values())
    runtime_env.update(session_s2_login_values())
    return runtime_env


def run_s2_refresh(mode: str, start_date: date | None = None, end_date: date | None = None) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(S2_REFRESH_SCRIPT),
        "--env-file",
        str(S2_ENV_FILE),
        "--mode",
        mode,
        "--lookup-only",
        "--page-size",
        S2_FAST_PAGE_SIZE,
        "--content-style-code",
        S2_NOVEL_CONTENT_STYLE_CODE,
    ]
    if mode == "custom" and start_date is not None and end_date is not None:
        command.extend(["--start-date", start_date.isoformat(), "--end-date", end_date.isoformat()])
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=900, env=s2_refresh_environment())


def run_s2_guard_refresh() -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(S2_GUARD_REFRESH_SCRIPT),
        "--env-file",
        str(S2_ENV_FILE),
        "--page-size",
        S2_FAST_PAGE_SIZE,
        "--content-style-code",
        S2_NOVEL_CONTENT_STYLE_CODE,
    ]
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=300, env=s2_refresh_environment())


def run_s2_service_content_refresh() -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(S2_SERVICE_CONTENT_REFRESH_SCRIPT),
        "--env-file",
        str(S2_ENV_FILE),
        "--content-style-code",
        S2_NOVEL_CONTENT_STYLE_CODE,
    ]
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=900, env=s2_refresh_environment())


def run_s2_auth_check() -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(S2_REFRESH_SCRIPT),
        "--env-file",
        str(S2_ENV_FILE),
        "--check-auth-only",
        "--auth-timeout",
        "10",
    ]
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=30, env=s2_refresh_environment())


def run_s2_full_replace() -> tuple[subprocess.CompletedProcess[str], str]:
    payment_completed = run_s2_refresh("full-replace")
    refresh_scope = f"{S2_REFRESH_START_DATE.isoformat()} ~ {date.today().isoformat()}"
    if payment_completed.returncode != 0:
        return payment_completed, refresh_scope

    guard_completed = run_s2_guard_refresh()
    service_content_completed = run_s2_service_content_refresh()
    combined = subprocess.CompletedProcess(
        args=[payment_completed.args, guard_completed.args, service_content_completed.args],
        returncode=guard_completed.returncode or service_content_completed.returncode,
        stdout=f"{payment_completed.stdout}\n{guard_completed.stdout}\n{service_content_completed.stdout}",
        stderr=f"{payment_completed.stderr}\n{guard_completed.stderr}\n{service_content_completed.stderr}",
    )
    return combined, f"{refresh_scope} + 누락/청구/판매채널콘텐츠 보조 기준"


def s2_refresh_error_message(completed: subprocess.CompletedProcess[str], refresh_scope: str) -> str:
    output = f"{completed.stdout}\n{completed.stderr}"
    detail = " ".join(str(output or "").split())[:300]
    if looks_like_s2_network_failure(output):
        message = f"{S2_NETWORK_FAILURE_HINT} API 다운로드를 시작하지 않았습니다. ({refresh_scope})"
        if detail:
            return f"{message} 원인 로그: {detail}"
        return message
    if looks_like_s2_auth_failure(output):
        return f"{S2_AUTH_FAILURE_HINT} API 다운로드를 진행하지 못했습니다. ({refresh_scope})"
    if refresh_scope == "로그인 확인":
        if detail:
            return f"S2 로그인 확인 실패: API 다운로드를 시작하지 않았습니다. ({refresh_scope}) 원인 로그: {detail}"
        return f"S2 로그인 확인 실패: API 다운로드를 시작하지 않았습니다. ({refresh_scope})"
    return f"S2 기준 전체 교체 실패: {refresh_scope}"


def ui_safe_refresh_log(raw_text: str) -> str:
    replacements = (
        ("kiss_payment_settlement", "s2_source"),
        ("payment_settlement", "s2_source"),
        ("kiss_refresh", "s2_refresh"),
        ("kiss_api", "s2_api"),
        ("KISS_API_BASE_URL", "S2_API_BASE_URL"),
        ("KIPM", "IPS"),
        ("KISS", "S2"),
        ("kiss", "s2"),
        ("pymt-setl", "s2-source"),
        ("pymtSetl", "s2Source"),
        ("cache_rows", "local_s2_rows"),
        ("cache=", "local_s2="),
        ("cache_path", "local_s2_path"),
        ("지급 정산 관리 목록", "S2 원천 목록"),
        ("지급 정산", "S2"),
        ("지급정산", "S2"),
    )
    safe_text = raw_text
    for old, new in replacements:
        safe_text = safe_text.replace(old, new)
    return safe_text


def load_manual_s2_reference(uploaded_file: object) -> tuple[pd.DataFrame, str, dict[str, object] | None]:
    try:
        payment_df = load_payment_settlement_list(uploaded_file)
        return to_s2_lookup(payment_df), "수동 S2 원천 엑셀", summarize_payment_settlement(payment_df)
    except Exception:
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(0)
        return drop_disabled_rows(read_first_sheet(uploaded_file)), "수동 S2 기준 리스트", None


def history_frame(limit: int = 10) -> pd.DataFrame:
    rows = latest_refresh_runs(S2_HISTORY_DB, limit=limit)
    if not rows:
        return pd.DataFrame()
    columns = [
        "id",
        "status",
        "mode",
        "search_start_date",
        "search_end_date",
        "api_total_rows",
        "fetched_rows",
        "fetched_pages",
        "cache_rows_after",
        "s2_lookup_rows",
        "s2_change_added",
        "s2_change_deleted",
        "s2_change_modified",
        "sales_channel_content_id_unique",
        "finished_at",
    ]
    frame = pd.DataFrame(rows)[[column for column in columns if column in rows[0]]]
    frame = frame.rename(
        columns={
            "id": "ID",
            "status": "상태",
            "mode": "조회범위",
            "search_start_date": "시작일",
            "search_end_date": "종료일",
            "api_total_rows": "S2 조회 행",
            "fetched_rows": "S2 수집 행",
            "fetched_pages": "S2 페이지",
            "cache_rows_after": "S2 저장 행",
            "s2_lookup_rows": "S2 기준 행",
            "s2_change_added": "신규",
            "s2_change_deleted": "삭제",
            "s2_change_modified": "변경",
            "sales_channel_content_id_unique": "S2 ID 고유값",
            "finished_at": "종료시각",
        }
    )
    if "조회범위" in frame.columns:
        frame["조회범위"] = frame["조회범위"].map(
            {
                "full-replace": "전체 교체",
                "initial": "전체 교체",
                "custom": "지정 범위 전체 교체",
                "rolling-3m": "과거 기록(부분 조회)",
            }
        ).fillna(frame["조회범위"])
    return frame


def repo_s2_baseline_summary() -> dict[str, Any]:
    doc_dir = ROOT / "doc"
    if not doc_dir.exists():
        return {}
    for path in sorted(doc_dir.glob(f"*/{S2_BASELINE_SUMMARY_NAME}"), reverse=True):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        summary = payload.get("summary")
        if isinstance(summary, dict):
            return {"path": str(path.relative_to(ROOT)), "payload": payload, "summary": summary}
    return {}


def git_commit_time_for_path(relative_path: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", relative_path],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if completed.returncode != 0:
        return ""
    return text(completed.stdout).splitlines()[0] if text(completed.stdout) else ""


def format_update_timestamp(value: object) -> str:
    raw = text(value)
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return raw.replace("T", " ")[:16]


def file_mtime_timestamp(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    except OSError:
        return ""


def repo_s2_baseline_updated_at(repo_baseline: dict[str, Any]) -> str:
    if not repo_baseline:
        return ""
    relative_path = text(repo_baseline.get("path"))
    commit_time = git_commit_time_for_path(relative_path) if relative_path else ""
    if commit_time:
        return format_update_timestamp(commit_time)

    payload = repo_baseline.get("payload") or {}
    summary = repo_baseline.get("summary") or {}
    for candidate in (
        payload.get("finished_at"),
        payload.get("created_at"),
        summary.get("generated_at"),
        summary.get("registered_at_max"),
    ):
        formatted = format_update_timestamp(candidate)
        if formatted:
            return formatted

    if relative_path:
        formatted = file_mtime_timestamp(ROOT / relative_path)
        if formatted:
            return formatted
    return ""


def parse_display_timestamp_date(value: object) -> date | None:
    raw = text(value)
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def s2_usage_status(updated_at: str, row_count: int) -> tuple[str, str]:
    if row_count <= 0:
        return "확인 필요", "warn"
    updated_date = parse_display_timestamp_date(updated_at)
    if updated_date == datetime.now(KST).date():
        return "사용 가능", "ok"
    return "확인 필요", "warn"


def s2_source_summary_frame(summary: dict[str, object]) -> pd.DataFrame:
    rows: list[tuple[str, object]] = []

    def add(label: str, value: object) -> None:
        if value not in (None, "", {}):
            rows.append((label, value))

    registered_min = text(summary.get("registered_at_min"))
    registered_max = text(summary.get("registered_at_max"))
    add("S2 원천 행 수", summary.get("rows"))
    add("판매채널콘텐츠ID 고유값", summary.get("sales_channel_content_id_unique"))
    add("콘텐츠ID 고유값", summary.get("content_id_unique"))
    if registered_min or registered_max:
        add("등록일 범위", f"{registered_min or '-'} ~ {registered_max or '-'}")
    add("동일 판매채널콘텐츠ID 중복 키", summary.get("sales_channel_content_id_duplicate_keys"))
    add("콘텐츠명 변경 후보", summary.get("sales_channel_content_id_multiple_titles"))
    add("S2 마스터ID 변경 후보", summary.get("sales_channel_content_id_multiple_master_ids"))
    add("콘텐츠ID 변경 후보", summary.get("sales_channel_content_id_multiple_content_ids"))
    add("콘텐츠형태", format_counts(summary.get("content_shape_counts")))
    add("승인상태", format_counts(summary.get("approval_status_counts")))
    add("상위 판매채널", format_counts(summary.get("top_channel_counts"), limit=10))
    return pd.DataFrame(rows, columns=["항목", "값"])


def s2_change_detail_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    columns = {
        "change_type": "변경유형",
        "sales_channel_content_id": "판매채널콘텐츠ID",
        "changed_fields": "변경필드",
        "old_content_name": "이전_콘텐츠명",
        "new_content_name": "신규_콘텐츠명",
        "old_s2_master_id": "이전_S2 마스터ID",
        "new_s2_master_id": "신규_S2 마스터ID",
        "old_content_id": "이전_콘텐츠ID",
        "new_content_id": "신규_콘텐츠ID",
        "old_author_info": "이전_작가정보",
        "new_author_info": "신규_작가정보",
    }
    frame = frame[[column for column in columns if column in frame.columns]].rename(columns=columns)
    if "변경유형" in frame.columns:
        frame["변경유형"] = frame["변경유형"].map({"added": "신규", "deleted": "삭제", "modified": "변경"}).fillna(frame["변경유형"])
    if "변경필드" in frame.columns:
        frame["변경필드"] = frame["변경필드"].map(lambda value: text(value).replace("S2마스터ID", "S2 마스터ID"))
    return frame


def format_counts(value: object, *, limit: int = 8) -> str:
    if not isinstance(value, dict) or not value:
        return ""
    parts = [f"{key}: {count}" for key, count in list(value.items())[:limit]]
    return " | ".join(parts)


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def admin_s2_refresh_window(month_anchor: date) -> tuple[date, date]:
    month_start = month_anchor.replace(day=1)
    previous_month = add_months(month_start, -1)
    previous_month_last = date(
        previous_month.year,
        previous_month.month,
        calendar.monthrange(previous_month.year, previous_month.month)[1],
    )
    return previous_month_last - timedelta(days=7), month_start.replace(day=7)


def next_admin_s2_refresh_window(today: date) -> tuple[date, date, bool]:
    current_start, current_end = admin_s2_refresh_window(today)
    if today <= current_end:
        return current_start, current_end, current_start <= today <= current_end
    next_start, next_end = admin_s2_refresh_window(add_months(today, 1))
    return next_start, next_end, next_start <= today <= next_end


def render_error_detail(exc: Exception) -> None:
    with st.expander("오류 상세", expanded=False):
        st.exception(exc)


def sanitize_output_stem(raw_name: object) -> str:
    file_name = text(raw_name)
    if file_name.lower().endswith(".xlsx"):
        file_name = file_name[:-5]
    return "".join(ch if ch not in r'\/:*?"<>|' else "_" for ch in file_name).strip()


def default_mapping_stem(uploaded_file: object) -> str:
    name = text(getattr(uploaded_file, "name", "")) or text(uploaded_file) or "mapping_result"
    return sanitize_output_stem(f"{Path(name).stem}_매핑") or f"mapping_result_{datetime.now().strftime('%Y%m%d_%H%M')}"


def s2_channel_for_file(uploaded_file: object, selected_s2_channel: str) -> str:
    if selected_s2_channel != AUTO_PLATFORM_OPTION:
        return selected_s2_channel
    detection = detect_s2_sales_channel(text(getattr(uploaded_file, "name", "")))
    return detection.sales_channel if detection else ""


def effective_platform_for_file(uploaded_file: object, selected_s2_channel: str) -> str:
    s2_channel = s2_channel_for_file(uploaded_file, selected_s2_channel)
    return platform_for_s2_sales_channel(s2_channel) or ""


def upload_detection_rows(settlement_files: list[object], selected_s2_channel: str) -> tuple[pd.DataFrame, list[str]]:
    rows: list[dict[str, str]] = []
    undetected_files: list[str] = []
    manual_mode = selected_s2_channel != AUTO_PLATFORM_OPTION
    for uploaded_file in settlement_files:
        file_name = text(getattr(uploaded_file, "name", ""))
        filename_detection = detect_s2_sales_channel(file_name)
        detected_channel = s2_channel_for_file(uploaded_file, selected_s2_channel)
        effective_platform = effective_platform_for_file(uploaded_file, selected_s2_channel)
        if not detected_channel or not effective_platform:
            undetected_files.append(file_name)
        if manual_mode:
            status = "직접 선택 적용"
        elif detected_channel and effective_platform:
            status = "정상"
        else:
            status = "수동 선택 필요"
        rows.append(
            {
                "파일명": file_name,
                "감지된 판매채널": filename_detection.sales_channel if filename_detection else "감지 실패",
                "처리 판매채널": detected_channel or "",
                "상태": status,
            }
        )
    return pd.DataFrame(rows, columns=["파일명", "감지된 판매채널", "처리 판매채널", "상태"]), undetected_files


def mapping_readiness_frame(
    *,
    settlement_file_count: int,
    has_s2_source: bool,
    all_platforms_ready: bool,
    selected_s2_channel: str,
) -> pd.DataFrame:
    if selected_s2_channel != AUTO_PLATFORM_OPTION and settlement_file_count:
        channel_status = "직접 선택 적용"
        channel_note = selected_s2_channel
    elif not settlement_file_count:
        channel_status = "대기 중"
        channel_note = "정산서를 업로드하면 파일명에서 감지합니다."
    elif all_platforms_ready:
        channel_status = "정상"
        channel_note = "파일명 기반 자동 감지 완료"
    else:
        channel_status = "확인 필요"
        channel_note = "감지 실패 파일은 파일명을 고치거나 판매채널을 직접 선택하세요."

    return pd.DataFrame(
        [
            {
                "항목": "S2 기준 데이터",
                "상태": "정상" if has_s2_source else "필요",
                "비고": "관리자 배포 기준 또는 수동 S2 기준 사용 가능" if has_s2_source else "관리자에게 최신화를 요청하거나 수동 S2 파일을 업로드하세요.",
            },
            {
                "항목": "정산서 엑셀 업로드",
                "상태": "정상" if settlement_file_count else "필요",
                "비고": f"{settlement_file_count:,}개 업로드됨" if settlement_file_count else "정산서 .xlsx 파일을 1개 이상 업로드하세요.",
            },
            {"항목": "판매채널 감지", "상태": channel_status, "비고": channel_note},
        ]
    )


def s2_channel_schema_frame() -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for s2_channel, platform in sorted(s2_sales_channel_to_platform().items(), key=lambda item: (item[1], item[0])):
        rows.append(
            {
                "업무명": platform,
                "파일명에 넣을 S2 판매채널명": s2_channel,
                "파일명 예시": f"2026-04_{s2_channel}_정산상세.xlsx",
            }
        )
    return pd.DataFrame(rows)


def load_selected_s2_basis(
    *,
    use_payment_cache: bool,
    payment_settlement_file: object | None,
    s2_file: object | None,
) -> tuple[pd.DataFrame, str, dict[str, Any] | None, S2ReferenceGuards, S2GuardFilterResult]:
    guards = load_s2_reference_guards(
        missing_path=S2_MISSING_LOOKUP,
        billing_path=S2_BILLING_LOOKUP,
        service_contents_path=S2_SERVICE_CONTENTS_LOOKUP,
    )
    if payment_settlement_file is not None:
        payment_df = load_payment_settlement_list(payment_settlement_file)
        guard_result = apply_missing_exclusions(to_s2_lookup(payment_df), guards)
        label = guarded_s2_source_label("수동 S2 원천 엑셀", guards, guard_result)
        return guard_result.frame, label, summarize_payment_settlement(payment_df), guards, guard_result
    if use_payment_cache:
        guard_result = apply_missing_exclusions(pd.read_csv(S2_SOURCE_LOOKUP, dtype=object), guards)
        label = guarded_s2_source_label("관리자 배포 S2 기준", guards, guard_result)
        return guard_result.frame, label, None, guards, guard_result
    s2_df, s2_source_label, payment_summary = load_manual_s2_reference(s2_file)
    guard_result = apply_missing_exclusions(s2_df, guards)
    label = guarded_s2_source_label(s2_source_label, guards, guard_result)
    return guard_result.frame, label, payment_summary, guards, guard_result


def guarded_s2_source_label(source_label: str, guards: S2ReferenceGuards, guard_result: S2GuardFilterResult) -> str:
    parts: list[str] = []
    if len(guards.missing):
        parts.append(f"누락 {guard_result.excluded_count:,} 제외")
    if len(guards.billing):
        parts.append(f"청구 {len(guards.billing):,} 보조")
    return f"{source_label} ({', '.join(parts)})" if parts else source_label


def uploaded_file_token(uploaded_file: object | None) -> dict[str, object] | None:
    if uploaded_file is None:
        return None
    return {
        "name": text(getattr(uploaded_file, "name", "")),
        "size": safe_int(getattr(uploaded_file, "size", 0)),
    }


def mapping_run_signature(
    *,
    settlement_files: list[object],
    selected_s2_channel: str,
    use_payment_cache: bool,
    payment_settlement_file: object | None,
    s2_file: object | None,
    single_output_name: str,
) -> str:
    payload = {
        "settlement_files": [uploaded_file_token(file) for file in settlement_files],
        "selected_s2_channel": selected_s2_channel,
        "use_payment_cache": use_payment_cache,
        "payment_settlement_file": uploaded_file_token(payment_settlement_file),
        "s2_file": uploaded_file_token(s2_file),
        "single_output_name": text(single_output_name),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def s2_id_nonblank_count(frame: pd.DataFrame) -> int:
    if "판매채널콘텐츠ID" not in frame.columns:
        return 0
    return int(frame["판매채널콘텐츠ID"].map(str).str.strip().ne("").sum())


def build_mapping_session_state(
    *,
    signature: str,
    results: list[dict[str, Any]],
    s2_df: pd.DataFrame,
    s2_source_label: str,
    payment_summary: dict[str, object] | None,
) -> dict[str, Any]:
    summary_frame = batch_summary_frame(results)
    work_order_frame = build_pd_work_order_report_frame(results)
    combined_report_frame = build_combined_mapping_report_frame(results)
    zip_name = f"mapping_results_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"
    return {
        "signature": signature,
        "s2_source_label": s2_source_label,
        "s2_rows": len(s2_df),
        "s2_id_rows": s2_id_nonblank_count(s2_df),
        "payment_summary": payment_summary,
        "summary_frame": summary_frame,
        "work_order_csv_bytes": work_order_frame.to_csv(index=False).encode("utf-8-sig"),
        "combined_csv_bytes": combined_report_frame.to_csv(index=False).encode("utf-8-sig"),
        "work_order_empty": work_order_frame.empty,
        "combined_report_empty": combined_report_frame.empty,
        "zip_name": zip_name,
        "zip_bytes": build_batch_zip(results, summary_frame, work_order_frame, combined_report_frame),
    }


def process_settlement_batch_item(
    *,
    settlement_file: object,
    selected_s2_channel: str,
    s2_df: pd.DataFrame,
    s2_guards: S2ReferenceGuards,
    s2_guard_filter: S2GuardFilterResult,
    master_df: pd.DataFrame | None,
    output_stem: str,
) -> dict[str, Any]:
    source_name = text(getattr(settlement_file, "name", "uploaded.xlsx"))
    s2_channel = s2_channel_for_file(settlement_file, selected_s2_channel)
    effective_platform = effective_platform_for_file(settlement_file, selected_s2_channel)
    result: dict[str, Any] = {
        "source_name": source_name,
        "output_stem": output_stem,
        "platform": effective_platform,
        "s2_sales_channel": s2_channel,
        "status": "failed",
        "error": "",
        "blocking_messages": [],
        "warning_messages": [],
        "info_messages": [],
        "mapping_bytes": b"",
        "transfer_bytes": b"",
    }

    try:
        if not s2_channel or not effective_platform:
            result["status"] = "blocked"
            result["error"] = S2_CHANNEL_FILENAME_GUIDE
            return result
        if hasattr(settlement_file, "seek"):
            settlement_file.seek(0)
        adapter_result = normalize_settlement(
            settlement_file,
            platform=effective_platform,
            source_name=source_name,
        )
        adapter_summary = summarize_normalization(adapter_result)
        audit_df = adapter_audit_dataframe(adapter_result)
        blocking_messages = adapter_blocking_messages(adapter_result)
        warning_messages = adapter_warning_messages(adapter_result)
        info_messages: list[str] = []
        result.update(
            {
                "adapter_result": adapter_result,
                "adapter_summary": adapter_summary,
                "audit_df": audit_df,
                "blocking_messages": blocking_messages,
                "warning_messages": warning_messages,
                "info_messages": info_messages,
            }
        )
        if blocking_messages:
            result["status"] = "blocked"
            result["error"] = " | ".join(blocking_messages)
            return result

        settlement_df = adapter_result.to_mapping_feed()
        s2_channel_filter = filter_s2_by_sales_channel(s2_df, sales_channel=s2_channel, source_name=source_name)
        result["s2_channel_filter"] = s2_channel_filter
        if s2_channel_filter.active and s2_channel_filter.after_rows == 0:
            warning_messages.append(s2_channel_filter.message())
        elif s2_channel_filter.active:
            info_messages.append(s2_channel_filter.message())
        elif s2_channel_filter.reason:
            warning_messages.append(s2_channel_filter.reason)
        mapping = build_mapping(s2_channel_filter.frame, settlement_df, master_df)
        mapping = annotate_mapping_result(
            mapping,
            s2_guards,
            sales_channel=s2_channel,
            s2_all_frame=s2_df,
            master_df=master_df,
        )
        filter_validation = s2_filter_validation_rows(s2_channel_filter)
        if not filter_validation.empty:
            mapping.input_validation = pd.concat([filter_validation, mapping.input_validation], ignore_index=True)
        if not s2_guard_filter.input_validation.empty:
            mapping.input_validation = pd.concat([s2_guard_filter.input_validation, mapping.input_validation], ignore_index=True)
        summary = dict(zip(mapping.summary["항목"], mapping.summary["값"]))
        summary["S2 정산정보 누락 제외 행 수"] = s2_guard_filter.excluded_count
        if s2_channel_filter.active:
            summary["S2 필터 전 행 수"] = s2_channel_filter.before_rows
            summary["S2 필터 후 행 수"] = s2_channel_filter.after_rows
        s2_transfer = build_s2_transfer(
            mapping.rows,
            amount_policy_locked=adapter_result.spec.s2_amount_policy_locked,
            s2_gate=adapter_result.spec.s2_gate,
        )
        result.update(
            {
                "status": "success",
                "mapping": mapping,
                "summary": summary,
                "s2_transfer": s2_transfer,
                "mapping_bytes": export_mapping(mapping),
                "transfer_bytes": export_s2_transfer(s2_transfer) if s2_transfer.exportable else b"",
            }
        )
        return result
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def batch_summary_frame(results: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for result in results:
        adapter_summary = result.get("adapter_summary", {})
        mapping_summary = result.get("summary", {})
        transfer = result.get("s2_transfer")
        transfer_exportable = "Y" if getattr(transfer, "exportable", False) else "N"
        rows.append(
            {
                "파일": result.get("source_name", ""),
                "상태": result.get("status", ""),
                "S2 판매채널": result.get("s2_sales_channel", ""),
                "플랫폼": result.get("platform", ""),
                "원본 파싱 행": adapter_summary.get("parsed_rows", ""),
                "S2 매핑 입력 행": adapter_summary.get("default_feed_rows", ""),
                "S2 matched": mapping_summary.get("S2 matched", ""),
                "S2 필터": (
                    f"{mapping_summary.get('S2 필터 전 행 수'):,} -> {mapping_summary.get('S2 필터 후 행 수'):,}"
                    if mapping_summary.get("S2 필터 전 행 수") not in (None, "")
                    else ""
                ),
                "검토필요": mapping_summary.get("검토필요 행 수", ""),
                "누락 후보": mapping_summary.get("S2 정산정보 누락 후보", ""),
                "청구 후보": mapping_summary.get("청구정산 후보", ""),
                "누락 제외": mapping_summary.get("S2 정산정보 누락 제외 행 수", ""),
                "S2 전송자료": transfer_exportable,
                "메시지": result.get("error", ""),
            }
        )
    return pd.DataFrame(rows)


def unique_archive_name(name: str, used_names: set[str]) -> str:
    candidate = name
    path = Path(name)
    suffix = path.suffix
    stem = path.stem
    counter = 2
    while candidate in used_names:
        candidate = f"{stem}_{counter}{suffix}"
        counter += 1
    used_names.add(candidate)
    return candidate


def build_batch_zip(
    results: list[dict[str, Any]],
    summary_frame: pd.DataFrame,
    work_order_frame: pd.DataFrame | None = None,
    combined_report_frame: pd.DataFrame | None = None,
) -> bytes:
    buffer = io.BytesIO()
    used_names: set[str] = set()
    work_order_frame = work_order_frame if work_order_frame is not None else build_pd_work_order_report_frame(results)
    combined_report_frame = (
        combined_report_frame if combined_report_frame is not None else build_combined_mapping_report_frame(results)
    )
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            unique_archive_name("batch_summary.csv", used_names),
            summary_frame.to_csv(index=False).encode("utf-8-sig"),
        )
        archive.writestr(
            unique_archive_name("PD_작업지시_종합리포트.csv", used_names),
            work_order_frame.to_csv(index=False).encode("utf-8-sig"),
        )
        archive.writestr(
            unique_archive_name("전체_행별매핑_종합.csv", used_names),
            combined_report_frame.to_csv(index=False).encode("utf-8-sig"),
        )
        for result in results:
            output_stem = text(result.get("output_stem")) or default_mapping_stem(result.get("source_name", "mapping_result"))
            mapping_bytes = result.get("mapping_bytes") or b""
            transfer_bytes = result.get("transfer_bytes") or b""
            if mapping_bytes:
                archive.writestr(unique_archive_name(f"{output_stem}.xlsx", used_names), mapping_bytes)
            if transfer_bytes:
                archive.writestr(unique_archive_name(f"{output_stem}_S2전송자료.xlsx", used_names), transfer_bytes)
            if result.get("status") != "success":
                error_text = text(result.get("error")) or "처리하지 못했습니다."
                archive.writestr(unique_archive_name(f"{output_stem}_오류.txt", used_names), error_text.encode("utf-8"))
    return buffer.getvalue()


def inject_compact_layout_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            --app-font-family: "Pretendard", "Noto Sans KR", "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif;
        }
        html,
        body,
        .stApp,
        .stApp *,
        button,
        input,
        textarea,
        select {
            font-family: var(--app-font-family) !important;
            letter-spacing: 0;
        }
        [data-testid="stIconMaterial"] {
            direction: ltr;
            display: inline-block;
            font-family: "Material Symbols Rounded" !important;
            font-feature-settings: "liga";
            font-style: normal;
            font-weight: 400;
            letter-spacing: normal;
            line-height: 1;
            text-transform: none;
            white-space: nowrap;
            word-wrap: normal;
            -webkit-font-feature-settings: "liga";
            -webkit-font-smoothing: antialiased;
        }
        .block-container {
            padding-top: 3.4rem !important;
            padding-bottom: 1.25rem;
            max-width: 1280px;
        }
        .app-title {
            display: block;
            color: #111827;
            font-size: 2rem;
            font-weight: 700;
            line-height: 1.45;
            margin: 0 0 0.2rem 0;
            padding: 0.2rem 0 0.05rem 0;
            overflow: visible;
        }
        h2, h3 {
            margin-top: 0.65rem !important;
            margin-bottom: 0.35rem !important;
        }
        div[data-testid="stVerticalBlock"] {
            gap: 0.45rem;
        }
        div[data-testid="stHorizontalBlock"] {
            gap: 0.7rem;
        }
        div[data-testid="stAlert"] {
            padding: 0.5rem 0.75rem;
        }
        section[data-testid="stSidebar"] div[data-testid="stMetricValue"] {
            font-size: 1.55rem;
            line-height: 1.15;
        }
        section[data-testid="stSidebar"] div[data-testid="stMetricLabel"] {
            font-size: 0.72rem;
        }
        .workflow-caption {
            color: #6b7280;
            font-size: 0.78rem;
            margin: 0.25rem 0 1.4rem 0;
        }
        .section-kicker {
            color: #6b7280;
            font-size: 0.82rem;
            margin: -0.1rem 0 0.5rem 0;
        }
        .example-strip {
            align-items: center;
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin: 0.25rem 0 0.85rem 0;
        }
        .example-strip-label {
            color: #6b7280;
            font-size: 0.78rem;
            margin-right: 0.1rem;
        }
        .example-chip {
            background: #f3f4f6;
            border: 1px solid #e5e7eb;
            border-radius: 0.35rem;
            color: #374151;
            display: inline-block;
            font-size: 0.74rem;
            line-height: 1.2;
            padding: 0.22rem 0.38rem;
        }
        .upload-status-strip {
            display: grid;
            gap: 0.55rem;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            margin: 0.15rem 0 0.35rem 0;
        }
        .upload-status-item {
            background: #f9fafb;
            border: 1px solid #e5e7eb;
            border-radius: 0.45rem;
            min-height: 3.6rem;
            padding: 0.55rem 0.7rem;
        }
        .upload-status-label {
            color: #6b7280;
            display: block;
            font-size: 0.72rem;
            line-height: 1.2;
            margin-bottom: 0.25rem;
        }
        .upload-status-value {
            color: #111827;
            display: block;
            font-size: 0.98rem;
            font-weight: 650;
            line-height: 1.25;
        }
        @media (max-width: 760px) {
            .upload-status-strip {
                grid-template-columns: 1fr;
            }
        }
        .sidebar-mini-notice {
            border-radius: 0.45rem;
            font-size: 0.55rem;
            line-height: 1.35;
            margin: 0.2rem 0 0.45rem 0;
            padding: 0.4rem 0.5rem;
        }
        .sidebar-mini-info {
            background: #eff6ff;
            color: #1d4ed8;
        }
        .sidebar-mini-warning {
            background: #fff7ed;
            color: #9a3412;
        }
        .sidebar-status-card {
            background: #f8fafc;
            border: 1px solid #e5e7eb;
            border-radius: 0.55rem;
            margin: 0.25rem 0 0.85rem 0;
            padding: 0.75rem 0.8rem;
        }
        .sidebar-status-label {
            color: #6b7280;
            font-size: 0.7rem;
            line-height: 1.2;
            margin-bottom: 0.25rem;
        }
        .sidebar-status-time {
            color: #111827;
            font-size: 1.18rem;
            font-weight: 760;
            line-height: 1.2;
            word-break: keep-all;
        }
        .sidebar-status-badge {
            border-radius: 999px;
            display: inline-block;
            font-size: 0.72rem;
            font-weight: 720;
            line-height: 1;
            margin-top: 0.55rem;
            padding: 0.32rem 0.52rem;
        }
        .sidebar-status-ok {
            background: #ecfdf5;
            color: #047857;
        }
        .sidebar-status-warn {
            background: #fff7ed;
            color: #c2410c;
        }
        div[data-testid="stFileUploader"] {
            min-width: 0;
        }
        div[data-testid="stFileUploader"] label {
            margin-bottom: 0.2rem;
        }
        section[data-testid="stFileUploaderDropzone"] {
            min-height: 4.35rem;
            padding: 0.65rem 0.9rem;
            overflow: hidden;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.25rem;
            border-color: #cbd5e1;
            border-style: dashed;
            border-radius: 0.45rem;
            background: #fafafa;
        }
        section[data-testid="stFileUploaderDropzone"]::before {
            content: "정산서 엑셀 끌어오기";
            color: #4b5563;
            font-size: 0.86rem;
            pointer-events: none;
        }
        section[data-testid="stFileUploaderDropzone"] > div {
            gap: 0.35rem;
        }
        section[data-testid="stFileUploaderDropzone"] button {
            min-height: 1.6rem;
            width: 3.8rem;
            padding: 0;
            border: 0;
            background: transparent;
            box-shadow: none;
            color: transparent;
            position: relative;
            white-space: nowrap;
        }
        section[data-testid="stFileUploaderDropzone"] button:hover,
        section[data-testid="stFileUploaderDropzone"] button:focus {
            background: transparent;
            border: 0;
            box-shadow: none;
        }
        section[data-testid="stFileUploaderDropzone"] button::after {
            content: "파일선택";
            position: absolute;
            left: 0;
            top: 50%;
            transform: translateY(-50%);
            color: #1f2937;
            font-weight: 600;
            text-decoration: underline;
        }
        section[data-testid="stFileUploaderDropzone"] small,
        div[data-testid="stFileUploaderDropzoneInstructions"] {
            display: none;
        }
        div[data-testid="stFileUploaderFile"] {
            min-height: 2rem;
            padding: 0.2rem 0.5rem;
            margin-top: 0.2rem;
        }
        div[data-testid="stFileUploaderFile"] p,
        div[data-testid="stFileUploaderFile"] span {
            max-width: 100%;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        div[data-testid="stFileUploader"] > div:last-child {
            max-height: 6.25rem;
            overflow: auto;
        }
        .upload-reset-caption {
            color: #6b7280;
            font-size: 0.78rem;
            line-height: 1.35;
            margin: -0.15rem 0 0.65rem 0;
            white-space: nowrap;
        }
        .upload-reset-spacer {
            height: 0.72rem;
        }
        @media (max-width: 760px) {
            .upload-reset-caption {
                line-height: 1.35;
                white-space: normal;
            }
            .upload-reset-spacer {
                height: 0;
            }
        }
        div[data-testid="stExpander"] details {
            padding-top: 0.15rem;
        }
        div[data-testid="stExpander"] summary {
            min-height: 2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_mini_notice(message: str, *, tone: str = "info") -> None:
    class_name = "sidebar-mini-warning" if tone == "warning" else "sidebar-mini-info"
    st.markdown(
        f'<div class="sidebar-mini-notice {class_name}">{html.escape(message)}</div>',
        unsafe_allow_html=True,
    )


def render_s2_status_card(updated_at: str, usage_label: str, usage_tone: str) -> None:
    badge_class = "sidebar-status-ok" if usage_tone == "ok" else "sidebar-status-warn"
    timestamp = html.escape(updated_at or "확인 필요")
    st.markdown(
        f"""
        <div class="sidebar-status-card">
          <div class="sidebar-status-label">S2 기준 업데이트</div>
          <div class="sidebar-status-time">{timestamp}</div>
          <span class="sidebar-status-badge {badge_class}">{html.escape(usage_label)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_upload_examples() -> None:
    examples = (
        "네이버_장르_2026-02.xlsx",
        "카카오페이지(소설)_2026-02.xlsx",
        "블라이스_일반결제_2026-02.xlsx",
    )
    chips = "".join(f'<span class="example-chip">{html.escape(example)}</span>' for example in examples)
    st.markdown(
        f'<div class="example-strip"><span class="example-strip-label">파일명 예시</span>{chips}</div>',
        unsafe_allow_html=True,
    )


def upload_detection_mode_label(selected_s2_channel: str) -> str:
    return "파일명 자동 감지" if selected_s2_channel == AUTO_PLATFORM_OPTION else f"직접 선택: {selected_s2_channel}"


def render_upload_status_card(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="upload-status-item">
          <span class="upload-status-label">{html.escape(label)}</span>
          <span class="upload-status-value">{html.escape(value)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.set_page_config(page_title="S2 소설 매핑", layout="wide")
inject_compact_layout_css()
st.markdown('<div class="app-title">S2 소설 매핑</div>', unsafe_allow_html=True)
st.caption("플랫폼별 정산서 엑셀을 S2 기준에 매핑합니다.")

with st.sidebar:
    st.subheader("S2 기준 상태")

    current_cache = cache_metrics(S2_SOURCE_LOOKUP)
    repo_baseline = repo_s2_baseline_summary()
    baseline_updated_at = repo_s2_baseline_updated_at(repo_baseline) if repo_baseline else ""
    usage_label, usage_tone = s2_usage_status(baseline_updated_at, current_cache["rows"])
    render_s2_status_card(baseline_updated_at, usage_label, usage_tone)
    st.caption("*매일 10시에 정규 업데이트됩니다.")

    if usage_tone != "ok":
        render_sidebar_mini_notice(
            "오늘 기준 업데이트가 아니면 관리자에게 요청하거나 본문 2번 `S2 기준`에서 수동 파일을 업로드하세요.",
            tone="warning",
        )

    cache_cols = st.columns(2)
    cache_cols[0].metric("현재 S2 기준 행", f"{current_cache['rows']:,}")
    cache_cols[1].metric("S2 ID", f"{current_cache['sales_channel_content_id_nonblank']:,}")
    guard_cols = st.columns(3)
    guard_cols[0].metric("누락 guard", f"{lookup_row_count(S2_MISSING_LOOKUP):,}")
    guard_cols[1].metric("청구 guard", f"{lookup_row_count(S2_BILLING_LOOKUP):,}")
    guard_cols[2].metric("콘텐츠 lookup", f"{lookup_row_count(S2_SERVICE_CONTENTS_LOOKUP):,}")


st.markdown('<div class="workflow-caption">정산서 업로드 -> 판매채널 확인 -> S2 매핑 -> 다운로드</div>', unsafe_allow_html=True)

st.subheader("1. 정산서 엑셀 업로드")
st.markdown('<div class="section-kicker">여러 플랫폼의 .xlsx 정산서를 한 번에 업로드할 수 있습니다.</div>', unsafe_allow_html=True)
render_upload_examples()
st.session_state.setdefault(SETTLEMENT_UPLOAD_RESET_COUNTER_KEY, 0)
settlement_upload_key = f"settlement_files_{safe_int(st.session_state.get(SETTLEMENT_UPLOAD_RESET_COUNTER_KEY))}"
upload_cols = st.columns([1.7, 0.85])
with upload_cols[0]:
    settlement_files = st.file_uploader(
        "정산서 엑셀 업로드",
        type=["xlsx"],
        accept_multiple_files=True,
        key=settlement_upload_key,
        help="파일명에 실제 S2 판매채널명이 들어 있으면 자동감지합니다. 여러 파일을 동시에 올릴 수 있습니다.",
    )
with upload_cols[1]:
    s2_channel_options = [AUTO_PLATFORM_OPTION] + sorted(s2_sales_channel_to_platform())
    selected_s2_channel = st.selectbox(
        "판매채널",
        s2_channel_options,
        help="기본은 파일명에서 실제 S2 판매채널명을 감지합니다. 싫으면 여기서 판매채널명을 직접 선택하세요.",
    )

settlement_files = list(settlement_files or [])
detection_frame, undetected_files = upload_detection_rows(settlement_files, selected_s2_channel)

status_cols = st.columns([1, 1, 0.18])
with status_cols[0]:
    render_upload_status_card("업로드된 파일", f"{len(settlement_files):,}개")
with status_cols[1]:
    render_upload_status_card("판매채널 감지", upload_detection_mode_label(selected_s2_channel))
with status_cols[2]:
    st.markdown('<div class="upload-reset-spacer"></div>', unsafe_allow_html=True)
    reset_upload_clicked = st.button(
        "리셋",
        help="잘못 올린 파일이나 현재 업로드 목록을 모두 비웁니다.",
        key="reset_settlement_uploads",
    )
st.markdown(
    '<div class="upload-reset-caption">잘못 들어간 파일이 보이면 리셋 후 .xlsx만 다시 올리세요.</div>',
    unsafe_allow_html=True,
)
if reset_upload_clicked:
    st.session_state.pop(MAPPING_RESULT_STATE_KEY, None)
    st.session_state[SETTLEMENT_UPLOAD_RESET_COUNTER_KEY] = safe_int(
        st.session_state.get(SETTLEMENT_UPLOAD_RESET_COUNTER_KEY)
    ) + 1
    st.rerun()

if settlement_files:
    if selected_s2_channel != AUTO_PLATFORM_OPTION:
        st.info(f"직접 선택한 S2 판매채널로 처리합니다: {selected_s2_channel}")
        if len(settlement_files) > 1:
            st.warning("직접 선택은 업로드한 모든 파일에 같은 S2 판매채널을 적용합니다. 채널이 서로 다르면 하나씩 처리하세요.")
    elif not undetected_files:
        st.success(f"파일명 기반 S2 판매채널 확인 완료: {len(settlement_files):,}개")
    else:
        st.error(
            f"S2 판매채널명을 감지하지 못한 파일 {len(undetected_files):,}개가 있습니다. "
            + S2_CHANNEL_FILENAME_GUIDE
        )
    st.dataframe(detection_frame, use_container_width=True, height=min(260, 45 + 35 * len(detection_frame)))
else:
    st.info("정산서 엑셀을 업로드하면 파일별 판매채널 감지 결과가 여기에 표시됩니다.")

with st.expander("판매채널명 스키마", expanded=False):
    st.caption("{S2정산플랫폼}에 넣을 수 있는 전체 목록입니다. 파일명 안에 아래 문자열 중 하나가 들어 있으면 자동감지됩니다.")
    st.dataframe(s2_channel_schema_frame(), use_container_width=True, height=260)


ADMIN_S2_SOURCE_OPTION = "관리자 배포 S2 기준 사용"
s2_source_options = ["수동 S2 파일 업로드"]
if S2_SOURCE_LOOKUP.exists():
    s2_source_options.insert(0, ADMIN_S2_SOURCE_OPTION)
s2_file = None
payment_settlement_file = None
use_payment_cache = bool(S2_SOURCE_LOOKUP.exists())
master_df = None

with st.expander("2. S2 기준", expanded=False):
    st.caption("기본은 관리자가 repo에 배포한 S2 지급정산 기준입니다. 수동 업로드는 최신화가 늦었을 때 쓰는 예외 모드입니다.")
    if S2_SOURCE_LOOKUP.exists():
        s2_source_mode = st.radio("S2 기준", s2_source_options, horizontal=True)
        use_payment_cache = s2_source_mode == ADMIN_S2_SOURCE_OPTION
        if not use_payment_cache:
            st.warning("수동 S2 업로드는 예외 모드입니다. 오늘 작업에 최신 기준이 필요하면 먼저 관리자에게 S2 최신화를 요청하세요.")
            st.markdown(
                f"""
                - **S2 원천 엑셀**: S2 `[5031] 정산정보(지급)관리`에서 받은 원본 엑셀입니다. 앱이 필요한 기준 컬럼으로 변환합니다.
                  다운로드 위치: [{S2_PAYMENT_MANAGEMENT_URL}]({S2_PAYMENT_MANAGEMENT_URL})
                  권장 조건: 조회기간 `{S2_REFRESH_START_DATE.isoformat()}~오늘`, 콘텐츠형태 `소설`.
                - **S2 기준 리스트**: 이미 `판매채널콘텐츠ID`, `콘텐츠ID`, `콘텐츠명`, `판매채널명` 등이 들어가도록 관리자가 가공한 기준 파일입니다. 일반 사용자는 보통 쓰지 않습니다.
                """
            )
            manual_cols = st.columns(2)
            with manual_cols[0]:
                payment_settlement_file = st.file_uploader(
                    "[5031] 정산정보(지급)관리 원천 엑셀",
                    type=["xlsx"],
                    help="S2 [5031] 정산정보(지급)관리에서 다운로드한 원본 엑셀입니다. 앱이 매핑 기준 컬럼으로 변환합니다.",
                )
            with manual_cols[1]:
                s2_file = st.file_uploader(
                    "관리자 가공 S2 기준 리스트",
                    type=["xlsx"],
                    help="판매채널콘텐츠ID, 콘텐츠ID, 콘텐츠명, 판매채널명이 이미 포함된 관리자 가공 기준 파일입니다.",
                )
        else:
            st.caption("관리자가 배포한 S2 기준을 사용합니다. 최신화가 의심되면 관리자에게 기준 갱신을 요청하세요.")
    else:
        use_payment_cache = False
        st.warning("관리자 배포 S2 기준이 없습니다. 관리자에게 최신화를 요청하거나 아래 수동 S2 파일을 업로드하세요.")
        manual_cols = st.columns(2)
        st.markdown(
            f"""
            - **S2 원천 엑셀**: S2 `[5031] 정산정보(지급)관리` 원본 엑셀입니다.
              다운로드 위치: [{S2_PAYMENT_MANAGEMENT_URL}]({S2_PAYMENT_MANAGEMENT_URL})
            - **S2 기준 리스트**: 관리자가 가공한 기준 파일입니다. 일반 사용자는 S2 원천 엑셀 쪽이 더 안전합니다.
            """
        )
        with manual_cols[0]:
            payment_settlement_file = st.file_uploader(
                "[5031] 정산정보(지급)관리 원천 엑셀",
                type=["xlsx"],
                help="S2 [5031] 정산정보(지급)관리에서 다운로드한 원본 엑셀입니다. 앱이 매핑 기준 컬럼으로 변환합니다.",
            )
        with manual_cols[1]:
            s2_file = st.file_uploader(
                "관리자 가공 S2 기준 리스트",
                type=["xlsx"],
                help="판매채널콘텐츠ID, 콘텐츠ID, 콘텐츠명, 판매채널명이 이미 포함된 관리자 가공 기준 파일입니다.",
            )


st.subheader("3. 정규화 및 S2 매핑")
single_output_name = ""
if len(settlement_files) == 1:
    single_output_name = st.text_input("결과 엑셀 파일명", value=default_mapping_stem(settlement_files[0]))
elif len(settlement_files) > 1:
    st.caption("복수 처리 결과는 파일별 `{원본파일명}_매핑.xlsx`로 만들고 ZIP으로 묶습니다.")

has_s2_source = s2_file is not None or payment_settlement_file is not None or use_payment_cache
all_platforms_ready = selected_s2_channel != AUTO_PLATFORM_OPTION or not undetected_files
can_run = bool(settlement_files) and has_s2_source and all_platforms_ready
st.markdown("**매핑 실행 준비 상태**")
st.dataframe(
    mapping_readiness_frame(
        settlement_file_count=len(settlement_files),
        has_s2_source=has_s2_source,
        all_platforms_ready=all_platforms_ready,
        selected_s2_channel=selected_s2_channel,
    ),
    use_container_width=True,
    hide_index=True,
    height=142,
)
if not can_run:
    st.caption("실행하려면 준비 상태의 `필요` 또는 `확인 필요` 항목을 먼저 처리하세요.")

run_signature = mapping_run_signature(
    settlement_files=settlement_files,
    selected_s2_channel=selected_s2_channel,
    use_payment_cache=use_payment_cache,
    payment_settlement_file=payment_settlement_file,
    s2_file=s2_file,
    single_output_name=single_output_name,
)
run_clicked = st.button("어댑터 정규화 및 S2 매핑 실행", type="primary", disabled=not can_run)

if run_clicked:
    try:
        results: list[dict[str, Any]] = []
        progress_slot = st.empty()
        with st.spinner("S2 기준과 정산서 엑셀을 처리하는 중..."):
            progress_slot.caption("S2 기준 불러오는 중")
            s2_df, s2_source_label, payment_summary, s2_guards, s2_guard_filter = load_selected_s2_basis(
                use_payment_cache=use_payment_cache,
                payment_settlement_file=payment_settlement_file,
                s2_file=s2_file,
            )

            for idx, settlement_file in enumerate(settlement_files, start=1):
                progress_slot.caption(f"{idx:,}/{len(settlement_files):,} 처리 중: {settlement_file.name}")
                if len(settlement_files) == 1:
                    raw_output_stem = single_output_name or default_mapping_stem(settlement_file)
                else:
                    raw_output_stem = default_mapping_stem(settlement_file)
                output_stem = sanitize_output_stem(raw_output_stem) or default_mapping_stem(settlement_file)
                results.append(
                    process_settlement_batch_item(
                        settlement_file=settlement_file,
                        selected_s2_channel=selected_s2_channel,
                        s2_df=s2_df,
                        s2_guards=s2_guards,
                        s2_guard_filter=s2_guard_filter,
                        master_df=master_df,
                        output_stem=output_stem,
                    )
                )
        progress_slot.empty()
        st.session_state[MAPPING_RESULT_STATE_KEY] = build_mapping_session_state(
            signature=run_signature,
            results=results,
            s2_df=s2_df,
            s2_source_label=s2_source_label,
            payment_summary=payment_summary,
        )
    except Exception as exc:
        if "progress_slot" in locals():
            progress_slot.empty()
        st.error("S2 기준 또는 입력 파일을 처리하지 못했습니다.")
        render_error_detail(exc)
        st.stop()

mapping_state = st.session_state.get(MAPPING_RESULT_STATE_KEY)
if not mapping_state or mapping_state.get("signature") != run_signature:
    st.stop()


st.subheader("S2 기준")
s2_cols = st.columns(3)
s2_cols[0].metric("소스", mapping_state.get("s2_source_label", ""))
s2_cols[1].metric("S2 기준 행", f"{safe_int(mapping_state.get('s2_rows')):,}")
s2_cols[2].metric("판매채널콘텐츠ID", f"{safe_int(mapping_state.get('s2_id_rows')):,}")
payment_summary = mapping_state.get("payment_summary")
if payment_summary is not None:
    with st.expander("수동 S2 원천 요약", expanded=False):
        st.dataframe(s2_source_summary_frame(payment_summary), use_container_width=True, height=260)


st.subheader("처리 결과")
summary_frame = mapping_state["summary_frame"]
status_counts = summary_frame["상태"].value_counts().to_dict() if not summary_frame.empty else {}
batch_cols = st.columns(4)
batch_cols[0].metric("전체 파일", f"{len(summary_frame):,}")
batch_cols[1].metric("성공", f"{safe_int(status_counts.get('success')):,}")
batch_cols[2].metric("차단", f"{safe_int(status_counts.get('blocked')):,}")
batch_cols[3].metric("실패", f"{safe_int(status_counts.get('failed')):,}")
st.dataframe(summary_frame, use_container_width=True, height=min(360, 45 + 35 * max(len(summary_frame), 1)))

report_download_cols = st.columns(2)
with report_download_cols[0]:
    st.download_button(
        "PD 작업지시 CSV 다운로드",
        mapping_state["work_order_csv_bytes"],
        file_name="PD_작업지시_종합리포트.csv",
        mime="text/csv",
        on_click="ignore",
        disabled=bool(mapping_state.get("work_order_empty")),
    )
with report_download_cols[1]:
    st.download_button(
        "전체 행별매핑 CSV 다운로드",
        mapping_state["combined_csv_bytes"],
        file_name="전체_행별매핑_종합.csv",
        mime="text/csv",
        on_click="ignore",
        disabled=bool(mapping_state.get("combined_report_empty")),
    )

st.download_button(
    "전체 결과 ZIP 다운로드",
    mapping_state["zip_bytes"],
    file_name=mapping_state["zip_name"],
    mime="application/zip",
    on_click="ignore",
    disabled=summary_frame.empty,
)
