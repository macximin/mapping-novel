from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from kiss_refresh_history import latest_refresh_runs, latest_s2_refresh_changes
from kiss_payment_settlement import load_payment_settlement_list, summarize_payment_settlement, to_s2_lookup
from mapping_core import build_mapping, export_mapping, load_master, read_first_sheet, text
from settlement_adapters import (
    adapter_audit_dataframe,
    adapter_blocking_messages,
    adapter_warning_messages,
    detect_platform,
    list_platforms,
    normalize_settlement,
    summarize_normalization,
)
from s2_transfer import build_s2_transfer, export_s2_transfer
from s2_auth import (
    S2_AUTH_ERROR_MESSAGE,
    S2_AUTH_FAILURE_HINT,
    has_s2_credentials,
    looks_like_s2_auth_failure,
    normalize_s2_login_values,
    normalize_s2_secret_values,
    read_env_file,
)


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
KIDARI_NOVEL_MASTER = DATA_DIR / "kidari_contents.xlsx"
S2_SOURCE_LOOKUP = DATA_DIR / "kiss_payment_settlement_s2_lookup.csv"
S2_HISTORY_DB = DATA_DIR / "kiss_refresh_history.sqlite"
S2_REFRESH_SCRIPT = ROOT / "scripts" / "refresh_kiss_payment_settlement.py"
S2_ENV_FILE = ROOT / ".env"
S2_FALLBACK_START_DATE = date(1900, 1, 1)
AUTO_PLATFORM_OPTION = "엑셀 파일명으로 자동감지"
S2_SESSION_USERNAME_KEY = "s2_session_username"
S2_SESSION_PASSWORD_KEY = "s2_session_password"
S2_REMEMBER_ID_KEY = "s2_remember_id"
S2_ID_MEMORY_COMPONENT_KEY = "s2_id_memory"
S2_ID_MEMORY_CLEAR_COUNTER_KEY = "s2_id_memory_clear_counter"
S2_ID_MEMORY_STORAGE_KEY = "mapping_novel_s2_id"


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


@st.cache_data(show_spinner=False)
def cached_master(path_text: str) -> pd.DataFrame:
    return load_master(Path(path_text))


def cache_metrics(path: Path) -> dict[str, int]:
    if not path.exists():
        return {"rows": 0, "sales_channel_content_id_nonblank": 0}
    frame = pd.read_csv(path, dtype=object)
    metrics = {"rows": len(frame), "sales_channel_content_id_nonblank": 0}
    if "판매채널콘텐츠ID" in frame.columns:
        metrics["sales_channel_content_id_nonblank"] = int(frame["판매채널콘텐츠ID"].map(str).str.strip().ne("").sum())
    return metrics


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
    ]
    if mode == "custom" and start_date is not None and end_date is not None:
        command.extend(["--start-date", start_date.isoformat(), "--end-date", end_date.isoformat()])
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=900, env=s2_refresh_environment())


def run_s2_full_replace() -> tuple[subprocess.CompletedProcess[str], str]:
    completed = run_s2_refresh("full-replace")
    if completed.returncode == 0 or not should_retry_s2_with_date_window(completed):
        return completed, "무기간 조회"

    today = date.today()
    fallback = run_s2_refresh("custom", S2_FALLBACK_START_DATE, today)
    return fallback, f"{S2_FALLBACK_START_DATE.isoformat()} ~ {today.isoformat()}"


def should_retry_s2_with_date_window(completed: subprocess.CompletedProcess[str]) -> bool:
    output = f"{completed.stdout}\n{completed.stderr}".lower()
    non_retry_tokens = [".env", "로그인", "인증", "id/pw", "token", "토큰", "password"]
    return not any(token in output for token in non_retry_tokens)


def s2_refresh_error_message(completed: subprocess.CompletedProcess[str], refresh_scope: str) -> str:
    output = f"{completed.stdout}\n{completed.stderr}"
    if looks_like_s2_auth_failure(output):
        return f"{S2_AUTH_FAILURE_HINT} API 다운로드를 진행하지 못했습니다. ({refresh_scope})"
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
        return read_first_sheet(uploaded_file), "수동 S2 기준 리스트", None


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
                "custom": "기간 보완 전체 교체",
                "rolling-3m": "과거 기록(부분 조회)",
            }
        ).fillna(frame["조회범위"])
    return frame


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


def render_error_detail(exc: Exception) -> None:
    with st.expander("오류 상세", expanded=False):
        st.exception(exc)


st.set_page_config(page_title="S2/IPS 소설 매핑", layout="wide")
st.title("S2/IPS 소설 매핑")
st.caption("플랫폼별 정산서 엑셀을 S2 기준에 매핑하고, IPS는 보조 검산으로 확인합니다.")

with st.sidebar:
    st.subheader("S2 최신화")
    sync_browser_s2_id_memory()
    st.caption(
        f"전체 교체 방식으로 고정합니다. 먼저 기간 없이 조회하고, S2가 기간을 요구하면 "
        f"{S2_FALLBACK_START_DATE.isoformat()}부터 오늘까지로 다시 조회합니다."
    )

    current_cache = cache_metrics(S2_SOURCE_LOOKUP)
    cache_cols = st.columns(2)
    cache_cols[0].metric("현재 S2 기준 행", f"{current_cache['rows']:,}")
    cache_cols[1].metric("S2 ID", f"{current_cache['sales_channel_content_id_nonblank']:,}")

    if "s2_refresh_message" in st.session_state:
        st.success(st.session_state.pop("s2_refresh_message"))
    if "s2_refresh_error" in st.session_state:
        st.error(st.session_state.pop("s2_refresh_error"))

    with st.expander("S2 최신화 로그인", expanded=not has_s2_credentials(s2_runtime_auth_config())):
        st.caption("S2 최신화에만 사용합니다. 앱은 ID/PW를 파일이나 Secrets에 저장하지 않습니다.")
        with st.form("s2_session_login_form"):
            st.text_input("S2 ID", key=S2_SESSION_USERNAME_KEY, autocomplete="username")
            st.text_input("S2 PW", key=S2_SESSION_PASSWORD_KEY, type="password", autocomplete="current-password")
            st.checkbox("S2 ID 기억", key=S2_REMEMBER_ID_KEY)
            auth_submitted = st.form_submit_button("이번 세션에 사용", use_container_width=True)
        if auth_submitted:
            if has_s2_credentials(session_s2_login_values()):
                st.success("이번 세션의 S2 ID/PW가 준비되었습니다.")
            else:
                st.warning("S2 ID와 PW를 모두 입력하세요.")
        if has_s2_credentials(session_s2_login_values()):
            st.caption("이번 세션의 S2 ID/PW가 설정되어 있습니다.")
            if st.button("세션 로그인 지우기", use_container_width=True):
                st.session_state.pop(S2_SESSION_PASSWORD_KEY, None)
                if not st.session_state.get(S2_REMEMBER_ID_KEY):
                    st.session_state.pop(S2_SESSION_USERNAME_KEY, None)
                st.rerun()
        if text(s2_id_memory_state().get("saved_id")):
            if st.button("저장된 S2 ID 지우기", use_container_width=True):
                clear_browser_s2_id_memory()
                st.rerun()

    refresh_disabled = not has_s2_credentials(s2_runtime_auth_config())
    if refresh_disabled:
        st.warning(S2_AUTH_ERROR_MESSAGE)
    else:
        st.caption("S2/IPS 접속 정보가 설정되어 있습니다.")

    if st.button("S2 기준 전체 교체", disabled=refresh_disabled, use_container_width=True):
        with st.spinner("S2 기준 전체 교체 중"):
            completed, refresh_scope = run_s2_full_replace()
        if completed.returncode == 0:
            st.session_state["s2_refresh_message"] = f"S2 기준 전체 교체 완료: {refresh_scope}"
            st.session_state["s2_refresh_output"] = completed.stdout
            st.rerun()
        else:
            st.session_state["s2_refresh_error"] = s2_refresh_error_message(completed, refresh_scope)
            st.session_state["s2_refresh_output"] = completed.stderr or completed.stdout
            st.rerun()

    if "s2_refresh_output" in st.session_state:
        with st.expander("최신화 실행 로그", expanded=False):
            st.code(ui_safe_refresh_log(st.session_state["s2_refresh_output"]))

    recent_history = history_frame(5)
    if not recent_history.empty:
        latest = recent_history.iloc[0]
        history_cols = st.columns(2)
        history_cols[0].metric("최근 상태", str(latest.get("상태", "")))
        history_cols[1].metric("최근 S2 기준 행", f"{safe_int(latest.get('S2 기준 행')):,}")
        change_cols = st.columns(3)
        change_cols[0].metric("신규", f"{safe_int(latest.get('신규')):,}")
        change_cols[1].metric("삭제", f"{safe_int(latest.get('삭제')):,}")
        change_cols[2].metric("변경", f"{safe_int(latest.get('변경')):,}")
        with st.expander("최신화 기록", expanded=False):
            st.dataframe(recent_history, use_container_width=True, height=180)
        change_detail = s2_change_detail_frame(latest_s2_refresh_changes(S2_HISTORY_DB, refresh_run_id=safe_int(latest.get("ID")), limit=500))
        if not change_detail.empty:
            with st.expander("최근 S2 변경 이력 상세", expanded=False):
                st.dataframe(change_detail, use_container_width=True, height=260)


st.info("정산서 업로드 -> 엑셀 파일명 기반 플랫폼 자동감지 -> 어댑터 정규화 -> S2 매핑 -> 결과 엑셀 다운로드")

st.subheader("1. 플랫폼별 정산서 업로드")
upload_cols = st.columns([2, 1])
with upload_cols[0]:
    settlement_file = st.file_uploader("플랫폼별 정산서 엑셀", type=["xlsx"])
with upload_cols[1]:
    platform_options = [AUTO_PLATFORM_OPTION] + list_platforms()
    selected_platform = st.selectbox(
        "플랫폼",
        platform_options,
        help="자동감지는 엑셀 파일명에 들어 있는 플랫폼명 또는 별칭만 사용합니다.",
    )

detected_platform = detect_platform(settlement_file.name) if settlement_file is not None else None
if settlement_file is not None:
    if selected_platform == AUTO_PLATFORM_OPTION:
        if detected_platform:
            st.success(f"엑셀 파일명 기반 자동감지 결과: {detected_platform}")
        else:
            st.warning("엑셀 파일명으로 플랫폼을 감지하지 못했습니다. 플랫폼을 직접 선택하세요.")
    else:
        st.info(f"직접 선택한 플랫폼으로 처리합니다: {selected_platform}")
else:
    st.caption("자동감지는 엑셀 파일명 기반입니다. 파일명에 플랫폼명이 없으면 플랫폼을 직접 선택하세요.")


st.subheader("2. S2 기준 선택")
s2_source_options = ["수동 S2 파일 업로드"]
if S2_SOURCE_LOOKUP.exists():
    s2_source_options.insert(0, "로컬 S2 기준 사용")
s2_source_mode = st.radio("S2 기준", s2_source_options, horizontal=True)
use_payment_cache = s2_source_mode == "로컬 S2 기준 사용"
s2_file = None
payment_settlement_file = None

if use_payment_cache:
    st.caption("로컬에 저장된 S2 기준을 사용합니다. 최신 데이터가 필요하면 왼쪽에서 전체 교체를 실행하세요.")
else:
    manual_cols = st.columns(2)
    with manual_cols[0]:
        payment_settlement_file = st.file_uploader(
            "S2 원천 엑셀",
            type=["xlsx"],
            help="S2에서 받은 원천 엑셀을 앱이 매핑 기준 컬럼으로 변환합니다.",
        )
    with manual_cols[1]:
        s2_file = st.file_uploader(
            "S2 기준 리스트",
            type=["xlsx"],
            help="이미 판매채널콘텐츠ID, 콘텐츠ID, 콘텐츠명을 포함하도록 정리된 S2 기준 파일입니다.",
        )


master_df = None
master_error = ""
if KIDARI_NOVEL_MASTER.exists():
    try:
        master_df = cached_master(str(KIDARI_NOVEL_MASTER))
    except Exception as exc:
        master_error = str(exc)

meta_cols = st.columns(4)
if master_df is not None:
    st.info("S2 기준으로 매핑합니다. IPS 기준 파일은 보조 검산으로만 사용합니다.")
    meta_cols[0].metric("IPS 보조 파일", KIDARI_NOVEL_MASTER.name)
    meta_cols[1].metric("IPS 보조 행 수", f"{len(master_df):,}")
elif master_error:
    st.warning(f"IPS 보조 검산은 건너뜁니다. IPS 기준 파일을 읽지 못했습니다: {master_error}")
else:
    st.info("S2 기준으로 매핑합니다. IPS 보조 검산 파일은 없습니다.")
    meta_cols[0].metric("IPS 보조 검산", "skipped")
if master_df is not None and "귀속법인" in master_df.columns:
    meta_cols[2].metric("귀속법인", " | ".join(master_df["귀속법인"].dropna().astype(str).unique()[:3]))
if master_df is not None and "콘텐츠형태" in master_df.columns:
    meta_cols[3].metric("콘텐츠형태", " | ".join(master_df["콘텐츠형태"].dropna().astype(str).unique()[:3]))


st.subheader("3. 정규화 및 S2 매핑")
default_name = "mapping_result"
if settlement_file is not None:
    default_name = f"{Path(settlement_file.name).stem}_매핑"
output_name = st.text_input("결과 엑셀 파일명", value=default_name)

has_s2_source = s2_file is not None or payment_settlement_file is not None or use_payment_cache
can_run = settlement_file is not None and has_s2_source and (selected_platform != AUTO_PLATFORM_OPTION or detected_platform is not None)
if not can_run:
    missing: list[str] = []
    if settlement_file is None:
        missing.append("플랫폼별 정산서 엑셀")
    if not has_s2_source:
        missing.append("S2 기준")
    if settlement_file is not None and selected_platform == AUTO_PLATFORM_OPTION and detected_platform is None:
        missing.append("플랫폼 직접 선택")
    st.warning(" / ".join(missing) + "이 필요합니다.")

run_clicked = st.button("어댑터 정규화 및 S2 매핑 실행", type="primary", disabled=not can_run)
if not run_clicked:
    st.stop()

try:
    payment_summary: dict[str, Any] | None = None
    with st.status("S2 기준과 정산서 엑셀을 처리하는 중", expanded=True) as status:
        st.write("S2 기준 불러오는 중")
        if payment_settlement_file is not None:
            payment_df = load_payment_settlement_list(payment_settlement_file)
            payment_summary = summarize_payment_settlement(payment_df)
            s2_df = to_s2_lookup(payment_df)
            s2_source_label = "수동 S2 원천 엑셀"
        elif use_payment_cache:
            s2_df = pd.read_csv(S2_SOURCE_LOOKUP, dtype=object)
            s2_source_label = "로컬 S2 기준"
        else:
            s2_df, s2_source_label, payment_summary = load_manual_s2_reference(s2_file)

        st.write("어댑터 정규화 중")
        adapter_result = normalize_settlement(
            settlement_file,
            platform=None if selected_platform == AUTO_PLATFORM_OPTION else selected_platform,
            source_name=settlement_file.name,
        )
        adapter_summary = summarize_normalization(adapter_result)
        settlement_df = adapter_result.to_mapping_feed()
        status.update(label="정규화 완료", state="complete")
except Exception as exc:
    st.error("입력 파일을 처리하지 못했습니다.")
    if settlement_file is not None:
        st.warning("어댑터가 이 엑셀 파일을 처리하지 못했거나, 엑셀 파일명 기반 자동감지에 실패했을 수 있습니다.")
    render_error_detail(exc)
    st.stop()


st.subheader("S2 기준")
s2_cols = st.columns(3)
s2_cols[0].metric("소스", s2_source_label)
s2_cols[1].metric("S2 기준 행", f"{len(s2_df):,}")
if "판매채널콘텐츠ID" in s2_df.columns:
    s2_cols[2].metric("판매채널콘텐츠ID", f"{s2_df['판매채널콘텐츠ID'].map(str).str.strip().ne('').sum():,}")
if payment_summary is not None:
    with st.expander("수동 S2 원천 요약", expanded=False):
        st.dataframe(s2_source_summary_frame(payment_summary), use_container_width=True, height=260)


st.subheader("어댑터 정규화")
adapter_cols = st.columns(5)
adapter_cols[0].metric("플랫폼", adapter_summary["platform"])
adapter_cols[1].metric("원본 파싱 행", f"{safe_int(adapter_summary['parsed_rows']):,}")
adapter_cols[2].metric("S2 매핑 입력 행", f"{safe_int(adapter_summary['default_feed_rows']):,}")
adapter_cols[3].metric("금액 검증", adapter_summary["amount_rule_status"])
adapter_cols[4].metric("S2 금액 잠금", "Y" if adapter_summary["s2_amount_policy_locked"] else "N")
st.caption(adapter_summary["s2_gate"])

audit_df = adapter_audit_dataframe(adapter_result)
blocking_messages = adapter_blocking_messages(adapter_result)
warning_messages = adapter_warning_messages(adapter_result)

for message in blocking_messages:
    st.error(message)
for message in warning_messages:
    st.warning(message)

if blocking_messages:
    st.error("어댑터 검증에서 차단되어 S2 매핑을 멈췄습니다.")
    st.dataframe(audit_df, use_container_width=True, height=260)
    st.stop()

try:
    mapping = build_mapping(s2_df, settlement_df, master_df)
except Exception as exc:
    st.error("표준화된 정산서 재료를 S2 매핑 엔진에 넣는 데 실패했습니다.")
    st.dataframe(audit_df, use_container_width=True, height=260)
    render_error_detail(exc)
    st.stop()


summary = dict(zip(mapping.summary["항목"], mapping.summary["값"]))
cols = st.columns(5)
cols[0].metric("정산서 행", f"{safe_int(summary.get('정산서 행 수')):,}")
cols[1].metric("검토필요", f"{safe_int(summary.get('검토필요 행 수')):,}")
cols[2].metric("S2 matched", f"{safe_int(summary.get('S2 matched')):,}")
cols[3].metric("S2 콘텐츠ID", f"{safe_int(summary.get('S2 콘텐츠ID present')):,}")
cols[4].metric("중복 후보키", f"{safe_int(summary.get('중복 후보 정제키 수')):,}")

with st.expander("어댑터 시트별 감사", expanded=False):
    st.dataframe(audit_df, use_container_width=True, height=260)

st.subheader("행별 매핑 결과")
st.dataframe(mapping.rows, use_container_width=True, height=420)

tab_review, tab_dups, tab_validation = st.tabs(["검토필요", "중복후보", "입력검증"])
with tab_review:
    st.dataframe(mapping.review_rows, use_container_width=True, height=320)
with tab_dups:
    st.dataframe(mapping.duplicate_candidates, use_container_width=True, height=320)
with tab_validation:
    st.dataframe(mapping.input_validation, use_container_width=True, height=320)


file_name = output_name.strip() or f"mapping_result_{datetime.now().strftime('%Y%m%d_%H%M')}"
if file_name.lower().endswith(".xlsx"):
    file_name = file_name[:-5]
file_name = "".join(ch if ch not in r'\/:*?"<>|' else "_" for ch in file_name)

st.subheader("S2 전송자료")
s2_transfer = build_s2_transfer(
    mapping.rows,
    amount_policy_locked=adapter_result.spec.s2_amount_policy_locked,
    s2_gate=adapter_result.spec.s2_gate,
)
transfer_summary = dict(zip(s2_transfer.summary["항목"], s2_transfer.summary["값"]))
transfer_cols = st.columns(3)
transfer_cols[0].metric("전송 가능", transfer_summary.get("전송 가능", "N"))
transfer_cols[1].metric("전송 후보 행", f"{safe_int(transfer_summary.get('전송 후보 행 수')):,}")
transfer_cols[2].metric("차단 행", f"{safe_int(transfer_summary.get('차단 행 수')):,}")
for message in s2_transfer.blocking_messages:
    st.warning(message)
with st.expander("S2 전송자료 사전검증", expanded=False):
    st.dataframe(s2_transfer.summary, use_container_width=True, height=160)
    if not s2_transfer.blocked_rows.empty:
        st.dataframe(s2_transfer.blocked_rows, use_container_width=True, height=260)
if s2_transfer.exportable:
    st.download_button(
        "S2 전송자료 다운로드",
        export_s2_transfer(s2_transfer),
        file_name=f"{file_name}_S2전송자료.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.download_button(
    "결과 엑셀 다운로드",
    export_mapping(mapping),
    file_name=f"{file_name}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
