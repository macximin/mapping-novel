from __future__ import annotations

import html
import io
import json
import os
import subprocess
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from kiss_refresh_history import latest_refresh_runs, latest_s2_refresh_changes
from kiss_payment_settlement import load_payment_settlement_list, summarize_payment_settlement, to_s2_lookup
from cleaning_rules import drop_disabled_rows, text
from mapping_core import build_mapping, export_mapping, load_master, read_first_sheet
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
S2_MISSING_LOOKUP = DATA_DIR / "s2_payment_missing_lookup.csv"
S2_BILLING_LOOKUP = DATA_DIR / "s2_billing_settlement_lookup.csv"
S2_HISTORY_DB = DATA_DIR / "kiss_refresh_history.sqlite"
S2_BASELINE_SUMMARY_NAME = "kiss_payment_settlement_refresh_summary.json"
S2_REFRESH_SCRIPT = ROOT / "scripts" / "refresh_kiss_payment_settlement.py"
S2_GUARD_REFRESH_SCRIPT = ROOT / "scripts" / "refresh_s2_reference_guards.py"
S2_ENV_FILE = ROOT / ".env"
S2_REFRESH_START_DATE = date(1900, 1, 1)
S2_FAST_PAGE_SIZE = "1000000"
S2_NOVEL_CONTENT_STYLE_CODE = "102"
AUTO_PLATFORM_OPTION = "엑셀 파일명으로 자동감지"
S2_SESSION_USERNAME_KEY = "s2_session_username"
S2_SESSION_PASSWORD_KEY = "s2_session_password"
S2_REMEMBER_ID_KEY = "s2_remember_id"
S2_ID_MEMORY_COMPONENT_KEY = "s2_id_memory"
S2_ID_MEMORY_CLEAR_COUNTER_KEY = "s2_id_memory_clear_counter"
S2_ID_MEMORY_STORAGE_KEY = "mapping_novel_s2_id"
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
    combined = subprocess.CompletedProcess(
        args=[payment_completed.args, guard_completed.args],
        returncode=guard_completed.returncode,
        stdout=f"{payment_completed.stdout}\n{guard_completed.stdout}",
        stderr=f"{payment_completed.stderr}\n{guard_completed.stderr}",
    )
    return combined, f"{refresh_scope} + 누락/청구 보조 기준"


def s2_refresh_error_message(completed: subprocess.CompletedProcess[str], refresh_scope: str) -> str:
    output = f"{completed.stdout}\n{completed.stderr}"
    if looks_like_s2_auth_failure(output):
        return f"{S2_AUTH_FAILURE_HINT} API 다운로드를 진행하지 못했습니다. ({refresh_scope})"
    if refresh_scope == "로그인 확인":
        detail = " ".join(str(output or "").split())[:300]
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
    guards = load_s2_reference_guards(missing_path=S2_MISSING_LOOKUP, billing_path=S2_BILLING_LOOKUP)
    if payment_settlement_file is not None:
        payment_df = load_payment_settlement_list(payment_settlement_file)
        guard_result = apply_missing_exclusions(to_s2_lookup(payment_df), guards)
        label = guarded_s2_source_label("수동 S2 원천 엑셀", guards, guard_result)
        return guard_result.frame, label, summarize_payment_settlement(payment_df), guards, guard_result
    if use_payment_cache:
        guard_result = apply_missing_exclusions(pd.read_csv(S2_SOURCE_LOOKUP, dtype=object), guards)
        label = guarded_s2_source_label("로컬 S2 기준", guards, guard_result)
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
        mapping = annotate_mapping_result(mapping, s2_guards, sales_channel=s2_channel)
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


def build_batch_zip(results: list[dict[str, Any]], summary_frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    used_names: set[str] = set()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            unique_archive_name("batch_summary.csv", used_names),
            summary_frame.to_csv(index=False).encode("utf-8-sig"),
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
        .block-container {
            padding-top: 3.4rem !important;
            padding-bottom: 1.25rem;
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
        .sidebar-mini-warning {
            background: #fffbe6;
            border-radius: 0.45rem;
            color: #8a5a00;
            font-size: 0.55rem;
            line-height: 1.35;
            margin: 0.2rem 0 0.45rem 0;
            padding: 0.4rem 0.5rem;
        }
        div[data-testid="stFileUploader"] {
            min-width: 0;
        }
        div[data-testid="stFileUploader"] label {
            margin-bottom: 0.2rem;
        }
        section[data-testid="stFileUploaderDropzone"] {
            min-height: 2.7rem;
            padding: 0.35rem 0.65rem;
            overflow: hidden;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.25rem;
            border-style: dashed;
            border-radius: 0.2rem;
            background: #ffffff;
        }
        section[data-testid="stFileUploaderDropzone"]::before {
            content: "📎 여기에 첨부 파일을 끌어 오세요. 또는";
            color: #4b5563;
            font-size: 0.88rem;
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


def render_sidebar_mini_warning(message: str) -> None:
    st.markdown(f'<div class="sidebar-mini-warning">{html.escape(message)}</div>', unsafe_allow_html=True)


st.set_page_config(page_title="S2 소설 매핑", layout="wide")
inject_compact_layout_css()
st.markdown('<div class="app-title">S2 소설 매핑</div>', unsafe_allow_html=True)
st.caption("플랫폼별 정산서 엑셀을 S2 기준에 매핑합니다.")

with st.sidebar:
    st.subheader("S2 최신화")
    sync_browser_s2_id_memory()
    st.caption(
        f"전체 교체 방식으로 고정합니다. 조회 범위는 "
        f"{S2_REFRESH_START_DATE.isoformat()}부터 오늘까지이며, 콘텐츠형태는 소설로 고정합니다."
    )
    render_sidebar_mini_warning("영구저장이 아니라, 서버에 임시 저장됩니다.")

    current_cache = cache_metrics(S2_SOURCE_LOOKUP)
    cache_cols = st.columns(2)
    cache_cols[0].metric("현재 S2 기준 행", f"{current_cache['rows']:,}")
    cache_cols[1].metric("S2 ID", f"{current_cache['sales_channel_content_id_nonblank']:,}")
    guard_cols = st.columns(2)
    guard_cols[0].metric("누락 guard", f"{lookup_row_count(S2_MISSING_LOOKUP):,}")
    guard_cols[1].metric("청구 guard", f"{lookup_row_count(S2_BILLING_LOOKUP):,}")

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
                with st.spinner("S2 로그인 확인 중"):
                    auth_completed = run_s2_auth_check()
                if auth_completed.returncode == 0:
                    st.success("S2 로그인 확인 완료. 이번 세션에서 사용할 수 있습니다.")
                else:
                    st.error(s2_refresh_error_message(auth_completed, "로그인 확인"))
                    st.session_state["s2_refresh_output"] = auth_completed.stderr or auth_completed.stdout
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
        render_sidebar_mini_warning(S2_AUTH_ERROR_MESSAGE)
    else:
        st.caption("S2 접속 정보가 설정되어 있습니다.")

    if st.button("S2 기준 전체 교체", disabled=refresh_disabled, use_container_width=True):
        with st.spinner("S2 로그인 확인 중"):
            auth_completed = run_s2_auth_check()
        if auth_completed.returncode != 0:
            st.session_state["s2_refresh_error"] = s2_refresh_error_message(auth_completed, "로그인 확인")
            st.session_state["s2_refresh_output"] = auth_completed.stderr or auth_completed.stdout
            st.rerun()

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

    repo_baseline = repo_s2_baseline_summary()
    if repo_baseline:
        payload = repo_baseline["payload"]
        summary = repo_baseline["summary"]
        baseline_cols = st.columns(2)
        baseline_cols[0].metric("Repo S2 기준 행", f"{safe_int(payload.get('s2_lookup_rows')):,}")
        baseline_cols[1].metric("Repo S2 원천 행", f"{safe_int(payload.get('cache_rows_after')):,}")
        st.caption(
            "Repo 기준은 앱 배포 시 기본으로 읽는 S2 데이터입니다. "
            "최신화 기록은 이 서버에서 버튼을 누른 실행 로그입니다."
        )
        with st.expander("Repo S2 기준 요약", expanded=False):
            st.dataframe(s2_source_summary_frame(summary), use_container_width=True, height=220)
            st.caption(f"요약 파일: {repo_baseline['path']}")

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
    else:
        st.caption("이 서버에서 실행한 S2 최신화 기록은 아직 없습니다.")


st.caption("정산서 업로드 -> S2 판매채널명 확인 -> S2 매핑 -> 다운로드")

st.subheader("1. 플랫폼별 정산서 업로드")
upload_cols = st.columns([2, 1])
with upload_cols[0]:
    settlement_files = st.file_uploader("플랫폼별 정산서 엑셀 (여러 개 가능)", type=["xlsx"], accept_multiple_files=True)
with upload_cols[1]:
    s2_channel_options = [AUTO_PLATFORM_OPTION] + sorted(s2_sales_channel_to_platform())
    selected_s2_channel = st.selectbox(
        "판매채널",
        s2_channel_options,
        help="기본은 파일명에서 실제 S2 판매채널명을 감지합니다. 싫으면 여기서 판매채널명을 직접 선택하세요.",
    )

settlement_files = list(settlement_files or [])
platform_rows = []
undetected_files = []
for uploaded_file in settlement_files:
    filename_detection = detect_s2_sales_channel(uploaded_file.name)
    detected_channel = s2_channel_for_file(uploaded_file, selected_s2_channel)
    effective_platform = effective_platform_for_file(uploaded_file, selected_s2_channel)
    if not detected_channel or not effective_platform:
        undetected_files.append(uploaded_file.name)
    platform_rows.append(
        {
            "파일": uploaded_file.name,
            "파일명 감지": filename_detection.sales_channel if filename_detection else "",
            "처리 S2 판매채널": detected_channel or "",
            "처리 어댑터": effective_platform or "파일명 수정 필요",
        }
    )

if settlement_files:
    if selected_s2_channel != AUTO_PLATFORM_OPTION:
        st.info(f"직접 선택한 S2 판매채널로 처리합니다: {selected_s2_channel}")
        if len(settlement_files) > 1:
            st.warning("직접 선택은 업로드한 모든 파일에 같은 S2 판매채널을 적용합니다. 채널이 서로 다르면 하나씩 처리하세요.")
    elif not undetected_files:
        st.success(f"파일명 기반 S2 판매채널 확인 완료: {len(settlement_files):,}개")
    else:
        st.warning(
            f"S2 판매채널명을 감지하지 못한 파일 {len(undetected_files):,}개가 있습니다. "
            + S2_CHANNEL_FILENAME_GUIDE
        )
    with st.expander("파일별 판매채널", expanded=bool(undetected_files)):
        st.dataframe(pd.DataFrame(platform_rows), use_container_width=True, height=min(180, 40 + 28 * len(platform_rows)))
else:
    st.caption(S2_CHANNEL_FILENAME_GUIDE)

with st.expander("판매채널명 스키마", expanded=False):
    st.caption("{S2정산플랫폼}에 넣을 수 있는 전체 목록입니다. 파일명 안에 아래 문자열 중 하나가 들어 있으면 자동감지됩니다.")
    st.dataframe(s2_channel_schema_frame(), use_container_width=True, height=260)


s2_source_options = ["수동 S2 파일 업로드"]
if S2_SOURCE_LOOKUP.exists():
    s2_source_options.insert(0, "로컬 S2 기준 사용")
s2_file = None
payment_settlement_file = None
use_payment_cache = bool(S2_SOURCE_LOOKUP.exists())
master_df = None
master_error = ""

with st.expander("2. S2 기준 / IPS 보조 검산", expanded=False):
    if S2_SOURCE_LOOKUP.exists():
        s2_source_mode = st.radio("S2 기준", s2_source_options, horizontal=True)
        use_payment_cache = s2_source_mode == "로컬 S2 기준 사용"
        if not use_payment_cache:
            st.warning("수동 S2 업로드는 예외 모드입니다. 가능한 한 로컬 S2 기준 전체 교체 결과를 사용하세요.")
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
        else:
            st.caption("로컬 S2 기준을 사용합니다. 최신 데이터가 필요하면 사이드바에서 S2 기준 전체 교체를 실행하세요.")
    else:
        use_payment_cache = False
        st.warning("로컬 S2 기준이 없습니다. 사이드바에서 S2 최신화를 실행하거나 S2 기준 파일을 업로드하세요.")
        manual_cols = st.columns(2)
        st.warning("수동 S2 업로드는 예외 모드입니다. 가능한 한 사이드바에서 S2 최신화를 먼저 실행하세요.")
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

    st.divider()
    st.markdown("**IPS 보조 검산**")
    st.caption("선택 검산용입니다. S2 매핑과 S2 전송자료 생성은 S2 기준만 사용합니다.")
    use_ips_aux = st.checkbox("IPS 보조 검산 사용", value=False)
    if use_ips_aux and KIDARI_NOVEL_MASTER.exists():
        try:
            master_df = cached_master(str(KIDARI_NOVEL_MASTER))
        except Exception as exc:
            master_error = str(exc)
    meta_cols = st.columns(4)
    if master_df is not None:
        meta_cols[0].metric("IPS 보조 파일", KIDARI_NOVEL_MASTER.name)
        meta_cols[1].metric("IPS 보조 행 수", f"{len(master_df):,}")
    elif master_error:
        st.warning(f"IPS 기준 파일을 읽지 못했습니다: {master_error}")
    elif use_ips_aux:
        meta_cols[0].metric("IPS 보조 검산", "file missing")
    else:
        meta_cols[0].metric("IPS 보조 검산", "skipped")
    if master_df is not None and "귀속법인" in master_df.columns:
        meta_cols[2].metric("귀속법인", " | ".join(master_df["귀속법인"].dropna().astype(str).unique()[:3]))
    if master_df is not None and "콘텐츠형태" in master_df.columns:
        meta_cols[3].metric("콘텐츠형태", " | ".join(master_df["콘텐츠형태"].dropna().astype(str).unique()[:3]))


st.subheader("3. 정규화 및 S2 매핑")
single_output_name = ""
if len(settlement_files) == 1:
    single_output_name = st.text_input("결과 엑셀 파일명", value=default_mapping_stem(settlement_files[0]))
elif len(settlement_files) > 1:
    st.caption("복수 처리 결과는 파일별 `{원본파일명}_매핑.xlsx`로 만들고 ZIP으로 묶습니다.")

has_s2_source = s2_file is not None or payment_settlement_file is not None or use_payment_cache
all_platforms_ready = selected_s2_channel != AUTO_PLATFORM_OPTION or not undetected_files
can_run = bool(settlement_files) and has_s2_source and all_platforms_ready
if not can_run:
    missing: list[str] = []
    if not settlement_files:
        missing.append("플랫폼별 정산서 엑셀")
    if not has_s2_source:
        missing.append("S2 기준")
    if settlement_files and not all_platforms_ready:
        missing.append("파일명 내 S2 판매채널명")
    st.warning(" / ".join(missing) + "이 필요합니다.")

run_clicked = st.button("어댑터 정규화 및 S2 매핑 실행", type="primary", disabled=not can_run)
if not run_clicked:
    st.stop()

try:
    results: list[dict[str, Any]] = []
    with st.status("S2 기준과 정산서 엑셀을 처리하는 중", expanded=True) as status:
        st.write("S2 기준 불러오는 중")
        s2_df, s2_source_label, payment_summary, s2_guards, s2_guard_filter = load_selected_s2_basis(
            use_payment_cache=use_payment_cache,
            payment_settlement_file=payment_settlement_file,
            s2_file=s2_file,
        )

        for idx, settlement_file in enumerate(settlement_files, start=1):
            st.write(f"{idx:,}/{len(settlement_files):,} 처리 중: {settlement_file.name}")
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
        status.update(label="처리 완료", state="complete")
except Exception as exc:
    st.error("S2 기준 또는 입력 파일을 처리하지 못했습니다.")
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


st.subheader("처리 결과")
summary_frame = batch_summary_frame(results)
status_counts = summary_frame["상태"].value_counts().to_dict() if not summary_frame.empty else {}
batch_cols = st.columns(4)
batch_cols[0].metric("전체 파일", f"{len(results):,}")
batch_cols[1].metric("성공", f"{safe_int(status_counts.get('success')):,}")
batch_cols[2].metric("차단", f"{safe_int(status_counts.get('blocked')):,}")
batch_cols[3].metric("실패", f"{safe_int(status_counts.get('failed')):,}")
st.dataframe(summary_frame, use_container_width=True, height=min(360, 45 + 35 * max(len(summary_frame), 1)))

zip_name = f"mapping_results_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"
st.download_button(
    "전체 결과 ZIP 다운로드",
    build_batch_zip(results, summary_frame),
    file_name=zip_name,
    mime="application/zip",
    disabled=not results,
)

for idx, result in enumerate(results, start=1):
    status_label = text(result.get("status"))
    source_name = text(result.get("source_name"))
    expanded = len(results) == 1
    with st.expander(f"{source_name} - {status_label}", expanded=expanded):
        adapter_summary = result.get("adapter_summary")
        if adapter_summary:
            st.subheader("어댑터 정규화")
            adapter_cols = st.columns(5)
            adapter_cols[0].metric("플랫폼", adapter_summary["platform"])
            adapter_cols[1].metric("원본 파싱 행", f"{safe_int(adapter_summary['parsed_rows']):,}")
            adapter_cols[2].metric("S2 매핑 입력 행", f"{safe_int(adapter_summary['default_feed_rows']):,}")
            adapter_cols[3].metric("금액 검증", adapter_summary["amount_rule_status"])
            adapter_cols[4].metric("S2 금액 잠금", "Y" if adapter_summary["s2_amount_policy_locked"] else "N")
            st.caption(adapter_summary["s2_gate"])

        for message in result.get("blocking_messages", []):
            st.error(message)
        for message in result.get("warning_messages", []):
            st.warning(message)
        for message in result.get("info_messages", []):
            st.info(message)

        audit_df = result.get("audit_df")
        if audit_df is not None:
            with st.expander("어댑터 시트별 감사", expanded=False):
                st.dataframe(audit_df, use_container_width=True, height=260)

        if status_label != "success":
            st.error(result.get("error") or "처리하지 못했습니다.")
            continue

        mapping = result["mapping"]
        mapping_summary = result["summary"]
        cols = st.columns(5)
        cols[0].metric("정산서 행", f"{safe_int(mapping_summary.get('정산서 행 수')):,}")
        cols[1].metric("검토필요", f"{safe_int(mapping_summary.get('검토필요 행 수')):,}")
        cols[2].metric("S2 matched", f"{safe_int(mapping_summary.get('S2 matched')):,}")
        cols[3].metric("S2 콘텐츠ID", f"{safe_int(mapping_summary.get('S2 콘텐츠ID present')):,}")
        cols[4].metric("중복 후보키", f"{safe_int(mapping_summary.get('중복 후보 정제키 수')):,}")

        st.subheader("행별 매핑 결과")
        st.dataframe(mapping.rows, use_container_width=True, height=420)

        tab_review, tab_dups, tab_validation = st.tabs(["검토필요", "중복후보", "입력검증"])
        with tab_review:
            st.dataframe(mapping.review_rows, use_container_width=True, height=320)
        with tab_dups:
            st.dataframe(mapping.duplicate_candidates, use_container_width=True, height=320)
        with tab_validation:
            st.dataframe(mapping.input_validation, use_container_width=True, height=320)

        st.subheader("S2 전송자료")
        s2_transfer = result["s2_transfer"]
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

        output_stem = text(result.get("output_stem")) or default_mapping_stem(source_name)
        download_cols = st.columns(2)
        with download_cols[0]:
            st.download_button(
                "결과 엑셀 다운로드",
                result["mapping_bytes"],
                file_name=f"{output_stem}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"mapping_download_{idx}",
            )
        with download_cols[1]:
            if s2_transfer.exportable:
                st.download_button(
                    "S2 전송자료 다운로드",
                    result["transfer_bytes"],
                    file_name=f"{output_stem}_S2전송자료.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"s2_transfer_download_{idx}",
                )
