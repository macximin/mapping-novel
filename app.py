from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from kiss_refresh_history import latest_refresh_runs
from kiss_payment_settlement import load_payment_settlement_list, summarize_payment_settlement, to_s2_lookup
from mapping_core import build_mapping, export_mapping, load_master, read_first_sheet
from settlement_adapters import (
    adapter_audit_dataframe,
    adapter_blocking_messages,
    adapter_warning_messages,
    list_platforms,
    normalize_settlement,
    summarize_normalization,
)
from s2_transfer import build_s2_transfer, export_s2_transfer


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
KIDARI_NOVEL_MASTER = DATA_DIR / "kidari_contents.xlsx"
S2_SOURCE_LOOKUP = DATA_DIR / "kiss_payment_settlement_s2_lookup.csv"
S2_HISTORY_DB = DATA_DIR / "kiss_refresh_history.sqlite"
S2_REFRESH_SCRIPT = ROOT / "scripts" / "refresh_kiss_payment_settlement.py"
S2_ENV_FILE = ROOT / ".env"


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


def run_s2_refresh(mode: str, start_date: date | None, end_date: date | None) -> subprocess.CompletedProcess[str]:
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
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=900)


def ui_safe_refresh_log(text: str) -> str:
    replacements = {
        "KISS": "S2",
        "kiss_api": "s2_api",
        "kiss_payment_settlement": "s2_source",
        "kiss_refresh": "s2_refresh",
    }
    safe_text = text
    for old, new in replacements.items():
        safe_text = safe_text.replace(old, new)
    return safe_text


def load_manual_s2_reference(uploaded_file: object) -> tuple[pd.DataFrame, str, dict[str, object] | None]:
    try:
        payment_df = load_payment_settlement_list(uploaded_file)
        return to_s2_lookup(payment_df), "수동 S2 원천 엑셀 업로드", summarize_payment_settlement(payment_df)
    except Exception:
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(0)
        return read_first_sheet(uploaded_file), "수동 S2 리스트 업로드", None


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
            "cache_rows_after": "S2 캐시 행",
            "s2_lookup_rows": "S2 기준 행",
            "sales_channel_content_id_unique": "S2 ID 고유값",
            "finished_at": "종료시각",
        }
    )
    if "조회범위" in frame.columns:
        frame["조회범위"] = frame["조회범위"].map(
            {
                "full-replace": "전체 교체",
                "initial": "전체 교체",
                "custom": "직접 범위 교체",
                "rolling-3m": "최근 3개월",
            }
        ).fillna(frame["조회범위"])
    return frame


st.set_page_config(page_title="IPS/S2 소설 매핑", layout="wide")
st.title("IPS/S2 소설 매핑")

st.caption("플랫폼별 정산서의 상품명을 S2 판매채널콘텐츠ID와 S2 콘텐츠ID에 매핑합니다.")

with st.sidebar:
    st.subheader("1. S2 최신화")
    current_cache = cache_metrics(S2_SOURCE_LOOKUP)
    cache_cols = st.columns(2)
    cache_cols[0].metric("S2 기준 행", f"{current_cache['rows']:,}")
    cache_cols[1].metric("S2 ID", f"{current_cache['sales_channel_content_id_nonblank']:,}")

    if "s2_refresh_message" in st.session_state:
        st.success(st.session_state.pop("s2_refresh_message"))
    if "s2_refresh_error" in st.session_state:
        st.error(st.session_state.pop("s2_refresh_error"))

    refresh_mode = st.selectbox(
        "교체 범위",
        ["full-replace", "custom"],
        format_func=lambda value: {
            "full-replace": "전체 교체(기간 제한 없음)",
            "custom": "직접 범위 교체",
        }[value],
        help="시작일/종료일을 비워 S2 소설 전체를 조회한 뒤 기존 S2 캐시를 통째로 교체합니다.",
    )
    refresh_start = None
    refresh_end = None
    if refresh_mode == "custom":
        today = date.today()
        refresh_start = st.date_input("시작일", value=today.replace(day=1))
        refresh_end = st.date_input("종료일", value=today)

    refresh_disabled = not S2_ENV_FILE.exists()
    if st.button("S2 기준 전체 교체", disabled=refresh_disabled, use_container_width=True):
        with st.spinner("S2 기준 전체 교체 중"):
            completed = run_s2_refresh(refresh_mode, refresh_start, refresh_end)
        if completed.returncode == 0:
            st.session_state["s2_refresh_message"] = "S2 기준 전체 교체 완료"
            st.session_state["s2_refresh_output"] = completed.stdout
            st.rerun()
        else:
            st.session_state["s2_refresh_error"] = "S2 기준 전체 교체 실패"
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
        history_cols[1].metric("최근 S2 기준 행", f"{int(latest.get('S2 기준 행') or 0):,}")
        with st.expander("최신화 기록", expanded=False):
            st.dataframe(recent_history, use_container_width=True, height=180)

    st.subheader("2. 플랫폼별 정산서")
    platform_options = ["파일명으로 자동 선택"] + list_platforms()
    selected_platform = st.selectbox(
        "플랫폼",
        platform_options,
        help="자동 선택은 엑셀 파일명에 들어 있는 플랫폼명/별칭만 보고 고릅니다. 못 찾으면 플랫폼을 직접 선택하세요.",
    )
    st.caption("자동 선택 기준: 엑셀 파일명에 포함된 플랫폼명/별칭")
    settlement_file = st.file_uploader("플랫폼별 정산서", type="xlsx")
    default_name = "mapping_result"
    if settlement_file is not None:
        default_name = f"{Path(settlement_file.name).stem}_매핑"
    output_name = st.text_input("저장 파일명", value=default_name)

    st.subheader("3. S2 기준")
    use_payment_cache = st.checkbox(
        "S2 캐시 사용(권장)",
        value=S2_SOURCE_LOOKUP.exists(),
        disabled=not S2_SOURCE_LOOKUP.exists(),
    )
    s2_file = None
    payment_settlement_file = None
    with st.expander("수동 S2 입력(비상용)", expanded=False):
        s2_file = st.file_uploader(
            "수동 S2 리스트 업로드",
            type="xlsx",
            help="S2 캐시 대신 사람이 받은 S2 리스트를 사용할 때만 업로드합니다. S2 원천 엑셀이 들어오면 자동 변환합니다.",
        )
        payment_settlement_file = st.file_uploader(
            "수동 S2 원천 엑셀 업로드",
            type="xlsx",
            help="S2 캐시 대신 사람이 받은 S2 원천 엑셀을 사용할 때만 업로드합니다.",
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

has_s2_source = s2_file is not None or payment_settlement_file is not None or use_payment_cache
if settlement_file is None or not has_s2_source:
    st.warning("플랫폼별 정산서와 S2 기준이 필요합니다. S2 캐시를 사용하거나 수동 S2 파일을 업로드하세요.")
    st.stop()

if st.button("매핑 실행", type="primary"):
    try:
        payment_summary = None
        if payment_settlement_file is not None:
            payment_df = load_payment_settlement_list(payment_settlement_file)
            payment_summary = summarize_payment_settlement(payment_df)
            s2_df = to_s2_lookup(payment_df)
            s2_source_label = "수동 S2 원천 엑셀 업로드"
        elif use_payment_cache:
            s2_df = pd.read_csv(S2_SOURCE_LOOKUP, dtype=object)
            s2_source_label = "S2 캐시"
        else:
            s2_df, s2_source_label, payment_summary = load_manual_s2_reference(s2_file)

        adapter_result = normalize_settlement(
            settlement_file,
            platform=None if selected_platform == "파일명으로 자동 선택" else selected_platform,
            source_name=settlement_file.name,
        )
        adapter_summary = summarize_normalization(adapter_result)
        settlement_df = adapter_result.to_mapping_feed()
    except Exception as exc:
        st.error("입력 파일을 처리하지 못했습니다.")
        st.exception(exc)
        st.stop()

    st.subheader("S2 기준")
    s2_cols = st.columns(3)
    s2_cols[0].metric("소스", s2_source_label)
    s2_cols[1].metric("S2 기준 행", f"{len(s2_df):,}")
    if "판매채널콘텐츠ID" in s2_df.columns:
        s2_cols[2].metric("판매채널콘텐츠ID", f"{s2_df['판매채널콘텐츠ID'].map(str).str.strip().ne('').sum():,}")
    if payment_summary is not None:
        with st.expander("수동 S2 원천 요약", expanded=False):
            st.json(payment_summary)

    st.subheader("어댑터 검증")
    adapter_cols = st.columns(5)
    adapter_cols[0].metric("플랫폼", adapter_summary["platform"])
    adapter_cols[1].metric("원본 파싱 행", f"{int(adapter_summary['parsed_rows']):,}")
    adapter_cols[2].metric("기본 feed 행", f"{int(adapter_summary['default_feed_rows']):,}")
    adapter_cols[3].metric("금액 정책", adapter_summary["amount_rule_status"])
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
        st.dataframe(audit_df, use_container_width=True, height=260)
        st.stop()

    try:
        mapping = build_mapping(s2_df, settlement_df, master_df)
    except Exception as exc:
        st.error("표준화된 정산서 재료를 매핑 엔진에 넣는 데 실패했습니다.")
        st.dataframe(audit_df, use_container_width=True, height=260)
        st.exception(exc)
        st.stop()

    summary = dict(zip(mapping.summary["항목"], mapping.summary["값"]))
    cols = st.columns(5)
    cols[0].metric("정산서 행", f"{int(summary.get('정산서 행 수', 0)):,}")
    cols[1].metric("검토필요", f"{int(summary.get('검토필요 행 수', 0)):,}")
    cols[2].metric("S2 matched", f"{int(summary.get('S2 matched', 0)):,}")
    cols[3].metric("S2 콘텐츠ID", f"{int(summary.get('S2 콘텐츠ID present', 0)):,}")
    cols[4].metric("중복 후보키", f"{int(summary.get('중복 후보 정제키 수', 0)):,}")

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
    transfer_cols[1].metric("전송 후보 행", f"{int(transfer_summary.get('전송 후보 행 수', 0)):,}")
    transfer_cols[2].metric("차단 행", f"{int(transfer_summary.get('차단 행 수', 0)):,}")
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
