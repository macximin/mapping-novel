from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd

from cleaning_rules import clean_title, text


COL_GRADE = "판정등급"
COL_PLATFORM = "플랫폼"
COL_S2_CHANNEL = "S2판매채널명"
COL_SALES_CHANNEL_CONTENT_ID = "판매채널콘텐츠ID"
COL_CONTENT_ID = "콘텐츠ID"
COL_CONTENT_TITLE = "콘텐츠명"
COL_CLEAN_KEY = "정제키"
COL_CONTENT_SHAPE = "판매채널콘텐츠_콘텐츠형태"
COL_PAYMENT_EXISTS = "지급정산관리_존재"
COL_DISABLED_MARKER = "사용안함_사용금지_표식"
COL_DEPARTMENT = "담당부서"
COL_MIXED_RISK = "혼합위험"
COL_STATUS = "정산상태"
COL_STATUS_REASON = "정산상태사유"
COL_AS_OF = "판정기준일"

STATUS_OK_PAYMENT_SETTLEMENT_EXISTS = "OK_PAYMENT_SETTLEMENT_EXISTS"
STATUS_OK_PAYMENT_LOOKUP_SOURCE = "OK_PAYMENT_LOOKUP_SOURCE"
STATUS_BLOCK_DISABLED_MARKER = "BLOCK_DISABLED_MARKER"
STATUS_HOLD_NO_PAYMENT_SETTLEMENT = "HOLD_NO_PAYMENT_SETTLEMENT"
STATUS_HOLD_MIXED_CONTENT_RISK = "HOLD_MIXED_CONTENT_RISK"
STATUS_REVIEW_UNKNOWN_STATUS = "REVIEW_UNKNOWN_STATUS"

BLOCKED_STATUSES = {
    STATUS_BLOCK_DISABLED_MARKER,
    STATUS_HOLD_NO_PAYMENT_SETTLEMENT,
    STATUS_HOLD_MIXED_CONTENT_RISK,
    STATUS_REVIEW_UNKNOWN_STATUS,
}
ALLOWED_STATUS_PREFIX = "OK_"

DEFAULT_CORE_NOVEL_DEPARTMENTS = (
    "소설1팀",
    "소설2팀",
    "소설편집팀",
    "소설유통팀",
    "소설사업부",
)

S2_ID_COLUMN_CANDIDATES = (
    "판매채널콘텐츠ID",
    "S2_판매채널콘텐츠ID",
    "매핑_판매채널콘텐츠ID",
)
TITLE_COLUMN_CANDIDATES = (
    "콘텐츠명",
    "S2_콘텐츠명",
    "정산서_콘텐츠명",
    "작품명",
    "상품명",
    "도서명",
)

GateMode = Literal["strict", "payment_lookup_safe", "audit_only"]
SourceKind = Literal["payment_lookup", "sales_channel_content", "manual_s2", "unknown"]


@dataclass
class SettlementGateResult:
    allowed: pd.DataFrame
    blocked: pd.DataFrame
    summary: pd.DataFrame
    warnings: list[str]
    mode: GateMode
    source_kind: SourceKind | str


def load_settlement_status_table(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=object)
    required = {COL_SALES_CHANNEL_CONTENT_ID, COL_STATUS}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"정산상태표 필수 컬럼이 없습니다: {sorted(missing)}")
    frame = frame.copy()
    frame[COL_SALES_CHANNEL_CONTENT_ID] = frame[COL_SALES_CHANNEL_CONTENT_ID].map(id_text)
    frame = frame[frame[COL_SALES_CHANNEL_CONTENT_ID].ne("")].copy()
    return frame.reset_index(drop=True)


def build_status_table(
    judgement: pd.DataFrame,
    ips: pd.DataFrame | None = None,
    *,
    departments: Iterable[str] | None = None,
    content_shape: str | None = None,
    as_of: str = "",
) -> pd.DataFrame:
    required = {
        COL_SALES_CHANNEL_CONTENT_ID,
        COL_CONTENT_ID,
        COL_CONTENT_TITLE,
        COL_PAYMENT_EXISTS,
        COL_DISABLED_MARKER,
    }
    missing = required - set(judgement.columns)
    if missing:
        raise ValueError(f"판정표 필수 컬럼이 없습니다: {sorted(missing)}")

    frame = judgement.copy()
    frame[COL_SALES_CHANNEL_CONTENT_ID] = frame[COL_SALES_CHANNEL_CONTENT_ID].map(id_text)
    frame[COL_CONTENT_ID] = frame[COL_CONTENT_ID].map(id_text)
    frame[COL_CONTENT_TITLE] = frame[COL_CONTENT_TITLE].map(text)
    frame = frame[frame[COL_SALES_CHANNEL_CONTENT_ID].ne("")].copy()

    content_has_payment = frame.groupby(COL_CONTENT_ID, dropna=False)[COL_PAYMENT_EXISTS].apply(lambda s: s.map(is_yes).any())

    if ips is not None and not ips.empty:
        if COL_CONTENT_ID not in ips.columns or COL_DEPARTMENT not in ips.columns:
            raise ValueError(f"IPS 보조자료에는 {COL_CONTENT_ID}, {COL_DEPARTMENT} 컬럼이 필요합니다.")
        ips_dept = ips[[COL_CONTENT_ID, COL_DEPARTMENT]].copy()
        ips_dept[COL_CONTENT_ID] = ips_dept[COL_CONTENT_ID].map(id_text)
        ips_dept[COL_DEPARTMENT] = ips_dept[COL_DEPARTMENT].map(text)
        ips_dept = ips_dept[ips_dept[COL_CONTENT_ID].ne("")].drop_duplicates(subset=[COL_CONTENT_ID])
        if COL_DEPARTMENT in frame.columns:
            frame = frame.drop(columns=[COL_DEPARTMENT])
        frame = frame.merge(ips_dept, on=COL_CONTENT_ID, how="left")
    elif COL_DEPARTMENT not in frame.columns:
        frame[COL_DEPARTMENT] = ""

    if content_shape is not None and COL_CONTENT_SHAPE in frame.columns:
        frame = frame[frame[COL_CONTENT_SHAPE].map(text).eq(content_shape)].copy()
    if departments is not None:
        department_set = {text(dept) for dept in departments}
        frame = frame[frame[COL_DEPARTMENT].map(text).isin(department_set)].copy()

    frame[COL_MIXED_RISK] = frame.apply(
        lambda row: "Y"
        if _has_mixed_content_risk(row, content_has_payment)
        else "N",
        axis=1,
    )
    frame[COL_STATUS] = frame.apply(_status_for_row, axis=1)
    frame[COL_STATUS_REASON] = frame.apply(_reason_for_row, axis=1)
    frame[COL_AS_OF] = as_of

    return _ordered_status_table(frame).reset_index(drop=True)


def apply_settlement_status_gate(
    s2_df: pd.DataFrame,
    status_table: pd.DataFrame,
    *,
    source_kind: SourceKind | str,
    mode: GateMode = "strict",
) -> SettlementGateResult:
    if mode not in {"strict", "payment_lookup_safe", "audit_only"}:
        raise ValueError(f"지원하지 않는 게이트 모드입니다: {mode}")

    id_col = pick_existing_column(S2_ID_COLUMN_CANDIDATES, s2_df, "S2 판매채널콘텐츠ID")
    working = s2_df.copy()
    working[id_col] = working[id_col].map(id_text)
    status = _status_lookup(status_table)
    status_cols = [COL_STATUS, COL_STATUS_REASON, COL_MIXED_RISK, COL_PAYMENT_EXISTS]
    optional_cols = [
        COL_PLATFORM,
        COL_S2_CHANNEL,
        COL_CONTENT_ID,
        COL_CONTENT_TITLE,
        COL_DEPARTMENT,
        COL_AS_OF,
    ]
    status_cols.extend([col for col in optional_cols if col in status.columns and col not in status_cols])

    enriched = working.merge(
        status[status_cols].reset_index(),
        left_on=id_col,
        right_on=COL_SALES_CHANNEL_CONTENT_ID,
        how="left",
        suffixes=("", "_정산상태표"),
    )
    if COL_SALES_CHANNEL_CONTENT_ID != id_col and COL_SALES_CHANNEL_CONTENT_ID in enriched.columns:
        enriched = enriched.drop(columns=[COL_SALES_CHANNEL_CONTENT_ID])

    missing_status = enriched[COL_STATUS].map(text).eq("")
    if source_kind == "payment_lookup":
        enriched.loc[missing_status, COL_STATUS] = STATUS_OK_PAYMENT_LOOKUP_SOURCE
        enriched.loc[missing_status, COL_STATUS_REASON] = "지급정산관리 lookup 소스에 존재하는 ID입니다."
    else:
        enriched.loc[missing_status, COL_STATUS] = STATUS_REVIEW_UNKNOWN_STATUS
        enriched.loc[missing_status, COL_STATUS_REASON] = "최신 정산상태표에서 확인되지 않은 ID입니다."

    allowed_mask = enriched[COL_STATUS].map(is_allowed_status)
    blocked = enriched[~allowed_mask].copy()
    allowed = enriched.copy() if mode == "audit_only" else enriched[allowed_mask].copy()
    warnings = _gate_warnings(blocked, mode=mode)
    summary = settlement_status_summary(enriched)
    return SettlementGateResult(
        allowed=allowed.reset_index(drop=True),
        blocked=blocked.reset_index(drop=True),
        summary=summary,
        warnings=warnings,
        mode=mode,
        source_kind=source_kind,
    )


def find_blocked_title_hits(settlement_df: pd.DataFrame, blocked_s2_df: pd.DataFrame) -> pd.DataFrame:
    settlement_keys = _title_key_frame(settlement_df, source="settlement")
    blocked_keys = _title_key_frame(blocked_s2_df, source="blocked_s2")
    if settlement_keys.empty or blocked_keys.empty:
        return pd.DataFrame()
    hits = settlement_keys.merge(blocked_keys, on=COL_CLEAN_KEY, how="inner", suffixes=("_정산서", "_차단후보"))
    return hits.reset_index(drop=True)


def find_only_blocked_candidate_alerts(
    settlement_df: pd.DataFrame,
    allowed_s2_df: pd.DataFrame,
    blocked_s2_df: pd.DataFrame,
) -> pd.DataFrame:
    settlement_keys = _title_key_frame(settlement_df, source="settlement")
    blocked_keys = _title_key_frame(blocked_s2_df, source="blocked_s2")
    if settlement_keys.empty or blocked_keys.empty:
        return pd.DataFrame()
    allowed_keys = set(_title_key_frame(allowed_s2_df, source="allowed_s2")[COL_CLEAN_KEY].map(text))
    blocked_only_keys = {key for key in blocked_keys[COL_CLEAN_KEY].map(text) if key and key not in allowed_keys}
    alerts = settlement_keys[settlement_keys[COL_CLEAN_KEY].map(text).isin(blocked_only_keys)].copy()
    if alerts.empty:
        return alerts
    blocked_counts = blocked_keys[blocked_keys[COL_CLEAN_KEY].map(text).isin(blocked_only_keys)][COL_CLEAN_KEY].value_counts()
    alerts["차단후보수"] = alerts[COL_CLEAN_KEY].map(lambda key: int(blocked_counts.get(key, 0)))
    alerts["경보"] = "정산서 제목과 일치하는 S2 후보가 있으나 정산정보 없음 게이트로 전부 보류됨"
    return alerts.reset_index(drop=True)


def settlement_status_summary(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or COL_STATUS not in frame.columns:
        return pd.DataFrame(columns=["항목", "값"])
    rows: list[tuple[str, object]] = [
        ("전체 행", len(frame)),
        ("판매채널콘텐츠ID 수", frame[COL_SALES_CHANNEL_CONTENT_ID].map(text).nunique() if COL_SALES_CHANNEL_CONTENT_ID in frame.columns else ""),
    ]
    if COL_PAYMENT_EXISTS in frame.columns:
        rows.append(("지급정산관리 없음", int(frame[COL_PAYMENT_EXISTS].map(lambda value: not is_yes(value)).sum())))
    rows.extend((f"상태:{status}", int(count)) for status, count in frame[COL_STATUS].value_counts(dropna=False).items())
    if COL_MIXED_RISK in frame.columns:
        rows.append(("혼합위험", int(frame[COL_MIXED_RISK].map(is_yes).sum())))
    return pd.DataFrame(rows, columns=["항목", "값"])


def summary_dict(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {
            "rows": 0,
            "sales_channel_content_ids": 0,
            "missing_payment_settlement": 0,
            "ab_checklist": 0,
            "mixed_risk": 0,
            "mixed_content_ids": 0,
            "status_counts": {},
        }
    hold_mask = frame[COL_STATUS].isin([STATUS_HOLD_NO_PAYMENT_SETTLEMENT, STATUS_HOLD_MIXED_CONTENT_RISK])
    mixed_mask = frame[COL_STATUS].eq(STATUS_HOLD_MIXED_CONTENT_RISK)
    return {
        "rows": int(len(frame)),
        "sales_channel_content_ids": int(frame[COL_SALES_CHANNEL_CONTENT_ID].map(text).nunique()),
        "missing_payment_settlement": int(frame[COL_PAYMENT_EXISTS].map(lambda value: not is_yes(value)).sum())
        if COL_PAYMENT_EXISTS in frame.columns
        else 0,
        "ab_checklist": int(hold_mask.sum()),
        "mixed_risk": int(mixed_mask.sum()),
        "mixed_content_ids": int(frame.loc[mixed_mask, COL_CONTENT_ID].map(text).nunique()) if COL_CONTENT_ID in frame.columns else 0,
        "status_counts": {str(k): int(v) for k, v in frame[COL_STATUS].value_counts(dropna=False).to_dict().items()},
    }


def id_text(value: object) -> str:
    value_text = text(value)
    if value_text.endswith(".0"):
        head = value_text[:-2]
        if head.isdigit():
            return head
    return value_text


def is_yes(value: object) -> bool:
    return text(value).upper() == "Y"


def is_allowed_status(status: object) -> bool:
    return text(status).startswith(ALLOWED_STATUS_PREFIX)


def pick_existing_column(candidates: Iterable[str], frame: pd.DataFrame, label: str) -> str:
    for column in candidates:
        if column in frame.columns:
            return column
    raise ValueError(f"{label} 컬럼을 찾을 수 없습니다: 후보={list(candidates)}, 현재={list(frame.columns)}")


def _status_lookup(status_table: pd.DataFrame) -> pd.DataFrame:
    required = {COL_SALES_CHANNEL_CONTENT_ID, COL_STATUS, COL_STATUS_REASON}
    missing = required - set(status_table.columns)
    if missing:
        raise ValueError(f"정산상태표 필수 컬럼이 없습니다: {sorted(missing)}")
    table = status_table.copy()
    table[COL_SALES_CHANNEL_CONTENT_ID] = table[COL_SALES_CHANNEL_CONTENT_ID].map(id_text)
    table = table[table[COL_SALES_CHANNEL_CONTENT_ID].ne("")].drop_duplicates(subset=[COL_SALES_CHANNEL_CONTENT_ID])
    return table.set_index(COL_SALES_CHANNEL_CONTENT_ID, drop=True)


def _status_for_row(row: pd.Series) -> str:
    if is_yes(row.get(COL_DISABLED_MARKER)):
        return STATUS_BLOCK_DISABLED_MARKER
    if not is_yes(row.get(COL_PAYMENT_EXISTS)) and is_yes(row.get(COL_MIXED_RISK)):
        return STATUS_HOLD_MIXED_CONTENT_RISK
    if not is_yes(row.get(COL_PAYMENT_EXISTS)):
        return STATUS_HOLD_NO_PAYMENT_SETTLEMENT
    return STATUS_OK_PAYMENT_SETTLEMENT_EXISTS


def _has_mixed_content_risk(row: pd.Series, content_has_payment: pd.Series) -> bool:
    content_id = id_text(row.get(COL_CONTENT_ID))
    return bool(content_id) and not is_yes(row.get(COL_PAYMENT_EXISTS)) and bool(content_has_payment.get(content_id, False))


def _reason_for_row(row: pd.Series) -> str:
    status = text(row.get(COL_STATUS)) or _status_for_row(row)
    if status == STATUS_BLOCK_DISABLED_MARKER:
        return "[사용안함]/[사용금지] 등 선차단 표식이 있습니다."
    if status == STATUS_HOLD_MIXED_CONTENT_RISK:
        return "동일 콘텐츠ID 안에 정상 지급정산 채널이 있으나 이 판매채널콘텐츠ID는 지급정산관리에 없습니다."
    if status == STATUS_HOLD_NO_PAYMENT_SETTLEMENT:
        return "판매채널콘텐츠에는 있으나 지급정산관리에 없습니다."
    if status == STATUS_OK_PAYMENT_SETTLEMENT_EXISTS:
        return "지급정산관리에 존재합니다."
    return "정산상태 확인이 필요합니다."


def _ordered_status_table(frame: pd.DataFrame) -> pd.DataFrame:
    preferred = [
        COL_STATUS,
        COL_STATUS_REASON,
        COL_MIXED_RISK,
        COL_GRADE,
        COL_PLATFORM,
        COL_S2_CHANNEL,
        COL_SALES_CHANNEL_CONTENT_ID,
        COL_CONTENT_ID,
        COL_CONTENT_TITLE,
        COL_CLEAN_KEY,
        COL_CONTENT_SHAPE,
        COL_DEPARTMENT,
        COL_PAYMENT_EXISTS,
        COL_DISABLED_MARKER,
        COL_AS_OF,
    ]
    ordered = [column for column in preferred if column in frame.columns]
    rest = [column for column in frame.columns if column not in ordered]
    return frame[ordered + rest].copy()


def _title_key_frame(frame: pd.DataFrame, *, source: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=[f"{source}_원본행번호", f"{source}_콘텐츠명", COL_CLEAN_KEY])
    title_col = pick_existing_column(TITLE_COLUMN_CANDIDATES, frame, f"{source} 제목")
    result = pd.DataFrame(
        {
            f"{source}_원본행번호": range(1, len(frame) + 1),
            f"{source}_콘텐츠명": frame[title_col].map(text),
            COL_CLEAN_KEY: frame[title_col].map(clean_title),
        }
    )
    for column in [COL_SALES_CHANNEL_CONTENT_ID, "S2_판매채널콘텐츠ID", COL_CONTENT_ID, COL_STATUS, COL_STATUS_REASON]:
        if column in frame.columns:
            result[f"{source}_{column}"] = frame[column].map(text)
    return result[result[COL_CLEAN_KEY].ne("")].reset_index(drop=True)


def _gate_warnings(blocked: pd.DataFrame, *, mode: GateMode) -> list[str]:
    warnings: list[str] = []
    if not blocked.empty and mode != "audit_only":
        warnings.append(f"정산상태 게이트로 보류/차단된 S2 후보 {len(blocked):,}개가 있습니다.")
    if not blocked.empty and mode == "audit_only":
        warnings.append(f"audit_only 모드: 보류/차단 후보 {len(blocked):,}개를 차단하지 않고 표시만 합니다.")
    return warnings
