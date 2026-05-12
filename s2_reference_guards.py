from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from cleaning_rules import clean_title, drop_disabled_rows, text
from mapping_core import MATCH_AMBIGUOUS, MATCH_BLANK, MATCH_NONE, MATCH_OK, MappingResult


ROOT = Path(__file__).resolve().parent
DEFAULT_MISSING_LOOKUP = ROOT / "data" / "s2_payment_missing_lookup.csv"
DEFAULT_BILLING_LOOKUP = ROOT / "data" / "s2_billing_settlement_lookup.csv"
DEFAULT_SERVICE_CONTENT_LOOKUP = ROOT / "data" / "s2_sales_channel_content_lookup.csv"
S2_DETAIL_REASON_COL = "S2_미매핑상세사유"
S2_DETAIL_EVIDENCE_COL = "S2_미매핑근거"
S2_DETAIL_ACTION_COL = "S2_권장조치"
S2_LOOKUP_TITLE_COL_CAND = ("콘텐츠명", "콘텐츠 제목", "Title", "ContentName", "제목")
S2_LOOKUP_CHANNEL_COL_CAND = ("판매채널명", "S2 판매채널", "판매채널", "salesChannelName")
S2_LOOKUP_CONTENT_ID_COL_CAND = ("콘텐츠ID", "ContentID", "ctnsId")
S2_LOOKUP_SALES_CHANNEL_CONTENT_ID_COL_CAND = (
    "판매채널콘텐츠ID",
    "판매채널컨텐츠ID",
    "판매채널 콘텐츠ID",
    "판매채널 컨텐츠ID",
    "schnCtnsId",
    "SalesChannelContentID",
)
MASTER_TITLE_COL_CAND = ("콘텐츠명", "콘텐츠 제목", "Title", "ContentName", "제목")
MASTER_ID_COL_CAND = ("콘텐츠ID", "판매채널콘텐츠ID", "ID", "ContentID")

MISSING_LOOKUP_COLUMNS = [
    "판매채널콘텐츠ID",
    "콘텐츠ID",
    "콘텐츠명",
    "정제_콘텐츠명",
    "판매채널ID",
    "판매채널명",
    "그룹콘텐츠ID",
    "그룹콘텐츠명",
    "플랫폼콘텐츠ID",
    "콘텐츠형태",
    "콘텐츠상태",
    "최초연재일",
    "담당자",
    "담당부서",
    "정산시작여부",
    "제외사유",
]

BILLING_LOOKUP_COLUMNS = [
    "청구정산마스터ID",
    "계약ID",
    "판매채널ID",
    "판매채널명",
    "대표콘텐츠명",
    "정제_대표콘텐츠명",
    "청구정산상태",
    "승인상태",
    "거래처명",
    "계약명",
    "계약종료일",
    "계약콘텐츠수",
    "담당자",
    "담당부서",
    "분리사유",
]

SERVICE_CONTENT_LOOKUP_COLUMNS = [
    "플랫폼",
    "판매채널ID",
    "판매채널명",
    "거래처코드",
    "거래처명",
    "콘텐츠형태",
    "판매채널콘텐츠ID",
    "콘텐츠ID",
    "콘텐츠명",
    "정제_콘텐츠명",
    "API_판매채널명",
]


@dataclass(frozen=True)
class S2ReferenceGuards:
    missing: pd.DataFrame
    billing: pd.DataFrame
    service_contents: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=SERVICE_CONTENT_LOOKUP_COLUMNS)
    )
    missing_path: Path = DEFAULT_MISSING_LOOKUP
    billing_path: Path = DEFAULT_BILLING_LOOKUP
    service_contents_path: Path = DEFAULT_SERVICE_CONTENT_LOOKUP


@dataclass(frozen=True)
class S2GuardFilterResult:
    frame: pd.DataFrame
    excluded_rows: pd.DataFrame
    input_validation: pd.DataFrame

    @property
    def excluded_count(self) -> int:
        return len(self.excluded_rows)


def id_text(value: Any) -> str:
    raw = text(value)
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


def empty_missing_lookup() -> pd.DataFrame:
    return pd.DataFrame(columns=MISSING_LOOKUP_COLUMNS)


def empty_billing_lookup() -> pd.DataFrame:
    return pd.DataFrame(columns=BILLING_LOOKUP_COLUMNS)


def empty_service_content_lookup() -> pd.DataFrame:
    return pd.DataFrame(columns=SERVICE_CONTENT_LOOKUP_COLUMNS)


def normalize_missing_rows(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows:
        title = text(row.get("ctnsNm"))
        records.append(
            {
                "판매채널콘텐츠ID": id_text(row.get("schnCtnsId")),
                "콘텐츠ID": id_text(row.get("ctnsId")),
                "콘텐츠명": title,
                "정제_콘텐츠명": clean_title(title),
                "판매채널ID": id_text(row.get("schnId")),
                "판매채널명": text(row.get("schnNm")),
                "그룹콘텐츠ID": id_text(row.get("grpCtnsId")),
                "그룹콘텐츠명": text(row.get("grpCtnsNm")),
                "플랫폼콘텐츠ID": id_text(row.get("pltfomCtnsId")),
                "콘텐츠형태": text(row.get("ctnsStleCdNm")),
                "콘텐츠상태": text(row.get("ctnsStsCdNm")),
                "최초연재일": text(row.get("frstSerlCmtDt")),
                "담당자": text(row.get("userNm")),
                "담당부서": text(row.get("deptNm")),
                "정산시작여부": text(row.get("setlBgnYn")),
                "제외사유": "S2 정산정보 누락 건 등재",
            }
        )
    frame = pd.DataFrame(records, columns=MISSING_LOOKUP_COLUMNS)
    if frame.empty:
        return empty_missing_lookup()
    frame = frame[frame["판매채널콘텐츠ID"].ne("")].copy()
    return frame.drop_duplicates(subset=["판매채널콘텐츠ID"], keep="first").reset_index(drop=True)


def normalize_billing_rows(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows:
        title = text(row.get("reprsntCtnsNm"))
        records.append(
            {
                "청구정산마스터ID": id_text(row.get("reqSetlId")),
                "계약ID": id_text(row.get("cntrId")),
                "판매채널ID": id_text(row.get("schnId")),
                "판매채널명": text(row.get("schnNm")),
                "대표콘텐츠명": title,
                "정제_대표콘텐츠명": clean_title(title),
                "청구정산상태": text(row.get("reqSetlStsCdNm")),
                "승인상태": text(row.get("cnfmStsCdNm")),
                "거래처명": text(row.get("bcncNm")),
                "계약명": text(row.get("cntrNm")),
                "계약종료일": text(row.get("cntrCclsDt")),
                "계약콘텐츠수": id_text(row.get("cntrCtnsCnt")),
                "담당자": text(row.get("chgerNm")),
                "담당부서": text(row.get("chrgDeptNm")),
                "분리사유": "청구정산 후보",
            }
        )
    frame = pd.DataFrame(records, columns=BILLING_LOOKUP_COLUMNS)
    if frame.empty:
        return empty_billing_lookup()
    frame = frame[frame["청구정산마스터ID"].ne("")].copy()
    return frame.drop_duplicates(subset=["청구정산마스터ID"], keep="first").reset_index(drop=True)


def normalize_service_content_rows(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows:
        title = text(row.get("ctnsNm") or row.get("콘텐츠명"))
        records.append(
            {
                "플랫폼": text(row.get("platform") or row.get("플랫폼")),
                "판매채널ID": id_text(row.get("schnId") or row.get("S2판매채널ID") or row.get("판매채널ID")),
                "판매채널명": text(row.get("schnNm") or row.get("S2판매채널명") or row.get("판매채널명")),
                "거래처코드": text(row.get("bcncCd") or row.get("거래처코드")),
                "거래처명": text(row.get("bcncNm") or row.get("거래처명")),
                "콘텐츠형태": text(row.get("ctnsStleCdNm") or row.get("판매채널콘텐츠_콘텐츠형태") or row.get("콘텐츠형태")),
                "판매채널콘텐츠ID": id_text(row.get("schnCtnsId") or row.get("판매채널콘텐츠ID")),
                "콘텐츠ID": id_text(row.get("ctnsId") or row.get("콘텐츠ID")),
                "콘텐츠명": title,
                "정제_콘텐츠명": clean_title(title),
                "API_판매채널명": text(row.get("API_판매채널명") or row.get("schnNm")),
            }
        )
    frame = pd.DataFrame(records, columns=SERVICE_CONTENT_LOOKUP_COLUMNS)
    if frame.empty:
        return empty_service_content_lookup()
    frame = frame[frame["판매채널콘텐츠ID"].ne("")].copy()
    return frame.drop_duplicates(subset=["판매채널콘텐츠ID"], keep="first").reset_index(drop=True)


def load_s2_reference_guards(
    *,
    missing_path: str | Path = DEFAULT_MISSING_LOOKUP,
    billing_path: str | Path = DEFAULT_BILLING_LOOKUP,
    service_contents_path: str | Path = DEFAULT_SERVICE_CONTENT_LOOKUP,
) -> S2ReferenceGuards:
    missing_file = Path(missing_path)
    billing_file = Path(billing_path)
    service_contents_file = Path(service_contents_path)
    missing = _load_lookup_csv(missing_file, MISSING_LOOKUP_COLUMNS)
    billing = _load_lookup_csv(billing_file, BILLING_LOOKUP_COLUMNS)
    service_contents = _load_lookup_csv(service_contents_file, SERVICE_CONTENT_LOOKUP_COLUMNS)
    return S2ReferenceGuards(
        missing=missing,
        billing=billing,
        service_contents=service_contents,
        missing_path=missing_file,
        billing_path=billing_file,
        service_contents_path=service_contents_file,
    )


def _load_lookup_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path, dtype=object).fillna("")
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    return frame[columns].copy()


def write_missing_lookup(frame: pd.DataFrame, path: str | Path = DEFAULT_MISSING_LOOKUP) -> Path:
    return _write_lookup(frame, path, MISSING_LOOKUP_COLUMNS)


def write_billing_lookup(frame: pd.DataFrame, path: str | Path = DEFAULT_BILLING_LOOKUP) -> Path:
    return _write_lookup(frame, path, BILLING_LOOKUP_COLUMNS)


def write_service_content_lookup(frame: pd.DataFrame, path: str | Path = DEFAULT_SERVICE_CONTENT_LOOKUP) -> Path:
    return _write_lookup(frame, path, SERVICE_CONTENT_LOOKUP_COLUMNS)


def _write_lookup(frame: pd.DataFrame, path: str | Path, columns: list[str]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    output = frame.copy()
    for column in columns:
        if column not in output.columns:
            output[column] = ""
    output[columns].to_csv(target, index=False, encoding="utf-8-sig")
    return target


def apply_missing_exclusions(s2_frame: pd.DataFrame, guards: S2ReferenceGuards) -> S2GuardFilterResult:
    frame = drop_disabled_rows(s2_frame).copy()
    if frame.empty or guards.missing.empty or "판매채널콘텐츠ID" not in frame.columns:
        return S2GuardFilterResult(
            frame=frame.reset_index(drop=True),
            excluded_rows=pd.DataFrame(),
            input_validation=_guard_validation(guards, 0),
        )

    frame["판매채널콘텐츠ID"] = frame["판매채널콘텐츠ID"].map(id_text)
    missing_ids = set(guards.missing["판매채널콘텐츠ID"].map(id_text))
    mask = frame["판매채널콘텐츠ID"].isin(missing_ids)
    kept = frame.loc[~mask].reset_index(drop=True)
    excluded = frame.loc[mask].copy().reset_index(drop=True)
    if not excluded.empty:
        excluded["S2_제외사유"] = "S2 정산정보 누락 건 등재"
    return S2GuardFilterResult(frame=kept, excluded_rows=excluded, input_validation=_guard_validation(guards, len(excluded)))


def annotate_mapping_result(
    mapping: MappingResult,
    guards: S2ReferenceGuards,
    *,
    sales_channel: str,
    s2_all_frame: pd.DataFrame | None = None,
    master_df: pd.DataFrame | None = None,
) -> MappingResult:
    rows = mapping.rows.copy()
    if rows.empty:
        return mapping

    channel = text(sales_channel)
    missing_index = _index_by_channel_and_key(
        guards.missing,
        channel_col="판매채널명",
        key_col="정제_콘텐츠명",
        fields=["판매채널콘텐츠ID", "콘텐츠ID", "콘텐츠명"],
    )
    billing_index = _index_by_channel_and_key(
        guards.billing,
        channel_col="판매채널명",
        key_col="정제_대표콘텐츠명",
        fields=["청구정산마스터ID", "계약ID", "대표콘텐츠명"],
    )
    service_index = _index_by_channel_and_key(
        guards.service_contents,
        channel_col="판매채널명",
        key_col="정제_콘텐츠명",
        fields=["판매채널콘텐츠ID", "콘텐츠ID", "콘텐츠명"],
    )
    payment_by_key = _s2_payment_index_by_key(s2_all_frame)
    master_by_key = _master_index_by_key(master_df)

    missing_counts: list[str] = []
    missing_ids: list[str] = []
    missing_content_ids: list[str] = []
    billing_counts: list[str] = []
    billing_ids: list[str] = []
    billing_contract_ids: list[str] = []
    split_reasons: list[str] = []
    detail_reasons: list[str] = []
    detail_evidence: list[str] = []
    detail_actions: list[str] = []
    service_counts: list[str] = []
    service_ids: list[str] = []
    service_content_ids: list[str] = []

    for _, row in rows.iterrows():
        key = text(row.get("정제_상품명"))
        status = text(row.get("S2_매칭상태"))
        eligible = status != MATCH_OK
        missing = missing_index.get((channel, key), {}) if eligible else {}
        billing = billing_index.get((channel, key), {}) if eligible else {}
        service = service_index.get((channel, key), {}) if eligible and not missing else {}
        other_payment = _filter_other_channel(payment_by_key.get(key, {}), channel) if eligible else {}
        master = master_by_key.get(key, {}) if eligible else {}
        reasons: list[str] = []
        if missing:
            reasons.append("S2 정산정보 누락 건 등재")
        if billing:
            reasons.append("청구정산 후보")
        missing_counts.append(str(missing.get("_count", 0) if missing else 0))
        missing_ids.append(text(missing.get("판매채널콘텐츠ID", "")) if missing else "")
        missing_content_ids.append(text(missing.get("콘텐츠ID", "")) if missing else "")
        billing_counts.append(str(billing.get("_count", 0) if billing else 0))
        billing_ids.append(text(billing.get("청구정산마스터ID", "")) if billing else "")
        billing_contract_ids.append(text(billing.get("계약ID", "")) if billing else "")
        service_counts.append(str(service.get("_count", 0) if service else 0))
        service_ids.append(text(service.get("판매채널콘텐츠ID", "")) if service else "")
        service_content_ids.append(text(service.get("콘텐츠ID", "")) if service else "")
        split_reasons.append(" | ".join(reasons))
        detail = _missing_detail_for(
            status=status,
            key=key,
            missing=missing,
            billing=billing,
            service=service,
            other_payment=other_payment,
            master=master,
        )
        detail_reasons.append(detail["reason"])
        detail_evidence.append(detail["evidence"])
        detail_actions.append(detail["action"])

    rows["S2_정산정보누락_후보수"] = missing_counts
    rows["S2_정산정보누락_판매채널콘텐츠ID목록"] = missing_ids
    rows["S2_정산정보누락_콘텐츠ID목록"] = missing_content_ids
    rows["청구정산_후보수"] = billing_counts
    rows["청구정산마스터ID목록"] = billing_ids
    rows["청구정산_계약ID목록"] = billing_contract_ids
    rows["S2_판매채널콘텐츠_후보수"] = service_counts
    rows["S2_판매채널콘텐츠_판매채널콘텐츠ID목록"] = service_ids
    rows["S2_판매채널콘텐츠_콘텐츠ID목록"] = service_content_ids
    rows["S2_분리사유"] = split_reasons
    rows[S2_DETAIL_REASON_COL] = detail_reasons
    rows[S2_DETAIL_EVIDENCE_COL] = detail_evidence
    rows[S2_DETAIL_ACTION_COL] = detail_actions

    rows["검토필요사유"] = [_append_reason(row.get("검토필요사유"), row.get("S2_분리사유")) for _, row in rows.iterrows()]
    rows["검토필요(Y/N)"] = rows["검토필요사유"].map(lambda value: "Y" if text(value) else "N")

    missing_candidate_rows = int(pd.to_numeric(rows["S2_정산정보누락_후보수"], errors="coerce").fillna(0).gt(0).sum())
    billing_candidate_rows = int(pd.to_numeric(rows["청구정산_후보수"], errors="coerce").fillna(0).gt(0).sum())
    service_candidate_rows = int(pd.to_numeric(rows["S2_판매채널콘텐츠_후보수"], errors="coerce").fillna(0).gt(0).sum())
    input_validation = pd.concat(
        [
            mapping.input_validation,
            pd.DataFrame(
                [
                    ("매핑행 S2 정산정보 누락 후보", missing_candidate_rows),
                    ("매핑행 청구정산 후보", billing_candidate_rows),
                    ("매핑행 판매채널콘텐츠 후보", service_candidate_rows),
                ],
                columns=["항목", "값"],
            ),
        ],
        ignore_index=True,
    )
    summary = _append_summary(
        mapping.summary,
        [
            ("S2 정산정보 누락 후보", missing_candidate_rows),
            ("청구정산 후보", billing_candidate_rows),
            ("판매채널콘텐츠 후보", service_candidate_rows),
            *[
                (f"S2 미매핑 상세: {reason}", count)
                for reason, count in rows[S2_DETAIL_REASON_COL].map(text).value_counts().items()
                if reason
            ],
        ],
    )
    return MappingResult(
        rows=rows,
        summary=summary,
        review_rows=rows[rows["검토필요(Y/N)"].eq("Y")].copy(),
        duplicate_candidates=mapping.duplicate_candidates,
        input_validation=input_validation,
    )


def _optional_column(frame: pd.DataFrame | None, candidates: Iterable[str]) -> str:
    if frame is None:
        return ""
    for column in candidates:
        if column in frame.columns:
            return column
    return ""


def _s2_payment_index_by_key(frame: pd.DataFrame | None) -> dict[str, dict[str, str | int]]:
    if frame is None or frame.empty:
        return {}
    title_col = _optional_column(frame, S2_LOOKUP_TITLE_COL_CAND)
    channel_col = _optional_column(frame, S2_LOOKUP_CHANNEL_COL_CAND)
    if not title_col or not channel_col:
        return {}
    content_id_col = _optional_column(frame, S2_LOOKUP_CONTENT_ID_COL_CAND)
    sales_channel_content_id_col = _optional_column(frame, S2_LOOKUP_SALES_CHANNEL_CONTENT_ID_COL_CAND)
    working = frame.copy()
    working["_정제키"] = working[title_col].map(clean_title)
    fields = {
        "판매채널명": channel_col,
        "콘텐츠명": title_col,
        "콘텐츠ID": content_id_col,
        "판매채널콘텐츠ID": sales_channel_content_id_col,
    }
    return _index_by_key(working, key_col="_정제키", fields=fields)


def _master_index_by_key(frame: pd.DataFrame | None) -> dict[str, dict[str, str | int]]:
    if frame is None or frame.empty:
        return {}
    title_col = _optional_column(frame, MASTER_TITLE_COL_CAND)
    if not title_col:
        return {}
    content_id_col = _optional_column(frame, MASTER_ID_COL_CAND)
    working = frame.copy()
    working["_정제키"] = working[title_col].map(clean_title)
    fields = {
        "콘텐츠명": title_col,
        "콘텐츠ID": content_id_col,
        "담당부서": "담당부서" if "담당부서" in working.columns else "",
        "담당자명": "담당자명" if "담당자명" in working.columns else "",
    }
    return _index_by_key(working, key_col="_정제키", fields=fields)


def _index_by_key(
    frame: pd.DataFrame,
    *,
    key_col: str,
    fields: dict[str, str],
) -> dict[str, dict[str, str | int]]:
    if frame.empty or key_col not in frame.columns:
        return {}
    working = frame.copy()
    working[key_col] = working[key_col].map(text)
    result: dict[str, dict[str, str | int]] = {}
    for key, group in working[working[key_col].ne("")].groupby(key_col, dropna=False):
        row: dict[str, str | int] = {"_count": len(group)}
        for output_name, source_col in fields.items():
            if source_col and source_col in group.columns:
                row[output_name] = _join_unique(group[source_col])
        result[text(key)] = row
    return result


def _filter_other_channel(candidates: dict[str, str | int], channel: str) -> dict[str, str | int]:
    if not candidates:
        return {}
    channels = [value for value in text(candidates.get("판매채널명")).split(" | ") if value]
    if not channels:
        return candidates
    other_channels = [value for value in channels if value != channel]
    if not other_channels:
        return {}
    result = dict(candidates)
    result["판매채널명"] = " | ".join(other_channels)
    return result


def _missing_detail_for(
    *,
    status: str,
    key: str,
    missing: dict[str, str | int],
    billing: dict[str, str | int],
    service: dict[str, str | int],
    other_payment: dict[str, str | int],
    master: dict[str, str | int],
) -> dict[str, str]:
    if status == MATCH_OK:
        return {"reason": "", "evidence": "", "action": ""}
    if status == MATCH_BLANK:
        return {
            "reason": "정제키 없음",
            "evidence": "정산서 콘텐츠명을 정제한 키가 비어 있음",
            "action": "정산서 원본 상품명 또는 제목 정제 규칙 확인",
        }
    if status == MATCH_AMBIGUOUS:
        return {
            "reason": "S2 중복 후보",
            "evidence": "동일 정제키에 S2 후보가 2개 이상 존재",
            "action": "S2 후보 ID 목록 중 실제 작품 선택",
        }
    if status != MATCH_NONE:
        return {
            "reason": "S2 매칭 상태 확인",
            "evidence": f"S2_매칭상태={status}",
            "action": "매칭 상태와 검토필요사유 확인",
        }

    reasons: list[str] = []
    evidence: list[str] = []
    actions: list[str] = []
    if missing:
        reasons.append("해당채널 판매채널콘텐츠 있음 / 지급정산 없음")
        evidence.append(_detail_piece("정산정보없음", missing, ["판매채널콘텐츠ID", "콘텐츠ID", "콘텐츠명"]))
        actions.append("S2 정산정보 누락 건 메뉴에서 지급정산 보강 또는 제외 여부 확인")
    if service:
        reasons.append("같은채널 판매채널콘텐츠 있음 / 지급정산 없음")
        evidence.append(_detail_piece("판매채널콘텐츠", service, ["판매채널콘텐츠ID", "콘텐츠ID", "콘텐츠명"]))
        actions.append("판매채널콘텐츠ID 기준 지급정산 생성/연결 여부 판단")
    if billing:
        reasons.append("청구정산 후보 있음")
        evidence.append(_detail_piece("청구정산", billing, ["청구정산마스터ID", "계약ID", "대표콘텐츠명"]))
        actions.append("청구정산 건인지 확인하고 지급정산 매핑 대상 제외/전환 여부 판단")
    if other_payment:
        reasons.append("해당채널 지급정산 없음 / 타채널 지급정산 존재")
        evidence.append(_detail_piece("타채널 지급정산", other_payment, ["판매채널명", "콘텐츠ID", "콘텐츠명"]))
        actions.append("해당 판매채널 지급정산 생성/보강 여부 판단")
    if master:
        reasons.append("콘텐츠마스터 있음 / S2 지급정산 없음")
        evidence.append(_detail_piece("콘텐츠마스터", master, ["콘텐츠ID", "콘텐츠명", "담당부서", "담당자명"]))
        actions.append("콘텐츠ID 기준 판매채널콘텐츠/지급정산 생성 여부 판단")

    if not reasons:
        return {
            "reason": "S2/콘텐츠마스터 정제키 후보 없음",
            "evidence": f"정제키={key}",
            "action": "신규 작품 등록 대상인지, 정산 제외/no-action 대상인지 확인",
        }
    return {
        "reason": " | ".join(dict.fromkeys(reasons)),
        "evidence": " || ".join(value for value in evidence if value),
        "action": " | ".join(dict.fromkeys(actions)),
    }


def _detail_piece(label: str, values: dict[str, str | int], fields: Iterable[str]) -> str:
    parts = [f"{field}={text(values.get(field))}" for field in fields if text(values.get(field))]
    return f"{label}: " + ", ".join(parts) if parts else label


def _index_by_channel_and_key(
    frame: pd.DataFrame,
    *,
    channel_col: str,
    key_col: str,
    fields: list[str],
) -> dict[tuple[str, str], dict[str, str | int]]:
    if frame.empty or channel_col not in frame.columns or key_col not in frame.columns:
        return {}
    working = frame.copy()
    working[channel_col] = working[channel_col].map(text)
    working[key_col] = working[key_col].map(text)
    result: dict[tuple[str, str], dict[str, str | int]] = {}
    for (channel, key), group in working[working[key_col].ne("")].groupby([channel_col, key_col], dropna=False):
        row: dict[str, str | int] = {"_count": len(group)}
        for field in fields:
            if field in group.columns:
                row[field] = _join_unique(group[field])
        result[(text(channel), text(key))] = row
    return result


def _join_unique(values: Iterable[Any], limit: int = 30) -> str:
    result: list[str] = []
    for value in values:
        value_text = text(value)
        if value_text and value_text not in result:
            result.append(value_text)
        if len(result) >= limit:
            break
    return " | ".join(result)


def _append_reason(base: Any, addition: Any) -> str:
    reasons: list[str] = []
    for chunk in [base, addition]:
        for part in text(chunk).split(" | "):
            part = text(part)
            if part and part not in reasons:
                reasons.append(part)
    return " | ".join(reasons)


def _guard_validation(guards: S2ReferenceGuards, excluded_count: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("S2 정산정보 누락 lookup 행 수", len(guards.missing)),
            ("청구정산 lookup 행 수", len(guards.billing)),
            ("판매채널콘텐츠 lookup 행 수", len(guards.service_contents)),
            ("S2 정산정보 누락 제외 행 수", excluded_count),
        ],
        columns=["항목", "값"],
    )


def _append_summary(summary: pd.DataFrame, rows: list[tuple[str, Any]]) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame(rows, columns=["항목", "값"])
    existing = summary[~summary["항목"].isin([name for name, _ in rows])].copy()
    return pd.concat([existing, pd.DataFrame(rows, columns=["항목", "값"])], ignore_index=True)
