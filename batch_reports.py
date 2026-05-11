from __future__ import annotations

from typing import Any

import pandas as pd

from cleaning_rules import text
from mapping_core import MATCH_AMBIGUOUS, MATCH_BLANK, MATCH_NONE, MATCH_OK


COMBINED_REPORT_COLUMNS = [
    "파일",
    "S2 판매채널",
    "플랫폼",
    "정산서_원본행번호",
    "정산서원본_source_row",
    "정산서_콘텐츠명",
    "정제_상품명",
    "S2_매칭상태",
    "S2_판매채널콘텐츠ID",
    "S2_콘텐츠ID",
    "S2_콘텐츠명",
    "S2_후보수",
    "S2_후보ID목록",
    "S2_후보콘텐츠명목록",
    "S2_정산정보누락_후보수",
    "S2_정산정보누락_판매채널콘텐츠ID목록",
    "S2_정산정보누락_콘텐츠ID목록",
    "청구정산_후보수",
    "청구정산마스터ID목록",
    "청구정산_계약ID목록",
    "S2_분리사유",
    "검토필요사유",
    "검토필요(Y/N)",
]

WORK_ORDER_COLUMNS = [
    "작업상태",
    "담당PD",
    "S2 판매채널",
    "S2 검색어",
    "플랫폼",
    "정제_상품명",
    "정산서_대표콘텐츠명",
    "정산서_콘텐츠명목록",
    "정산서 행 수",
    "파일목록",
    "원본행번호목록",
    "엑셀행번호목록",
    "S2_매칭상태",
    "권장액션",
    "검토필요사유",
    "S2_판매채널콘텐츠ID",
    "S2_콘텐츠ID",
    "S2_콘텐츠명",
    "S2_후보수",
    "S2_후보ID목록",
    "S2_후보콘텐츠명목록",
    "S2_정산정보누락_후보수",
    "S2_정산정보누락_판매채널콘텐츠ID목록",
    "S2_정산정보누락_콘텐츠ID목록",
    "청구정산_후보수",
    "청구정산마스터ID목록",
    "청구정산_계약ID목록",
    "PD 확인 메모",
]


def build_combined_mapping_report_frame(results: list[dict[str, Any]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for result in results:
        mapping = result.get("mapping")
        if text(result.get("status")) != "success" or mapping is None:
            continue
        rows = getattr(mapping, "rows", pd.DataFrame()).copy()
        if rows.empty:
            continue
        rows.insert(0, "플랫폼", text(result.get("platform")))
        rows.insert(0, "S2 판매채널", text(result.get("s2_sales_channel")))
        rows.insert(0, "파일", text(result.get("source_name")))
        frames.append(_ensure_columns(rows, COMBINED_REPORT_COLUMNS))

    if not frames:
        return pd.DataFrame(columns=COMBINED_REPORT_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    return _ensure_columns(combined, COMBINED_REPORT_COLUMNS)


def build_pd_work_order_report_frame(results: list[dict[str, Any]]) -> pd.DataFrame:
    combined = build_combined_mapping_report_frame(results)
    if combined.empty:
        return pd.DataFrame(columns=WORK_ORDER_COLUMNS)

    review_rows = combined[combined["검토필요(Y/N)"].map(text).eq("Y")].copy()
    if review_rows.empty:
        return pd.DataFrame(columns=WORK_ORDER_COLUMNS)

    grouped_rows: list[dict[str, Any]] = []
    group_cols = [
        "S2 판매채널",
        "플랫폼",
        "정제_상품명",
        "S2_매칭상태",
        "검토필요사유",
        "S2_후보ID목록",
        "S2_후보콘텐츠명목록",
        "S2_분리사유",
        "S2_정산정보누락_판매채널콘텐츠ID목록",
        "청구정산마스터ID목록",
    ]
    for _, group in review_rows.groupby(group_cols, dropna=False, sort=False):
        first = group.iloc[0]
        grouped_rows.append(
            {
                "작업상태": "대기",
                "담당PD": "",
                "S2 판매채널": first["S2 판매채널"],
                "S2 검색어": first["정제_상품명"],
                "플랫폼": first["플랫폼"],
                "정제_상품명": first["정제_상품명"],
                "정산서_대표콘텐츠명": first["정산서_콘텐츠명"],
                "정산서_콘텐츠명목록": _join_unique(group["정산서_콘텐츠명"]),
                "정산서 행 수": len(group),
                "파일목록": _join_unique(group["파일"]),
                "원본행번호목록": _join_unique(group["정산서_원본행번호"]),
                "엑셀행번호목록": _join_unique(group["정산서원본_source_row"]),
                "S2_매칭상태": first["S2_매칭상태"],
                "권장액션": _suggest_action(first),
                "검토필요사유": first["검토필요사유"],
                "S2_판매채널콘텐츠ID": first["S2_판매채널콘텐츠ID"],
                "S2_콘텐츠ID": first["S2_콘텐츠ID"],
                "S2_콘텐츠명": first["S2_콘텐츠명"],
                "S2_후보수": first["S2_후보수"],
                "S2_후보ID목록": first["S2_후보ID목록"],
                "S2_후보콘텐츠명목록": first["S2_후보콘텐츠명목록"],
                "S2_정산정보누락_후보수": first["S2_정산정보누락_후보수"],
                "S2_정산정보누락_판매채널콘텐츠ID목록": first["S2_정산정보누락_판매채널콘텐츠ID목록"],
                "S2_정산정보누락_콘텐츠ID목록": first["S2_정산정보누락_콘텐츠ID목록"],
                "청구정산_후보수": first["청구정산_후보수"],
                "청구정산마스터ID목록": first["청구정산마스터ID목록"],
                "청구정산_계약ID목록": first["청구정산_계약ID목록"],
                "PD 확인 메모": "",
            }
        )

    return pd.DataFrame(grouped_rows, columns=WORK_ORDER_COLUMNS)


def _ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result[columns]


def _join_unique(values: pd.Series) -> str:
    seen: list[str] = []
    for value in values:
        value_text = text(value)
        if value_text and value_text not in seen:
            seen.append(value_text)
    return " | ".join(seen)


def _suggest_action(row: pd.Series) -> str:
    reason = text(row.get("검토필요사유"))
    status = text(row.get("S2_매칭상태"))
    if "청구정산 후보" in reason:
        return "청구정산 건인지 확인하고 지급정산 매핑 대상 제외/전환 여부 판단"
    if "S2 정산정보 누락 건 등재" in reason:
        return "S2 정산정보 누락 건 메뉴에서 확인 후 지급정산 보강 또는 제외 요청"
    if status == MATCH_NONE:
        return "S2 판매채널에서 정제 제목으로 검색, 없으면 판매채널콘텐츠ID 생성/보강 요청"
    if status == MATCH_AMBIGUOUS:
        return "S2 후보 ID 목록 중 실제 작품 선택"
    if status == MATCH_BLANK:
        return "정산서 원본 상품명 확인 후 제목 정제 규칙 또는 파일 헤더 보정"
    if status != MATCH_OK:
        return "매칭 상태 확인"
    return "검토필요사유 확인"
