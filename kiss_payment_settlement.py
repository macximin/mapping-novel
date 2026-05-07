from __future__ import annotations

import json
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO
from xml.etree import ElementTree as ET

import pandas as pd

from mapping_core import extract_master_work_title, text


OOXML_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

REQUIRED_COLUMNS = [
    "승인상태",
    "지급정산상태",
    "판매채널명",
    "콘텐츠형태",
    "콘텐츠명",
    "지급정산마스터ID",
    "지급정산상세ID",
    "콘텐츠ID",
    "판매채널콘텐츠ID",
]

DATE_COLUMNS = ["지급정산마스터 등록 일자"]
API_RAW_SHEET_NAME = "원본데이터"
API_RAW_COLUMN_ALIASES = {
    "cnfmStsCdNm": "승인상태",
    "pymtSetlStsCdNm": "지급정산상태",
    "schnNm": "판매채널명",
    "ctnsStleCdNm": "콘텐츠형태",
    "ctnsNm": "콘텐츠명",
    "pymtSetlId": "지급정산마스터ID",
    "pymtSetlDtlId": "지급정산상세ID",
    "ctnsId": "콘텐츠ID",
    "schnCtnsId": "판매채널콘텐츠ID",
    "cretDtm": "지급정산마스터 등록 일자",
}


@dataclass(frozen=True)
class PaymentSettlementImportResult:
    source_rows: int
    cache_rows_before: int
    cache_rows_after: int
    s2_lookup_rows: int
    output_cache: Path
    output_s2_lookup: Path
    summary: dict[str, Any]


def load_payment_settlement_list(source: str | Path | BinaryIO) -> pd.DataFrame:
    """Read S2 source exports.

    S2-generated xlsx files can contain non-standard style attributes that make
    openpyxl fail before data is read. The OOXML fallback reads sheet cell values
    directly and avoids style parsing.
    """

    first_frame = _normalize_frame(_read_first_sheet(source))
    frame = _coerce_payment_settlement_frame(first_frame)
    if frame is None:
        raw_frame = _read_named_sheet(source, API_RAW_SHEET_NAME)
        if raw_frame is not None:
            frame = _coerce_payment_settlement_frame(_normalize_frame(raw_frame))
    if frame is None:
        validate_payment_settlement_columns(first_frame)
        frame = first_frame

    return _prepare_payment_settlement_frame(frame)


def _prepare_payment_settlement_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _normalize_frame(frame)
    validate_payment_settlement_columns(frame)
    for column in DATE_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].map(normalize_excel_date)
    return frame


def _read_first_sheet(source: str | Path | BinaryIO) -> pd.DataFrame:
    try:
        if hasattr(source, "seek"):
            source.seek(0)
        frame = pd.read_excel(source, dtype=object, engine="openpyxl")
    except Exception:
        if hasattr(source, "seek"):
            source.seek(0)
        frame = _read_first_sheet_ooxml(source)
    return frame


def _read_named_sheet(source: str | Path | BinaryIO, sheet_name: str) -> pd.DataFrame | None:
    try:
        if hasattr(source, "seek"):
            source.seek(0)
        return pd.read_excel(source, sheet_name=sheet_name, dtype=object, engine="openpyxl")
    except Exception:
        return None


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.rename(columns={column: normalize_header(column) for column in frame.columns})
    return frame.dropna(how="all").reset_index(drop=True)


def _coerce_payment_settlement_frame(frame: pd.DataFrame) -> pd.DataFrame | None:
    if _has_required_columns(frame):
        return frame
    api_frame = _coerce_api_raw_frame(frame)
    if api_frame is not None and _has_required_columns(api_frame):
        return api_frame
    return None


def _has_required_columns(frame: pd.DataFrame) -> bool:
    return all(column in frame.columns for column in REQUIRED_COLUMNS)


def _coerce_api_raw_frame(frame: pd.DataFrame) -> pd.DataFrame | None:
    if not {"schnCtnsId", "ctnsId", "ctnsNm", "pymtSetlId", "pymtSetlDtlId"}.issubset(frame.columns):
        return None
    converted = frame.copy()
    for raw_column, standard_column in API_RAW_COLUMN_ALIASES.items():
        if raw_column in converted.columns and standard_column not in converted.columns:
            converted[standard_column] = converted[raw_column]
    return converted


def validate_payment_settlement_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        available = ", ".join(map(str, frame.columns))
        raise ValueError(f"지급 정산 관리 목록 필수 컬럼이 없습니다: {missing}. 현재 컬럼: {available}")


def to_s2_lookup(frame: pd.DataFrame) -> pd.DataFrame:
    validate_payment_settlement_columns(frame)
    working = frame.copy()
    working["판매채널콘텐츠ID"] = working["판매채널콘텐츠ID"].map(_id_text)
    working = working[working["판매채널콘텐츠ID"].ne("")].copy()
    if "지급정산마스터 등록 일자" in working.columns:
        working = working.sort_values("지급정산마스터 등록 일자", ascending=False, kind="stable")
    working = working.drop_duplicates(subset=["판매채널콘텐츠ID"], keep="first")

    result = pd.DataFrame(
        {
            "콘텐츠명": working["콘텐츠명"].map(extract_master_work_title).map(text),
            "판매채널콘텐츠ID": working["판매채널콘텐츠ID"],
            "콘텐츠ID": working["콘텐츠ID"].map(_id_text),
            "판매채널명": working["판매채널명"].map(text),
            "콘텐츠형태": working["콘텐츠형태"].map(text),
            "지급정산상태": working["지급정산상태"].map(text),
            "승인상태": working["승인상태"].map(text),
            "지급정산마스터ID": working["지급정산마스터ID"].map(_id_text),
            "지급정산상세ID": working["지급정산상세ID"].map(_id_text),
        }
    )
    if "지급정산마스터 등록 일자" in working.columns:
        result["지급정산마스터_등록일자"] = working["지급정산마스터 등록 일자"].map(text)
    return result.reset_index(drop=True)


def merge_payment_settlement_cache(existing: pd.DataFrame | None, incoming: pd.DataFrame) -> pd.DataFrame:
    frames = [incoming]
    if existing is not None and not existing.empty:
        frames.append(existing)
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.rename(columns={column: normalize_header(column) for column in merged.columns})
    if "지급정산마스터 등록 일자" in merged.columns:
        merged["지급정산마스터 등록 일자"] = merged["지급정산마스터 등록 일자"].map(normalize_excel_date)
        merged = merged.sort_values("지급정산마스터 등록 일자", ascending=False, kind="stable")
    if "지급정산상세ID" in merged.columns:
        merged["지급정산상세ID"] = merged["지급정산상세ID"].map(_id_text)
        merged = merged.drop_duplicates(subset=["지급정산상세ID"], keep="first")
    return merged.reset_index(drop=True)


def summarize_payment_settlement(frame: pd.DataFrame) -> dict[str, Any]:
    validate_payment_settlement_columns(frame)
    dates = frame.get("지급정산마스터 등록 일자", pd.Series(dtype=object)).map(text)
    nonblank_dates = dates[dates.ne("")]
    sale_channel_ids = frame["판매채널콘텐츠ID"].map(_id_text)
    content_ids = frame["콘텐츠ID"].map(_id_text)
    conflict_counts = sales_channel_content_conflict_counts(frame)
    return {
        "rows": len(frame),
        "columns": len(frame.columns),
        "content_shape_counts": _counts(frame["콘텐츠형태"]),
        "approval_status_counts": _counts(frame["승인상태"]),
        "payment_settlement_status_counts": _counts(frame["지급정산상태"]),
        "top_channel_counts": _counts(frame["판매채널명"], limit=20),
        "content_id_nonblank": int(content_ids.ne("").sum()),
        "content_id_unique": int(content_ids[content_ids.ne("")].nunique()),
        "sales_channel_content_id_nonblank": int(sale_channel_ids.ne("").sum()),
        "sales_channel_content_id_unique": int(sale_channel_ids[sale_channel_ids.ne("")].nunique()),
        "registered_at_min": nonblank_dates.min() if not nonblank_dates.empty else "",
        "registered_at_max": nonblank_dates.max() if not nonblank_dates.empty else "",
        **conflict_counts,
    }


def sales_channel_content_conflict_counts(frame: pd.DataFrame) -> dict[str, int]:
    validate_payment_settlement_columns(frame)
    working = frame.copy()
    working["_판매채널콘텐츠ID"] = working["판매채널콘텐츠ID"].map(_id_text)
    working = working[working["_판매채널콘텐츠ID"].ne("")].copy()
    if working.empty:
        return {
            "sales_channel_content_id_duplicate_keys": 0,
            "sales_channel_content_id_multiple_titles": 0,
            "sales_channel_content_id_multiple_master_ids": 0,
            "sales_channel_content_id_multiple_content_ids": 0,
        }
    working["_콘텐츠명"] = working["콘텐츠명"].map(text)
    working["_지급정산마스터ID"] = working["지급정산마스터ID"].map(_id_text)
    working["_콘텐츠ID"] = working["콘텐츠ID"].map(_id_text)
    grouped = working.groupby("_판매채널콘텐츠ID", dropna=False)
    return {
        "sales_channel_content_id_duplicate_keys": int((grouped.size() > 1).sum()),
        "sales_channel_content_id_multiple_titles": int((grouped["_콘텐츠명"].nunique() > 1).sum()),
        "sales_channel_content_id_multiple_master_ids": int((grouped["_지급정산마스터ID"].nunique() > 1).sum()),
        "sales_channel_content_id_multiple_content_ids": int((grouped["_콘텐츠ID"].nunique() > 1).sum()),
    }


def import_payment_settlement_export(
    source: str | Path,
    *,
    cache_path: str | Path,
    s2_lookup_path: str | Path,
    merge_existing: bool = True,
) -> PaymentSettlementImportResult:
    incoming = load_payment_settlement_list(source)
    return import_payment_settlement_frame(
        incoming,
        cache_path=cache_path,
        s2_lookup_path=s2_lookup_path,
        merge_existing=merge_existing,
    )


def import_payment_settlement_frame(
    incoming: pd.DataFrame,
    *,
    cache_path: str | Path,
    s2_lookup_path: str | Path,
    merge_existing: bool = True,
) -> PaymentSettlementImportResult:
    cache = Path(cache_path)
    s2_lookup = Path(s2_lookup_path)
    incoming = _prepare_payment_settlement_frame(incoming)
    existing = pd.read_csv(cache, dtype=object) if cache.exists() else None
    before = 0 if existing is None else len(existing)
    merged = merge_payment_settlement_cache(existing if merge_existing else None, incoming)
    lookup = to_s2_lookup(merged)

    cache.parent.mkdir(parents=True, exist_ok=True)
    s2_lookup.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(cache, index=False, encoding="utf-8-sig")
    lookup.to_csv(s2_lookup, index=False, encoding="utf-8-sig")
    summary = summarize_payment_settlement(merged)
    return PaymentSettlementImportResult(
        source_rows=len(incoming),
        cache_rows_before=before,
        cache_rows_after=len(merged),
        s2_lookup_rows=len(lookup),
        output_cache=cache,
        output_s2_lookup=s2_lookup,
        summary=summary,
    )


def payment_settlement_frame_from_api_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = _normalize_frame(pd.DataFrame(rows))
    converted = _coerce_payment_settlement_frame(frame)
    if converted is None:
        validate_payment_settlement_columns(frame)
        converted = frame
    return _prepare_payment_settlement_frame(converted)


def save_summary(path: str | Path, result: PaymentSettlementImportResult) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_rows": result.source_rows,
        "cache_rows_before": result.cache_rows_before,
        "cache_rows_after": result.cache_rows_after,
        "s2_lookup_rows": result.s2_lookup_rows,
        "output_cache": str(result.output_cache),
        "output_s2_lookup": str(result.output_s2_lookup),
        "summary": result.summary,
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_first_sheet_ooxml(source: str | Path | BinaryIO) -> pd.DataFrame:
    with zipfile.ZipFile(source) as archive:
        shared_strings = _shared_strings(archive)
        sheet_path = _first_sheet_path(archive)
        root = ET.fromstring(archive.read(sheet_path))
        matrix: list[list[Any]] = []
        for row in root.findall("a:sheetData/a:row", OOXML_NS):
            row_idx = int(row.attrib.get("r", len(matrix) + 1)) - 1
            while len(matrix) <= row_idx:
                matrix.append([])
            for cell in row.findall("a:c", OOXML_NS):
                col_idx = _cell_col_index(cell.attrib.get("r", "")) - 1
                while len(matrix[row_idx]) <= col_idx:
                    matrix[row_idx].append("")
                matrix[row_idx][col_idx] = _cell_value(cell, shared_strings)
        if not matrix:
            return pd.DataFrame()
        width = max(len(row) for row in matrix)
        matrix = [row + [""] * (width - len(row)) for row in matrix]
        headers = [normalize_header(value) or f"column_{idx + 1}" for idx, value in enumerate(matrix[0])]
        return pd.DataFrame(matrix[1:], columns=_unique(headers))


def _shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    result = []
    for item in root.findall("a:si", OOXML_NS):
        result.append("".join(text_node.text or "" for text_node in item.findall(".//a:t", OOXML_NS)))
    return result


def _first_sheet_path(archive: zipfile.ZipFile) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    relmap = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    sheet = workbook.find("a:sheets/a:sheet", OOXML_NS)
    if sheet is None:
        raise ValueError("엑셀 파일에 시트가 없습니다.")
    rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
    target = relmap[rel_id]
    return "xl/" + target.lstrip("/") if not target.startswith("xl/") else target


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    if cell.attrib.get("t") == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//a:t", OOXML_NS))
    value = cell.find("a:v", OOXML_NS)
    if value is None:
        return ""
    raw = value.text or ""
    if cell.attrib.get("t") == "s":
        return shared_strings[int(raw)] if raw else ""
    return raw


def _cell_col_index(reference: str) -> int:
    match = re.match(r"([A-Z]+)", reference)
    if not match:
        return 1
    index = 0
    for char in match.group(1):
        index = index * 26 + ord(char) - ord("A") + 1
    return index


def normalize_header(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\n", " ")).strip()


def normalize_excel_date(value: Any) -> str:
    raw = text(value)
    if not raw:
        return ""
    if re.fullmatch(r"\d+(?:\.\d+)?", raw):
        try:
            date_value = datetime(1899, 12, 30) + timedelta(days=float(raw))
            return date_value.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return raw
    return raw


def _id_text(value: Any) -> str:
    raw = text(value)
    if not raw:
        return ""
    if re.fullmatch(r"\d+(?:\.0+)?", raw):
        return str(int(float(raw)))
    return raw


def _counts(series: pd.Series, *, limit: int | None = None) -> dict[str, int]:
    counts = series.map(text).replace("", "(blank)").value_counts()
    if limit is not None:
        counts = counts.head(limit)
    return {str(key): int(value) for key, value in counts.items()}


def _unique(headers: list[str]) -> list[str]:
    result: list[str] = []
    seen: dict[str, int] = {}
    for header in headers:
        count = seen.get(header, 0) + 1
        seen[header] = count
        result.append(header if count == 1 else f"{header}__{count}")
    return result
