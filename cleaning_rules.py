from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Pattern

import pandas as pd


ANGLE_TITLE_PATTERNS = (
    re.compile(r"<([^<>]+)>"),
    re.compile(r"＜([^＜＞]+)＞"),
    re.compile(r"〈([^〈〉]+)〉"),
    re.compile(r"《([^《》]+)》"),
)
DISABLED_ROW_MARKERS = ("[사용안함]", "(사용안함)", "[사용금지]", "(사용금지)")
TITLE_EXCEPTIONS = ("24/7", "실명마제", "라마대제")


@dataclass(frozen=True)
class CleaningPolicy:
    disabled_row_markers: tuple[str, ...] = DISABLED_ROW_MARKERS
    title_exceptions: tuple[str, ...] = TITLE_EXCEPTIONS
    angle_title_patterns: tuple[Pattern[str], ...] = ANGLE_TITLE_PATTERNS

    def text(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        return str(value).strip()

    def has_disabled_row_marker(self, value: Any) -> bool:
        normalized = unicodedata.normalize("NFKC", self.text(value))
        return any(marker in normalized for marker in self.disabled_row_markers)

    def disabled_row_mask(self, frame: pd.DataFrame, columns: Iterable[Any] | None = None) -> pd.Series:
        if columns is None:
            selected_columns = list(frame.columns)
        else:
            selected_columns = [column for column in columns if column in frame.columns]
        mask = pd.Series(False, index=frame.index)
        for column in selected_columns:
            mask = mask | frame[column].map(self.has_disabled_row_marker)
        return mask

    def drop_disabled_rows(self, frame: pd.DataFrame, columns: Iterable[Any] | None = None) -> pd.DataFrame:
        if frame.empty:
            return frame.copy().reset_index(drop=True)
        return frame.loc[~self.disabled_row_mask(frame, columns=columns)].reset_index(drop=True)

    def clean_title(self, value: Any) -> str:
        t = str(value).strip()

        t = re.sub(r"\s*~[^~]+~\s*$", "", t)
        t = re.sub(r"\s+\d+부(?:\s*-\s*.*)?$", "", t)

        for exception in self.title_exceptions:
            if exception in t:
                return exception.lower()

        if isinstance(value, (datetime, date)):
            return f"{value.month}월{value.day}일".lower()

        if re.fullmatch(r"\d{1,2}월\d{1,2}일", t):
            return t.lower()

        t = re.sub(r"\s*\d+/\d+$", "", t).lower()
        t = re.sub(r"\s*제\s*\d+[권화]", "", t)
        for old, new in {"Un-holyNight": "UnholyNight", "?": "", "~": "", ",": "", "-": "", "_": ""}.items():
            t = t.replace(old, new)

        t = re.sub(r"\([^)]*\)|\[[^\]]*\]", "", t)
        t = re.sub(r"【[^】]*】", "", t)

        for pattern in ["세트구매", "난세의 서 편", "초혼의 사자 편", "전설의 부활 편"]:
            t = re.sub(pattern, "", t)

        t = unicodedata.normalize("NFKC", t)
        t = re.sub(r"\d+[권화부회]", "", t)

        for keyword in [
            "개정판 l",
            "개정판",
            "외전",
            "무삭제본",
            "무삭제판",
            "합본",
            "단행본",
            "시즌",
            "세트",
            "연재",
            "특별",
            "최종화",
            "완결",
            "2부",
            "무삭제",
            "완전판",
            "세개정판",
            "19세개정판",
        ]:
            t = t.replace(keyword, "")

        t = re.sub(r"\d+", "", t).rstrip(".")
        t = re.sub(r"[\.\~\-–—!@#$%^&*_=+\\|/:;\"''`<>?，｡､{}()]", "", t)
        t = t.replace("[", "").replace("]", "")
        t = re.sub(r"특별$", "", t)
        t = "".join(t.split())
        return t.strip().lower()

    def extract_master_work_title(self, value: Any) -> str:
        raw = unicodedata.normalize("NFKC", self.text(value))
        if not raw:
            return ""

        for pattern in self.angle_title_patterns:
            match = pattern.search(raw)
            if match and self.text(match.group(1)):
                return self.text(match.group(1))

        parts = [part.strip() for part in raw.split("_") if part.strip()]
        if len(parts) >= 5 and self.clean_title(parts[-1]) == self.clean_title("확정"):
            candidate = "_".join(parts[:-4]).strip()
            if candidate:
                return candidate

        return raw

    def clean_master_title(self, value: Any) -> str:
        return self.clean_title(self.extract_master_work_title(value))


DEFAULT_CLEANING_POLICY = CleaningPolicy()


def text(value: Any) -> str:
    return DEFAULT_CLEANING_POLICY.text(value)


def has_disabled_row_marker(value: Any) -> bool:
    return DEFAULT_CLEANING_POLICY.has_disabled_row_marker(value)


def disabled_row_mask(frame: pd.DataFrame, columns: Iterable[Any] | None = None) -> pd.Series:
    return DEFAULT_CLEANING_POLICY.disabled_row_mask(frame, columns=columns)


def drop_disabled_rows(frame: pd.DataFrame, columns: Iterable[Any] | None = None) -> pd.DataFrame:
    return DEFAULT_CLEANING_POLICY.drop_disabled_rows(frame, columns=columns)


def clean_title(value: Any) -> str:
    return DEFAULT_CLEANING_POLICY.clean_title(value)


def extract_master_work_title(value: Any) -> str:
    return DEFAULT_CLEANING_POLICY.extract_master_work_title(value)


def clean_master_title(value: Any) -> str:
    return DEFAULT_CLEANING_POLICY.clean_master_title(value)
