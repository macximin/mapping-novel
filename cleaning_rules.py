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
DISABLED_ROW_MARKERS = (
    "[사용안함]",
    "(사용안함)",
    "[사용금지]",
    "(사용금지)",
    "[정산정보없음]",
    "(정산정보없음)",
)
TITLE_EXCEPTIONS = ("24/7", "실명마제", "라마대제")
MASTER_CONFIRMED_STATUS = "확정"
MASTER_CONFIRMED_TRAILING_FIELD_COUNT = 4
STRUCTURED_S2_TITLE_PATTERN = re.compile(r"^\d+_([^_]+)_[^_]+_일반$")
EXACT_CLEAN_TITLE_ALIASES = {
    "늙은경비에게조교당하는스튜어디스의이야기": "늙은경비에게조교당하는스튜어디스",
    "늙은경비에게조교당하는스튜어디스이야기": "늙은경비에게조교당하는스튜어디스",
    "천대받는f급힐러라좋았는데요": "천대받는f급힐러라서좋았는데요",
    "던전에서성자가하는일": "던전에서성자性者가하는일",
    "백치공주시리즈": "백치공주",
}
SQUARE_WRAPPER_TAGS = (
    "bl",
    "gl",
    "tl",
    "19",
    "19금",
    "성인",
    "연재",
    "단행본",
    "외전",
    "완결",
)


@dataclass(frozen=True)
class CleaningPolicy:
    disabled_row_markers: tuple[str, ...] = DISABLED_ROW_MARKERS
    title_exceptions: tuple[str, ...] = TITLE_EXCEPTIONS
    angle_title_patterns: tuple[Pattern[str], ...] = ANGLE_TITLE_PATTERNS
    master_confirmed_status: str = MASTER_CONFIRMED_STATUS
    master_confirmed_trailing_field_count: int = MASTER_CONFIRMED_TRAILING_FIELD_COUNT

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

    def _whole_square_wrapped_title(self, value: str) -> str:
        match = re.fullmatch(r"\[([^\[\]]+)\]", value)
        if not match:
            return ""

        inner = self.text(match.group(1))
        if not inner:
            return ""

        normalized_inner = unicodedata.normalize("NFKC", inner)
        marker_names = {
            marker.strip("[]()").lower()
            for marker in self.disabled_row_markers
        }
        if normalized_inner.lower() in marker_names or normalized_inner.lower() in SQUARE_WRAPPER_TAGS:
            return ""
        return inner

    def _structured_s2_title_segment(self, value: str) -> str:
        match = STRUCTURED_S2_TITLE_PATTERN.fullmatch(value)
        if not match:
            return ""
        return self.text(match.group(1))

    def _apply_exact_title_alias(self, key: str) -> str:
        return EXACT_CLEAN_TITLE_ALIASES.get(key, key)

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

        structured_s2_title = self._structured_s2_title_segment(t)
        if structured_s2_title:
            t = structured_s2_title

        t = re.sub(r"\s*\d+/\d+$", "", t).lower()
        t = re.sub(r"(^|\s)제\s*\d+[권화]", " ", t)
        for old, new in {"Un-holyNight": "UnholyNight", "?": "", "~": "", ",": "", "-": "", "_": ""}.items():
            t = t.replace(old, new)

        whole_square_wrapped_title = self._whole_square_wrapped_title(t)
        if whole_square_wrapped_title:
            t = whole_square_wrapped_title

        t = re.sub(r"\([^)]*\)|\[[^\]]*\]", "", t)
        t = re.sub(r"【[^】]*】", "", t)

        for pattern in ["세트구매", "난세의 서 편", "초혼의 사자 편", "전설의 부활 편"]:
            t = re.sub(pattern, "", t)

        t = unicodedata.normalize("NFKC", t)
        t = re.sub(r"\d+\s*[권화부회]", "", t)

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
        t = re.sub(r"[\.,\~\-–—!@#$%^&*_=+\\|/:;\"''`<>?，｡､{}()]", "", t)
        t = t.replace("[", "").replace("]", "")
        t = re.sub(r"특별$", "", t)
        t = "".join(t.split())
        return self._apply_exact_title_alias(t.strip().lower())

    def extract_master_work_title(self, value: Any) -> str:
        raw = unicodedata.normalize("NFKC", self.text(value))
        if not raw:
            return ""

        for pattern in self.angle_title_patterns:
            match = pattern.search(raw)
            if match and self.text(match.group(1)):
                return self.text(match.group(1))

        confirmed_master_title = self.extract_confirmed_master_title(raw)
        if confirmed_master_title:
            return confirmed_master_title

        return raw

    def extract_confirmed_master_title(self, value: Any) -> str:
        raw = unicodedata.normalize("NFKC", self.text(value))
        parts = [part.strip() for part in raw.split("_") if part.strip()]
        tail_count = self.master_confirmed_trailing_field_count
        if len(parts) <= tail_count:
            return ""
        if self.clean_title(parts[-1]) != self.clean_title(self.master_confirmed_status):
            return ""
        return "_".join(parts[:-tail_count]).strip()

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


def extract_confirmed_master_title(value: Any) -> str:
    return DEFAULT_CLEANING_POLICY.extract_confirmed_master_title(value)


def clean_master_title(value: Any) -> str:
    return DEFAULT_CLEANING_POLICY.clean_master_title(value)
