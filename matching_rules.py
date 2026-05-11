from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from cleaning_rules import text


S2_CHANNEL_COLUMN = "판매채널명"


@dataclass(frozen=True)
class S2ChannelRule:
    exact_channels: tuple[str, ...] = ()
    include_tokens: tuple[str, ...] = ()
    exclude_tokens: tuple[str, ...] = ()
    label: str = ""


@dataclass(frozen=True)
class S2SalesChannelDetection:
    sales_channel: str
    platform: str


@dataclass(frozen=True)
class S2ChannelFilterResult:
    frame: pd.DataFrame
    platform: str
    source_name: str
    before_rows: int
    after_rows: int
    matched_channels: tuple[str, ...]
    rule_label: str
    active: bool
    reason: str = ""

    @property
    def row_delta_label(self) -> str:
        return f"{self.before_rows:,} -> {self.after_rows:,}"

    @property
    def channel_label(self) -> str:
        return " | ".join(self.matched_channels)

    def message(self) -> str:
        if not self.active:
            return self.reason
        channels = self.channel_label or "-"
        return f"S2 판매채널 필터: {self.row_delta_label}행 ({channels})"


class S2ChannelPolicy:
    def filter_by_sales_channel(
        self,
        frame: pd.DataFrame,
        *,
        sales_channel: str,
        source_name: str = "",
    ) -> S2ChannelFilterResult:
        before_rows = len(frame)
        normalized_channel = text(sales_channel)
        normalized_source = text(source_name)
        if frame.empty:
            return S2ChannelFilterResult(
                frame=frame.copy(),
                platform="",
                source_name=normalized_source,
                before_rows=0,
                after_rows=0,
                matched_channels=(),
                rule_label=normalized_channel,
                active=False,
                reason="S2 기준이 비어 있어 판매채널 필터를 건너뜁니다.",
            )
        if S2_CHANNEL_COLUMN not in frame.columns:
            return S2ChannelFilterResult(
                frame=frame.copy(),
                platform="",
                source_name=normalized_source,
                before_rows=before_rows,
                after_rows=before_rows,
                matched_channels=(),
                rule_label=normalized_channel,
                active=False,
                reason="S2 기준에 판매채널명 컬럼이 없어 판매채널 필터를 건너뜁니다.",
            )
        if not normalized_channel:
            return S2ChannelFilterResult(
                frame=frame.copy(),
                platform="",
                source_name=normalized_source,
                before_rows=before_rows,
                after_rows=before_rows,
                matched_channels=(),
                rule_label="",
                active=False,
                reason="파일명에서 S2 판매채널명을 찾지 못해 판매채널 필터를 건너뜁니다.",
            )

        channels = frame[S2_CHANNEL_COLUMN].map(text)
        mask = channels.eq(normalized_channel)
        filtered = frame.loc[mask].reset_index(drop=True)
        matched_channels = tuple(sorted(channels[mask].drop_duplicates().tolist()))
        return S2ChannelFilterResult(
            frame=filtered,
            platform=platform_for_s2_sales_channel(normalized_channel) or "",
            source_name=normalized_source,
            before_rows=before_rows,
            after_rows=len(filtered),
            matched_channels=matched_channels,
            rule_label=normalized_channel,
            active=True,
        )

    def filter(self, frame: pd.DataFrame, *, platform: str, source_name: str = "") -> S2ChannelFilterResult:
        before_rows = len(frame)
        normalized_platform = text(platform)
        normalized_source = text(source_name)
        if frame.empty:
            return S2ChannelFilterResult(
                frame=frame.copy(),
                platform=normalized_platform,
                source_name=normalized_source,
                before_rows=0,
                after_rows=0,
                matched_channels=(),
                rule_label="",
                active=False,
                reason="S2 기준이 비어 있어 판매채널 필터를 건너뜁니다.",
            )
        if S2_CHANNEL_COLUMN not in frame.columns:
            return S2ChannelFilterResult(
                frame=frame.copy(),
                platform=normalized_platform,
                source_name=normalized_source,
                before_rows=before_rows,
                after_rows=before_rows,
                matched_channels=(),
                rule_label="",
                active=False,
                reason="S2 기준에 판매채널명 컬럼이 없어 판매채널 필터를 건너뜁니다.",
            )

        rule = self.rule_for(normalized_platform, normalized_source)
        if rule is None:
            return S2ChannelFilterResult(
                frame=frame.copy(),
                platform=normalized_platform,
                source_name=normalized_source,
                before_rows=before_rows,
                after_rows=before_rows,
                matched_channels=(),
                rule_label="",
                active=False,
                reason=f"{normalized_platform or '미감지 플랫폼'} 판매채널 필터 규칙이 없습니다.",
            )

        channels = frame[S2_CHANNEL_COLUMN].map(text)
        mask = pd.Series(False, index=frame.index)
        if rule.exact_channels:
            mask = channels.isin(rule.exact_channels)
        for token in rule.include_tokens:
            mask = mask | channels.str.contains(token, regex=False, na=False)
        for token in rule.exclude_tokens:
            mask = mask & ~channels.str.contains(token, regex=False, na=False)

        filtered = frame.loc[mask].reset_index(drop=True)
        matched_channels = tuple(sorted(channels[mask].drop_duplicates().tolist()))
        return S2ChannelFilterResult(
            frame=filtered,
            platform=normalized_platform,
            source_name=normalized_source,
            before_rows=before_rows,
            after_rows=len(filtered),
            matched_channels=matched_channels,
            rule_label=rule.label,
            active=True,
        )

    def rule_for(self, platform: str, source_name: str = "") -> S2ChannelRule | None:
        source = source_name.replace(" ", "")
        if platform == "네이버":
            if "광고" in source and "장르" in source:
                return S2ChannelRule(exact_channels=("네이버_장르(광고수익)",), label="네이버 장르 광고")
            if "광고" in source and "연재" in source:
                return S2ChannelRule(exact_channels=("네이버_연재(광고수익)",), label="네이버 연재 광고")
            if "장르" in source:
                return S2ChannelRule(exact_channels=("네이버_장르",), label="네이버 장르")
            if "연재" in source:
                return S2ChannelRule(exact_channels=("네이버_연재",), label="네이버 연재")
            if "일반" in source:
                return S2ChannelRule(exact_channels=("네이버_일반",), label="네이버 일반")
            return S2ChannelRule(
                exact_channels=("네이버_장르", "네이버_연재", "네이버_일반"),
                label="네이버 기본",
            )
        if platform == "문피아" and "후원" in source:
            return S2ChannelRule(exact_channels=("문피아(후원금 정산)",), label="문피아 후원")
        if platform == "리디북스" and "이벤트" in source:
            return S2ChannelRule(exact_channels=("리디북스(이벤트)",), label="리디북스 이벤트")
        if platform == "카카오":
            if "선투자" in source:
                return S2ChannelRule(exact_channels=("카카오페이지(선투자)",), label="카카오 선투자")
            if "창작지원금" in source:
                return S2ChannelRule(exact_channels=("카카오페이지(창작지원금)",), label="카카오 창작지원금")
            return S2ChannelRule(exact_channels=("카카오페이지(소설)",), label="카카오 소설")

        exact_channels = PLATFORM_EXACT_CHANNELS.get(platform)
        if exact_channels:
            return S2ChannelRule(exact_channels=exact_channels, label=platform)
        return None


PLATFORM_EXACT_CHANNELS: dict[str, tuple[str, ...]] = {
    "교보": ("교보문고(소설)", "교보문고(톡소다)"),
    "구글": ("구글(소설)",),
    "노벨피아": ("노벨피아 (소설)",),
    "로망띠끄": ("로망띠끄",),
    "리디북스": ("리디북스(소설)",),
    "모픽": ("모픽(mofic)",),
    "무툰": ("무툰(소설 서비스)",),
    "문피아": ("문피아",),
    "미소설": ("미소설",),
    "미스터블루": ("미스터블루(소설)",),
    "밀리의서재": ("밀리의 서재",),
    "보인&국립장애인도서관": ("보인", "국립장애인도서관"),
    "부커스": ("부커스",),
    "북큐브": ("북큐브(소설)",),
    "북팔": ("북팔(소설)", "예원북스(북팔)"),
    "블라이스": ("블라이스_일반결제", "블라이스_인앱결제", "블라이스 셀렉트"),
    "스낵북": ("스낵북(알에스)",),
    "스토린랩": ("스토린랩(원스토어)",),
    "신영미디어": ("신영미디어",),
    "알라딘": ("알라딘(소설)(1068659313)",),
    "에이블리": ("에이블리(소설)",),
    "에피루스": ("에피루스 이북클럽(B2C)",),
    "예스24": ("Yes24(서점)", "Yes24(시프트북스)"),
    "올툰": ("올툰(소설)", "올툰"),
    "원스토어": ("원스토어(소설)",),
    "윌라": ("윌라(전자책)", "윌라(오디오북)"),
    "조아라": ("조아라(소설)",),
    "큐툰": ("큐툰(소설 서비스)",),
    "토스(구루컴퍼니)": ("토스(소설)", "구루컴퍼니"),
    "판무림": ("판무림",),
    "피우리(누온)": ("누온",),
    "피플앤스토리": ("피플앤스토리 (P2P 웹소설 서비스)",),
    "하이북": ("웹소설 하이북",),
    "한아름": ("한아름(P000003716)",),
}

SPECIAL_EXACT_CHANNELS: dict[str, tuple[str, ...]] = {
    "네이버": (
        "네이버_장르(광고수익)",
        "네이버_연재(광고수익)",
        "네이버_장르",
        "네이버_연재",
        "네이버_일반",
    ),
    "문피아": ("문피아(후원금 정산)",),
    "리디북스": ("리디북스(이벤트)",),
    "카카오": ("카카오페이지(선투자)", "카카오페이지(창작지원금)", "카카오페이지(소설)"),
}


def _filename_text(source_name: Any) -> str:
    raw = text(source_name).replace("\\", "/")
    return raw.rsplit("/", 1)[-1] if "/" in raw else Path(raw).name


def _channel_key(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", text(value)).lower()
    return re.sub(r"[\s_\-–—/\\:;.,()[\]{}<>〈〉《》'\"`|+]+", "", normalized)


def s2_sales_channel_to_platform() -> dict[str, str]:
    result: dict[str, str] = {}
    for platform, channels in PLATFORM_EXACT_CHANNELS.items():
        for channel in channels:
            result[channel] = platform
    for platform, channels in SPECIAL_EXACT_CHANNELS.items():
        for channel in channels:
            result[channel] = platform
    return result


def platform_for_s2_sales_channel(sales_channel: str) -> str | None:
    return s2_sales_channel_to_platform().get(text(sales_channel))


def detect_s2_sales_channel(source_name: Any) -> S2SalesChannelDetection | None:
    filename = _filename_text(source_name)
    explicit_channel = _explicit_s2_channel_from_filename(filename)
    if explicit_channel:
        return explicit_channel

    haystack = _channel_key(filename)
    if not haystack:
        return None
    matches: list[tuple[int, str, str]] = []
    for channel, platform in s2_sales_channel_to_platform().items():
        key = _channel_key(channel)
        if key and key in haystack:
            matches.append((len(key), channel, platform))
    if not matches:
        return None
    _, channel, platform = sorted(matches, reverse=True)[0]
    return S2SalesChannelDetection(sales_channel=channel, platform=platform)


def _explicit_s2_channel_from_filename(filename: str) -> S2SalesChannelDetection | None:
    parts = Path(filename).stem.split("__")
    if len(parts) < 3:
        return None
    candidate_key = _channel_key(parts[1])
    if not candidate_key:
        return None
    for channel, platform in s2_sales_channel_to_platform().items():
        if _channel_key(channel) == candidate_key:
            return S2SalesChannelDetection(sales_channel=channel, platform=platform)
    return None


DEFAULT_S2_CHANNEL_POLICY = S2ChannelPolicy()


def filter_s2_by_platform(frame: pd.DataFrame, *, platform: str, source_name: str = "") -> S2ChannelFilterResult:
    return DEFAULT_S2_CHANNEL_POLICY.filter(frame, platform=platform, source_name=source_name)


def filter_s2_by_sales_channel(
    frame: pd.DataFrame,
    *,
    sales_channel: str,
    source_name: str = "",
) -> S2ChannelFilterResult:
    return DEFAULT_S2_CHANNEL_POLICY.filter_by_sales_channel(
        frame,
        sales_channel=sales_channel,
        source_name=source_name,
    )


def s2_filter_validation_rows(result: S2ChannelFilterResult) -> pd.DataFrame:
    if not result.active:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            ("S2 판매채널 필터", result.rule_label),
            ("S2 필터 전 행 수", result.before_rows),
            ("S2 필터 후 행 수", result.after_rows),
            ("S2 필터 채널", result.channel_label),
        ],
        columns=["항목", "값"],
    )
