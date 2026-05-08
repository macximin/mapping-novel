from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


SHEET_NAME = "콘텐츠 목록"
REQUIRED_COLUMNS = [
    "콘텐츠ID",
    "콘텐츠형태",
    "귀속법인",
    "콘텐츠명",
    "서비스유형",
    "작가필명",
    "등급",
    "장르",
    "세부장르",
    "독점구분",
    "자체제작여부",
    "글로벌서비스가능여부",
    "예상연재월",
    "담당부서",
    "담당자명",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh repo IPS auxiliary workbooks from a full IPS content export.")
    parser.add_argument("source", type=Path, help="Full IPS export xlsx, usually named IPS_YYYYMMDD.xlsx.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()

    source = args.source
    if not source.exists():
        raise SystemExit(f"IPS export not found: {source}")

    data_dir = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    frame = pd.read_excel(source, sheet_name=SHEET_NAME, dtype=object, engine="openpyxl")
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise SystemExit(f"IPS export is missing required columns: {missing}")

    frame = frame[REQUIRED_COLUMNS].dropna(how="all").copy()
    kidari = frame[frame["귀속법인"].astype(str).str.strip().eq("키다리스튜디오")].copy()
    novel = kidari[kidari["콘텐츠형태"].astype(str).str.strip().eq("소설")].copy()
    webtoon = kidari[kidari["콘텐츠형태"].astype(str).str.strip().eq("웹툰")].copy()

    outputs = [
        (data_dir / "all_contents.xlsx", kidari),
        (data_dir / "kidari_contents.xlsx", novel),
        (data_dir / "kidari_webtoon.xlsx", webtoon),
    ]
    for path, output in outputs:
        output.to_excel(path, sheet_name=SHEET_NAME, index=False, engine="openpyxl")
        print(f"{path}: {len(output):,} rows")


if __name__ == "__main__":
    main()
