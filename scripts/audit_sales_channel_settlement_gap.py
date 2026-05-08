from __future__ import annotations

import argparse
import json
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cleaning_rules import clean_master_title, clean_title, has_disabled_row_marker, text
from matching_rules import PLATFORM_EXACT_CHANNELS, SPECIAL_EXACT_CHANNELS
from scripts.refresh_kiss_payment_settlement import create_authenticated_session, load_env
from settlement_adapters import normalize_settlement


DEFAULT_WORK_ROOT = ROOT / "판매채널-지급정산 불일치"
DEFAULT_SAMPLE_ROOT = DEFAULT_WORK_ROOT / "01_공유폴더_정산서_샘플" / "37개플랫폼_최신월_정산상세"
DEFAULT_OUTPUT_ROOT = DEFAULT_WORK_ROOT / "05_불일치_판정"
DEFAULT_SERVICE_OUTPUT_ROOT = DEFAULT_WORK_ROOT / "03_S2_판매채널콘텐츠_증거" / "플랫폼별_추출결과"
OOXML_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


@dataclass(frozen=True)
class ChannelTarget:
    platform: str
    schn_id: str
    schn_nm: str
    bcnc_cd: str
    bcnc_nm: str
    ctns_stle_cd: str
    ctns_stle_cd_nm: str


def main() -> None:
    args = parse_args()
    load_env(Path(args.env_file))

    output_date = args.today or date.today().strftime("%Y%m%d")
    service_dir = Path(args.service_output_root) / output_date
    judgement_dir = Path(args.output_root) / output_date
    service_dir.mkdir(parents=True, exist_ok=True)
    judgement_dir.mkdir(parents=True, exist_ok=True)

    print("loading settlement lookup...")
    settlement_lookup = load_settlement_lookup(Path(args.s2_lookup))
    settlement_ids = set(settlement_lookup["판매채널콘텐츠ID"])
    settlement_channels = settlement_lookup.groupby("판매채널콘텐츠ID", dropna=False).first().to_dict("index")

    print("parsing settlement samples...")
    settlement_title_index, sample_audit = build_settlement_title_index(Path(args.sample_root))
    sample_audit.to_csv(judgement_dir / "00_정산서샘플_파싱감사.csv", index=False, encoding="utf-8-sig")

    print("fetching S2 channel catalog...")
    session = create_authenticated_session()
    try:
        api_channels = fetch_api_channels(session)
        targets = build_channel_targets(api_channels)
        target_frame = pd.DataFrame([target.__dict__ for target in targets])
        target_frame.to_csv(service_dir / "00_대상_S2판매채널.csv", index=False, encoding="utf-8-sig")

        print(f"fetching sales-channel contents: targets={len(targets)}")
        service_rows, fetch_audit = fetch_all_service_contents(session, targets)
    finally:
        session.close()

    service_frame = pd.DataFrame(service_rows)
    fetch_audit_frame = pd.DataFrame(fetch_audit)
    service_frame.to_csv(service_dir / "01_판매채널콘텐츠_전체.csv", index=False, encoding="utf-8-sig")
    fetch_audit_frame.to_csv(service_dir / "02_판매채널콘텐츠_추출감사.csv", index=False, encoding="utf-8-sig")

    print("classifying gap rows...")
    judgement = classify_rows(service_frame, settlement_ids, settlement_channels, settlement_title_index)
    judgement.to_csv(judgement_dir / "판매채널콘텐츠_vs_지급정산관리_전체판정.csv", index=False, encoding="utf-8-sig")

    write_class_outputs(judgement, judgement_dir)
    write_summary(judgement, target_frame, fetch_audit_frame, sample_audit, judgement_dir, service_dir, output_date)

    print(f"service_dir={service_dir}")
    print(f"judgement_dir={judgement_dir}")
    print(judgement["판정등급"].value_counts().to_string())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit S2 service-available sales-channel content against payment-settlement data."
    )
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--s2-lookup", default=str(ROOT / "data" / "kiss_payment_settlement_s2_lookup.csv"))
    parser.add_argument("--sample-root", default=str(DEFAULT_SAMPLE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--service-output-root", default=str(DEFAULT_SERVICE_OUTPUT_ROOT))
    parser.add_argument("--today", default="")
    return parser.parse_args()


def load_settlement_lookup(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=object)
    required = {"판매채널콘텐츠ID", "판매채널명", "콘텐츠ID", "콘텐츠명"}
    missing = required - set(frame.columns)
    if missing:
        raise SystemExit(f"S2 lookup missing columns: {sorted(missing)}")
    frame = frame.copy()
    frame["판매채널콘텐츠ID"] = frame["판매채널콘텐츠ID"].map(id_text)
    frame = frame[frame["판매채널콘텐츠ID"].ne("")].copy()
    return frame


def fetch_api_channels(session: requests.Session) -> pd.DataFrame:
    base = session.headers["X-KISS-API-BASE-URL"]
    response = session.get(f"{base}/ssmgmt/cmm/schn", timeout=60)
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected channel catalog response: {type(rows).__name__}")
    return pd.DataFrame(rows)


def build_channel_targets(api_channels: pd.DataFrame) -> list[ChannelTarget]:
    rules = {**PLATFORM_EXACT_CHANNELS, **SPECIAL_EXACT_CHANNELS}
    wanted: list[tuple[str, str]] = []
    for platform, channels in rules.items():
        for channel in channels:
            wanted.append((platform, text(channel)))

    api = api_channels.copy()
    api["_schnNm"] = api["schnNm"].map(text)
    targets: list[ChannelTarget] = []
    missing: list[tuple[str, str]] = []
    for platform, channel in wanted:
        matched = api[api["_schnNm"].eq(channel)]
        if matched.empty:
            missing.append((platform, channel))
            continue
        for _, row in matched.iterrows():
            targets.append(
                ChannelTarget(
                    platform=platform,
                    schn_id=id_text(row.get("schnId")),
                    schn_nm=text(row.get("schnNm")),
                    bcnc_cd=text(row.get("bcncCd")),
                    bcnc_nm=text(row.get("bcncNm")),
                    ctns_stle_cd=text(row.get("ctnsStleCd")),
                    ctns_stle_cd_nm=text(row.get("ctnsStleCdNm")),
                )
            )
    if missing:
        missing_text = ", ".join(f"{platform}:{channel}" for platform, channel in missing)
        raise RuntimeError(f"S2 channel catalog missing expected channels: {missing_text}")
    dedup: dict[tuple[str, str, str], ChannelTarget] = {}
    for target in targets:
        dedup[(target.platform, target.schn_id, target.bcnc_cd)] = target
    return sorted(dedup.values(), key=lambda x: (x.platform, x.schn_nm, x.schn_id))


def fetch_all_service_contents(
    session: requests.Session,
    targets: list[ChannelTarget],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for idx, target in enumerate(targets, start=1):
        try:
            fetched = fetch_service_contents(session, target)
            status = "success"
            error = ""
        except Exception as exc:  # keep platform audit going
            fetched = []
            status = "failed"
            error = str(exc)
        for row in fetched:
            rows.append(
                {
                    "플랫폼": target.platform,
                    "S2판매채널명": target.schn_nm,
                    "S2판매채널ID": target.schn_id,
                    "거래처코드": target.bcnc_cd,
                    "거래처명": target.bcnc_nm,
                    "채널_콘텐츠형태코드": target.ctns_stle_cd,
                    "채널_콘텐츠형태": target.ctns_stle_cd_nm,
                    "판매채널콘텐츠ID": id_text(row.get("schnCtnsId")),
                    "콘텐츠ID": id_text(row.get("ctnsId")),
                    "콘텐츠명": text(row.get("ctnsNm")),
                    "판매채널콘텐츠_콘텐츠형태": text(row.get("ctnsStleCdNm")),
                    "API_판매채널명": text(row.get("schnNm")),
                }
            )
        audit.append(
            {
                "순번": idx,
                "플랫폼": target.platform,
                "S2판매채널명": target.schn_nm,
                "S2판매채널ID": target.schn_id,
                "거래처코드": target.bcnc_cd,
                "거래처명": target.bcnc_nm,
                "상태": status,
                "행수": len(fetched),
                "오류": error,
            }
        )
        print(f"[{idx}/{len(targets)}] {target.platform} / {target.schn_nm}: {status} rows={len(fetched)}")
    return rows, audit


def fetch_service_contents(session: requests.Session, target: ChannelTarget) -> list[dict[str, Any]]:
    if not target.bcnc_cd or not target.schn_id:
        raise RuntimeError("bcncCd/schnId is required")
    base = session.headers["X-KISS-API-BASE-URL"]
    response = session.get(
        f"{base}/sale/ext/ext-salm/schn-ctns",
        params={"bcncCd": target.bcnc_cd, "schnIds": target.schn_id},
        timeout=120,
    )
    if response.status_code in {401, 403}:
        raise RuntimeError(f"S2 auth rejected: HTTP {response.status_code} {response.text[:300]}")
    if not response.ok:
        raise RuntimeError(f"sales-channel content fetch failed: HTTP {response.status_code} {response.text[:300]}")
    payload = response.json()
    rows = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected sales-channel content response: {type(rows).__name__}")
    return rows


def build_settlement_title_index(sample_root: Path) -> tuple[dict[str, dict[str, set[str]]], pd.DataFrame]:
    title_index: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    audit_rows: list[dict[str, Any]] = []
    if not sample_root.exists():
        return title_index, pd.DataFrame()

    for path in sorted(sample_root.rglob("*.xlsx")):
        if path.name.startswith("~$"):
            continue
        rel = path.relative_to(sample_root)
        platform = rel.parts[0] if rel.parts else ""
        try:
            result = normalize_settlement(path, platform=platform, source_name=str(rel))
            feed = result.to_mapping_feed()
            titles = feed.get("상품명", pd.Series(dtype=object)).map(text)
            title_count = int(titles.ne("").sum())
            for raw_title in titles[titles.ne("")]:
                key = clean_title(raw_title)
                if key:
                    title_index[platform][key].add(str(rel))
            status = result.file_status
            error = ""
            parsed_rows = len(result.rows)
            feed_rows = len(feed)
        except Exception as exc:
            fallback_titles = read_xlsx_text_cells(path)
            for raw_title in fallback_titles:
                key = clean_title(raw_title)
                if key:
                    title_index[platform][key].add(str(rel))
            title_count = len(fallback_titles)
            status = "fallback_ooxml_text" if fallback_titles else "failed"
            error = str(exc)
            parsed_rows = 0
            feed_rows = len(fallback_titles)
        audit_rows.append(
            {
                "플랫폼": platform,
                "파일": str(rel),
                "상태": status,
                "파싱행수": parsed_rows,
                "매핑입력행수": feed_rows,
                "상품명행수": title_count,
                "오류": error,
            }
        )
    return title_index, pd.DataFrame(audit_rows)


def read_xlsx_text_cells(path: Path) -> list[str]:
    """Read text-like cells directly from xlsx XML when openpyxl rejects styles."""

    try:
        with zipfile.ZipFile(path) as archive:
            shared_strings = read_shared_strings(archive)
            values: list[str] = []
            for name in archive.namelist():
                if not name.startswith("xl/worksheets/") or not name.endswith(".xml"):
                    continue
                root = ET.fromstring(archive.read(name))
                for cell in root.findall(".//a:c", OOXML_NS):
                    raw = ""
                    cell_type = cell.attrib.get("t")
                    if cell_type == "s":
                        value = cell.find("a:v", OOXML_NS)
                        if value is not None and value.text:
                            try:
                                raw = shared_strings[int(value.text)]
                            except (ValueError, IndexError):
                                raw = ""
                    elif cell_type == "inlineStr":
                        raw = "".join(node.text or "" for node in cell.findall(".//a:t", OOXML_NS))
                    else:
                        inline_text = "".join(node.text or "" for node in cell.findall(".//a:t", OOXML_NS))
                        raw = inline_text
                    raw = text(raw)
                    if len(raw) >= 2 and not raw.isdigit():
                        values.append(raw)
            return sorted(set(values))
    except Exception:
        return []


def read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    return ["".join(node.text or "" for node in item.findall(".//a:t", OOXML_NS)) for item in root.findall("a:si", OOXML_NS)]


def classify_rows(
    service_frame: pd.DataFrame,
    settlement_ids: set[str],
    settlement_channels: dict[str, dict[str, Any]],
    settlement_title_index: dict[str, dict[str, set[str]]],
) -> pd.DataFrame:
    if service_frame.empty:
        return pd.DataFrame()
    frame = service_frame.copy()
    frame["판매채널콘텐츠ID"] = frame["판매채널콘텐츠ID"].map(id_text)
    frame["콘텐츠ID"] = frame["콘텐츠ID"].map(id_text)
    frame["콘텐츠명"] = frame["콘텐츠명"].map(text)
    frame["정제키"] = frame["콘텐츠명"].map(clean_master_title)
    frame["지급정산관리_존재"] = frame["판매채널콘텐츠ID"].map(lambda x: "Y" if x in settlement_ids else "N")
    frame["사용안함_사용금지_표식"] = frame.apply(row_has_disabled_marker, axis=1).map(lambda x: "Y" if x else "N")

    appears: list[str] = []
    appears_files: list[str] = []
    for _, row in frame.iterrows():
        platform = text(row.get("플랫폼"))
        key = text(row.get("정제키"))
        files = sorted(settlement_title_index.get(platform, {}).get(key, set()))
        appears.append("Y" if files else "N")
        appears_files.append(" | ".join(files[:10]))
    frame["정산서샘플_등장"] = appears
    frame["정산서샘플_등장파일"] = appears_files

    payment_info = frame["판매채널콘텐츠ID"].map(lambda x: settlement_channels.get(x, {}))
    frame["지급정산관리_판매채널명"] = payment_info.map(lambda row: text(row.get("판매채널명")) if row else "")
    frame["지급정산관리_콘텐츠명"] = payment_info.map(lambda row: text(row.get("콘텐츠명")) if row else "")
    frame["지급정산관리_콘텐츠ID"] = payment_info.map(lambda row: id_text(row.get("콘텐츠ID")) if row else "")

    frame["판정등급"] = frame.apply(classify_grade, axis=1)
    frame["조치"] = frame["판정등급"].map(action_for_grade)
    ordered = [
        "판정등급",
        "조치",
        "플랫폼",
        "S2판매채널명",
        "S2판매채널ID",
        "거래처코드",
        "거래처명",
        "판매채널콘텐츠ID",
        "콘텐츠ID",
        "콘텐츠명",
        "정제키",
        "판매채널콘텐츠_콘텐츠형태",
        "지급정산관리_존재",
        "정산서샘플_등장",
        "사용안함_사용금지_표식",
        "지급정산관리_판매채널명",
        "지급정산관리_콘텐츠명",
        "지급정산관리_콘텐츠ID",
        "정산서샘플_등장파일",
    ]
    return frame[ordered].sort_values(["판정등급", "플랫폼", "S2판매채널명", "콘텐츠명"]).reset_index(drop=True)


def row_has_disabled_marker(row: pd.Series) -> bool:
    return any(has_disabled_row_marker(row.get(column)) for column in ["콘텐츠명", "S2판매채널명", "API_판매채널명"])


def classify_grade(row: pd.Series) -> str:
    if row.get("사용안함_사용금지_표식") == "Y":
        return "C_사용안함_사용금지_선차단"
    if row.get("지급정산관리_존재") == "N" and row.get("정산서샘플_등장") == "Y":
        return "A_정산서등장_정산정보없음"
    if row.get("지급정산관리_존재") == "N":
        return "B_잠재오염_정산정보없음"
    return "OK_지급정산관리존재"


def action_for_grade(grade: str) -> str:
    if grade.startswith("A_"):
        return "[정산정보없음] suffix 후보. 지급정산관리 설정 확인 전 자동 매핑 금지."
    if grade.startswith("B_"):
        return "감리 보관. 정산서 등장 시 [정산정보없음] 후보로 승격."
    if grade.startswith("C_"):
        return "매핑 기준에서 선 차단."
    return "정상 기준 후보."


def write_class_outputs(judgement: pd.DataFrame, output_dir: Path) -> None:
    outputs = {
        "01_A급_정산서등장_정산정보없음.csv": judgement["판정등급"].eq("A_정산서등장_정산정보없음"),
        "02_B급_잠재오염_정산정보없음.csv": judgement["판정등급"].eq("B_잠재오염_정산정보없음"),
        "03_C급_사용안함_사용금지_선차단.csv": judgement["판정등급"].eq("C_사용안함_사용금지_선차단"),
        "04_OK_지급정산관리존재.csv": judgement["판정등급"].eq("OK_지급정산관리존재"),
    }
    for filename, mask in outputs.items():
        judgement.loc[mask].to_csv(output_dir / filename, index=False, encoding="utf-8-sig")


def write_summary(
    judgement: pd.DataFrame,
    target_frame: pd.DataFrame,
    fetch_audit: pd.DataFrame,
    sample_audit: pd.DataFrame,
    output_dir: Path,
    service_dir: Path,
    output_date: str,
) -> None:
    counts = judgement["판정등급"].value_counts().to_dict() if "판정등급" in judgement.columns else {}
    by_platform = (
        judgement.pivot_table(index="플랫폼", columns="판정등급", values="판매채널콘텐츠ID", aggfunc="count", fill_value=0)
        .reset_index()
        .sort_values("플랫폼")
        if not judgement.empty
        else pd.DataFrame()
    )
    by_platform.to_csv(output_dir / "플랫폼별_판정요약.csv", index=False, encoding="utf-8-sig")

    summary = {
        "created_at": output_date,
        "target_channels": int(len(target_frame)),
        "target_platforms": int(target_frame["platform"].nunique()) if "platform" in target_frame else 0,
        "service_rows": int(len(judgement)),
        "grade_counts": {str(k): int(v) for k, v in counts.items()},
        "failed_service_fetches": int(fetch_audit["상태"].eq("failed").sum()) if "상태" in fetch_audit else 0,
        "settlement_sample_files": int(len(sample_audit)),
        "failed_settlement_sample_parses": int(sample_audit["상태"].eq("failed").sum()) if "상태" in sample_audit else 0,
        "service_dir": str(service_dir),
        "judgement_dir": str(output_dir),
    }
    (output_dir / "판정요약.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 판매채널콘텐츠 vs 지급정산관리 판정 요약",
        "",
        f"작성일: {output_date}",
        "",
        f"- 대상 S2 판매채널: {summary['target_channels']:,}개",
        f"- 대상 플랫폼: {summary['target_platforms']:,}개",
        f"- 판매채널콘텐츠 행: {summary['service_rows']:,}개",
        f"- S2 판매채널콘텐츠 추출 실패: {summary['failed_service_fetches']:,}개",
        f"- 정산서 샘플 파일: {summary['settlement_sample_files']:,}개",
        f"- 정산서 샘플 파싱 실패: {summary['failed_settlement_sample_parses']:,}개",
        "",
        "## 판정 카운트",
        "",
    ]
    for grade, count in sorted(summary["grade_counts"].items()):
        lines.append(f"- {grade}: {count:,}")
    lines.extend(
        [
            "",
            "## 파일",
            "",
            "- `판매채널콘텐츠_vs_지급정산관리_전체판정.csv`",
            "- `01_A급_정산서등장_정산정보없음.csv`",
            "- `02_B급_잠재오염_정산정보없음.csv`",
            "- `03_C급_사용안함_사용금지_선차단.csv`",
            "- `플랫폼별_판정요약.csv`",
            "",
            "## 판정 기준",
            "",
            "- A급: 판매채널콘텐츠에는 있고 지급정산관리에는 없으며, 정산서 샘플에도 등장합니다.",
            "- B급: 판매채널콘텐츠에는 있고 지급정산관리에는 없지만, 정산서 샘플에는 아직 등장하지 않았습니다.",
            "- C급: `[사용안함]`, `(사용안함)`, `[사용금지]`, `(사용금지)` 표식이 있어 선 차단합니다.",
            "- OK: 지급정산관리에도 존재합니다.",
        ]
    )
    (output_dir / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def id_text(value: Any) -> str:
    raw = text(value)
    if not raw:
        return ""
    try:
        value_float = float(raw)
    except ValueError:
        return raw
    if value_float.is_integer():
        return str(int(value_float))
    return raw


if __name__ == "__main__":
    main()
