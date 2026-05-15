from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEFAULT_SSOT_ROOT = Path(r"C:\Users\wjjo\Desktop\업무자동화_ssot")
DEFAULT_REPORT = ROOT / "igignore" / "mapping_results_20260515_0203" / "PD_작업지시_종합리포트.csv"
DEFAULT_IPS_XLSX = ROOT / "ips_20260515.xlsx"
DEFAULT_OUTPUT_DIR = ROOT / "igignore" / "ips_safe_channel_backfill_20260515"
EXACT_REASON = "해당채널 지급정산 없음 / 타채널 지급정산 존재"
SPECIAL_CHANNEL_MARKERS = ("네이버", "카카오")


def text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def id_text(value: Any) -> str:
    raw = text(value)
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


def split_pipe_ids(raw: Any) -> list[str]:
    return [id_text(part) for part in text(raw).split("|") if id_text(part)]


def numeric_zero(value: Any) -> bool:
    raw = text(value)
    if raw == "":
        return True
    try:
        return float(raw) == 0
    except ValueError:
        return False


def is_special_channel(channel: str) -> bool:
    return any(marker in text(channel) for marker in SPECIAL_CHANNEL_MARKERS)


def extract_cid_segment(evidence: str) -> str:
    match = re.search(r"콘텐츠ID=([^,]+)", evidence)
    return text(match.group(1)) if match else ""


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    if not fieldnames:
        fieldnames = ["status"]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_ips_ids(path: Path) -> set[str]:
    frame = pd.read_excel(path, dtype=str).fillna("")
    if "콘텐츠ID" not in frame.columns:
        raise SystemExit(f"IPS workbook missing 콘텐츠ID column: {path}")
    return {id_text(value) for value in frame["콘텐츠ID"] if id_text(value)}


def build_candidates(args: argparse.Namespace) -> dict[str, Any]:
    report_path = Path(args.report)
    ips_path = Path(args.ips)
    output_path = Path(args.output)
    excluded_path = Path(args.excluded_output)
    summary_path = Path(args.summary)

    report = pd.read_csv(report_path, dtype=str).fillna("")
    ips_ids = load_ips_ids(ips_path)
    safe_rows: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []

    for frame_index, row in report.iterrows():
        row_number = frame_index + 2
        reason = text(row.get("S2_미매핑상세사유"))
        if reason != EXACT_REASON:
            continue

        channel = text(row.get("S2 판매채널"))
        evidence = text(row.get("S2_미매핑근거"))
        cid_segment = extract_cid_segment(evidence)
        cid_values = split_pipe_ids(cid_segment)
        cid = cid_values[0] if len(cid_values) == 1 else ""

        exclusions: list[str] = []
        if is_special_channel(channel):
            exclusions.append("special_channel_needs_cid_routing")
        if len(cid_values) != 1:
            exclusions.append(f"cid_count={len(cid_values)}")
        if cid and cid not in ips_ids:
            exclusions.append("cid_not_in_local_ips")
        for column in ("S2_판매채널콘텐츠_후보수", "S2_정산정보누락_후보수", "청구정산_후보수"):
            if column in row and not numeric_zero(row.get(column)):
                exclusions.append(f"{column}={text(row.get(column))}")

        output_row = {
            "source_report_row": row_number,
            "work_cid": cid or " | ".join(cid_values),
            "input_platform": channel,
            "next_action": "add_platform_in_ips" if not exclusions else "manual_review",
            "정제_상품명": text(row.get("정제_상품명")),
            "정산서_대표콘텐츠명": text(row.get("정산서_대표콘텐츠명")),
            "S2_미매핑상세사유": reason,
            "S2_미매핑근거": evidence,
            "cid_segment": cid_segment,
            "cid_count": len(cid_values),
            "exclusion_reason": " | ".join(exclusions),
        }
        if exclusions:
            excluded_rows.append(output_row)
        else:
            safe_rows.append(output_row)

    write_csv_rows(output_path, safe_rows)
    write_csv_rows(excluded_path, excluded_rows)
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "report": str(report_path),
        "ips": str(ips_path),
        "exact_reason": EXACT_REASON,
        "safe_count": len(safe_rows),
        "excluded_count": len(excluded_rows),
        "safe_channel_counts": dict(Counter(row["input_platform"] for row in safe_rows)),
        "excluded_reason_counts": dict(Counter(row["exclusion_reason"] for row in excluded_rows)),
        "output": str(output_path),
        "excluded_output": str(excluded_path),
    }
    write_json(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def configure_ssot_imports(ssot_root: str) -> Path:
    root = Path(ssot_root) if ssot_root else DEFAULT_SSOT_ROOT
    scripts_root = root / "scripts"
    project_root = root / "SIAAN Project"
    if not scripts_root.exists():
        raise FileNotFoundError(f"SSOT scripts directory not found: {scripts_root}")
    if not project_root.exists():
        raise FileNotFoundError(f"SIAAN Project directory not found: {project_root}")
    for path in (str(project_root), str(scripts_root)):
        if path in sys.path:
            sys.path.remove(path)
    sys.path.insert(0, str(project_root))
    sys.path.insert(0, str(scripts_root))
    return project_root


def live_lookup(args: argparse.Namespace) -> list[dict[str, Any]]:
    project_root = configure_ssot_imports(args.ssot_root)

    from ips.core.auth import resolve_env_path  # noqa: PLC0415
    from ips.core.browser import BrowserSettings  # noqa: PLC0415
    from ips.core.harness import IPSHarness  # noqa: PLC0415
    from ips.sites import get_site  # noqa: PLC0415
    from ips_sales_channel_harness import (  # noqa: PLC0415
        DEFAULT_LOGIN_PATH,
        axios_get_detail,
        build_edit_url,
        format_platform_snapshot,
        match_platform_rows,
    )

    input_rows = read_csv_rows(Path(args.input))
    output_path = Path(args.output)
    json_path = Path(args.json_output)
    rows: list[dict[str, Any]] = []

    settings = BrowserSettings(
        headless=args.headless,
        slow_mo_ms=args.slow_mo_ms,
        timeout_ms=args.timeout_ms,
        artifacts_root=project_root / "output" / "ips_harness",
    )
    env_path = resolve_env_path(args.env_file)
    site = get_site("kipm")

    with IPSHarness(site, settings=settings, env_path=env_path) as harness:
        harness.ensure_logged_in(path=DEFAULT_LOGIN_PATH)
        for index, source_row in enumerate(input_rows, start=1):
            row = dict(source_row)
            cid = id_text(row.get("work_cid"))
            platform = text(row.get("input_platform"))
            row["lookup_index"] = index
            row["edit_url"] = build_edit_url(cid) if cid else ""
            row["detail_status"] = ""
            row["platform_match_status"] = ""
            row["matched_platform_name"] = ""
            row["sales_channel_content_id"] = ""
            row["existing_platform_count"] = ""
            row["existing_platform_snapshot"] = ""
            row["lookup_error"] = ""
            try:
                detail_resp = axios_get_detail(harness.page, cid)
                if not detail_resp.get("ok"):
                    row["detail_status"] = f"detail_failed:{detail_resp.get('status')}"
                    row["lookup_error"] = text(detail_resp.get("error") or detail_resp.get("data"))[:500]
                    row["next_action"] = "check_ips_login_or_cid"
                else:
                    detail_payload = detail_resp.get("data") if isinstance(detail_resp.get("data"), dict) else {}
                    matched_platform, platform_rows = match_platform_rows(detail_payload, platform)
                    row["detail_status"] = "loaded"
                    row["existing_platform_count"] = len(platform_rows)
                    row["existing_platform_snapshot"] = format_platform_snapshot(platform_rows)
                    if matched_platform:
                        row["platform_match_status"] = "found"
                        row["matched_platform_name"] = text(matched_platform.get("lwerSchnNm"))
                        row["sales_channel_content_id"] = id_text(matched_platform.get("schnCtnsId"))
                        row["next_action"] = "paste_sales_channel_content_id"
                    else:
                        row["platform_match_status"] = "missing_platform"
                        row["next_action"] = "add_platform_in_ips"
            except Exception as exc:  # noqa: BLE001
                row["detail_status"] = "lookup_failed"
                row["lookup_error"] = str(exc)
                row["next_action"] = "check_ips_login_or_cid"

            rows.append(row)
            write_csv_rows(output_path, rows)
            write_json(json_path, rows)
            print(
                f"[{index}/{len(input_rows)}] cid={cid} platform={platform} "
                f"detail={row['detail_status']} action={row.get('next_action')}",
                flush=True,
            )

    summary = {
        "row_count": len(rows),
        "action_counts": dict(Counter(text(row.get("next_action")) for row in rows)),
        "detail_status_counts": dict(Counter(text(row.get("detail_status")) for row in rows)),
        "output": str(output_path),
        "json_output": str(json_path),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return rows


def add_platforms(args: argparse.Namespace) -> list[dict[str, Any]]:
    project_root = configure_ssot_imports(args.ssot_root)

    from ips.core.auth import resolve_env_path  # noqa: PLC0415
    from ips.core.browser import BrowserSettings  # noqa: PLC0415
    from ips.core.harness import IPSHarness  # noqa: PLC0415
    from ips.sites import get_site  # noqa: PLC0415
    from ips_sales_channel_adder import add_platform_via_detail, build_detail_view_url  # noqa: PLC0415

    input_rows = read_csv_rows(Path(args.input))
    output_path = Path(args.output)
    json_path = Path(args.json_output)
    rows: list[dict[str, Any]] = []
    pending_indexes = [
        idx for idx, row in enumerate(input_rows)
        if text(row.get("next_action")) == "add_platform_in_ips"
    ]
    if args.limit:
        pending_indexes = pending_indexes[: args.limit]
    pending_set = set(pending_indexes)

    settings = BrowserSettings(
        headless=args.headless,
        slow_mo_ms=args.slow_mo_ms,
        timeout_ms=args.timeout_ms,
        artifacts_root=project_root / "output" / "ips_harness",
    )
    env_path = resolve_env_path(args.env_file)
    site = get_site("kipm")

    with IPSHarness(site, settings=settings, env_path=env_path) as harness:
        harness.ensure_logged_in(path="/ip/cntsd/cntslt/ctns-list?pageNum=1&pageSize=10")
        page = harness.page
        for index, source_row in enumerate(input_rows):
            row = dict(source_row)
            cid = id_text(row.get("work_cid"))
            platform = text(row.get("input_platform"))
            row["detail_view_url"] = build_detail_view_url(cid) if cid else ""
            if index not in pending_set:
                row.setdefault("addition_status", "skipped")
                rows.append(row)
                continue

            run_index = len([value for value in pending_indexes if value <= index])
            try:
                result = add_platform_via_detail(page, cid, platform)
                row.update(result)
                row["detail_status"] = "loaded"
                row["platform_match_status"] = "found"
                row["next_action"] = "paste_sales_channel_content_id"
                row["addition_error"] = ""
            except Exception as exc:  # noqa: BLE001
                row["addition_status"] = "failed"
                row["addition_error"] = str(exc)
            rows.append(row)
            write_csv_rows(output_path, rows)
            write_json(json_path, rows)
            print(
                f"[{run_index}/{len(pending_indexes)}] cid={cid} platform={platform} "
                f"addition={row.get('addition_status')} id={row.get('sales_channel_content_id', '')}",
                flush=True,
            )

    summary = {
        "input_rows": len(input_rows),
        "processed_add_rows": len(pending_indexes),
        "addition_status_counts": dict(Counter(text(row.get("addition_status")) for row in rows)),
        "action_counts": dict(Counter(text(row.get("next_action")) for row in rows)),
        "output": str(output_path),
        "json_output": str(json_path),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return rows


def verify_s2(args: argparse.Namespace) -> dict[str, Any]:
    additions = pd.read_csv(args.additions, dtype=str).fillna("")
    s2 = pd.read_csv(args.s2_lookup, dtype=str).fillna("")
    required = {"판매채널콘텐츠ID", "판매채널명", "콘텐츠ID", "콘텐츠명"}
    missing = required - set(s2.columns)
    if missing:
        raise SystemExit(f"S2 lookup missing columns: {sorted(missing)}")

    additions["sales_channel_content_id"] = additions["sales_channel_content_id"].map(id_text)
    s2["판매채널콘텐츠ID"] = s2["판매채널콘텐츠ID"].map(id_text)
    s2_ids = set(s2["판매채널콘텐츠ID"])
    rows: list[dict[str, Any]] = []
    for _, row in additions.iterrows():
        channel_id = id_text(row.get("sales_channel_content_id"))
        if not channel_id:
            continue
        matched = s2[s2["판매채널콘텐츠ID"].eq(channel_id)]
        found = not matched.empty
        first = matched.iloc[0].to_dict() if found else {}
        rows.append(
            {
                "work_cid": id_text(row.get("work_cid")),
                "input_platform": text(row.get("input_platform")),
                "sales_channel_content_id": channel_id,
                "addition_status": text(row.get("addition_status")),
                "s2_lookup_found": "Y" if channel_id in s2_ids else "N",
                "s2_판매채널명": text(first.get("판매채널명")),
                "s2_콘텐츠ID": id_text(first.get("콘텐츠ID")),
                "s2_콘텐츠명": text(first.get("콘텐츠명")),
            }
        )

    output_path = Path(args.output)
    summary_path = Path(args.summary)
    write_csv_rows(output_path, rows)
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "additions": str(args.additions),
        "s2_lookup": str(args.s2_lookup),
        "checked_ids": len(rows),
        "found_count": sum(1 for row in rows if row["s2_lookup_found"] == "Y"),
        "missing_count": sum(1 for row in rows if row["s2_lookup_found"] != "Y"),
        "output": str(output_path),
    }
    write_json(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def sanitize_output_stem(name: str) -> str:
    base = name[:-5] if name.lower().endswith(".xlsx") else name
    return "".join(ch if ch not in r'\/:*?"<>|' else "_" for ch in base).strip()


def _source_channel_from_name(path: Path) -> str:
    parts = path.stem.split("__")
    return text(parts[1]) if len(parts) >= 3 else ""


def _input_files_for_safe_channels(input_dir: Path, safe_candidates: Path) -> list[Path]:
    candidates = pd.read_csv(safe_candidates, dtype=str).fillna("")
    channels = {text(value) for value in candidates["input_platform"] if text(value)}
    files: list[Path] = []
    for path in sorted(input_dir.glob("*.xlsx")):
        if path.name.startswith("~$"):
            continue
        if _source_channel_from_name(path) in channels:
            files.append(path)
    missing_channels = channels - {_source_channel_from_name(path) for path in files}
    if missing_channels:
        raise SystemExit(f"Missing SSOT input files for channels: {sorted(missing_channels)}")
    return files


def _summary_row(result: dict[str, Any]) -> dict[str, Any]:
    summary = result.get("summary", {}) or {}
    adapter_summary = result.get("adapter_summary", {}) or {}
    return {
        "파일": result.get("source_name", ""),
        "상태": result.get("status", ""),
        "S2 판매채널": result.get("s2_sales_channel", ""),
        "플랫폼": result.get("platform", ""),
        "원본 파싱 행": adapter_summary.get("parsed_rows", ""),
        "S2 매핑 입력 행": adapter_summary.get("default_feed_rows", ""),
        "S2 matched": summary.get("S2 matched", ""),
        "검토필요": summary.get("검토필요 행 수", ""),
        "누락 후보": summary.get("S2 정산정보 누락 후보", ""),
        "청구 후보": summary.get("청구정산 후보", ""),
        "메시지": result.get("error", ""),
    }


def rerun_mapping(args: argparse.Namespace) -> dict[str, Any]:
    from batch_reports import (  # noqa: PLC0415
        build_combined_mapping_report_frame,
        build_pd_work_order_report_frame_from_combined,
    )
    from mapping_core import build_mapping, build_s2_mapping_reference, export_mapping, read_first_sheet  # noqa: PLC0415
    from matching_rules import (  # noqa: PLC0415
        build_s2_sales_channel_filter_cache,
        detect_s2_sales_channel,
        filter_s2_by_sales_channel_cache,
        platform_for_s2_sales_channel,
        s2_filter_validation_rows,
    )
    from s2_reference_guards import (  # noqa: PLC0415
        annotate_mapping_result,
        apply_missing_exclusions,
        build_s2_guard_runtime_context,
        load_s2_reference_guards,
    )
    from settlement_adapters import (  # noqa: PLC0415
        adapter_blocking_messages,
        normalize_settlement,
        summarize_normalization,
    )

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = _input_files_for_safe_channels(input_dir, Path(args.safe_candidates))

    guards = load_s2_reference_guards(
        missing_path=ROOT / "data" / "s2_payment_missing_lookup.csv",
        billing_path=ROOT / "data" / "s2_billing_settlement_lookup.csv",
        service_contents_path=ROOT / "data" / "s2_sales_channel_content_lookup.csv",
    )
    s2_source = pd.read_csv(args.s2_lookup, dtype=object).fillna("")
    guard_result = apply_missing_exclusions(s2_source, guards)
    s2_df = guard_result.frame
    master_df = read_first_sheet(Path(args.ips)) if args.ips else None
    s2_filter_cache = build_s2_sales_channel_filter_cache(s2_df)
    s2_guard_context = build_s2_guard_runtime_context(
        guards,
        s2_all_frame=s2_df,
        master_df=master_df,
    )

    channels = []
    for path in files:
        detection = detect_s2_sales_channel(path.name)
        channels.append(detection.sales_channel if detection else _source_channel_from_name(path))
    references = {}
    for channel in sorted(set(channels)):
        filtered = filter_s2_by_sales_channel_cache(s2_filter_cache, sales_channel=channel, source_name=channel)
        if filtered.active:
            references[channel] = build_s2_mapping_reference(filtered.frame)

    results: list[dict[str, Any]] = []
    for index, path in enumerate(files, start=1):
        source_name = path.name
        detection = detect_s2_sales_channel(source_name)
        s2_channel = detection.sales_channel if detection else _source_channel_from_name(path)
        platform = platform_for_s2_sales_channel(s2_channel) or ""
        result: dict[str, Any] = {
            "source_name": source_name,
            "output_stem": f"{sanitize_output_stem(path.stem)}_매핑",
            "platform": platform,
            "s2_sales_channel": s2_channel,
            "status": "failed",
            "error": "",
        }
        try:
            if not s2_channel or not platform:
                raise ValueError(f"S2 channel/platform detection failed: {source_name}")
            adapter_result = normalize_settlement(path, platform=platform, source_name=source_name)
            blocking_messages = adapter_blocking_messages(adapter_result)
            result["adapter_summary"] = summarize_normalization(adapter_result)
            if blocking_messages:
                result["status"] = "blocked"
                result["error"] = " | ".join(blocking_messages)
                results.append(result)
                continue
            settlement_df = adapter_result.to_mapping_feed()
            s2_channel_filter = filter_s2_by_sales_channel_cache(
                s2_filter_cache,
                sales_channel=s2_channel,
                source_name=source_name,
            )
            mapping = build_mapping(
                s2_channel_filter.frame,
                settlement_df,
                master_df,
                s2_reference=references.get(s2_channel),
            )
            mapping = annotate_mapping_result(
                mapping,
                guards,
                sales_channel=s2_channel,
                s2_all_frame=s2_df,
                master_df=master_df,
                runtime_context=s2_guard_context,
            )
            filter_validation = s2_filter_validation_rows(s2_channel_filter)
            if not filter_validation.empty:
                mapping.input_validation = pd.concat([filter_validation, mapping.input_validation], ignore_index=True)
            if not guard_result.input_validation.empty:
                mapping.input_validation = pd.concat([guard_result.input_validation, mapping.input_validation], ignore_index=True)
            summary = dict(zip(mapping.summary["항목"], mapping.summary["값"]))
            summary["S2 정산정보 누락 제외 행 수"] = guard_result.excluded_count
            if s2_channel_filter.active:
                summary["S2 필터 전 행 수"] = s2_channel_filter.before_rows
                summary["S2 필터 후 행 수"] = s2_channel_filter.after_rows
            result.update({"status": "success", "mapping": mapping, "summary": summary})
            if args.write_xlsx:
                (output_dir / f"{result['output_stem']}.xlsx").write_bytes(export_mapping(mapping))
        except Exception as exc:  # noqa: BLE001
            result["status"] = "failed"
            result["error"] = f"{type(exc).__name__}: {exc}"
        results.append(result)
        print(f"[{index}/{len(files)}] {source_name} -> {result['status']} {result.get('error', '')}", flush=True)

    summary_frame = pd.DataFrame([_summary_row(result) for result in results])
    combined = build_combined_mapping_report_frame(results)
    work_order = build_pd_work_order_report_frame_from_combined(combined)
    summary_frame.to_csv(output_dir / "batch_summary.csv", index=False, encoding="utf-8-sig")
    combined.to_csv(output_dir / "전체_행별매핑_종합.csv", index=False, encoding="utf-8-sig")
    work_order.to_csv(output_dir / "PD_작업지시_종합리포트.csv", index=False, encoding="utf-8-sig")

    safe = pd.read_csv(args.safe_candidates, dtype=str).fillna("")
    additions = pd.read_csv(args.additions, dtype=str).fillna("")
    additions["sales_channel_content_id"] = additions["sales_channel_content_id"].map(id_text)
    combined["S2_판매채널콘텐츠ID"] = combined.get("S2_판매채널콘텐츠ID", pd.Series(dtype=str)).map(id_text)
    reflection_rows: list[dict[str, Any]] = []
    for _, add_row in additions.iterrows():
        channel = text(add_row.get("input_platform"))
        clean_key = text(add_row.get("정제_상품명"))
        generated_id = id_text(add_row.get("sales_channel_content_id"))
        matched = combined[
            combined["S2 판매채널"].map(text).eq(channel)
            & combined["정제_상품명"].map(text).eq(clean_key)
        ].copy()
        ids = [value for value in dict.fromkeys(matched["S2_판매채널콘텐츠ID"].map(id_text)) if value]
        still_same_reason = 0
        if not work_order.empty:
            same_reason = work_order[
                work_order["S2 판매채널"].map(text).eq(channel)
                & work_order["정제_상품명"].map(text).eq(clean_key)
                & work_order["S2_미매핑상세사유"].map(text).eq(EXACT_REASON)
            ]
            still_same_reason = len(same_reason)
        reflection_rows.append(
            {
                "work_cid": id_text(add_row.get("work_cid")),
                "input_platform": channel,
                "정제_상품명": clean_key,
                "sales_channel_content_id": generated_id,
                "addition_status": text(add_row.get("addition_status")),
                "rerun_row_count": len(matched),
                "rerun_s2_ids": " | ".join(ids),
                "generated_id_in_rerun": "Y" if generated_id and generated_id in ids else "N",
                "still_same_unmapped_reason_count": still_same_reason,
            }
        )
    reflection = pd.DataFrame(reflection_rows)
    reflection_path = output_dir / "05_mapping_reflection_verify.csv"
    reflection.to_csv(reflection_path, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input_dir": str(input_dir),
        "safe_candidates": str(args.safe_candidates),
        "processed_files": len(files),
        "status_counts": dict(Counter(text(result.get("status")) for result in results)),
        "combined_rows": len(combined),
        "work_order_rows": len(work_order),
        "safe_addition_rows": len(additions),
        "generated_id_in_rerun_count": int(reflection["generated_id_in_rerun"].eq("Y").sum()) if not reflection.empty else 0,
        "generated_id_missing_in_rerun_count": int(reflection["generated_id_in_rerun"].ne("Y").sum()) if not reflection.empty else 0,
        "still_same_unmapped_reason_count": int(pd.to_numeric(reflection["still_same_unmapped_reason_count"], errors="coerce").fillna(0).sum())
        if not reflection.empty
        else 0,
        "output_dir": str(output_dir),
        "reflection_output": str(reflection_path),
    }
    write_json(output_dir / "05_mapping_rerun_summary.json", payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe IPS sales-channel backfill from PD work-order rows.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build-candidates")
    build.add_argument("--report", default=str(DEFAULT_REPORT))
    build.add_argument("--ips", default=str(DEFAULT_IPS_XLSX))
    build.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR / "01_safe_candidates.csv"))
    build.add_argument("--excluded-output", default=str(DEFAULT_OUTPUT_DIR / "01_excluded_candidates.csv"))
    build.add_argument("--summary", default=str(DEFAULT_OUTPUT_DIR / "01_candidate_summary.json"))
    build.set_defaults(func=build_candidates)

    lookup = subparsers.add_parser("lookup")
    lookup.add_argument("--input", default=str(DEFAULT_OUTPUT_DIR / "01_safe_candidates.csv"))
    lookup.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR / "02_kipm_lookup.csv"))
    lookup.add_argument("--json-output", default=str(DEFAULT_OUTPUT_DIR / "02_kipm_lookup.json"))
    lookup.add_argument("--env-file", default=str(ROOT / ".env"))
    lookup.add_argument("--ssot-root", default=str(DEFAULT_SSOT_ROOT))
    lookup.add_argument("--headless", action="store_true")
    lookup.add_argument("--slow-mo-ms", type=int, default=0)
    lookup.add_argument("--timeout-ms", type=int, default=25_000)
    lookup.set_defaults(func=live_lookup)

    add = subparsers.add_parser("add")
    add.add_argument("--input", default=str(DEFAULT_OUTPUT_DIR / "02_kipm_lookup.csv"))
    add.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR / "03_kipm_additions.csv"))
    add.add_argument("--json-output", default=str(DEFAULT_OUTPUT_DIR / "03_kipm_additions.json"))
    add.add_argument("--env-file", default=str(ROOT / ".env"))
    add.add_argument("--ssot-root", default=str(DEFAULT_SSOT_ROOT))
    add.add_argument("--headless", action="store_true")
    add.add_argument("--slow-mo-ms", type=int, default=0)
    add.add_argument("--timeout-ms", type=int, default=30_000)
    add.add_argument("--limit", type=int, default=0)
    add.set_defaults(func=add_platforms)

    verify = subparsers.add_parser("verify-s2")
    verify.add_argument("--additions", default=str(DEFAULT_OUTPUT_DIR / "03_kipm_additions.csv"))
    verify.add_argument("--s2-lookup", default=str(ROOT / "data" / "kiss_payment_settlement_s2_lookup.csv"))
    verify.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR / "04_s2_lookup_verify.csv"))
    verify.add_argument("--summary", default=str(DEFAULT_OUTPUT_DIR / "04_s2_lookup_verify_summary.json"))
    verify.set_defaults(func=verify_s2)

    rerun = subparsers.add_parser("rerun-mapping")
    rerun.add_argument("--safe-candidates", default=str(DEFAULT_OUTPUT_DIR / "01_safe_candidates.csv"))
    rerun.add_argument("--additions", default=str(DEFAULT_OUTPUT_DIR / "03_kipm_additions.csv"))
    rerun.add_argument("--input-dir", default=str(ROOT / "igignore" / "2026-02_app_upload_확정_2"))
    rerun.add_argument("--s2-lookup", default=str(ROOT / "data" / "kiss_payment_settlement_s2_lookup.csv"))
    rerun.add_argument("--ips", default=str(DEFAULT_IPS_XLSX))
    rerun.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR / "05_mapping_rerun_affected_channels"))
    rerun.add_argument("--write-xlsx", action="store_true")
    rerun.set_defaults(func=rerun_mapping)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
