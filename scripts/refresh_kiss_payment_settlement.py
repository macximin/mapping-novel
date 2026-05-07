from __future__ import annotations

import argparse
import calendar
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kiss_refresh_history import now_iso, record_refresh_run
from kiss_refresh_lock import refresh_lock
from kiss_payment_settlement import (
    import_payment_settlement_frame,
    payment_settlement_frame_from_api_rows,
    save_summary,
)


DEFAULT_PAGE_SIZE = 1000
KISS_API_BASE_URL = "https://kiss-api.kld.kr"
KISS_COMPANY_CODE = "1000"
NOVEL_CONTENT_STYLE_CODE = "102"
JWT_PATTERN = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")


class KISSRefreshError(RuntimeError):
    pass


@dataclass(frozen=True)
class QueryWindow:
    mode: str
    start_date: str
    end_date: str


def main() -> None:
    args = parse_args()
    started_at = now_iso()
    window: QueryWindow | None = None
    summary_path: Path | None = None
    try:
        today = resolve_today(args.today)
        summary_path = Path(args.summary) if args.summary else ROOT / "doc" / today.isoformat() / "kiss_payment_settlement_refresh_summary.json"
        window = resolve_query_window(args.mode, today=today, start_date=args.start_date, end_date=args.end_date)
        with refresh_lock(args.lock_dir):
            load_env(Path(args.env_file))

            rows, total_rows, fetched_pages = fetch_payment_settlement_rows(
                window=window,
                page_size=args.page_size,
                limit_pages=max(0, args.limit_pages),
            )
            frame = payment_settlement_frame_from_api_rows(rows)
            result = import_payment_settlement_frame(
                frame,
                cache_path=args.cache,
                s2_lookup_path=args.s2_lookup,
                merge_existing=args.merge_existing,
            )
            save_summary(summary_path, result)
        history_id = record_refresh_run(
            args.history_db,
            started_at=started_at,
            finished_at=now_iso(),
            status="success",
            source="kiss_api",
            mode=window.mode,
            search_start_date=window.start_date,
            search_end_date=window.end_date,
            page_size=args.page_size,
            limit_pages=max(0, args.limit_pages),
            api_total_rows=total_rows,
            fetched_rows=len(rows),
            fetched_pages=fetched_pages,
            source_rows=result.source_rows,
            cache_rows_before=result.cache_rows_before,
            cache_rows_after=result.cache_rows_after,
            s2_lookup_rows=result.s2_lookup_rows,
            sales_channel_content_id_unique=result.summary.get("sales_channel_content_id_unique"),
            content_id_unique=result.summary.get("content_id_unique"),
            summary_json_path=summary_path,
            cache_path=result.output_cache,
            s2_lookup_path=result.output_s2_lookup,
            script=Path(__file__).name,
        )

        print(f"mode={window.mode}")
        print(f"search_start_date={window.start_date or '<blank>'}")
        print(f"search_end_date={window.end_date or '<blank>'}")
        print(f"api_total_rows={total_rows}")
        print(f"fetched_rows={len(rows)}")
        print(f"fetched_pages={fetched_pages}")
        print(f"cache_policy={'merge' if args.merge_existing else 'replace'}")
        print(f"cache_rows_before={result.cache_rows_before}")
        print(f"cache_rows_after={result.cache_rows_after}")
        print(f"s2_lookup_rows={result.s2_lookup_rows}")
        print(f"cache={result.output_cache}")
        print(f"s2_lookup={result.output_s2_lookup}")
        print(f"summary={summary_path}")
        print(f"history_db={args.history_db}")
        print(f"history_id={history_id}")
    except Exception as exc:
        record_refresh_run(
            args.history_db,
            started_at=started_at,
            finished_at=now_iso(),
            status="failed",
            source="kiss_api",
            mode=args.mode,
            search_start_date=window.start_date if window else args.start_date,
            search_end_date=window.end_date if window else args.end_date,
            page_size=getattr(args, "page_size", None),
            limit_pages=max(0, getattr(args, "limit_pages", 0)),
            summary_json_path=summary_path or "",
            cache_path=getattr(args, "cache", ""),
            s2_lookup_path=getattr(args, "s2_lookup", ""),
            error_message=exc,
            script=Path(__file__).name,
        )
        raise SystemExit(f"S2 최신화 실패: {exc}") from None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh local S2 lookup cache from S2 API.")
    parser.add_argument("--mode", choices=("full-replace", "rolling-3m", "initial", "custom"), default="full-replace")
    parser.add_argument("--start-date", default="", help="YYYY-MM-DD. Required for --mode custom.")
    parser.add_argument("--end-date", default="", help="YYYY-MM-DD. Required for --mode custom.")
    parser.add_argument("--today", default="", help="Override today's date in YYYY-MM-DD for tests.")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--limit-pages", type=int, default=0, help="0 means fetch all pages.")
    parser.add_argument("--cache", default=str(ROOT / "data" / "kiss_payment_settlement_cache.csv"))
    parser.add_argument("--s2-lookup", default=str(ROOT / "data" / "kiss_payment_settlement_s2_lookup.csv"))
    parser.add_argument("--summary", default="")
    parser.add_argument("--history-db", default=str(ROOT / "data" / "kiss_refresh_history.sqlite"))
    parser.add_argument("--lock-dir", default=str(ROOT / "data" / "s2_refresh.lock"))
    parser.add_argument("--merge-existing", action="store_true", help="Merge incoming rows into the existing cache instead of replacing it.")
    return parser.parse_args()


def load_env(path: Path) -> None:
    if not path.exists():
        raise KISSRefreshError(f".env 파일이 없습니다: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if key:
            import os

            os.environ[key] = value.strip().strip("'\"")


def fetch_payment_settlement_rows(
    *,
    window: QueryWindow,
    page_size: int,
    limit_pages: int,
) -> tuple[list[dict[str, Any]], int, int]:
    session = create_authenticated_session()
    try:
        rows: list[dict[str, Any]] = []
        page_num = 1
        total_rows = 0
        fetched_pages = 0
        while True:
            total_rows, page_rows = fetch_page(session, window=window, page_num=page_num, page_size=page_size)
            rows.extend(page_rows)
            fetched_pages += 1
            print(f"[page {page_num}] fetched={len(page_rows)} total_accumulated={len(rows)} / total={total_rows}")
            if not page_rows or len(rows) >= total_rows or (limit_pages and fetched_pages >= limit_pages):
                break
            page_num += 1
        return rows, total_rows, fetched_pages
    finally:
        session.close()


def create_authenticated_session() -> requests.Session:
    import os

    username = first_env("KLD_LOGIN_ID", "S2_ID", "IPS_ID", "KISS_ID", "KIPM_ID")
    password = first_env("KLD_LOGIN_PW", "S2_PW", "IPS_PW", "KISS_PW", "KIPM_PW")
    if not username or not password:
        raise KISSRefreshError("S2/IPS 로그인 ID/PW가 .env에 없습니다.")

    api_base_url = os.getenv("KISS_API_BASE_URL", KISS_API_BASE_URL).rstrip("/")
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=utf-8",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    response = session.post(
        f"{api_base_url}/user/login",
        json={"username": username, "password": password, "cprCd": KISS_COMPANY_CODE},
        timeout=30,
    )
    if not response.ok:
        raise KISSRefreshError(f"S2 로그인 실패: HTTP {response.status_code} {response.text[:300]}")
    response.encoding = "utf-8"
    token = extract_jwt(response.json())
    if not token:
        raise KISSRefreshError("S2 로그인 응답에서 인증 토큰을 찾지 못했습니다.")
    session.headers["Authorization"] = f"Bearer {token}"
    session.headers["X-KISS-API-BASE-URL"] = api_base_url
    return session


def first_env(*keys: str) -> str:
    import os

    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return ""


def extract_jwt(payload: Any) -> str:
    if isinstance(payload, str) and JWT_PATTERN.match(payload):
        return payload
    if isinstance(payload, dict):
        for value in payload.values():
            token = extract_jwt(value)
            if token:
                return token
    if isinstance(payload, list):
        for value in payload:
            token = extract_jwt(value)
            if token:
                return token
    return ""


def fetch_page(
    session: requests.Session,
    *,
    window: QueryWindow,
    page_num: int,
    page_size: int,
) -> tuple[int, list[dict[str, Any]]]:
    api_base_url = session.headers["X-KISS-API-BASE-URL"]
    response = session.get(
        f"{api_base_url}/mst/setl/pymt-setl",
        params=build_query_params(window, page_num=page_num, page_size=page_size),
        timeout=120,
    )
    if not response.ok:
        raise KISSRefreshError(f"정산 목록 조회 실패: HTTP {response.status_code} {response.text[:300]}")
    response.encoding = "utf-8"
    payload = response.json()
    if isinstance(payload.get("data"), dict):
        payload = payload["data"]
    rows = payload.get("list")
    total = payload.get("total")
    if not isinstance(rows, list) or total is None:
        raise KISSRefreshError(f"정산 목록 응답 형식이 예상과 다릅니다: keys={list(payload.keys())}")
    return int(total), rows


def build_query_params(window: QueryWindow, *, page_num: int, page_size: int) -> dict[str, Any]:
    return {
        "searchBgnDt": window.start_date,
        "searchEndDt": window.end_date,
        "ctnsNm": "",
        "cnfmStsCd": "",
        "pymtSetlStsCd": "",
        "ctnsStleCd": NOVEL_CONTENT_STYLE_CODE,
        "schnSeCd": "",
        "uperSchnCd": "",
        "schnId": "",
        "bcncNm": "",
        "chgerNm": "",
        "chrgDeptNm": "",
        "cntrId": "",
        "pageNum": page_num,
        "pageSize": page_size,
    }


def resolve_query_window(mode: str, *, today: date, start_date: str, end_date: str) -> QueryWindow:
    if mode in {"full-replace", "initial"}:
        return QueryWindow(mode=mode, start_date="", end_date="")
    if mode == "rolling-3m":
        return QueryWindow(mode=mode, start_date=subtract_calendar_months(today, 3).isoformat(), end_date=today.isoformat())
    if not start_date.strip() or not end_date.strip():
        raise KISSRefreshError("--mode custom 에서는 --start-date 와 --end-date 를 모두 넣어야 합니다.")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise KISSRefreshError("start-date 가 end-date 보다 늦을 수 없습니다.")
    return QueryWindow(mode=mode, start_date=start.isoformat(), end_date=end.isoformat())


def resolve_today(raw_value: str) -> date:
    return date.fromisoformat(raw_value) if raw_value.strip() else date.today()


def subtract_calendar_months(anchor: date, months: int) -> date:
    target_year = anchor.year
    target_month = anchor.month - months
    while target_month <= 0:
        target_month += 12
        target_year -= 1
    last_day = calendar.monthrange(target_year, target_month)[1]
    return date(target_year, target_month, min(anchor.day, last_day))


if __name__ == "__main__":
    main()
