from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_HISTORY_DB = Path("data") / "kiss_refresh_history.sqlite"

REFRESH_RUN_COLUMNS = [
    "id",
    "started_at",
    "finished_at",
    "status",
    "source",
    "mode",
    "search_start_date",
    "search_end_date",
    "page_size",
    "limit_pages",
    "api_total_rows",
    "fetched_rows",
    "fetched_pages",
    "source_rows",
    "cache_rows_before",
    "cache_rows_after",
    "s2_lookup_rows",
    "sales_channel_content_id_unique",
    "content_id_unique",
    "summary_json_path",
    "cache_path",
    "s2_lookup_path",
    "source_file_path",
    "error_message",
    "script",
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def init_history_db(path: str | Path) -> Path:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
                source TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT '',
                search_start_date TEXT NOT NULL DEFAULT '',
                search_end_date TEXT NOT NULL DEFAULT '',
                page_size INTEGER,
                limit_pages INTEGER,
                api_total_rows INTEGER,
                fetched_rows INTEGER,
                fetched_pages INTEGER,
                source_rows INTEGER,
                cache_rows_before INTEGER,
                cache_rows_after INTEGER,
                s2_lookup_rows INTEGER,
                sales_channel_content_id_unique INTEGER,
                content_id_unique INTEGER,
                summary_json_path TEXT NOT NULL DEFAULT '',
                cache_path TEXT NOT NULL DEFAULT '',
                s2_lookup_path TEXT NOT NULL DEFAULT '',
                source_file_path TEXT NOT NULL DEFAULT '',
                error_message TEXT NOT NULL DEFAULT '',
                script TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_refresh_runs_started_at ON refresh_runs(started_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_refresh_runs_status ON refresh_runs(status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_refresh_runs_source ON refresh_runs(source)")
        connection.commit()
    return db_path


def record_refresh_run(path: str | Path, **fields: Any) -> int:
    db_path = init_history_db(path)
    row = _normalize_fields(fields)
    columns = [column for column in REFRESH_RUN_COLUMNS if column != "id"]
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO refresh_runs ({', '.join(columns)}) VALUES ({placeholders})"
    with closing(sqlite3.connect(db_path)) as connection:
        cursor = connection.execute(sql, [row[column] for column in columns])
        connection.commit()
        return int(cursor.lastrowid)


def latest_refresh_runs(path: str | Path, *, limit: int = 10) -> list[dict[str, Any]]:
    db_path = init_history_db(path)
    safe_limit = max(1, min(int(limit), 1000))
    with closing(sqlite3.connect(db_path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM refresh_runs ORDER BY id DESC LIMIT ?",
            (safe_limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def _normalize_fields(fields: dict[str, Any]) -> dict[str, Any]:
    now = now_iso()
    row: dict[str, Any] = {
        "started_at": _text(fields.get("started_at")) or now,
        "finished_at": _text(fields.get("finished_at")) or now,
        "status": _status(fields.get("status")),
        "source": _text(fields.get("source")) or "unknown",
        "mode": _text(fields.get("mode")),
        "search_start_date": _text(fields.get("search_start_date")),
        "search_end_date": _text(fields.get("search_end_date")),
        "page_size": _int_or_none(fields.get("page_size")),
        "limit_pages": _int_or_none(fields.get("limit_pages")),
        "api_total_rows": _int_or_none(fields.get("api_total_rows")),
        "fetched_rows": _int_or_none(fields.get("fetched_rows")),
        "fetched_pages": _int_or_none(fields.get("fetched_pages")),
        "source_rows": _int_or_none(fields.get("source_rows")),
        "cache_rows_before": _int_or_none(fields.get("cache_rows_before")),
        "cache_rows_after": _int_or_none(fields.get("cache_rows_after")),
        "s2_lookup_rows": _int_or_none(fields.get("s2_lookup_rows")),
        "sales_channel_content_id_unique": _int_or_none(fields.get("sales_channel_content_id_unique")),
        "content_id_unique": _int_or_none(fields.get("content_id_unique")),
        "summary_json_path": _path_text(fields.get("summary_json_path")),
        "cache_path": _path_text(fields.get("cache_path")),
        "s2_lookup_path": _path_text(fields.get("s2_lookup_path")),
        "source_file_path": _path_text(fields.get("source_file_path")),
        "error_message": _error_text(fields.get("error_message")),
        "script": _text(fields.get("script")),
    }
    return row


def _status(value: Any) -> str:
    raw = _text(value)
    return raw if raw in {"success", "failed"} else "failed"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _path_text(value: Any) -> str:
    return str(value or "").strip()


def _error_text(value: Any) -> str:
    return " ".join(str(value or "").split())[:1000]


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
