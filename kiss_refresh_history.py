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
    "s2_change_added",
    "s2_change_deleted",
    "s2_change_modified",
    "sales_channel_content_id_unique",
    "content_id_unique",
    "summary_json_path",
    "cache_path",
    "s2_lookup_path",
    "source_file_path",
    "error_message",
    "script",
]

REFRESH_RUN_EXTRA_COLUMNS = {
    "s2_change_added": "INTEGER",
    "s2_change_deleted": "INTEGER",
    "s2_change_modified": "INTEGER",
}

S2_CHANGE_COLUMNS = [
    "id",
    "refresh_run_id",
    "change_type",
    "sales_channel_content_id",
    "changed_fields",
    "old_content_name",
    "new_content_name",
    "old_s2_master_id",
    "new_s2_master_id",
    "old_content_id",
    "new_content_id",
    "old_author_info",
    "new_author_info",
]

S2_CHANGE_FIELD_MAP = {
    "변경유형": "change_type",
    "판매채널콘텐츠ID": "sales_channel_content_id",
    "변경필드": "changed_fields",
    "이전_콘텐츠명": "old_content_name",
    "신규_콘텐츠명": "new_content_name",
    "이전_S2마스터ID": "old_s2_master_id",
    "신규_S2마스터ID": "new_s2_master_id",
    "이전_콘텐츠ID": "old_content_id",
    "신규_콘텐츠ID": "new_content_id",
    "이전_작가정보": "old_author_info",
    "신규_작가정보": "new_author_info",
}


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
                s2_change_added INTEGER,
                s2_change_deleted INTEGER,
                s2_change_modified INTEGER,
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
        _ensure_refresh_run_columns(connection)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS s2_refresh_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                refresh_run_id INTEGER NOT NULL,
                change_type TEXT NOT NULL CHECK (change_type IN ('added', 'deleted', 'modified')),
                sales_channel_content_id TEXT NOT NULL DEFAULT '',
                changed_fields TEXT NOT NULL DEFAULT '',
                old_content_name TEXT NOT NULL DEFAULT '',
                new_content_name TEXT NOT NULL DEFAULT '',
                old_s2_master_id TEXT NOT NULL DEFAULT '',
                new_s2_master_id TEXT NOT NULL DEFAULT '',
                old_content_id TEXT NOT NULL DEFAULT '',
                new_content_id TEXT NOT NULL DEFAULT '',
                old_author_info TEXT NOT NULL DEFAULT '',
                new_author_info TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(refresh_run_id) REFERENCES refresh_runs(id)
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_refresh_runs_started_at ON refresh_runs(started_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_refresh_runs_status ON refresh_runs(status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_refresh_runs_source ON refresh_runs(source)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_s2_refresh_changes_run ON s2_refresh_changes(refresh_run_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_s2_refresh_changes_key ON s2_refresh_changes(sales_channel_content_id)")
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


def record_s2_refresh_changes(path: str | Path, refresh_run_id: int, changes: Any) -> int:
    db_path = init_history_db(path)
    rows = _change_records(changes)
    if not rows:
        return 0
    columns = [column for column in S2_CHANGE_COLUMNS if column != "id"]
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO s2_refresh_changes ({', '.join(columns)}) VALUES ({placeholders})"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.executemany(sql, [[refresh_run_id if column == "refresh_run_id" else row[column] for column in columns] for row in rows])
        connection.commit()
    return len(rows)


def latest_s2_refresh_changes(path: str | Path, *, refresh_run_id: int | None = None, limit: int = 500) -> list[dict[str, Any]]:
    db_path = init_history_db(path)
    safe_limit = max(1, min(int(limit), 5000))
    with closing(sqlite3.connect(db_path)) as connection:
        connection.row_factory = sqlite3.Row
        if refresh_run_id is None:
            run = connection.execute(
                """
                SELECT id
                FROM refresh_runs
                WHERE status = 'success'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if run is None:
                return []
            refresh_run_id = int(run["id"])
        rows = connection.execute(
            """
            SELECT *
            FROM s2_refresh_changes
            WHERE refresh_run_id = ?
            ORDER BY
                CASE change_type WHEN 'modified' THEN 1 WHEN 'added' THEN 2 ELSE 3 END,
                sales_channel_content_id
            LIMIT ?
            """,
            (refresh_run_id, safe_limit),
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
        "s2_change_added": _int_or_none(fields.get("s2_change_added")),
        "s2_change_deleted": _int_or_none(fields.get("s2_change_deleted")),
        "s2_change_modified": _int_or_none(fields.get("s2_change_modified")),
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


def _ensure_refresh_run_columns(connection: sqlite3.Connection) -> None:
    existing = {row[1] for row in connection.execute("PRAGMA table_info(refresh_runs)").fetchall()}
    for column, column_type in REFRESH_RUN_EXTRA_COLUMNS.items():
        if column not in existing:
            connection.execute(f"ALTER TABLE refresh_runs ADD COLUMN {column} {column_type}")


def _change_records(changes: Any) -> list[dict[str, str]]:
    if changes is None:
        return []
    if hasattr(changes, "to_dict"):
        raw_rows = changes.to_dict("records")
    else:
        raw_rows = list(changes)

    rows: list[dict[str, str]] = []
    for raw in raw_rows:
        row = {column: "" for column in S2_CHANGE_COLUMNS if column not in {"id", "refresh_run_id"}}
        for source, target in S2_CHANGE_FIELD_MAP.items():
            row[target] = _text(raw.get(source) if isinstance(raw, dict) else "")
        change_type = row["change_type"]
        if change_type in {"added", "deleted", "modified"}:
            rows.append(row)
    return rows


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
