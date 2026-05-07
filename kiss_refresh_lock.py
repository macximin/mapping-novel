from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator


class RefreshLockError(RuntimeError):
    pass


@contextmanager
def refresh_lock(path: str | Path, *, stale_after_seconds: int = 3600) -> Iterator[Path]:
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    acquired = False
    try:
        try:
            lock_path.mkdir()
            acquired = True
        except FileExistsError as exc:
            if _is_stale(lock_path, stale_after_seconds=stale_after_seconds):
                shutil.rmtree(lock_path, ignore_errors=True)
                lock_path.mkdir()
                acquired = True
            else:
                raise RefreshLockError(f"S2 최신화가 이미 실행 중입니다: {lock_path}") from exc

        metadata = "\n".join(
            [
                f"pid={os.getpid()}",
                f"started_at={datetime.now().astimezone().isoformat(timespec='seconds')}",
            ]
        )
        (lock_path / "lock.txt").write_text(metadata, encoding="utf-8")
        yield lock_path
    finally:
        if acquired:
            shutil.rmtree(lock_path, ignore_errors=True)


def _is_stale(path: Path, *, stale_after_seconds: int) -> bool:
    if stale_after_seconds <= 0:
        return False
    try:
        modified_at = datetime.fromtimestamp(path.stat().st_mtime).astimezone()
    except OSError:
        return True
    return datetime.now().astimezone() - modified_at > timedelta(seconds=stale_after_seconds)
