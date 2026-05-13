from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import io
import os
import queue
from typing import Any, Callable, Sequence


MAPPING_PARALLEL_WORKERS_ENV = "MAPPING_PARALLEL_WORKERS"
MAPPING_PARALLEL_WORKERS_MAX_ENV = "MAPPING_PARALLEL_WORKERS_MAX"
DEFAULT_MAPPING_WORKERS = 2
MAX_MAPPING_WORKERS = 2


@dataclass(frozen=True)
class ProgressEvent:
    index: int
    slot: int
    source_name: str
    stage: str
    status: str = "running"


class NamedBytesIO(io.BytesIO):
    def __init__(self, payload: bytes, *, name: str = "uploaded.xlsx") -> None:
        super().__init__(payload)
        self.name = name
        self.size = len(payload)


ProgressCallback = Callable[[str, str], None]
SnapshotItem = Callable[[int, int, Any], Any]
ProcessItem = Callable[[int, int, Any, ProgressCallback], dict[str, Any]]
FailedResult = Callable[[int, int, Any, BaseException], dict[str, Any]]
ProgressHandler = Callable[[ProgressEvent], None]
ResultHandler = Callable[[dict[str, Any]], None]


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def resolve_mapping_worker_count(file_count: int, env_value: str | None = None) -> int:
    if file_count <= 1:
        return 1
    raw = os.environ.get(MAPPING_PARALLEL_WORKERS_ENV, "") if env_value is None else env_value
    raw = str(raw).strip()
    if not raw:
        requested = DEFAULT_MAPPING_WORKERS
    else:
        try:
            requested = int(raw)
        except ValueError:
            return 1
    max_workers = _env_int(MAPPING_PARALLEL_WORKERS_MAX_ENV, MAX_MAPPING_WORKERS) if env_value is None else MAX_MAPPING_WORKERS
    return max(1, min(requested, max_workers, file_count))


def snapshot_uploaded_file(uploaded_file: object) -> NamedBytesIO:
    name = str(getattr(uploaded_file, "name", "") or "uploaded.xlsx")
    if hasattr(uploaded_file, "seek"):
        uploaded_file.seek(0)
    if hasattr(uploaded_file, "getvalue"):
        payload = uploaded_file.getvalue()
    elif hasattr(uploaded_file, "read"):
        payload = uploaded_file.read()
    else:
        raise TypeError(f"Cannot read uploaded file payload: {type(uploaded_file).__name__}")
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return NamedBytesIO(bytes(payload), name=name)


def run_ordered_parallel_tasks(
    items: Sequence[Any],
    *,
    worker_count: int,
    snapshot_item: SnapshotItem,
    process_item: ProcessItem,
    failed_result: FailedResult,
    on_progress: ProgressHandler | None = None,
    on_result: ResultHandler | None = None,
    poll_interval: float = 0.1,
) -> list[dict[str, Any]]:
    total = len(items)
    if total == 0:
        return []
    worker_count = max(1, min(worker_count, total))
    progress_queue: queue.Queue[ProgressEvent] = queue.Queue()
    results: list[dict[str, Any] | None] = [None] * total
    next_index = 0
    active: dict[Future[dict[str, Any]], tuple[int, int, str]] = {}

    def source_name_for(value: object) -> str:
        return str(getattr(value, "name", "") or value or "uploaded.xlsx")

    def emit(event: ProgressEvent) -> None:
        progress_queue.put(event)

    def drain_progress() -> None:
        if on_progress is None:
            while not progress_queue.empty():
                progress_queue.get_nowait()
            return
        while True:
            try:
                on_progress(progress_queue.get_nowait())
            except queue.Empty:
                return

    def set_result(index: int, result: dict[str, Any]) -> None:
        result.setdefault("input_index", index)
        results[index] = result
        if on_result is not None:
            on_result(result)

    def submit_next(executor: ThreadPoolExecutor, slot: int) -> bool:
        nonlocal next_index
        if next_index >= total:
            return False
        index = next_index
        next_index += 1
        raw_item = items[index]
        try:
            payload = snapshot_item(index, slot, raw_item)
        except BaseException as exc:
            result = failed_result(index, slot, raw_item, exc)
            result.setdefault("worker_slot", slot)
            set_result(index, result)
            return False
        source_name = source_name_for(payload)
        emit(ProgressEvent(index=index, slot=slot, source_name=source_name, stage="처리 시작"))

        def progress(stage: str, status: str = "running") -> None:
            emit(ProgressEvent(index=index, slot=slot, source_name=source_name, stage=stage, status=status))

        future = executor.submit(process_item, index, slot, payload, progress)
        active[future] = (index, slot, source_name)
        return True

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for slot in range(1, worker_count + 1):
            while next_index < total and not submit_next(executor, slot):
                drain_progress()
        drain_progress()
        while active:
            done, _ = wait(active.keys(), timeout=poll_interval, return_when=FIRST_COMPLETED)
            drain_progress()
            if not done:
                continue
            for future in done:
                index, slot, source_name = active.pop(future)
                try:
                    result = future.result()
                except BaseException as exc:
                    result = failed_result(index, slot, source_name, exc)
                result.setdefault("worker_slot", slot)
                set_result(index, result)
                emit(
                    ProgressEvent(
                        index=index,
                        slot=slot,
                        source_name=source_name,
                        stage=str(result.get("status", "완료")),
                        status=str(result.get("status", "done")),
                    )
                )
                while next_index < total and not submit_next(executor, slot):
                    drain_progress()
            drain_progress()

    return [
        result if result is not None else failed_result(index, 0, items[index], RuntimeError("Task did not return a result"))
        for index, result in enumerate(results)
    ]
