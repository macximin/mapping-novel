from __future__ import annotations

import io
import time
import unittest

from parallel_mapping import (
    NamedBytesIO,
    resolve_mapping_worker_count,
    run_ordered_parallel_tasks,
    snapshot_uploaded_file,
)


class ParallelMappingTests(unittest.TestCase):
    def test_resolve_mapping_worker_count_clamps_to_two(self) -> None:
        self.assertEqual(resolve_mapping_worker_count(1, env_value="2"), 1)
        self.assertEqual(resolve_mapping_worker_count(3, env_value=""), 2)
        self.assertEqual(resolve_mapping_worker_count(3, env_value="1"), 1)
        self.assertEqual(resolve_mapping_worker_count(3, env_value="99"), 2)
        self.assertEqual(resolve_mapping_worker_count(3, env_value="bad"), 1)

    def test_snapshot_uploaded_file_returns_independent_payload(self) -> None:
        source = NamedBytesIO(b"abc", name="sample.xlsx")
        source.seek(2)

        snapshot = snapshot_uploaded_file(source)

        self.assertEqual(snapshot.name, "sample.xlsx")
        self.assertEqual(snapshot.size, 3)
        self.assertEqual(snapshot.read(), b"abc")
        self.assertIsNot(snapshot, source)

    def test_parallel_runner_preserves_input_order(self) -> None:
        delays = {"slow": 0.03, "fast": 0.0, "mid": 0.01}

        def snapshot(index: int, slot: int, item: str) -> io.BytesIO:
            payload = NamedBytesIO(item.encode("utf-8"), name=f"{item}.xlsx")
            payload.item = item
            return payload

        def process(index: int, slot: int, payload: io.BytesIO, progress) -> dict[str, object]:
            time.sleep(delays[payload.item])
            progress("완료", "success")
            return {"source_name": payload.name, "status": "success", "value": payload.item}

        def failed(index: int, slot: int, item: object, exc: BaseException) -> dict[str, object]:
            return {"source_name": str(item), "status": "failed", "error": str(exc)}

        results = run_ordered_parallel_tasks(
            ["slow", "fast", "mid"],
            worker_count=2,
            snapshot_item=snapshot,
            process_item=process,
            failed_result=failed,
        )

        self.assertEqual([result["value"] for result in results], ["slow", "fast", "mid"])
        self.assertEqual([result["input_index"] for result in results], [0, 1, 2])

    def test_parallel_runner_isolates_item_failure(self) -> None:
        def snapshot(index: int, slot: int, item: str) -> NamedBytesIO:
            return NamedBytesIO(item.encode("utf-8"), name=f"{item}.xlsx")

        def process(index: int, slot: int, payload: NamedBytesIO, progress) -> dict[str, object]:
            if payload.name.startswith("bad"):
                raise RuntimeError("boom")
            return {"source_name": payload.name, "status": "success"}

        def failed(index: int, slot: int, item: object, exc: BaseException) -> dict[str, object]:
            return {"source_name": str(getattr(item, "name", item)), "status": "failed", "error": str(exc)}

        results = run_ordered_parallel_tasks(
            ["good", "bad", "later"],
            worker_count=2,
            snapshot_item=snapshot,
            process_item=process,
            failed_result=failed,
        )

        self.assertEqual([result["status"] for result in results], ["success", "failed", "success"])
        self.assertIn("boom", results[1]["error"])


if __name__ == "__main__":
    unittest.main()
