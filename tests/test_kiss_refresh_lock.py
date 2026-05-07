from __future__ import annotations

import tempfile
import unittest
import os
import time
from pathlib import Path

from kiss_refresh_lock import RefreshLockError, refresh_lock


class KissRefreshLockTest(unittest.TestCase):
    def test_refresh_lock_blocks_nested_acquire(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "refresh.lock"
            with refresh_lock(lock_path):
                self.assertTrue(lock_path.exists())
                with self.assertRaises(RefreshLockError):
                    with refresh_lock(lock_path):
                        pass
            self.assertFalse(lock_path.exists())

    def test_refresh_lock_reclaims_stale_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "refresh.lock"
            lock_path.mkdir()
            old_time = time.time() - 7200
            os.utime(lock_path, (old_time, old_time))

            with refresh_lock(lock_path, stale_after_seconds=1):
                self.assertTrue((lock_path / "lock.txt").exists())

            self.assertFalse(lock_path.exists())


if __name__ == "__main__":
    unittest.main()
