"""Storage node periodic full integrity scanner."""

import asyncio
import time
from typing import Optional
from vault_core.logging_config import logger
from vault_core.storage import LocalChunkStorage


class IntegrityScanner:
    """
    Runs periodic and on-demand full integrity scans across all chunks on a storage node.
    Recalculates SHA-256 for all stored chunks, detects bit rot, and isolates corrupted
    chunks to quarantine so they are never served.
    """

    def __init__(self, storage: LocalChunkStorage, scan_interval_seconds: float = 60.0) -> None:
        self.storage = storage
        self.scan_interval_seconds = scan_interval_seconds
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.last_scan_result: Optional[dict] = None

    def scan_once(self) -> dict:
        """Execute a full synchronous scan immediately and record results."""
        result = self.storage.scan_all_chunks()
        self.last_scan_result = result
        if result.get("corrupt_detected", 0) > 0:
            logger.warning(
                f"Integrity scan detected and quarantined {result['corrupt_detected']} corrupt chunks "
                f"out of {result['chunks_verified'] + result['corrupt_detected']} total"
            )
        else:
            logger.info(
                f"Integrity scan completed: {result['chunks_verified']} chunks verified, "
                f"0 corrupt ({result['duration_seconds']:.3f}s)"
            )
        return result

    async def _scan_loop(self) -> None:
        """Background loop executing periodic full scans."""
        while self._running:
            try:
                await asyncio.to_thread(self.scan_once)
            except Exception as exc:
                logger.error(f"Error during scheduled integrity scan: {exc}")

            try:
                await asyncio.sleep(self.scan_interval_seconds)
            except asyncio.CancelledError:
                break

    def start(self) -> None:
        """Start the background scanner task if not already running."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._scan_loop())
            logger.info(f"IntegrityScanner started (interval={self.scan_interval_seconds}s)")

    def stop(self) -> None:
        """Stop background scanner task."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("IntegrityScanner stopped")
