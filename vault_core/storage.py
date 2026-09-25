"""Local chunk storage engine with atomic writes and SHA-256 integrity verification."""

import os
import shutil
import time
from pathlib import Path
from typing import AsyncIterable, Optional

from vault_core.hashing import sha256_bytes
from vault_core.manifest import ChunkInfo
from vault_core.metrics import (
    CHUNKS_TOTAL,
    BYTES_STORED_TOTAL,
    CORRUPT_CHUNKS_DETECTED_TOTAL,
    CORRUPT_CHUNKS_TOTAL,
    CHUNKS_VERIFIED_TOTAL,
    LAST_FULL_SCAN_AT,
    QUARANTINED_CHUNKS_TOTAL,
)


class ChecksumMismatchError(Exception):
    """Raised when an uploaded chunk fails SHA-256 verification."""
    pass


class CorruptedChunkError(Exception):
    """Raised when a chunk stored on disk fails SHA-256 verification upon read."""
    pass


class ChunkNotFoundError(Exception):
    """Raised when a requested chunk file does not exist on disk."""
    pass


class LocalChunkStorage:
    """Manages immutable chunk files, atomic disk writes, and quarantine isolation."""

    def __init__(self, base_data_dir: Path) -> None:
        self.base_dir = Path(base_data_dir)
        self.chunks_dir = self.base_dir / "chunks"
        self.quarantine_dir = self.base_dir / "quarantine"
        self.tmp_dir = self.base_dir / "tmp"

        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Create required storage directory trees."""
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def _chunk_path(self, bucket: str, version_id: str, chunk_index: int) -> Path:
        """Get canonical destination path for an immutable chunk file."""
        return self.chunks_dir / bucket / version_id / f"chunk_{chunk_index}.dat"

    def write_chunk(
        self,
        bucket: str,
        version_id: str,
        chunk_index: int,
        data: bytes,
        expected_sha256: Optional[str] = None
    ) -> ChunkInfo:
        """
        Atomically write a chunk to disk, verifying SHA-256 checksum.
        Never leaves partial or corrupt files in the active chunks directory.
        """
        calculated_sha = sha256_bytes(data)
        if expected_sha256 and expected_sha256 != calculated_sha:
            raise ChecksumMismatchError(
                f"Chunk {chunk_index} checksum mismatch: expected {expected_sha256}, got {calculated_sha}"
            )

        target_file = self._chunk_path(bucket, version_id, chunk_index)
        target_file.parent.mkdir(parents=True, exist_ok=True)

        # Write to temp file first for atomic rename
        temp_file = self.tmp_dir / f"tmp_{version_id}_{chunk_index}_{time.time_ns()}.tmp"
        with open(temp_file, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        # Atomic replacement
        shutil.move(str(temp_file), str(target_file))

        # Write sidecar SHA-256 checksum for background integrity scanner
        sha_sidecar = target_file.with_suffix(".sha256")
        sha_sidecar.write_text(calculated_sha, encoding="utf-8")

        # Metrics
        CHUNKS_TOTAL.inc()
        BYTES_STORED_TOTAL.inc(len(data))

        chunk_id = f"{version_id}_{chunk_index}"
        return ChunkInfo(
            chunk_index=chunk_index,
            chunk_id=chunk_id,
            sha256=calculated_sha,
            size_bytes=len(data),
            placement_nodes=["node-local"],
            stored_path=str(target_file.relative_to(self.base_dir)),
        )

    def read_chunk(
        self,
        bucket: str,
        version_id: str,
        chunk_index: int,
        expected_sha256: str
    ) -> bytes:
        """
        Read chunk from disk, enforcing cryptographic checksum validation.
        If corruption is detected, immediately quarantines the file and raises CorruptedChunkError.
        """
        target_file = self._chunk_path(bucket, version_id, chunk_index)
        if not target_file.is_file():
            raise ChunkNotFoundError(f"Chunk {chunk_index} not found at {target_file}")

        with open(target_file, "rb") as f:
            data = f.read()

        actual_sha = sha256_bytes(data)
        if actual_sha != expected_sha256:
            # Corrupted! Quarantine file immediately.
            self.quarantine_chunk(target_file, actual_sha, expected_sha256)
            raise CorruptedChunkError(
                f"Corrupt chunk detected for {bucket}/{version_id}/{chunk_index}: "
                f"expected {expected_sha256}, read {actual_sha}. Quarantined."
            )

        CHUNKS_VERIFIED_TOTAL.inc()
        return data

    def quarantine_chunk(self, file_path: Path, actual_sha: str, expected_sha256: str) -> Path:
        """Move a corrupted chunk into the quarantine area for administrative inspection."""
        CORRUPT_CHUNKS_DETECTED_TOTAL.inc()
        CORRUPT_CHUNKS_TOTAL.inc()
        timestamp = int(time.time())
        dest_filename = f"{file_path.stem}_corrupt_{timestamp}_{actual_sha[:8]}.dat"
        dest_path = self.quarantine_dir / dest_filename

        try:
            shutil.move(str(file_path), str(dest_path))
            QUARANTINED_CHUNKS_TOTAL.inc()
            CHUNKS_TOTAL.dec()

            # Remove or clean up sidecar checksum so it's not orphaned
            sha_sidecar = file_path.with_suffix(".sha256")
            if sha_sidecar.is_file():
                sha_sidecar.unlink()
        except Exception:
            pass

        return dest_path

    def scan_all_chunks(self) -> dict:
        """
        Execute a full synchronous integrity scan over all stored chunks on this node.
        Recalculates SHA-256 for each chunk, compares against stored checksum,
        and isolates corrupted files into quarantine.
        """
        start_time = time.time()
        chunks_verified = 0
        corrupt_detected = 0
        quarantined = 0

        # Scan all .dat files in chunks directory: /chunks/{bucket}/{version_id}/chunk_{index}.dat
        for dat_file in list(self.chunks_dir.glob("*/*/*.dat")):
            if not dat_file.is_file():
                continue

            sha_file = dat_file.with_suffix(".sha256")
            if not sha_file.is_file():
                # If sidecar missing, create it or skip
                continue

            expected_sha = sha_file.read_text(encoding="utf-8").strip()

            try:
                with open(dat_file, "rb") as f:
                    data = f.read()

                actual_sha = sha256_bytes(data)
                if actual_sha == expected_sha:
                    chunks_verified += 1
                    CHUNKS_VERIFIED_TOTAL.inc()
                else:
                    corrupt_detected += 1
                    self.quarantine_chunk(dat_file, actual_sha, expected_sha)
                    quarantined += 1
            except Exception:
                pass

        duration = time.time() - start_time
        LAST_FULL_SCAN_AT.set(time.time())

        return {
            "chunks_verified": chunks_verified,
            "corrupt_detected": corrupt_detected,
            "quarantined": quarantined,
            "duration_seconds": duration,
            "scanned_at": time.time(),
        }
