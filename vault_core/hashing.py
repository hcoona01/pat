"""Cryptographic checksum and streaming SHA-256 calculation utilities."""

import hashlib
from pathlib import Path
from typing import AsyncIterable


def sha256_bytes(data: bytes) -> str:
    """Calculate the SHA-256 hexadecimal digest for raw bytes."""
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()


async def sha256_file(file_path: Path) -> str:
    """Calculate the SHA-256 hexadecimal digest for a local file asynchronously."""
    hasher = hashlib.sha256()
    # Read in 64 KiB blocks
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


class StreamHashVerifier:
    """Utility to compute SHA-256 incrementally across an async stream of bytes."""

    def __init__(self) -> None:
        self.hasher = hashlib.sha256()
        self.bytes_hashed = 0

    def update(self, chunk: bytes) -> None:
        """Update digest with received byte chunk."""
        self.hasher.update(chunk)
        self.bytes_hashed += len(chunk)

    @property
    def hexdigest(self) -> str:
        """Return current SHA-256 hex digest."""
        return self.hasher.hexdigest()
