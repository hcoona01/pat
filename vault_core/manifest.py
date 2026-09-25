"""Object manifest definitions and validation schemas."""

import time
import uuid
from typing import List, Optional
from pydantic import BaseModel, Field


class ChunkInfo(BaseModel):
    """Metadata describing a single object chunk or fragment."""
    chunk_index: int = Field(..., description="Zero-based index of this chunk within the object")
    chunk_id: str = Field(..., description="Globally unique chunk identifier")
    sha256: str = Field(..., description="Hexadecimal SHA-256 digest of chunk content")
    size_bytes: int = Field(..., description="Size of chunk in bytes")
    placement_nodes: List[str] = Field(default_factory=list, description="IDs of storage nodes holding this chunk")
    stored_path: Optional[str] = Field(default=None, description="Local relative path on node if stored locally")


class ObjectManifest(BaseModel):
    """Complete manifest representing an immutable version of an object."""
    bucket: str = Field(..., description="Bucket name")
    key: str = Field(..., description="Object key name")
    version_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Immutable version UUID")
    logical_version: int = Field(default=1, description="Monotonically increasing version number for this key")
    previous_version_id: Optional[str] = Field(default=None, description="Preceding version UUID if this is an update")
    size_bytes: int = Field(default=0, description="Total size of the object in bytes")
    content_hash: str = Field(..., description="Full object SHA-256 digest")
    content_type: str = Field(default="application/octet-stream", description="MIME content type")
    policy: str = Field(default="hot", description="Durability policy applied to this object")
    chunks: List[ChunkInfo] = Field(default_factory=list, description="Ordered chunk list")
    is_tombstone: bool = Field(default=False, description="True if this version represents an object deletion")
    idempotency_key: Optional[str] = Field(default=None, description="Client idempotency key for this write")
    created_at: float = Field(default_factory=time.time, description="Unix timestamp of creation")
    deleted_at: Optional[float] = Field(default=None, description="Unix timestamp when deleted / tombstoned")

    def to_summary_dict(self) -> dict:
        """Return high-level summary suitable for API responses."""
        return {
            "bucket": self.bucket,
            "key": self.key,
            "version_id": self.version_id,
            "logical_version": self.logical_version,
            "size_bytes": self.size_bytes,
            "content_hash": self.content_hash,
            "content_type": self.content_type,
            "policy": self.policy,
            "chunk_count": len(self.chunks),
            "is_tombstone": self.is_tombstone,
            "created_at": self.created_at,
        }
