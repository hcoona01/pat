"""Object manifest definitions, replica states, and garbage collection rules."""

from enum import Enum
import time
import uuid
from typing import List, Optional, Tuple
from pydantic import BaseModel, Field


class ReplicaState(str, Enum):
    """Explicit lifecycle state of a chunk replica across storage nodes."""
    HEALTHY = "healthy"         # Present on node and SHA-256 matches latest committed manifest
    STALE = "stale"             # Present on node, but belongs to an older superseded version
    CORRUPT = "corrupt"         # Present on node, but failed cryptographic checksum validation
    MISSING = "missing"         # Node is reachable, but chunk file does not exist
    UNREACHABLE = "unreachable" # Node timed out or refused connection


class ReplicaAudit(BaseModel):
    """Audit outcome for a single chunk replica on a specific storage node."""
    node_id: str = Field(..., description="Storage node ID holding or expected to hold replica")
    chunk_index: int = Field(..., description="Chunk index within object")
    expected_version_id: str = Field(..., description="Target version UUID according to Raft manifest")
    expected_sha256: str = Field(..., description="Cryptographic hash recorded in committed manifest")
    state: ReplicaState = Field(..., description="Audited state of the replica")
    actual_sha256: Optional[str] = Field(default=None, description="Actual hash read from disk if available")
    details: Optional[str] = Field(default=None, description="Human-readable diagnostics")


class ReplicaAuditReport(BaseModel):
    """Aggregated replica audit report comparing physical node inventory against Raft manifest."""
    bucket: str
    key: str
    version_id: str
    logical_version: int
    replicas: List[ReplicaAudit]
    healthy_count: int
    stale_count: int
    corrupt_count: int
    missing_count: int
    unreachable_count: int


class FragmentInfo(BaseModel):
    """Metadata describing a single Reed-Solomon erasure coding fragment."""
    fragment_index: int = Field(..., description="Zero-based fragment index (0..K-1 data, K..M-1 parity)")
    sha256: str = Field(..., description="Cryptographic SHA-256 hash of this fragment")
    size_bytes: int = Field(..., description="Size of this fragment in bytes")
    node_id: str = Field(..., description="Storage node ID holding this fragment")
    is_parity: bool = Field(default=False, description="True if parity fragment, False if data fragment")


class ChunkInfo(BaseModel):
    """Metadata describing a single object chunk or fragment."""
    chunk_index: int = Field(..., description="Zero-based index of this chunk within the object")
    chunk_id: str = Field(..., description="Globally unique chunk identifier")
    sha256: str = Field(..., description="Hexadecimal SHA-256 digest of chunk content")
    size_bytes: int = Field(..., description="Size of chunk in bytes")
    placement_nodes: List[str] = Field(default_factory=list, description="IDs of storage nodes holding this chunk or its fragments")
    stored_path: Optional[str] = Field(default=None, description="Local relative path on node if stored locally")
    is_erasure_coded: bool = Field(default=False, description="Whether this chunk uses Reed-Solomon erasure coding")
    fragments: Optional[List[FragmentInfo]] = Field(default=None, description="Individual fragments if erasure coded")
    original_chunk_size: Optional[int] = Field(default=None, description="Unpadded chunk size before EC encoding")


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
            "deleted_at": self.deleted_at,
        }


def evaluate_gc_eligibility(
    manifest: ObjectManifest,
    current_manifest: Optional[ObjectManifest],
    retention_period_seconds: int = 86400,
    current_time: Optional[float] = None
) -> Tuple[bool, str]:
    """
    Safely determine whether a historical or tombstoned manifest's chunk data is eligible for garbage collection.
    Rules:
    1. If the manifest is the active current version and not tombstoned: NEVER eligible.
    2. If the manifest is superseded or tombstoned, it is only eligible if its age exceeds retention_period_seconds.
    3. Prevents premature deletion during concurrent in-flight reads.
    """
    now = current_time if current_time is not None else time.time()

    # Rule 1: Current active version can never be collected
    if current_manifest and current_manifest.version_id == manifest.version_id and not current_manifest.is_tombstone:
        return False, "Active current version: not eligible for GC"

    # Rule 2: If tombstoned, calculate age from deleted_at
    if manifest.is_tombstone:
        tombstone_time = manifest.deleted_at or manifest.created_at
        age = now - tombstone_time
        if age < retention_period_seconds:
            return False, f"Tombstone within retention grace period ({age:.1f}s < {retention_period_seconds}s)"
        return True, f"Tombstoned version expired retention ({age:.1f}s >= {retention_period_seconds}s)"

    # Rule 3: Superseded historical version
    age = now - manifest.created_at
    if age < retention_period_seconds:
        return False, f"Superseded version within retention grace period ({age:.1f}s < {retention_period_seconds}s)"

    return True, f"Superseded version expired retention ({age:.1f}s >= {retention_period_seconds}s)"
