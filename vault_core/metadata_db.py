"""SQLite metadata repository with WAL mode and atomic state persistence."""

import json
import time
from pathlib import Path
from typing import Optional, Tuple
import aiosqlite

from vault_core.manifest import ObjectManifest
from vault_core.metrics import OBJECTS_TOTAL


class MetadataRepository:
    """Manages metadata persistence using SQLite with WAL mode."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    async def initialize(self) -> None:
        """Create tables and indexes with WAL journal mode."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")

            # Table for all immutable versions of manifests
            await db.execute("""
                CREATE TABLE IF NOT EXISTS manifests (
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    logical_version INTEGER NOT NULL,
                    previous_version_id TEXT,
                    size_bytes INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    policy TEXT NOT NULL,
                    is_tombstone INTEGER NOT NULL DEFAULT 0,
                    idempotency_key TEXT,
                    created_at REAL NOT NULL,
                    deleted_at REAL,
                    manifest_json TEXT NOT NULL,
                    PRIMARY KEY (bucket, key, version_id)
                );
            """)

            # Table pointing to the current (latest) version for fast lookups
            await db.execute("""
                CREATE TABLE IF NOT EXISTS current_objects (
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    logical_version INTEGER NOT NULL,
                    is_tombstone INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (bucket, key)
                );
            """)

            # Table for strict idempotency deduplication
            await db.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_records (
                    idempotency_key TEXT PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
            """)

            await db.execute("CREATE INDEX IF NOT EXISTS idx_manifest_key ON manifests(bucket, key);")
            await db.commit()

    async def get_idempotent_result(self, idempotency_key: str) -> Optional[dict]:
        """Retrieve existing response for an idempotency key if previously processed."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT response_json FROM idempotency_records WHERE idempotency_key = ?",
                (idempotency_key,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
        return None

    async def get_latest_manifest(self, bucket: str, key: str) -> Optional[ObjectManifest]:
        """Fetch the current committed manifest for a bucket and key."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT m.manifest_json
                FROM current_objects c
                JOIN manifests m ON c.bucket = m.bucket AND c.key = m.key AND c.version_id = m.version_id
                WHERE c.bucket = ? AND c.key = ?
                """,
                (bucket, key)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return ObjectManifest.model_validate_json(row[0])
        return None

    async def get_next_logical_version(self, bucket: str, key: str) -> Tuple[int, Optional[str]]:
        """Get the next logical version number and current version ID for CAS precondition handling."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT logical_version, version_id FROM current_objects WHERE bucket = ? AND key = ?",
                (bucket, key)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return row[0] + 1, row[1]
        return 1, None

    async def commit_manifest(
        self,
        manifest: ObjectManifest,
        response_payload: Optional[dict] = None
    ) -> None:
        """Atomically persist manifest, update current object pointer, and record idempotency."""
        manifest_json = manifest.model_dump_json()

        async with aiosqlite.connect(self.db_path) as db:
            # 1. Insert immutable manifest entry
            await db.execute(
                """
                INSERT INTO manifests (
                    bucket, key, version_id, logical_version, previous_version_id,
                    size_bytes, content_hash, content_type, policy, is_tombstone,
                    idempotency_key, created_at, deleted_at, manifest_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest.bucket,
                    manifest.key,
                    manifest.version_id,
                    manifest.logical_version,
                    manifest.previous_version_id,
                    manifest.size_bytes,
                    manifest.content_hash,
                    manifest.content_type,
                    manifest.policy,
                    1 if manifest.is_tombstone else 0,
                    manifest.idempotency_key,
                    manifest.created_at,
                    manifest.deleted_at,
                    manifest_json,
                )
            )

            # 2. Update current object pointer
            await db.execute(
                """
                INSERT INTO current_objects (bucket, key, version_id, logical_version, is_tombstone, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(bucket, key) DO UPDATE SET
                    version_id = excluded.version_id,
                    logical_version = excluded.logical_version,
                    is_tombstone = excluded.is_tombstone,
                    updated_at = excluded.updated_at
                """,
                (
                    manifest.bucket,
                    manifest.key,
                    manifest.version_id,
                    manifest.logical_version,
                    1 if manifest.is_tombstone else 0,
                    manifest.created_at,
                )
            )

            # 3. If idempotency key provided, record it
            if manifest.idempotency_key and response_payload:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO idempotency_records (
                        idempotency_key, bucket, key, version_id, response_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        manifest.idempotency_key,
                        manifest.bucket,
                        manifest.key,
                        manifest.version_id,
                        json.dumps(response_payload),
                        manifest.created_at,
                    )
                )

            await db.commit()

        # Update gauge metrics
        if not manifest.is_tombstone:
            OBJECTS_TOTAL.inc()
        else:
            OBJECTS_TOTAL.dec()

    async def create_tombstone(
        self,
        bucket: str,
        key: str,
        idempotency_key: Optional[str] = None
    ) -> Optional[ObjectManifest]:
        """Commit a tombstone version for an object. Returns tombstone manifest or None if not found."""
        current = await self.get_latest_manifest(bucket, key)
        if not current or current.is_tombstone:
            return None

        next_version, prev_version_id = await self.get_next_logical_version(bucket, key)
        now = time.time()
        tombstone = ObjectManifest(
            bucket=bucket,
            key=key,
            logical_version=next_version,
            previous_version_id=prev_version_id,
            size_bytes=0,
            content_hash="",
            policy=current.policy,
            chunks=[],
            is_tombstone=True,
            idempotency_key=idempotency_key,
            created_at=now,
            deleted_at=now,
        )

        await self.commit_manifest(tombstone, response_payload={"status": "tombstoned"})
        return tombstone
