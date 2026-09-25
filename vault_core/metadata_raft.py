"""Raft-based metadata consensus state machine and cluster synchronization."""

import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from pysyncobj import SyncObj, SyncObjConf, replicated

logger = logging.getLogger("vault.raft")


class RaftError(Exception):
    """Base exception for Raft metadata cluster errors."""
    pass


class NotLeaderError(RaftError):
    """Raised when a write is attempted on a non-leader Raft node."""
    def __init__(self, leader_address: Optional[str] = None):
        super().__init__(f"Node is not the Raft leader. Current leader: {leader_address or 'Unknown'}")
        self.leader_address = leader_address


class RaftQuorumError(RaftError):
    """Raised when Raft cannot achieve consensus quorum."""
    pass


class CASConflictError(RaftError):
    """Raised when an update fails Compare-And-Swap precondition (HTTP 409 Conflict)."""
    def __init__(self, expected_version: Optional[int], current_version: int):
        super().__init__(
            f"CAS Precondition Failed: expected version {expected_version}, but current logical version is {current_version}"
        )
        self.expected_version = expected_version
        self.current_version = current_version


class ManifestNotFoundError(RaftError):
    """Raised when an object or version is not found in committed metadata."""
    pass


class RaftMetadataStateMachine(SyncObj):
    """
    Raft Replicated State Machine storing object manifests, logical versions,
    tombstones, idempotency keys, and cluster membership.
    """

    def __init__(
        self,
        self_address: str,
        partner_addresses: List[str],
        data_dir: Optional[Path] = None,
        auto_tick_period: float = 0.05,
    ) -> None:
        conf = SyncObjConf(
            autoTickPeriod=auto_tick_period,
            appendEntriesUseBatch=True,
            dynamicMembershipChange=True,
        )

        if data_dir:
            dir_path = Path(data_dir)
            dir_path.mkdir(parents=True, exist_ok=True)
            # Raft log and snapshot dump
            conf.fullDumpFile = str(dir_path / "raft_snapshot.bin")

        super().__init__(self_address, partner_addresses, conf)

        self.self_address = self_address
        # In-memory replicated state
        self._manifests: Dict[str, Dict[str, dict]] = {}  # "bucket/key" -> {version_id: manifest_dict}
        self._current_versions: Dict[str, dict] = {}      # "bucket/key" -> {current_version_id, logical_version, is_tombstone, updated_at}
        self._tombstones: Dict[str, List[dict]] = {}       # "bucket/key" -> [tombstone_summaries]
        self._idempotency_keys: Dict[str, dict] = {}      # idempotency_key -> {response, created_at}
        self._membership: Dict[str, dict] = {}            # node_id -> node_info

    # =========================================================================
    # REPLICATED ACTIONS (COMMITTED ONLY THROUGH RAFT CONSENSUS)
    # =========================================================================

    @replicated
    def _apply_commit_manifest(
        self,
        manifest_dict: dict,
        expected_version: Optional[int],
        idempotency_key: Optional[str],
        response_payload: Optional[dict]
    ) -> dict:
        """
        Replicated commit of an object manifest with atomic CAS version validation.
        Executed deterministically on all Raft nodes upon log commit.
        """
        bucket = manifest_dict["bucket"]
        key = manifest_dict["key"]
        version_id = manifest_dict["version_id"]
        obj_key = f"{bucket}/{key}"

        # 1. Idempotency Check
        if idempotency_key and idempotency_key in self._idempotency_keys:
            cached = self._idempotency_keys[idempotency_key]
            return {
                "success": True,
                "idempotent_hit": True,
                "response": cached["response_payload"],
                "logical_version": cached["logical_version"],
            }

        # 2. CAS Version Precondition Check
        curr = self._current_versions.get(obj_key)
        curr_logical = curr["logical_version"] if curr else 0
        is_tombstone = curr["is_tombstone"] if curr else True

        if expected_version is not None:
            # If expected_version is 0, caller expects object not to exist
            if expected_version != curr_logical:
                return {
                    "success": False,
                    "error": "CAS_CONFLICT",
                    "expected_version": expected_version,
                    "current_version": curr_logical,
                }

        next_logical = curr_logical + 1
        manifest_dict["logical_version"] = next_logical

        # 3. Store Immutable Manifest
        if obj_key not in self._manifests:
            self._manifests[obj_key] = {}
        self._manifests[obj_key][version_id] = manifest_dict

        # 4. Update Current Object Pointer
        self._current_versions[obj_key] = {
            "current_version_id": version_id,
            "logical_version": next_logical,
            "is_tombstone": False,
            "updated_at": manifest_dict.get("created_at", time.time()),
        }

        # 5. Record Idempotency
        if idempotency_key and response_payload:
            self._idempotency_keys[idempotency_key] = {
                "response_payload": response_payload,
                "logical_version": next_logical,
                "created_at": time.time(),
            }

        return {
            "success": True,
            "idempotent_hit": False,
            "logical_version": next_logical,
            "version_id": version_id,
        }

    @replicated
    def _apply_tombstone(
        self,
        bucket: str,
        key: str,
        tombstone_dict: dict,
        expected_version: Optional[int],
        idempotency_key: Optional[str]
    ) -> dict:
        """
        Replicated tombstone recording. Hides object atomically across all nodes.
        """
        obj_key = f"{bucket}/{key}"
        curr = self._current_versions.get(obj_key)

        if not curr or curr["is_tombstone"]:
            return {"success": False, "error": "NOT_FOUND"}

        curr_logical = curr["logical_version"]
        if expected_version is not None and expected_version != curr_logical:
            return {
                "success": False,
                "error": "CAS_CONFLICT",
                "expected_version": expected_version,
                "current_version": curr_logical,
            }

        next_logical = curr_logical + 1
        version_id = tombstone_dict["version_id"]
        tombstone_dict["logical_version"] = next_logical
        tombstone_dict["is_tombstone"] = True

        if obj_key not in self._manifests:
            self._manifests[obj_key] = {}
        self._manifests[obj_key][version_id] = tombstone_dict

        self._current_versions[obj_key] = {
            "current_version_id": version_id,
            "logical_version": next_logical,
            "is_tombstone": True,
            "updated_at": tombstone_dict.get("deleted_at", time.time()),
        }

        if obj_key not in self._tombstones:
            self._tombstones[obj_key] = []
        self._tombstones[obj_key].append(tombstone_dict)

        if idempotency_key:
            self._idempotency_keys[idempotency_key] = {
                "response_payload": {"status": "tombstoned"},
                "logical_version": next_logical,
                "created_at": time.time(),
            }

        return {"success": True, "logical_version": next_logical, "version_id": version_id}

    @replicated
    def _apply_membership_update(self, node_id: str, node_info: dict, action: str) -> dict:
        """Replicated cluster storage node membership change."""
        if action == "remove":
            self._membership.pop(node_id, None)
        else:
            self._membership[node_id] = node_info
        return {"success": True, "action": action, "node_id": node_id}

    # =========================================================================
    # PUBLIC ACCESS API
    # =========================================================================

    def is_leader(self) -> bool:
        """Check if this node is currently the elected Raft leader."""
        return self.isReady() and bool(self._isLeader())

    def get_leader_address(self) -> Optional[str]:
        """Return the network address of the current Raft leader, if known."""
        if self._isLeader():
            return self.self_address
        leader = self._getLeader()
        return str(leader) if leader else None

    def commit_manifest(
        self,
        manifest_dict: dict,
        expected_version: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        response_payload: Optional[dict] = None,
        timeout: float = 3.0,
    ) -> dict:
        """
        Commit manifest through Raft.
        Enforces:
        - Only executed through Raft leader.
        - Blocks until quorum log replication completes.
        - Competing writes with same expected version: exactly one succeeds.
        - Stale writes raise CASConflictError.
        """
        if not self.is_leader():
            raise NotLeaderError(self.get_leader_address())

        try:
            res = self._apply_commit_manifest(
                manifest_dict,
                expected_version,
                idempotency_key,
                response_payload,
                sync=True,
                timeout=timeout,
            )
        except Exception as exc:
            raise RaftQuorumError(f"Failed to achieve Raft quorum within {timeout}s: {exc}") from exc

        if not res or not res.get("success"):
            if res and res.get("error") == "CAS_CONFLICT":
                raise CASConflictError(res.get("expected_version"), res.get("current_version", 0))
            raise RaftError(f"Raft commit rejected: {res}")

        return res

    def create_tombstone(
        self,
        bucket: str,
        key: str,
        expected_version: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        timeout: float = 3.0,
    ) -> dict:
        """Commit an object deletion tombstone through Raft quorum."""
        if not self.is_leader():
            raise NotLeaderError(self.get_leader_address())

        tombstone_dict = {
            "bucket": bucket,
            "key": key,
            "version_id": str(uuid.uuid4()),
            "is_tombstone": True,
            "deleted_at": time.time(),
        }

        try:
            res = self._apply_tombstone(
                bucket,
                key,
                tombstone_dict,
                expected_version,
                idempotency_key,
                sync=True,
                timeout=timeout,
            )
        except Exception as exc:
            raise RaftQuorumError(f"Failed to achieve Raft quorum for tombstone: {exc}") from exc

        if not res or not res.get("success"):
            if res and res.get("error") == "CAS_CONFLICT":
                raise CASConflictError(res.get("expected_version"), res.get("current_version", 0))
            if res and res.get("error") == "NOT_FOUND":
                raise ManifestNotFoundError(f"Object {bucket}/{key} not found or already tombstoned")
            raise RaftError(f"Tombstone commit failed: {res}")

        return res

    def get_latest_manifest(self, bucket: str, key: str) -> Optional[dict]:
        """
        Retrieve latest committed manifest.
        Returns None if object does not exist or has been tombstoned.
        """
        obj_key = f"{bucket}/{key}"
        curr = self._current_versions.get(obj_key)
        if not curr or curr["is_tombstone"]:
            return None

        version_id = curr["current_version_id"]
        return self._manifests.get(obj_key, {}).get(version_id)

    def get_manifest_by_version(self, bucket: str, key: str, version_id: str) -> Optional[dict]:
        """Retrieve a specific immutable historical version of an object manifest."""
        obj_key = f"{bucket}/{key}"
        return self._manifests.get(obj_key, {}).get(version_id)

    def get_cluster_health(self) -> dict:
        """Return status snapshot of this Raft node and consensus state."""
        return {
            "node_address": self.self_address,
            "is_ready": self.isReady(),
            "is_leader": self.is_leader(),
            "leader_address": self.get_leader_address(),
            "partner_count": len(self._syncObjConf.partners) if hasattr(self, "_syncObjConf") else 0,
            "total_objects": len(self._current_versions),
            "active_objects": sum(1 for v in self._current_versions.values() if not v["is_tombstone"]),
            "tombstoned_objects": sum(1 for v in self._current_versions.values() if v["is_tombstone"]),
        }
