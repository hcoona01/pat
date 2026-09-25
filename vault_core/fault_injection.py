"""Repeatable network fault injection controller and transport simulator.

Supports:
- Node isolation (connection refused / network unreachable)
- Zone-level network partition isolation
- Latency injection and packet drop simulation
- Compatibility with external Toxiproxy instances when running under Docker
"""

import asyncio
import logging
from typing import Dict, Optional, Set
import httpx

from vault_core.cluster import ClusterConfig

logger = logging.getLogger("vault.chaos")


class FaultInjectionTransport(httpx.AsyncBaseTransport):
    """
    Programmable HTTP transport simulating real-world network partitions,
    zone outages, connection drops, and latency between distributed nodes.
    """

    def __init__(self, node_apps: Dict[str, httpx.ASGITransport]) -> None:
        self.node_transports = dict(node_apps)
        self.isolated_nodes: Set[str] = set()
        self.node_delays: Dict[str, float] = {}
        self.node_timeouts: Set[str] = set()
        self._lock = asyncio.Lock()

    def add_node_transport(self, node_id: str, transport: httpx.ASGITransport) -> None:
        """Register a node's transport interface."""
        self.node_transports[node_id] = transport

    def isolate_node(self, node_id: str) -> None:
        """Simulate complete network partition for a specific node."""
        self.isolated_nodes.add(node_id)
        logger.warning(f"[Chaos] Node isolated: {node_id}")

    def heal_node(self, node_id: str) -> None:
        """Restore network connectivity for a partitioned node."""
        self.isolated_nodes.discard(node_id)
        self.node_timeouts.discard(node_id)
        self.node_delays.pop(node_id, None)
        logger.info(f"[Chaos] Node healed: {node_id}")

    def isolate_zone(self, zone: str, cluster: ClusterConfig) -> Set[str]:
        """Isolate all storage nodes residing within a target availability zone."""
        isolated = set()
        for node in cluster.storage_nodes:
            if node.zone == zone:
                self.isolate_node(node.node_id)
                isolated.add(node.node_id)
        logger.warning(f"[Chaos] Zone isolated: {zone} (nodes: {isolated})")
        return isolated

    def heal_zone(self, zone: str, cluster: ClusterConfig) -> Set[str]:
        """Restore network connectivity to all nodes in an availability zone."""
        healed = set()
        for node in cluster.storage_nodes:
            if node.zone == zone:
                self.heal_node(node.node_id)
                healed.add(node.node_id)
        logger.info(f"[Chaos] Zone healed: {zone} (nodes: {healed})")
        return healed

    def inject_delay(self, node_id: str, delay_seconds: float) -> None:
        """Inject artificial network latency before forwarding to node."""
        self.node_delays[node_id] = delay_seconds

    def inject_timeout(self, node_id: str) -> None:
        """Simulate connection/read timeout for target node."""
        self.node_timeouts.add(node_id)

    def heal_all(self) -> None:
        """Clear all active network partitions, delays, and timeouts."""
        self.isolated_nodes.clear()
        self.node_timeouts.clear()
        self.node_delays.clear()
        logger.info("[Chaos] All network partitions healed")

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        host = getattr(request.url, "host", "")

        # Match request destination against registered node IDs
        matched_node_id = None
        for node_id in self.node_transports:
            if node_id == host or f"://{node_id}:" in url_str or f"://{node_id}/" in url_str:
                matched_node_id = node_id
                break

        if not matched_node_id:
            raise httpx.ConnectError(f"Host unreachable: {request.url}")

        # 1. Network Partition Isolation
        if matched_node_id in self.isolated_nodes:
            raise httpx.ConnectError(
                f"[Chaos] Connection refused: target node '{matched_node_id}' is network-isolated"
            )

        # 2. Connection Timeout
        if matched_node_id in self.node_timeouts:
            await asyncio.sleep(0.5)
            raise httpx.TimeoutException(
                f"[Chaos] Read timed out: target node '{matched_node_id}' dropped packets"
            )

        # 3. Latency Injection
        if matched_node_id in self.node_delays:
            await asyncio.sleep(self.node_delays[matched_node_id])

        transport = self.node_transports[matched_node_id]
        return await transport.handle_async_request(request)
