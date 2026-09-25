"""Integration tests for single-node Vault storage service."""

import hashlib
import os
from pathlib import Path
import pytest
import httpx

from apps.storage_node.service import create_vault_app
from vault_core.settings import VaultSettings


@pytest.fixture
def test_app(tmp_path: Path):
    """Instantiate a fresh Vault single-node application with isolated storage."""
    custom_settings = VaultSettings(
        data_dir=tmp_path / "data",
        chunk_size_bytes=8 * 1024 * 1024,  # 8 MiB chunks
    )
    app = create_vault_app(custom_settings)
    return app


@pytest.fixture
async def client(test_app):
    """Create an async HTTP client connected to the test application."""
    # Ensure startup event (database tables init) runs
    await test_app.state.metadata.initialize()
    transport = httpx.ASGITransport(app=test_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


@pytest.mark.asyncio
async def test_put_requires_idempotency_key(client: httpx.AsyncClient):
    """Verify that PUT without an idempotency key returns 400 Bad Request."""
    resp = await client.put(
        "/v1/objects/my-bucket/missing-idemp.txt",
        content=b"some content"
    )
    assert resp.status_code == 400
    assert "Missing required Idempotency-Key" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_put_get_head_small_object(client: httpx.AsyncClient):
    """Test standard PUT, HEAD, and GET flow for a single-chunk object."""
    bucket = "test-bucket"
    key = "documents/report.pdf"
    content = b"PDF-1.4 Mock binary stream content for Vault verification test." * 500
    expected_hash = hashlib.sha256(content).hexdigest()

    # 1. PUT upload
    put_resp = await client.put(
        f"/v1/objects/{bucket}/{key}",
        content=content,
        headers={
            "X-Idempotency-Key": "idemp-put-001",
            "Content-Type": "application/pdf"
        }
    )
    assert put_resp.status_code == 201
    put_data = put_resp.json()
    assert put_data["bucket"] == bucket
    assert put_data["key"] == key
    assert put_data["size_bytes"] == len(content)
    assert put_data["content_hash"] == expected_hash
    assert put_data["logical_version"] == 1
    version_id = put_data["version_id"]

    # 2. HEAD verification
    head_resp = await client.head(f"/v1/objects/{bucket}/{key}")
    assert head_resp.status_code == 200
    assert head_resp.headers["Content-Length"] == str(len(content))
    assert head_resp.headers["Content-Type"] == "application/pdf"
    assert head_resp.headers["ETag"] == f'"{expected_hash}"'
    assert head_resp.headers["X-Vault-Version-Id"] == version_id
    assert head_resp.headers["X-Vault-Logical-Version"] == "1"

    # 3. GET download stream
    get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
    assert get_resp.status_code == 200
    assert get_resp.headers["Content-Length"] == str(len(content))
    assert get_resp.headers["ETag"] == f'"{expected_hash}"'
    assert get_resp.content == content
    assert hashlib.sha256(get_resp.content).hexdigest() == expected_hash


@pytest.mark.asyncio
async def test_idempotent_retry(client: httpx.AsyncClient):
    """Verify that repeating a PUT with identical idempotency key returns cached result."""
    bucket = "test-bucket"
    key = "idemp-test.dat"
    content = b"Immutable idempotent test payload"

    headers = {"X-Idempotency-Key": "idemp-duplicate-key-12345"}

    # First write -> 201 Created
    resp1 = await client.put(f"/v1/objects/{bucket}/{key}", content=content, headers=headers)
    assert resp1.status_code == 201
    v1_id = resp1.json()["version_id"]

    # Duplicate write with same key -> 200 OK with cached response and header
    resp2 = await client.put(f"/v1/objects/{bucket}/{key}", content=content, headers=headers)
    assert resp2.status_code == 200
    assert resp2.headers.get("X-Vault-Idempotent-Hit") == "true"
    assert resp2.json()["version_id"] == v1_id


@pytest.mark.asyncio
async def test_delete_tombstone(client: httpx.AsyncClient):
    """Verify tombstone commit hides object from GET and HEAD immediately."""
    bucket = "test-bucket"
    key = "delete-me.txt"
    content = b"Temporary content to be deleted"

    # Upload
    await client.put(
        f"/v1/objects/{bucket}/{key}",
        content=content,
        headers={"X-Idempotency-Key": "delete-test-idemp"}
    )

    # Confirm exists
    head_before = await client.head(f"/v1/objects/{bucket}/{key}")
    assert head_before.status_code == 200

    # DELETE -> 204
    del_resp = await client.delete(
        f"/v1/objects/{bucket}/{key}",
        headers={"X-Idempotency-Key": "del-idemp-001"}
    )
    assert del_resp.status_code == 204
    assert del_resp.headers.get("X-Vault-Tombstone") == "true"
    assert del_resp.headers.get("X-Vault-Logical-Version") == "2"

    # Subsequent GET -> 404
    get_after = await client.get(f"/v1/objects/{bucket}/{key}")
    assert get_after.status_code == 404

    # Subsequent HEAD -> 404
    head_after = await client.head(f"/v1/objects/{bucket}/{key}")
    assert head_after.status_code == 404

    # DELETE non-existent / already deleted -> 404
    del_again = await client.delete(f"/v1/objects/{bucket}/{key}")
    assert del_again.status_code == 404


@pytest.mark.asyncio
async def test_checksum_corruption_detection_and_quarantine(test_app, client: httpx.AsyncClient):
    """
    Verify that if a chunk on disk becomes corrupt:
    1. Checksum validation detects the mismatch.
    2. The chunk is moved to quarantine.
    3. The service rejects serving corrupted data.
    """
    bucket = "test-bucket"
    key = "bitrot/target-file.bin"
    content = b"Critical data block that will suffer simulated bit corruption." * 100

    # Upload object
    put_resp = await client.put(
        f"/v1/objects/{bucket}/{key}",
        content=content,
        headers={"X-Idempotency-Key": "bitrot-test-key"}
    )
    assert put_resp.status_code == 201
    version_id = put_resp.json()["version_id"]

    # Locate chunk file on disk
    storage = test_app.state.storage
    chunk_path = storage.chunks_dir / bucket / version_id / "chunk_0.dat"
    assert chunk_path.is_file()

    # Simulate silent bit rot: flip bytes in the chunk file
    with open(chunk_path, "r+b") as f:
        f.seek(10)
        original_byte = f.read(1)
        f.seek(10)
        # Flip bits
        corrupted_byte = bytes([original_byte[0] ^ 0xFF])
        f.write(corrupted_byte)

    # Attempt to GET the corrupted object
    # Expect corrupted chunk error / aborted stream
    with pytest.raises(Exception):
        await client.get(f"/v1/objects/{bucket}/{key}")

    # Verify that the corrupt file was removed from active chunk dir and moved to quarantine
    assert not chunk_path.is_file()
    quarantine_files = list(storage.quarantine_dir.glob("*.dat"))
    assert len(quarantine_files) == 1
    assert "corrupt" in quarantine_files[0].name


@pytest.mark.asyncio
async def test_streamed_128mib_object(test_app, client: httpx.AsyncClient):
    """
    Upload and retrieve a 128 MiB object in streaming chunks.
    Validates:
    - 8 MiB chunk boundary splitting (exactly 16 chunks for 128 MiB)
    - Full end-to-end SHA-256 integrity
    - Streaming without excessive memory overhead
    """
    bucket = "large-objects"
    key = "benchmark/128MiB.iso"
    chunk_size = 8 * 1024 * 1024  # 8 MiB
    total_chunks = 16
    total_size = total_chunks * chunk_size  # 134,217,728 bytes (128 MiB)

    # Generator streaming 128 MiB in 1 MiB slices
    pattern = b"0123456789ABCDEF" * 65536  # Exactly 1,048,576 bytes = 1 MiB
    assert len(pattern) == 1024 * 1024

    overall_hasher = hashlib.sha256()

    async def generate_128mib_stream():
        for i in range(total_chunks * 8):  # 128 1-MiB blocks
            overall_hasher.update(pattern)
            yield pattern

    # Stream upload
    put_resp = await client.put(
        f"/v1/objects/{bucket}/{key}",
        content=generate_128mib_stream(),
        headers={
            "X-Idempotency-Key": "large-128m-stream-test",
            "Content-Type": "application/octet-stream"
        }
    )
    assert put_resp.status_code == 201
    put_data = put_resp.json()
    expected_hash = overall_hasher.hexdigest()

    assert put_data["size_bytes"] == total_size
    assert put_data["content_hash"] == expected_hash
    assert put_data["chunk_count"] == 16  # Exactly 16 x 8 MiB chunks

    # Verify storage has 16 chunk files on disk
    storage = test_app.state.storage
    version_id = put_data["version_id"]
    chunk_files = list((storage.chunks_dir / bucket / version_id).glob("chunk_*.dat"))
    assert len(chunk_files) == 16

    # Verify each chunk on disk is 8 MiB
    for chunk_f in chunk_files:
        assert chunk_f.stat().st_size == chunk_size

    # Stream download and verify complete hash
    get_hasher = hashlib.sha256()
    received_bytes = 0

    async with client.stream("GET", f"/v1/objects/{bucket}/{key}") as get_resp:
        assert get_resp.status_code == 200
        assert get_resp.headers["Content-Length"] == str(total_size)
        assert get_resp.headers["ETag"] == f'"{expected_hash}"'

        async for block in get_resp.aiter_bytes(chunk_size=65536):
            get_hasher.update(block)
            received_bytes += len(block)

    assert received_bytes == total_size
    assert get_hasher.hexdigest() == expected_hash


@pytest.mark.asyncio
async def test_metrics_endpoint(client: httpx.AsyncClient):
    """Verify /metrics returns standard Prometheus format."""
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["Content-Type"]
    metrics_text = resp.text
    assert "vault_http_requests_total" in metrics_text
    assert "vault_objects_total" in metrics_text
    assert "vault_chunks_total" in metrics_text
    assert "vault_bytes_stored_total" in metrics_text
