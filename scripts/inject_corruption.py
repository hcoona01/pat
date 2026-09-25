#!/usr/bin/env python3
"""
Vault Script: inject_corruption.py
Simulates disk bitrot and checksum corruption on stored chunk and fragment files.
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path


def calculate_sha256(filepath: Path) -> str:
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def find_target_file(args: argparse.Namespace) -> Path:
    if args.target_file:
        p = Path(args.target_file)
        if not p.is_file():
            print(f"Error: Specified file does not exist: {p}", file=sys.stderr)
            sys.exit(1)
        return p

    # Search in data directory
    base_dirs = []
    if args.data_dir:
        base_dirs.append(Path(args.data_dir))
    if args.node_id:
        base_dirs.extend([
            Path(f"data/{args.node_id}"),
            Path(f"/data/{args.node_id}"),
            Path(f"data/{args.node_id}/chunks"),
            Path(f"/data/{args.node_id}/chunks"),
            Path(f"/tmp/vault_test_cluster/{args.node_id}"),
        ])
    base_dirs.extend([Path("data"), Path("/data"), Path(".")])

    filename = f"chunk_{args.chunk_index}.dat"
    if args.fragment_index is not None:
        filename = f"chunk_{args.chunk_index}_frag_{args.fragment_index}.dat"

    for base in base_dirs:
        if not base.exists():
            continue
        candidate = base / "chunks" / args.bucket / args.version_id / filename
        if candidate.is_file():
            return candidate
        candidate2 = base / args.bucket / args.version_id / filename
        if candidate2.is_file():
            return candidate2
        # Direct glob search
        for found in base.glob(f"**/{args.bucket}/{args.version_id}/{filename}"):
            if found.is_file():
                return found

    print(
        f"Error: Could not locate chunk file for bucket='{args.bucket}', "
        f"version_id='{args.version_id}', chunk={args.chunk_index}",
        file=sys.stderr,
    )
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Inject bitrot or corruption into a Vault chunk file.")
    parser.add_argument("--target-file", type=str, help="Direct path to the chunk/fragment .dat file")
    parser.add_argument("--data-dir", type=str, help="Root data directory of storage node")
    parser.add_argument("--node-id", type=str, default="", help="Node ID (e.g. storage-1, store-1)")
    parser.add_argument("--bucket", type=str, default="test-bucket", help="Bucket name")
    parser.add_argument("--version-id", type=str, default="", help="Object version UUID")
    parser.add_argument("--chunk-index", type=int, default=0, help="Chunk index")
    parser.add_argument("--fragment-index", type=int, default=None, help="Fragment index (for EC archive)")
    parser.add_argument("--offset", type=int, default=0, help="Byte offset to corrupt")
    parser.add_argument("--mode", choices=["flip", "overwrite", "truncate"], default="overwrite", help="Corruption mode")
    parser.add_argument("--pattern", type=str, default="CORRUPT_BITROT_FAULT_INJECTION", help="Pattern to write")

    args = parser.parse_args()

    target_path = find_target_file(args)
    original_size = target_path.stat().st_size
    original_sha = calculate_sha256(target_path)

    print(f"Target chunk file: {target_path}")
    print(f"Original size:    {original_size} bytes")
    print(f"Original SHA-256: {original_sha}")

    if args.mode == "flip":
        with open(target_path, "r+b") as f:
            f.seek(args.offset)
            byte = f.read(1)
            f.seek(args.offset)
            flipped = bytes([byte[0] ^ 0xFF]) if byte else b"\xFF"
            f.write(flipped)
        print(f"Injected bit flip at offset {args.offset}.")
    elif args.mode == "overwrite":
        corrupt_bytes = args.pattern.encode("utf-8")
        with open(target_path, "r+b") as f:
            f.seek(args.offset)
            f.write(corrupt_bytes)
        print(f"Overwrote {len(corrupt_bytes)} bytes at offset {args.offset} with '{args.pattern}'.")
    elif args.mode == "truncate":
        new_len = max(0, original_size // 2)
        with open(target_path, "r+b") as f:
            f.truncate(new_len)
        print(f"Truncated file from {original_size} to {new_len} bytes.")

    new_sha = calculate_sha256(target_path)
    new_size = target_path.stat().st_size

    print(f"New size:         {new_size} bytes")
    print(f"New SHA-256:      {new_sha}")
    assert original_sha != new_sha, "Corruption failed: hash did not change!"
    print("SUCCESS: File successfully corrupted.")


if __name__ == "__main__":
    main()
