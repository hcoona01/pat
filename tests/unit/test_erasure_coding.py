"""Unit tests for Reed-Solomon Erasure Coding (4 data + 2 parity fragments).

Validates:
- Encode and decode correctness across varied payload sizes
- Reconstruction with 1 missing fragment (any index)
- Reconstruction with 2 missing fragments (data-data, data-parity, parity-parity)
- Detection and isolation of corrupt fragments via SHA-256
- Reconstruction failure when fewer than K=4 valid fragments remain
- Reconstruction of individual missing fragments for repair workers
"""

import hashlib
import os
import pytest

from vault_core.erasure_coding import (
    ErasureCodec,
    InsufficientFragmentsError,
)


@pytest.fixture
def codec() -> ErasureCodec:
    """Standard 4+2 Erasure Codec fixture."""
    return ErasureCodec(data_fragments=4, parity_fragments=2)


# =============================================================================
# 1. ENCODE / DECODE CORRECTNESS
# =============================================================================

@pytest.mark.parametrize(
    "payload_size",
    [
        0,          # 0-byte empty object
        1,          # 1 byte
        3,          # Non-multiple of 4
        4,          # Exact multiple of 4
        15,         # Odd length
        16,         # Exact multiple of 4
        1024,       # 1 KiB
        65537,      # 64 KiB + 1 byte
        1048576,    # 1 MiB
    ],
)
def test_erasure_coding_encode_decode_correctness(codec: ErasureCodec, payload_size: int):
    """Verify that encoding and decoding produces bit-for-bit identical data for all sizes."""
    if payload_size == 0:
        data = b""
    else:
        data = os.urandom(payload_size)

    fragments = codec.encode(data)

    # Must produce exactly N = K + M = 6 fragments
    assert len(fragments) == 6
    assert codec.n == 6
    assert codec.k == 4
    assert codec.m == 2

    frag_dict = {}
    for idx, frag_bytes, frag_sha in fragments:
        # Fragment index must be within 0..5
        assert 0 <= idx < 6
        # Fragment SHA-256 must match the bytes
        assert hashlib.sha256(frag_bytes).hexdigest() == frag_sha
        frag_dict[idx] = frag_bytes

    # Decode using all 6 fragments
    decoded = codec.decode(frag_dict, len(data))
    assert decoded == data
    assert hashlib.sha256(decoded).hexdigest() == hashlib.sha256(data).hexdigest()


# =============================================================================
# 2. RECONSTRUCTION WITH ONE MISSING FRAGMENT (5 SURVIVING)
# =============================================================================

@pytest.mark.parametrize("missing_idx", [0, 1, 2, 3, 4, 5])
def test_reconstruction_with_one_missing_fragment(codec: ErasureCodec, missing_idx: int):
    """Verify object can be fully reconstructed when any single fragment is lost."""
    data = b"Erasure coding fault tolerance test: single missing fragment test payload " * 100
    fragments = codec.encode(data)

    # Build dictionary missing exactly fragment `missing_idx`
    surviving = {idx: f_bytes for idx, f_bytes, _ in fragments if idx != missing_idx}
    assert len(surviving) == 5

    decoded = codec.decode(surviving, len(data))
    assert decoded == data
    assert hashlib.sha256(decoded).hexdigest() == hashlib.sha256(data).hexdigest()


# =============================================================================
# 3. RECONSTRUCTION WITH TWO MISSING FRAGMENTS (4 SURVIVING)
# =============================================================================

@pytest.mark.parametrize(
    "missing_pair",
    [
        (0, 1),  # Both data fragments missing
        (2, 3),  # Both data fragments missing
        (0, 4),  # One data (0) and one parity (4) missing
        (3, 5),  # One data (3) and one parity (5) missing
        (4, 5),  # Both parity fragments missing (survive on pure data fragments)
        (1, 4),  # Arbitrary pair
    ],
)
def test_reconstruction_with_two_missing_fragments(codec: ErasureCodec, missing_pair: tuple):
    """Verify object can be reconstructed from any 4 valid fragments (tolerance = 2 failures)."""
    data = b"Vault archive erasure coding test: survive two node failures concurrently " * 150
    fragments = codec.encode(data)

    surviving = {idx: f_bytes for idx, f_bytes, _ in fragments if idx not in missing_pair}
    assert len(surviving) == 4

    decoded = codec.decode(surviving, len(data))
    assert decoded == data
    assert hashlib.sha256(decoded).hexdigest() == hashlib.sha256(data).hexdigest()


# =============================================================================
# 4. FAILED READ WITH FEWER THAN FOUR VALID FRAGMENTS (3 SURVIVING)
# =============================================================================

@pytest.mark.parametrize("surviving_count", [0, 1, 2, 3])
def test_failed_read_with_fewer_than_four_valid_fragments(codec: ErasureCodec, surviving_count: int):
    """Verify that attempting to reconstruct with < 4 fragments strictly fails with InsufficientFragmentsError."""
    data = b"Testing strict failure when quorum falls below K=4 fragments."
    fragments = codec.encode(data)

    surviving = {idx: f_bytes for idx, f_bytes, _ in fragments[:surviving_count]}
    assert len(surviving) == surviving_count

    with pytest.raises(InsufficientFragmentsError) as exc_info:
        codec.decode(surviving, len(data))

    assert "required 4 valid fragments" in str(exc_info.value)


# =============================================================================
# 5. RECONSTRUCT INDIVIDUAL MISSING FRAGMENT FOR BACKGROUND REPAIR
# =============================================================================

@pytest.mark.parametrize("target_idx", [0, 1, 2, 3, 4, 5])
def test_reconstruct_single_fragment_for_repair(codec: ErasureCodec, target_idx: int):
    """Verify that repair worker can regenerate the exact bit-for-bit missing fragment from surviving 4 or 5 fragments."""
    data = b"Regeneration of individual damaged or lost fragment bit-for-bit identical!" * 80
    fragments = codec.encode(data)

    original_target_bytes = fragments[target_idx][1]
    original_target_sha = fragments[target_idx][2]

    # Provide only 4 surviving fragments (excluding target_idx and one other)
    other_excluded = (target_idx + 1) % 6
    surviving_4 = {
        idx: f_bytes
        for idx, f_bytes, _ in fragments
        if idx not in (target_idx, other_excluded)
    }
    assert len(surviving_4) == 4

    regen_bytes, regen_sha = codec.reconstruct_fragment(
        surviving_fragments=surviving_4,
        original_size=len(data),
        target_fragment_index=target_idx,
    )

    assert regen_bytes == original_target_bytes
    assert regen_sha == original_target_sha


# =============================================================================
# 6. CORRUPT FRAGMENT DETECTION AND ISOLATION
# =============================================================================

def test_corrupt_fragment_detected_and_isolated(codec: ErasureCodec):
    """Verify that corrupt fragment is detected by SHA-256 and discarded, allowing surviving 5 to succeed."""
    data = b"Detecting bit rot corruption in erasure coded fragment and repairing!" * 50
    fragments = codec.encode(data)

    # Intentionally corrupt fragment 2
    corrupted_frag_2 = bytearray(fragments[2][1])
    corrupted_frag_2[0] ^= 0xFF  # Flip bits
    corrupted_frag_2 = bytes(corrupted_frag_2)

    # Check SHA-256 integrity check
    assert hashlib.sha256(corrupted_frag_2).hexdigest() != fragments[2][2]

    # Filter out corrupt fragment:
    filtered_surviving = {}
    for idx, f_bytes, exp_sha in fragments:
        candidate_bytes = corrupted_frag_2 if idx == 2 else f_bytes
        if hashlib.sha256(candidate_bytes).hexdigest() == exp_sha:
            filtered_surviving[idx] = candidate_bytes

    # Should have 5 valid fragments
    assert len(filtered_surviving) == 5
    assert 2 not in filtered_surviving

    decoded = codec.decode(filtered_surviving, len(data))
    assert decoded == data
