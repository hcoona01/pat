"""Reed-Solomon erasure coding codec using zfec (4 data fragments + 2 parity fragments)."""

import hashlib
import math
from typing import Dict, List, Optional, Tuple
import zfec

from vault_core.hashing import sha256_bytes


class ErasureCodingError(Exception):
    """Base error for erasure coding operations."""
    pass


class InsufficientFragmentsError(ErasureCodingError):
    """Raised when fewer than K fragments are available for reconstruction."""
    pass


class CorruptedFragmentError(ErasureCodingError):
    """Raised when a fragment fails cryptographic checksum verification."""
    pass


class ErasureCodec:
    """
    Manages Reed-Solomon erasure coding (default K=4 data, M=2 parity -> 6 total fragments).
    Guarantees:
    - Encodes data into K+M fragments with individual SHA-256 digests.
    - Decodes and reconstructs original payload from ANY K valid fragments.
    - Re-encodes missing fragments from surviving fragments for background repair.
    - Rejects reconstruction if fewer than K valid fragments survive.
    """

    def __init__(self, data_fragments: int = 4, parity_fragments: int = 2) -> None:
        self.k = data_fragments
        self.m = parity_fragments
        self.total = self.k + self.m
        self.n = self.total
        self._encoder = zfec.Encoder(self.k, self.total)
        self._decoder = zfec.Decoder(self.k, self.total)

    def encode(self, data: bytes) -> List[Tuple[int, bytes, str]]:
        """
        Encode raw bytes into K+M fragments.
        Returns a list of tuples: (fragment_index, fragment_bytes, sha256_hash).
        """
        original_size = len(data)

        # Pad data so its length is evenly divisible by K
        pad_len = (self.k - (original_size % self.k)) % self.k
        padded_data = data + (b"\x00" * pad_len) if pad_len else data

        block_size = len(padded_data) // self.k
        input_blocks = [
            padded_data[i * block_size : (i + 1) * block_size]
            for i in range(self.k)
        ]

        encoded_blocks = self._encoder.encode(input_blocks)

        results: List[Tuple[int, bytes, str]] = []
        for idx, block in enumerate(encoded_blocks):
            sha = sha256_bytes(block)
            results.append((idx, block, sha))

        return results

    def decode(self, fragments: Dict[int, bytes], original_size: int) -> bytes:
        """
        Reconstruct original data from any K valid fragments.
        Args:
            fragments: Dict mapping fragment_index (0..K+M-1) to fragment bytes.
            original_size: Exact original byte count before encoding padding.
        Returns:
            Original reconstructed payload bytes.
        """
        if len(fragments) < self.k:
            raise InsufficientFragmentsError(
                f"Cannot reconstruct: required {self.k} valid fragments, only received {len(fragments)}"
            )

        # Select any K available fragments and their block numbers
        selected_indexes = sorted(list(fragments.keys()))[: self.k]
        selected_blocks = [fragments[idx] for idx in selected_indexes]

        decoded_blocks = self._decoder.decode(selected_blocks, selected_indexes)
        concatenated = b"".join(decoded_blocks)

        # Truncate any padding to return exact original payload
        return concatenated[:original_size]

    def reconstruct_fragment(
        self,
        surviving_fragments: Dict[int, bytes],
        original_size: int,
        target_fragment_index: int,
    ) -> Tuple[bytes, str]:
        """
        Reconstruct a specific missing or corrupted fragment from surviving fragments.
        Returns (reconstructed_fragment_bytes, sha256_hash).
        """
        # 1. Reconstruct full data from surviving fragments
        full_data = self.decode(surviving_fragments, original_size)

        # 2. Re-encode full data
        all_fragments = self.encode(full_data)

        # 3. Extract the target fragment
        for idx, frag_bytes, sha in all_fragments:
            if idx == target_fragment_index:
                return frag_bytes, sha

        raise ErasureCodingError(f"Target fragment index {target_fragment_index} out of range")

    @property
    def storage_amplification(self) -> float:
        """Calculate nominal storage amplification: (K + M) / K."""
        return float(self.total) / float(self.k)
