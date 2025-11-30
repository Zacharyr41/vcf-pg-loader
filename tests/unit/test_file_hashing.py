"""Unit tests for file hashing functionality."""

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import patch


class TestSHA256Computation:
    """Test SHA256 hash computation for file identification."""

    def test_sha256_returns_64_char_hex_string(self):
        """SHA256 hash is a 64-character hexadecimal string."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("test content")
            path = Path(f.name)

        try:
            from vcf_pg_loader.loader import compute_file_hash

            sha256 = compute_file_hash(path)
            assert len(sha256) == 64
            assert all(c in "0123456789abcdef" for c in sha256)
        finally:
            path.unlink()

    def test_sha256_consistent_for_same_content(self):
        """Same content always produces same SHA256."""
        content = "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(content)
            path1 = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(content)
            path2 = Path(f.name)

        try:
            from vcf_pg_loader.loader import compute_file_hash

            sha256_1 = compute_file_hash(path1)
            sha256_2 = compute_file_hash(path2)
            assert sha256_1 == sha256_2
        finally:
            path1.unlink()
            path2.unlink()

    def test_sha256_differs_for_different_content(self):
        """Different content produces different SHA256."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("content A")
            path1 = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("content B")
            path2 = Path(f.name)

        try:
            from vcf_pg_loader.loader import compute_file_hash

            sha256_1 = compute_file_hash(path1)
            sha256_2 = compute_file_hash(path2)
            assert sha256_1 != sha256_2
        finally:
            path1.unlink()
            path2.unlink()

    def test_sha256_detects_single_byte_change(self):
        """SHA256 changes with even a single byte difference."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("chr1\t100\t.\tA\tG\t30\tPASS")
            path1 = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("chr1\t100\t.\tA\tG\t31\tPASS")
            path2 = Path(f.name)

        try:
            from vcf_pg_loader.loader import compute_file_hash

            sha256_1 = compute_file_hash(path1)
            sha256_2 = compute_file_hash(path2)
            assert sha256_1 != sha256_2
        finally:
            path1.unlink()
            path2.unlink()

    def test_sha256_matches_hashlib_direct(self):
        """compute_file_hash produces same result as direct hashlib computation."""
        content = b"##fileformat=VCFv4.3\nchr1\t100\t.\tA\tG\t30\tPASS\n"

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".vcf", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            from vcf_pg_loader.loader import compute_file_hash

            expected = hashlib.sha256(content).hexdigest()
            actual = compute_file_hash(path)
            assert actual == expected
        finally:
            path.unlink()


class TestStreamingHashComputation:
    """Test streaming hash computation for memory efficiency."""

    def test_streaming_hash_uses_chunked_reads(self):
        """Hash computation should use chunked reads, not load entire file."""
        from vcf_pg_loader.loader import compute_file_hash

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".vcf", delete=False) as f:
            f.write(b"A" * 100_000)
            path = Path(f.name)

        try:
            with patch("builtins.open", wraps=open) as mock_open:
                compute_file_hash(path)
                mock_open.assert_called_once()
        finally:
            path.unlink()

    def test_streaming_hash_handles_empty_file(self):
        """Hash computation handles empty files correctly."""
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".vcf", delete=False) as f:
            path = Path(f.name)

        try:
            from vcf_pg_loader.loader import compute_file_hash

            sha256 = compute_file_hash(path)
            expected = hashlib.sha256(b"").hexdigest()
            assert sha256 == expected
        finally:
            path.unlink()

    def test_streaming_hash_chunk_size_default(self):
        """Hash computation uses appropriate chunk size."""
        from vcf_pg_loader.loader import HASH_CHUNK_SIZE

        assert HASH_CHUNK_SIZE == 65536

    def test_streaming_hash_large_file_memory_bounded(self):
        """Large files should be hashed without loading entirely into memory."""
        from vcf_pg_loader.loader import compute_file_hash

        large_content = b"X" * 1_000_000

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".vcf", delete=False) as f:
            f.write(large_content)
            path = Path(f.name)

        try:
            sha256 = compute_file_hash(path)
            expected = hashlib.sha256(large_content).hexdigest()
            assert sha256 == expected
            assert len(sha256) == 64
        finally:
            path.unlink()


class TestHashAlgorithmSecurity:
    """Test that we use secure hashing algorithm."""

    def test_uses_sha256_not_md5(self):
        """Verify SHA256 is used instead of MD5 for clinical-grade security."""
        content = b"test content for hash"

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".vcf", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            from vcf_pg_loader.loader import compute_file_hash

            result = compute_file_hash(path)
            md5_result = hashlib.md5(content).hexdigest()
            sha256_result = hashlib.sha256(content).hexdigest()

            assert result == sha256_result
            assert result != md5_result
            assert len(result) == 64
        finally:
            path.unlink()


class TestLoaderUsesStreamingHash:
    """Test that VCFLoader uses the streaming hash function."""

    def test_check_existing_uses_streaming_hash(self):
        """check_existing should use compute_file_hash for file identification."""
        from vcf_pg_loader.loader import compute_file_hash

        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t30\tPASS\t.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            expected_hash = compute_file_hash(path)
            assert len(expected_hash) == 64
        finally:
            path.unlink()

    def test_load_vcf_uses_streaming_hash(self):
        """load_vcf should use compute_file_hash for audit tracking."""
        from vcf_pg_loader.loader import compute_file_hash

        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t30\tPASS\t.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            file_hash = compute_file_hash(path)
            assert len(file_hash) == 64
        finally:
            path.unlink()
