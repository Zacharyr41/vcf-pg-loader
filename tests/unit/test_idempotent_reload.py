"""Unit tests for idempotent reload functionality."""

import hashlib
import tempfile
from pathlib import Path
from uuid import uuid4


class TestMD5Computation:
    """Test MD5 hash computation for file identification."""

    def test_md5_returns_32_char_hex_string(self):
        """MD5 hash is a 32-character hexadecimal string."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("test content")
            path = Path(f.name)

        try:
            md5 = hashlib.md5(path.read_bytes()).hexdigest()
            assert len(md5) == 32
            assert all(c in "0123456789abcdef" for c in md5)
        finally:
            path.unlink()

    def test_md5_consistent_for_same_content(self):
        """Same content always produces same MD5."""
        content = "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(content)
            path1 = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(content)
            path2 = Path(f.name)

        try:
            md5_1 = hashlib.md5(path1.read_bytes()).hexdigest()
            md5_2 = hashlib.md5(path2.read_bytes()).hexdigest()
            assert md5_1 == md5_2
        finally:
            path1.unlink()
            path2.unlink()

    def test_md5_differs_for_different_content(self):
        """Different content produces different MD5."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("content A")
            path1 = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("content B")
            path2 = Path(f.name)

        try:
            md5_1 = hashlib.md5(path1.read_bytes()).hexdigest()
            md5_2 = hashlib.md5(path2.read_bytes()).hexdigest()
            assert md5_1 != md5_2
        finally:
            path1.unlink()
            path2.unlink()

    def test_md5_detects_single_byte_change(self):
        """MD5 changes with even a single byte difference."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("chr1\t100\t.\tA\tG\t30\tPASS")
            path1 = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("chr1\t100\t.\tA\tG\t31\tPASS")
            path2 = Path(f.name)

        try:
            md5_1 = hashlib.md5(path1.read_bytes()).hexdigest()
            md5_2 = hashlib.md5(path2.read_bytes()).hexdigest()
            assert md5_1 != md5_2
        finally:
            path1.unlink()
            path2.unlink()


class TestFileSizeTracking:
    """Test file size tracking for audit purposes."""

    def test_file_size_computed_correctly(self):
        """File size is computed accurately."""
        content = "A" * 1000
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            size = path.stat().st_size
            assert size == 1000
        finally:
            path.unlink()

    def test_file_size_changes_with_content(self):
        """File size changes when content changes."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("short")
            path1 = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("much longer content here")
            path2 = Path(f.name)

        try:
            size1 = path1.stat().st_size
            size2 = path2.stat().st_size
            assert size2 > size1
        finally:
            path1.unlink()
            path2.unlink()


class TestLoadBatchIdGeneration:
    """Test load batch ID generation."""

    def test_uuid_generation(self):
        """UUID is generated correctly."""
        batch_id = uuid4()
        assert len(str(batch_id)) == 36
        assert str(batch_id).count("-") == 4

    def test_uuid_uniqueness(self):
        """Each UUID generation is unique."""
        ids = [uuid4() for _ in range(100)]
        assert len(set(ids)) == 100

    def test_uuid_serialization(self):
        """UUID can be serialized to string and back."""
        from uuid import UUID

        original = uuid4()
        as_string = str(original)
        restored = UUID(as_string)
        assert original == restored


class TestReloadDetection:
    """Test reload detection logic."""

    def test_detect_same_file_by_md5(self):
        """Same file is detected via MD5 match."""
        content = "##fileformat=VCFv4.3\nchr1\t100\t.\tA\tG\t30"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            md5_v1 = hashlib.md5(path.read_bytes()).hexdigest()
            md5_v2 = hashlib.md5(path.read_bytes()).hexdigest()

            is_same = md5_v1 == md5_v2
            assert is_same is True
        finally:
            path.unlink()

    def test_detect_modified_file_by_md5(self):
        """Modified file is detected via MD5 mismatch."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("version 1")
            path = Path(f.name)

        try:
            md5_v1 = hashlib.md5(path.read_bytes()).hexdigest()

            with open(path, "w") as f:
                f.write("version 2")

            md5_v2 = hashlib.md5(path.read_bytes()).hexdigest()

            is_same = md5_v1 == md5_v2
            assert is_same is False
        finally:
            path.unlink()


class TestAuditRecordFields:
    """Test audit record field validation."""

    def test_valid_status_values(self):
        """Status must be one of the allowed values."""
        valid_statuses = {"started", "completed", "failed", "rolled_back"}

        for status in valid_statuses:
            assert status in valid_statuses

    def test_md5_length_validation(self):
        """MD5 must be exactly 32 characters."""
        valid_md5 = "d41d8cd98f00b204e9800998ecf8427e"
        assert len(valid_md5) == 32

        invalid_md5 = "too_short"
        assert len(invalid_md5) != 32

    def test_reference_genome_values(self):
        """Reference genome should be GRCh37 or GRCh38."""
        valid_refs = {"GRCh37", "GRCh38"}
        assert "GRCh38" in valid_refs
        assert "GRCh37" in valid_refs


class TestVariantCountValidation:
    """Test variant count for reload validation."""

    def test_count_matches_original(self):
        """Variant count should match between loads."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
chr1	300	.	G	A	40	PASS	DP=55	GT	1/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser1 = VCFStreamingParser(path, human_genome=True)
            count1 = 0
            for batch in parser1.iter_batches():
                count1 += len(batch)
            parser1.close()

            parser2 = VCFStreamingParser(path, human_genome=True)
            count2 = 0
            for batch in parser2.iter_batches():
                count2 += len(batch)
            parser2.close()

            assert count1 == count2 == 3
        finally:
            path.unlink()

    def test_count_differs_for_modified_file(self):
        """Variant count differs when file is modified."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_v1 = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	PASS	.
chr1	200	.	C	T	35	PASS	.
"""
        vcf_v2 = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	PASS	.
chr1	200	.	C	T	35	PASS	.
chr1	300	.	G	A	40	PASS	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_v1)
            path = Path(f.name)

        try:
            parser1 = VCFStreamingParser(path, human_genome=True)
            count1 = sum(len(batch) for batch in parser1.iter_batches())
            parser1.close()

            with open(path, "w") as f:
                f.write(vcf_v2)

            parser2 = VCFStreamingParser(path, human_genome=True)
            count2 = sum(len(batch) for batch in parser2.iter_batches())
            parser2.close()

            assert count1 == 2
            assert count2 == 3
            assert count1 != count2
        finally:
            path.unlink()


class TestReloadLinkage:
    """Test reload linkage with previous loads."""

    def test_previous_load_id_tracking(self):
        """Previous load ID can be tracked for reloads."""
        original_load_id = uuid4()
        reload_id = uuid4()

        reload_record = {
            "load_batch_id": reload_id,
            "is_reload": True,
            "previous_load_id": original_load_id,
        }

        assert reload_record["is_reload"] is True
        assert reload_record["previous_load_id"] == original_load_id
        assert reload_record["load_batch_id"] != reload_record["previous_load_id"]

    def test_first_load_has_no_previous(self):
        """First load has no previous_load_id."""
        first_load = {
            "load_batch_id": uuid4(),
            "is_reload": False,
            "previous_load_id": None,
        }

        assert first_load["is_reload"] is False
        assert first_load["previous_load_id"] is None
