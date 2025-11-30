"""Tests for variant normalization integration in the loading pipeline.

These tests verify that normalization is correctly integrated into the
VCF parsing and loading workflow. They should FAIL until normalization
is wired into the pipeline.
"""

from pathlib import Path

from vcf_pg_loader.loader import LoadConfig
from vcf_pg_loader.models import VariantRecord
from vcf_pg_loader.vcf_parser import VariantParser, VCFHeaderParser, VCFStreamingParser

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestNormalizationConfig:
    """Tests for normalization configuration."""

    def test_load_config_normalize_default_true(self):
        """LoadConfig.normalize should default to True."""
        config = LoadConfig()
        assert config.normalize is True

    def test_load_config_normalize_can_be_disabled(self):
        """LoadConfig.normalize should be configurable."""
        config = LoadConfig(normalize=False)
        assert config.normalize is False


class TestVariantParserNormalization:
    """Tests for normalization in VariantParser."""

    def test_variant_parser_accepts_normalize_flag(self):
        """VariantParser should accept a normalize parameter."""
        header_parser = VCFHeaderParser()
        parser = VariantParser(header_parser, normalize=True)
        assert parser.normalize is True

        parser_no_norm = VariantParser(header_parser, normalize=False)
        assert parser_no_norm.normalize is False

    def test_variant_parser_normalize_defaults_to_false(self):
        """VariantParser.normalize should default to False for backwards compat."""
        header_parser = VCFHeaderParser()
        parser = VariantParser(header_parser)
        assert parser.normalize is False


class TestStreamingParserNormalization:
    """Tests for normalization in VCFStreamingParser."""

    def test_streaming_parser_accepts_normalize_flag(self):
        """VCFStreamingParser should accept a normalize parameter."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        parser = VCFStreamingParser(vcf_path, normalize=True)
        assert parser.normalize is True
        parser.close()

    def test_streaming_parser_normalize_defaults_to_false(self):
        """VCFStreamingParser.normalize should default to False."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        parser = VCFStreamingParser(vcf_path)
        assert parser.normalize is False
        parser.close()


class TestNormalizationApplied:
    """Tests that verify normalization is actually applied during parsing."""

    def test_unnormalized_variant_gets_normalized(self):
        """Variants should be normalized when normalize=True.

        This test uses a known un-normalized indel pattern:
        ATG -> AG should normalize to AT -> A (right-trim G)
        """
        vcf_content = '''##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tATG\tAG\t30\tPASS\tDP=50
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, normalize=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            records = batches[0]
            assert len(records) == 1

            record = records[0]
            assert record.ref == "AT", f"REF should be normalized to 'AT', got '{record.ref}'"
            assert record.alt == "A", f"ALT should be normalized to 'A', got '{record.alt}'"
        finally:
            vcf_path.unlink()

    def test_normalize_false_preserves_original(self):
        """Variants should NOT be normalized when normalize=False."""
        vcf_content = '''##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tATG\tAG\t30\tPASS\tDP=50
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, normalize=False)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            records = batches[0]
            assert len(records) == 1

            record = records[0]
            assert record.ref == "ATG", f"REF should be preserved as 'ATG', got '{record.ref}'"
            assert record.alt == "AG", f"ALT should be preserved as 'AG', got '{record.alt}'"
        finally:
            vcf_path.unlink()

    def test_insertion_normalization(self):
        """Insertions should be left-aligned when normalized.

        A -> AA at position 100 should normalize (if context allows).
        Without reference genome, simple right-trim should still work.
        """
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tAG\tATG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, normalize=True)
            batches = list(parser.iter_batches())
            parser.close()

            record = batches[0][0]
            assert record.ref == "A", f"REF should be 'A', got '{record.ref}'"
            assert record.alt == "AT", f"ALT should be 'AT', got '{record.alt}'"
        finally:
            vcf_path.unlink()

    def test_snp_unchanged_by_normalization(self):
        """SNPs should be unchanged by normalization."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, normalize=True)
            batches = list(parser.iter_batches())
            parser.close()

            record = batches[0][0]
            assert record.pos == 100
            assert record.ref == "A"
            assert record.alt == "G"
        finally:
            vcf_path.unlink()

    def test_multiallelic_normalization(self):
        """Multi-allelic variants should have each ALT normalized independently."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tATG\tAG,ATCG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, normalize=True)
            batches = list(parser.iter_batches())
            parser.close()

            records = batches[0]
            assert len(records) == 2

            record1 = records[0]
            assert record1.ref == "AT", f"First ALT: REF should be 'AT', got '{record1.ref}'"
            assert record1.alt == "A", f"First ALT: ALT should be 'A', got '{record1.alt}'"

            record2 = records[1]
            assert record2.ref == "T", f"Second ALT: REF should be 'T', got '{record2.ref}'"
            assert record2.alt == "TC", f"Second ALT: ALT should be 'TC', got '{record2.alt}'"
        finally:
            vcf_path.unlink()


class TestVariantRecordNormalized:
    """Tests for tracking normalization state on VariantRecord."""

    def test_variant_record_has_normalized_flag(self):
        """VariantRecord should have a normalized attribute."""
        record = VariantRecord(
            chrom="chr1",
            pos=100,
            ref="A",
            alt="G",
            qual=30.0,
            filter=[],
            rs_id=None,
            info={},
            normalized=True
        )
        assert record.normalized is True

    def test_variant_record_normalized_defaults_false(self):
        """VariantRecord.normalized should default to False."""
        record = VariantRecord(
            chrom="chr1",
            pos=100,
            ref="A",
            alt="G",
            qual=30.0,
            filter=[],
            rs_id=None,
            info={}
        )
        assert record.normalized is False

    def test_variant_record_has_original_pos(self):
        """VariantRecord should store original position before normalization."""
        record = VariantRecord(
            chrom="chr1",
            pos=99,
            ref="AT",
            alt="A",
            qual=30.0,
            filter=[],
            rs_id=None,
            info={},
            original_pos=100,
            original_ref="ATG",
            original_alt="AG",
            normalized=True
        )
        assert record.original_pos == 100
        assert record.original_ref == "ATG"
        assert record.original_alt == "AG"
