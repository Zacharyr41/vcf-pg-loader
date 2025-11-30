"""Tests for streaming VCF parser with batching.

The streaming parser should:
1. Iterate through a VCF file and yield batches of VariantRecords
2. Support configurable batch sizes
3. Handle multi-allelic decomposition within batches
4. Provide progress/count information
"""

from pathlib import Path

from vcf_pg_loader.models import VariantRecord
from vcf_pg_loader.vcf_parser import VCFHeaderParser, VCFStreamingParser

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestVCFStreamingParser:
    """Tests for the streaming VCF parser."""

    def test_streaming_parser_exists(self):
        """VCFStreamingParser class should exist."""
        assert VCFStreamingParser is not None

    def test_streaming_parser_init(self):
        """Streaming parser should accept a VCF path."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path)
        assert parser.vcf_path == vcf_path

    def test_streaming_parser_with_batch_size(self):
        """Streaming parser should accept a batch size."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path, batch_size=100)
        assert parser.batch_size == 100

    def test_streaming_parser_default_batch_size(self):
        """Streaming parser should have a sensible default batch size."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path)
        assert parser.batch_size == 10000

    def test_iterate_batches(self):
        """Should iterate through VCF yielding batches of VariantRecords."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path, batch_size=2)

        batches = list(parser.iter_batches())

        assert len(batches) >= 1
        for batch in batches:
            assert isinstance(batch, list)
            assert len(batch) <= 2
            for record in batch:
                assert isinstance(record, VariantRecord)

    def test_all_variants_yielded(self):
        """All variants should be yielded across batches."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"

        parser = VCFStreamingParser(vcf_path, batch_size=2)
        total_records = sum(len(batch) for batch in parser.iter_batches())

        assert total_records == 4
        assert parser.variant_count == 4

    def test_multiallelic_decomposition_in_batches(self):
        """Multi-allelic variants should be decomposed within batches."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        parser = VCFStreamingParser(vcf_path, batch_size=100)

        all_records = []
        for batch in parser.iter_batches():
            all_records.extend(batch)

        multiallelic_records = [r for r in all_records if r.pos == 2049437]
        assert len(multiallelic_records) == 7

    def test_header_parser_accessible(self):
        """Header parser should be accessible after initialization."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path)

        assert parser.header_parser is not None
        assert isinstance(parser.header_parser, VCFHeaderParser)
        assert "AC" in parser.header_parser._info_fields

    def test_samples_accessible(self):
        """Sample names should be accessible."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path)

        assert parser.samples == ["HG002", "HG003", "HG004"]

    def test_variant_count(self):
        """Should track total variant count after iteration."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path, batch_size=2)

        for _ in parser.iter_batches():
            pass

        assert parser.variant_count == 4

    def test_record_count_with_decomposition(self):
        """Should track total record count (after decomposition)."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        parser = VCFStreamingParser(vcf_path, batch_size=100)

        for _ in parser.iter_batches():
            pass

        assert parser.record_count == 8


class TestStreamingParserNumberHandling:
    """Tests that streaming parser properly handles Number=A/R/G fields."""

    def test_number_a_extraction_in_stream(self):
        """Number=A fields should be properly indexed in streamed records."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path)

        for batch in parser.iter_batches():
            for record in batch:
                ac_value = record.info.get("AC")
                if ac_value is not None:
                    assert not isinstance(ac_value, (list, tuple)), (
                        f"AC should be scalar after decomposition, got {ac_value}"
                    )

    def test_number_1_fields_preserved(self):
        """Number=1 fields should remain unchanged in streamed records."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        parser = VCFStreamingParser(vcf_path)

        for batch in parser.iter_batches():
            for record in batch:
                dp_value = record.info.get("DP")
                if dp_value is not None:
                    assert isinstance(dp_value, int)


class TestStreamingParserContextManager:
    """Tests for context manager support."""

    def test_context_manager(self):
        """Should support context manager protocol."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"

        with VCFStreamingParser(vcf_path) as parser:
            batches = list(parser.iter_batches())
            assert len(batches) >= 1

    def test_context_manager_closes_resources(self):
        """Context manager should close underlying VCF handle."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"

        with VCFStreamingParser(vcf_path) as parser:
            _ = list(parser.iter_batches())

        assert parser._vcf is None or parser._closed
