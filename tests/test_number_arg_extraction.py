"""Tests for Number=A/R/G field extraction during multi-allelic decomposition.

When a multi-allelic variant like:
    REF=C, ALT=[G,T], AF=[0.3, 0.2]

is decomposed into two records, each record should get its own AF value:
    Record 1: ALT=G, AF=0.3
    Record 2: ALT=T, AF=0.2

Currently, VariantParser stores the entire INFO dict without indexing
into Number=A fields. These tests expose that gap.
"""

from pathlib import Path

from cyvcf2 import VCF

from vcf_pg_loader.vcf_parser import VariantParser, VCFHeaderParser

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestNumberAExtraction:
    """Tests for Number=A field extraction (one value per ALT allele)."""

    def test_multiallelic_ac_extraction(self):
        """Each decomposed record should get its own AC value."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        for variant in vcf:
            ac_values = variant.INFO.get("AC")
            if ac_values is None:
                continue

            records = variant_parser.parse_variant(variant, [])

            for i, record in enumerate(records):
                if isinstance(ac_values, (list, tuple)) and len(ac_values) > 1:
                    assert record.info.get("AC") == ac_values[i], (
                        f"Record {i} should have AC={ac_values[i]}, "
                        f"got {record.info.get('AC')}"
                    )

    def test_multiallelic_af_extraction(self):
        """Each decomposed record should get its own AF value."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        for variant in vcf:
            af_values = variant.INFO.get("AF")
            if af_values is None:
                continue

            records = variant_parser.parse_variant(variant, [])

            for i, record in enumerate(records):
                if isinstance(af_values, (list, tuple)) and len(af_values) > 1:
                    assert record.info.get("AF") == af_values[i]


class TestNumberRExtraction:
    """Tests for Number=R field extraction (one value per allele including REF)."""

    def test_multiallelic_mbq_extraction(self):
        """Number=R INFO fields like MBQ should be properly indexed for multi-allelics."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"
        vcf = VCF(str(vcf_path))

        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        mbq_meta = header_parser.get_info_field("MBQ")
        assert mbq_meta is not None
        assert mbq_meta["Number"] == "R"

        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        multiallelic_tested = 0
        for variant in vcf:
            n_alts = len(variant.ALT)
            if n_alts <= 1:
                continue

            mbq_values = variant.INFO.get("MBQ")
            if mbq_values is None:
                continue

            records = variant_parser.parse_variant(variant, [])
            multiallelic_tested += 1

            for record in records:
                mbq_in_record = record.info.get("MBQ")
                if mbq_in_record is not None:
                    assert isinstance(mbq_in_record, (list, tuple)), (
                        "MBQ should be a list with [REF, ALT] values"
                    )
            if multiallelic_tested >= 3:
                break

        assert multiallelic_tested >= 1, "No multi-allelic variants with MBQ found"


class TestNumberGExtraction:
    """Tests for Number=G field extraction (genotype likelihoods).

    Note: Number=G INFO fields are rare in practice. Most PL fields are FORMAT,
    not INFO. This test verifies that the parser can handle multi-allelic
    variants generally, which implicitly tests the decomposition logic.
    """

    def test_multiallelic_decomposition_produces_correct_count(self):
        """Multi-allelic variants should decompose to correct number of records."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"
        vcf = VCF(str(vcf_path))

        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        multiallelic_tested = 0
        for variant in vcf:
            n_alts = len(variant.ALT)
            if n_alts <= 1:
                continue

            records = variant_parser.parse_variant(variant, [])
            assert len(records) == n_alts, (
                f"Multi-allelic with {n_alts} ALTs should produce {n_alts} records, "
                f"got {len(records)}"
            )
            multiallelic_tested += 1
            if multiallelic_tested >= 5:
                break

        assert multiallelic_tested >= 1, "No multi-allelic variants found"


class TestScalarFieldsUnchanged:
    """Tests that Number=1 fields remain unchanged during decomposition."""

    def test_dp_unchanged(self):
        """Number=1 fields like DP should be the same for all decomposed records."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        for variant in vcf:
            dp_value = variant.INFO.get("DP")
            if dp_value is None:
                continue

            records = variant_parser.parse_variant(variant, [])

            for record in records:
                assert record.info.get("DP") == dp_value


class TestVariantParserRequiresHeader:
    """Tests that VariantParser requires header metadata for proper extraction."""

    def test_parser_uses_header_metadata(self):
        """VariantParser should use header metadata to determine field Number."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        ac_meta = header_parser.get_info_field("AC")
        assert ac_meta["Number"] == "A"

        variant_parser = VariantParser(header_parser)
        assert variant_parser.header_parser is header_parser
