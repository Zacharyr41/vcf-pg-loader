"""Tests for cyvcf2-based streaming VCF parser."""

from pathlib import Path

from cyvcf2 import VCF

from vcf_pg_loader.models import VariantRecord
from vcf_pg_loader.vcf_parser import VariantParser, get_array_size

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestCyvcf2StreamingParser:
    """Integration tests for cyvcf2 streaming parser with real VCF files."""

    def test_stream_multiallelic_vcf(self):
        """Test streaming a VCF with multi-allelic variants."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        vcf = VCF(str(vcf_path))

        variants = list(vcf)
        assert len(variants) == 2

        simple_variant = variants[0]
        assert simple_variant.CHROM == "1"
        assert simple_variant.POS == 54712
        assert simple_variant.REF == "C"
        assert simple_variant.ALT == ["T"]

        multiallelic = variants[1]
        assert multiallelic.CHROM == "1"
        assert multiallelic.POS == 2049437
        assert multiallelic.REF == "C"
        assert len(multiallelic.ALT) == 7

    def test_parse_number_a_fields_malformed(self):
        """Test parsing Number=A INFO fields - this VCF has a known bug with mismatched AF count."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        vcf = VCF(str(vcf_path))

        variants = list(vcf)
        multiallelic = variants[1]

        af_values = multiallelic.INFO.get("AF")
        assert af_values is not None
        assert len(af_values) == 9
        assert len(multiallelic.ALT) == 7

    def test_stream_annotated_vcf(self):
        """Test streaming a VCF with BCSQ annotations."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        variants = list(vcf)
        assert len(variants) == 4

        for variant in variants:
            assert "BCSQ" in dict(variant.INFO)
            assert variant.FILTER is None or variant.FILTER == "PASS"

    def test_extract_genotype_data(self):
        """Test extracting genotype data from variants."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        assert len(vcf.samples) == 3
        assert vcf.samples == ["HG002", "HG003", "HG004"]

        variant = next(vcf)
        gt_types = variant.gt_types
        assert len(gt_types) == 3

        gt_depths = variant.gt_depths
        assert len(gt_depths) == 3
        assert all(d > 0 for d in gt_depths)

    def test_header_info_field_parsing_via_cyvcf2(self):
        """Test parsing INFO field definitions via cyvcf2's native API."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        ac_info = vcf.get_header_type("AC")
        assert ac_info["Number"] == "A"
        assert ac_info["Type"] == "Integer"

        af_info = vcf.get_header_type("AF")
        assert af_info["Number"] == "A"
        assert af_info["Type"] == "Float"

        dp_info = vcf.get_header_type("DP")
        assert dp_info["Number"] == "1"

        db_info = vcf.get_header_type("DB")
        assert db_info["Number"] == "0"
        assert db_info["Type"] == "Flag"

    def test_header_format_field_parsing_via_cyvcf2(self):
        """Test parsing FORMAT field definitions via cyvcf2's native API."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        gt_info = vcf.get_header_type("GT")
        assert gt_info["Number"] == "1"
        assert gt_info["Type"] == "String"

        ad_info = vcf.get_header_type("AD")
        assert ad_info["Type"] == "Integer"

        pl_info = vcf.get_header_type("PL")
        assert pl_info["Number"] == "G"

    def test_variant_parser_integration(self):
        """Test VariantParser with real cyvcf2 variants."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VariantParser()

        all_records: list[VariantRecord] = []
        for variant in vcf:
            records = parser.parse_variant(variant, [])
            all_records.extend(records)

        assert len(all_records) == 4

        for record in all_records:
            assert record.chrom.startswith("chr")
            assert record.pos > 0
            assert len(record.ref) > 0
            assert len(record.alt) > 0

    def test_variant_parser_multiallelic_decomposition(self):
        """Test that multi-allelic variants are decomposed into separate records."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        vcf = VCF(str(vcf_path))

        parser = VariantParser()

        all_records: list[VariantRecord] = []
        for variant in vcf:
            records = parser.parse_variant(variant, [])
            all_records.extend(records)

        assert len(all_records) == 8

        multiallelic_records = [r for r in all_records if r.pos == 2049437]
        assert len(multiallelic_records) == 7
        alts = {r.alt for r in multiallelic_records}
        assert "CCTTTTTTTT" in alts
        assert "CCTTTTTT" in alts

    def test_streaming_iteration_memory_efficiency(self):
        """Test that streaming iteration doesn't load all variants at once."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        count = 0
        for variant in vcf:
            count += 1
            assert variant.CHROM is not None

        assert count == 4

    def test_qual_and_filter_extraction(self):
        """Test extraction of QUAL and FILTER fields."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VariantParser()

        for variant in vcf:
            records = parser.parse_variant(variant, [])
            for record in records:
                assert record.qual is None or record.qual > 0
                assert isinstance(record.filter, list)

    def test_info_field_type_coercion(self):
        """Test that INFO field values are properly typed."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        variant = next(vcf)

        ac = variant.INFO.get("AC")
        assert ac is not None
        assert isinstance(ac, (int, tuple, list))

        af = variant.INFO.get("AF")
        assert af is not None

        dp = variant.INFO.get("DP")
        assert dp is not None
        assert isinstance(dp, int)


class TestArraySizeWithRealData:
    """Test array size calculations against real VCF data."""

    def test_number_a_expected_size(self):
        """Verify Number=A size calculation matches ALT count."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        for variant in vcf:
            n_alts = len(variant.ALT)
            expected_size = get_array_size("A", n_alts)
            assert expected_size == n_alts

            ac = variant.INFO.get("AC")
            if ac is not None:
                if isinstance(ac, (list, tuple)):
                    assert len(ac) == expected_size
                else:
                    assert expected_size == 1

    def test_number_r_includes_ref(self):
        """Verify Number=R fields include REF allele."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        for variant in vcf:
            n_alts = len(variant.ALT)
            expected_r_size = get_array_size("R", n_alts)
            assert expected_r_size == n_alts + 1

    def test_number_g_genotype_count(self):
        """Verify Number=G fields match genotype count formula."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        for variant in vcf:
            n_alts = len(variant.ALT)
            expected_g_size = get_array_size("G", n_alts, ploidy=2)

            n_alleles = n_alts + 1
            manual_g_size = (n_alleles * (n_alleles + 1)) // 2
            assert expected_g_size == manual_g_size
