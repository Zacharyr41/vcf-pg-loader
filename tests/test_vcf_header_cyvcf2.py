"""Tests for VCF header parsing with cyvcf2 integration.

These tests expose the gap between VCFHeaderParser (which parses raw strings)
and the need to integrate with cyvcf2's native header API.
"""

from pathlib import Path

from cyvcf2 import VCF

from vcf_pg_loader.vcf_parser import VCFHeaderParser

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestVCFHeaderParserCyvcf2Integration:
    """Tests that VCFHeaderParser works with cyvcf2 VCF objects."""

    def test_parse_info_fields_from_cyvcf2(self):
        """VCFHeaderParser should be able to parse headers from a cyvcf2 VCF object."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VCFHeaderParser()
        info_fields = parser.parse_info_fields_from_vcf(vcf)

        assert "AC" in info_fields
        assert info_fields["AC"]["Number"] == "A"
        assert info_fields["AC"]["Type"] == "Integer"

        assert "AF" in info_fields
        assert info_fields["AF"]["Number"] == "A"

        assert "DP" in info_fields
        assert info_fields["DP"]["Number"] == "1"

        assert "DB" in info_fields
        assert info_fields["DB"]["Number"] == "0"
        assert info_fields["DB"]["Type"] == "Flag"

    def test_parse_format_fields_from_cyvcf2(self):
        """VCFHeaderParser should be able to parse FORMAT fields from a cyvcf2 VCF object."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VCFHeaderParser()
        format_fields = parser.parse_format_fields_from_vcf(vcf)

        assert "GT" in format_fields
        assert format_fields["GT"]["Number"] == "1"
        assert format_fields["GT"]["Type"] == "String"

        assert "AD" in format_fields
        assert format_fields["AD"]["Type"] == "Integer"

        assert "DP" in format_fields
        assert format_fields["DP"]["Number"] == "1"

        assert "PL" in format_fields
        assert format_fields["PL"]["Number"] == "G"

    def test_get_info_field_metadata(self):
        """Should retrieve metadata for a specific INFO field."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VCFHeaderParser()
        parser.parse_info_fields_from_vcf(vcf)

        ac_meta = parser.get_info_field("AC")
        assert ac_meta is not None
        assert ac_meta["Number"] == "A"
        assert ac_meta["Type"] == "Integer"

        missing = parser.get_info_field("NONEXISTENT")
        assert missing is None

    def test_get_format_field_metadata(self):
        """Should retrieve metadata for a specific FORMAT field."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VCFHeaderParser()
        parser.parse_format_fields_from_vcf(vcf)

        gt_meta = parser.get_format_field("GT")
        assert gt_meta is not None
        assert gt_meta["Number"] == "1"

        pl_meta = parser.get_format_field("PL")
        assert pl_meta is not None
        assert pl_meta["Number"] == "G"

    def test_parse_all_from_vcf(self):
        """Convenience method to parse all header info at once."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VCFHeaderParser()
        parser.parse_from_vcf(vcf)

        assert parser.get_info_field("AC") is not None
        assert parser.get_format_field("GT") is not None
        assert len(parser.samples) == 3
        assert parser.samples == ["HG002", "HG003", "HG004"]

    def test_samples_extracted(self):
        """Should extract sample names from VCF header."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VCFHeaderParser()
        parser.parse_from_vcf(vcf)

        assert parser.samples == ["HG002", "HG003", "HG004"]

    def test_contigs_extracted(self):
        """Should extract contig information from VCF header."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        vcf = VCF(str(vcf_path))

        parser = VCFHeaderParser()
        parser.parse_from_vcf(vcf)

        assert len(parser.contigs) > 0
        assert "1" in parser.contigs
