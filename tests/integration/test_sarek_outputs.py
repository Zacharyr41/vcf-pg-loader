"""Tests for loading nf-core/sarek VCF outputs."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator
from vcf_pg_loader.vcf_parser import VCFStreamingParser


@pytest.mark.integration
class TestSarekVEPAnnotations:
    """Test loading sarek VEP-annotated VCFs."""

    def test_csq_fields_extracted(self, vep_csq_vcf_file):
        """VEP CSQ fields are extracted from variants."""
        parser = VCFStreamingParser(vep_csq_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            assert len(batches) == 1
            records = batches[0]
            assert len(records) == 1

            record = records[0]
            assert record.gene == "BRCA1"
            assert record.consequence == "missense_variant"
            assert record.impact == "MODERATE"
        finally:
            parser.close()

    def test_csq_fields_in_header(self, vep_csq_vcf_file):
        """CSQ field structure is parsed from header."""
        parser = VCFStreamingParser(vep_csq_vcf_file, human_genome=True)
        try:
            csq_fields = parser.header_parser.csq_fields
            assert "Allele" in csq_fields
            assert "Consequence" in csq_fields
            assert "IMPACT" in csq_fields
            assert "SYMBOL" in csq_fields
        finally:
            parser.close()


@pytest.mark.integration
class TestSarekSomaticVCF:
    """Test somatic (tumor-normal) VCF handling."""

    @pytest.mark.nf_core
    def test_mutect2_output_loads(self, test_data_manager):
        """Mutect2 somatic VCF loads correctly."""
        vcf_path = test_data_manager.get_sarek_somatic_output("mutect2")
        if vcf_path is None or not vcf_path.exists():
            pytest.skip("No Mutect2 somatic output available")

        parser = VCFStreamingParser(vcf_path, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            total_variants = sum(len(b) for b in batches)
            assert total_variants >= 0
        finally:
            parser.close()


@pytest.mark.integration
class TestVCFParsingCorrectness:
    """Test correct parsing of VCF fields."""

    def test_multiallelic_decomposition(self, multiallelic_vcf_file):
        """Multi-allelic sites are decomposed to biallelic."""
        parser = VCFStreamingParser(multiallelic_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 3
            alts = {r.alt for r in records}
            assert alts == {"G", "T", "C"}
        finally:
            parser.close()

    def test_filter_field_parsing(self):
        """FILTER field is correctly parsed."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1", pos=100, ref="A", alt=["G"], filter="PASS"
            ),
            SyntheticVariant(
                chrom="chr1", pos=200, ref="C", alt=["T"], filter="LowQual"
            ),
            SyntheticVariant(
                chrom="chr1", pos=300, ref="G", alt=["A"], filter="LowQual;LowDP"
            ),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert records[0].filter == []
            assert records[1].filter == ["LowQual"]
            assert records[2].filter == ["LowQual", "LowDP"]
        finally:
            vcf_file.unlink()
            parser.close()

    def test_qual_field_parsing(self):
        """QUAL field is correctly parsed."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1", pos=100, ref="A", alt=["G"], qual=99.5
            ),
            SyntheticVariant(
                chrom="chr1", pos=200, ref="C", alt=["T"], qual=None
            ),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert records[0].qual == 99.5
            assert records[1].qual is None
        finally:
            vcf_file.unlink()
            parser.close()

    def test_rsid_parsing(self):
        """rsID is correctly parsed."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1", pos=100, ref="A", alt=["G"], rs_id="rs12345"
            ),
            SyntheticVariant(
                chrom="chr1", pos=200, ref="C", alt=["T"], rs_id="."
            ),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert records[0].rs_id == "rs12345"
            assert records[1].rs_id is None
        finally:
            vcf_file.unlink()
            parser.close()


@pytest.mark.integration
class TestNormalizationIntegration:
    """Test normalization during parsing."""

    def test_normalization_enabled(self, unnormalized_vcf_file):
        """Variants are normalized when enabled."""
        parser = VCFStreamingParser(
            unnormalized_vcf_file, human_genome=True, normalize=True
        )
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            normalized_count = sum(1 for r in records if r.normalized)
            assert normalized_count >= 1
        finally:
            parser.close()

    def test_normalization_disabled(self, unnormalized_vcf_file):
        """Variants are not normalized when disabled."""
        parser = VCFStreamingParser(
            unnormalized_vcf_file, human_genome=True, normalize=False
        )
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            normalized_count = sum(1 for r in records if r.normalized)
            assert normalized_count == 0
        finally:
            parser.close()
