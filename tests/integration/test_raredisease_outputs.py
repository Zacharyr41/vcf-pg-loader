"""Tests for loading nf-core/raredisease GENMOD-annotated VCFs."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator
from vcf_pg_loader.vcf_parser import VCFStreamingParser


@pytest.mark.integration
class TestRarediseaseGENMOD:
    """Test loading nf-core/raredisease GENMOD-annotated VCFs."""

    def test_genetic_models_extracted(self, genmod_vcf_file):
        """GENMOD GeneticModels field is parsed."""
        parser = VCFStreamingParser(genmod_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            genetic_models_found = False
            for record in records:
                if "GeneticModels" in record.info:
                    genetic_models_found = True
                    assert "FAM001" in record.info["GeneticModels"]

            assert genetic_models_found
        finally:
            parser.close()

    def test_compounds_field_extracted(self, genmod_vcf_file):
        """GENMOD Compounds field is parsed."""
        parser = VCFStreamingParser(genmod_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            compounds_found = False
            for record in records:
                if "Compounds" in record.info:
                    compounds_found = True
                    assert "GENE1" in record.info["Compounds"]

            assert compounds_found
        finally:
            parser.close()

    def test_rank_scores_extracted(self, genmod_vcf_file):
        """GENMOD RankScore is parsed."""
        parser = VCFStreamingParser(genmod_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            rank_scores_found = False
            for record in records:
                if "RankScore" in record.info:
                    rank_scores_found = True

            assert rank_scores_found
        finally:
            parser.close()


@pytest.mark.integration
class TestGENMODFieldParsing:
    """Test GENMOD-specific field parsing."""

    def test_parse_genetic_models_format(self):
        """GeneticModels field format is handled."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=1000,
                ref="A",
                alt=["G"],
                info={"GeneticModels": "FAM001:AR_hom,FAM001:AR_comp"},
            ),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            gm = records[0].info.get("GeneticModels")
            assert "FAM001" in gm
            assert "AR_hom" in gm
        finally:
            vcf_file.unlink()
            parser.close()

    def test_parse_compound_pairs_format(self):
        """Compounds field format is handled."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=1000,
                ref="A",
                alt=["G"],
                info={"Compounds": "BRCA1:chr17_43094464_C_T>chr17_43094500_G_A"},
            ),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            compounds = records[0].info.get("Compounds")
            assert "BRCA1" in compounds
        finally:
            vcf_file.unlink()
            parser.close()


@pytest.mark.integration
class TestTrioInheritancePatterns:
    """Test trio-based inheritance pattern variants."""

    def test_trio_vcf_parsing(self, trio_vcf_file):
        """Trio VCF with inheritance patterns parses correctly."""
        parser = VCFStreamingParser(trio_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 4

            samples = parser.samples
            assert len(samples) == 3
            assert "proband" in samples
            assert "father" in samples
            assert "mother" in samples
        finally:
            parser.close()


@pytest.mark.integration
@pytest.mark.nf_core
class TestRealRarediseaseOutputs:
    """Test with real nf-core/raredisease outputs."""

    def test_raredisease_output_loads(self, test_data_manager):
        """Real raredisease output loads without errors."""
        vcf_path = test_data_manager.get_nf_core_output("raredisease", "variants")
        if vcf_path is None:
            pytest.skip("Raredisease test output not available")

        parser = VCFStreamingParser(vcf_path, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            total_variants = sum(len(b) for b in batches)
            assert total_variants >= 0
        finally:
            parser.close()
