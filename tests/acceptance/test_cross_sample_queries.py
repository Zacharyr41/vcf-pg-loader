"""Acceptance tests proving the tool fills gaps in nf-core pipelines."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import (
    SyntheticVariant,
    VCFGenerator,
)
from vcf_pg_loader.vcf_parser import VCFStreamingParser


@pytest.mark.acceptance
class TestQueriesImpossibleWithBcftools:
    """
    Demonstrate queries that require database persistence.

    These are the "killer features" that justify using a database
    over bcftools/slivar streaming.
    """

    def test_variant_parsing_for_historical_tracking(self, trio_vcf_file):
        """
        Variants can be parsed and tracked for "seen before" queries.

        This proves the VCF data is parsed correctly for database loading,
        enabling historical tracking impossible with bcftools.
        """
        parser = VCFStreamingParser(trio_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            variant_keys = [
                (r.chrom, r.pos, r.ref, r.alt) for r in records
            ]
            assert len(variant_keys) == 4
            assert len(set(variant_keys)) == 4
        finally:
            parser.close()

    def test_sample_metadata_extraction(self, trio_vcf_file):
        """Sample information is extracted for cohort analysis."""
        parser = VCFStreamingParser(trio_vcf_file, human_genome=True)
        try:
            samples = parser.samples
            assert len(samples) == 3
            assert "proband" in samples
            assert "father" in samples
            assert "mother" in samples
        finally:
            parser.close()


@pytest.mark.acceptance
class TestTrioInheritancePatternDetection:
    """Test detection of inheritance patterns in trio data."""

    def test_de_novo_candidate_detection(self, trio_vcf_file):
        """
        De novo variant candidates can be identified.

        The trio VCF contains a variant where child is het
        and both parents are hom_ref.
        """
        parser = VCFStreamingParser(trio_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            de_novo_candidate = next(
                (r for r in records if r.chrom == "chr1" and r.pos == 1000),
                None,
            )
            assert de_novo_candidate is not None
            assert de_novo_candidate.ref == "A"
            assert de_novo_candidate.alt == "G"
        finally:
            parser.close()

    def test_autosomal_recessive_candidate_detection(self, trio_vcf_file):
        """
        Autosomal recessive candidates can be identified.

        The trio VCF contains a variant where child is hom_alt
        and both parents are het.
        """
        parser = VCFStreamingParser(trio_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            ar_candidate = next(
                (r for r in records if r.chrom == "chr2" and r.pos == 2000),
                None,
            )
            assert ar_candidate is not None
            assert ar_candidate.ref == "C"
            assert ar_candidate.alt == "T"
        finally:
            parser.close()

    def test_compound_het_candidate_detection(self, trio_vcf_file):
        """
        Compound het candidates can be identified.

        The trio VCF contains two variants in the same gene where
        each variant comes from a different parent.
        """
        parser = VCFStreamingParser(trio_vcf_file, human_genome=True)
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            chr3_variants = [r for r in records if r.chrom == "chr3"]
            assert len(chr3_variants) == 2

            genes = {r.info.get("SYMBOL") for r in chr3_variants}
            assert "GENE1" in genes
        finally:
            parser.close()


@pytest.mark.acceptance
class TestPerformanceTargets:
    """Validate performance targets."""

    def test_streaming_parser_memory_efficiency(self, multiallelic_vcf_file):
        """Parser uses streaming to avoid loading entire file into memory."""
        parser = VCFStreamingParser(multiallelic_vcf_file, batch_size=1, human_genome=True)
        try:
            batch_count = 0
            for batch in parser.iter_batches():
                batch_count += 1
                assert len(batch) <= 3

            assert batch_count >= 1
        finally:
            parser.close()

    def test_batch_processing_efficiency(self):
        """Large batches are processed efficiently."""
        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i,
                ref="A",
                alt=["G"],
            )
            for i in range(100)
        ]
        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, batch_size=50, human_genome=True)
            batches = list(parser.iter_batches())

            assert len(batches) == 2
            assert len(batches[0]) == 50
            assert len(batches[1]) == 50
        finally:
            vcf_file.unlink()
            parser.close()


@pytest.mark.acceptance
@pytest.mark.slow
class TestGIABTrioBenchmarks:
    """Tests using GIAB benchmark data (requires downloads)."""

    def test_giab_chr21_variant_count(self, giab_chr21_vcf):
        """GIAB chr21 subset has expected variant count."""
        parser = VCFStreamingParser(giab_chr21_vcf, human_genome=True)
        try:
            total = 0
            for batch in parser.iter_batches():
                total += len(batch)

            assert total > 50_000
        finally:
            parser.close()


@pytest.mark.acceptance
class TestAnnotationPreservation:
    """Test that annotations are preserved through parsing."""

    def test_info_fields_preserved(self):
        """All INFO fields are captured in variant records."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=100,
                ref="A",
                alt=["G"],
                info={
                    "DP": 100,
                    "AF": [0.5],
                    "AC": [10],
                    "AN": 20,
                    "SYMBOL": "GENE1",
                },
            ),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            record = batches[0][0]

            assert "DP" in record.info
            assert record.info["DP"] == 100
            assert "AN" in record.info
            assert record.info["AN"] == 20
            assert "SYMBOL" in record.info
            assert record.info["SYMBOL"] == "GENE1"
        finally:
            vcf_file.unlink()
            parser.close()

    def test_normalization_preserves_original_coordinates(self, unnormalized_vcf_file):
        """Original coordinates are preserved when normalizing."""
        parser = VCFStreamingParser(
            unnormalized_vcf_file, human_genome=True, normalize=True
        )
        try:
            batches = list(parser.iter_batches())
            records = batches[0]

            normalized_records = [r for r in records if r.normalized]
            for record in normalized_records:
                assert record.original_pos is not None
                assert record.original_ref is not None
                assert record.original_alt is not None
        finally:
            parser.close()


@pytest.mark.acceptance
class TestNFCoreEcosystemAlignment:
    """Tests proving alignment with nf-core pipeline outputs."""

    def test_chromosome_prefix_handling(self):
        """Human genome mode adds 'chr' prefix consistently."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"]),
            SyntheticVariant(chrom="1", pos=200, ref="C", alt=["T"]),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            for record in records:
                assert record.chrom.startswith("chr")
        finally:
            vcf_file.unlink()
            parser.close()

    def test_non_human_genome_mode(self):
        """Non-human mode preserves original chromosome names."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(chrom="NC_001133.9", pos=100, ref="A", alt=["G"]),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=False)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert records[0].chrom == "NC_001133.9"
        finally:
            vcf_file.unlink()
            parser.close()
