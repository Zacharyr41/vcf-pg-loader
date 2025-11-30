"""Integration tests using real VCF files.

This file tests end-to-end parsing of various VCF formats:
- Clinical annotation pipeline output (annotated_ranked.vcf.gz)
- GRIDSS structural variants with BND format
- dbSNP VCF 4.0 format with many Flag fields
- Empty VCF files (header only)
"""

from pathlib import Path

import pytest
from cyvcf2 import VCF

from vcf_pg_loader.vcf_parser import VariantParser, VCFHeaderParser, VCFStreamingParser

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.mark.integration
class TestAnnotatedVCFIntegration:
    """End-to-end integration tests with annotated_ranked.vcf.gz."""

    @pytest.fixture
    def vcf_path(self):
        return FIXTURES_DIR / "annotated_ranked.vcf.gz"

    @pytest.fixture
    def header_parser(self, vcf_path):
        vcf = VCF(str(vcf_path))
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)
        return header_parser

    def test_streaming_parser_full_file(self, vcf_path):
        """Should stream parse entire file without errors."""
        parser = VCFStreamingParser(vcf_path)

        records = []
        for batch in parser.iter_batches():
            records.extend(batch)

        assert parser.variant_count == 113
        assert len(records) >= 113

    def test_all_chromosomes_present(self, vcf_path, header_parser):
        """Should parse variants from all chromosomes in file."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        chromosomes = set()
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                chromosomes.add(record.chrom)

        assert "chr16" in chromosomes
        assert "chrX" in chromosomes
        assert len(chromosomes) == 2

    def test_multiallelic_at_same_position(self, vcf_path, header_parser):
        """Should correctly decompose multi-allelics at chr16:160070."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        pos_160070_records = []
        for variant in vcf:
            if variant.POS == 160070:
                records = variant_parser.parse_variant(variant, [])
                pos_160070_records.extend(records)

        assert len(pos_160070_records) >= 2
        alts = [r.alt for r in pos_160070_records]
        assert len(set(alts)) == len(alts)

    def test_large_insertion_handling(self, vcf_path, header_parser):
        """Should handle 600+ bp insertions."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        large_insertions = []
        for variant in vcf:
            for alt in variant.ALT:
                if len(alt) > 500:
                    records = variant_parser.parse_variant(variant, [])
                    large_insertions.extend(records)
                    break

        assert len(large_insertions) >= 1
        for record in large_insertions:
            assert len(record.alt) > 500

    def test_csq_field_extraction(self, vcf_path, header_parser):
        """Should extract CSQ annotations from variants."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        records_with_csq = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                if "CSQ" in record.info:
                    records_with_csq += 1

        assert records_with_csq >= 100

    def test_genetic_models_preserved(self, vcf_path, header_parser):
        """Should preserve genetic model annotations."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        genetic_models_found = set()
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                gm = record.info.get("GeneticModels")
                if gm:
                    if isinstance(gm, str):
                        for model in gm.split("|"):
                            if ":" in model:
                                genetic_models_found.add(model.split(":")[1])
                            else:
                                genetic_models_found.add(model)

        assert "AR_hom" in genetic_models_found or len(genetic_models_found) >= 1

    def test_rank_scores_present(self, vcf_path, header_parser):
        """Should preserve rank score annotations."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        records_with_rank = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                if "RankScore" in record.info:
                    records_with_rank += 1

        assert records_with_rank >= 100

    def test_cadd_scores_present(self, vcf_path, header_parser):
        """Should preserve CADD score annotations."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        records_with_cadd = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                if "cadd_phred" in record.info or "cadd_raw" in record.info:
                    records_with_cadd += 1

        assert records_with_cadd >= 100

    def test_compound_het_annotations(self, vcf_path, header_parser):
        """Should preserve compound heterozygous annotations."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        records_with_compounds = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                if "Compounds" in record.info:
                    records_with_compounds += 1

        assert records_with_compounds >= 1

    def test_filter_values_parsed(self, vcf_path, header_parser):
        """Should parse FILTER values correctly."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert isinstance(record.filter, list)

    def test_batch_sizes_work(self, vcf_path):
        """Should work with various batch sizes."""
        for batch_size in [1, 10, 50, 100]:
            parser = VCFStreamingParser(vcf_path, batch_size=batch_size)
            total = sum(len(batch) for batch in parser.iter_batches())
            assert total >= 113

    def test_context_manager_cleanup(self, vcf_path):
        """Should properly clean up resources."""
        with VCFStreamingParser(vcf_path) as parser:
            total = sum(len(batch) for batch in parser.iter_batches())
            assert total >= 113

        assert parser._vcf is None or parser._closed

    def test_variant_record_fields(self, vcf_path, header_parser):
        """Should populate all required VariantRecord fields."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert record.chrom is not None
                assert record.pos > 0
                assert record.ref is not None and len(record.ref) >= 1
                assert record.alt is not None and len(record.alt) >= 1
                assert isinstance(record.info, dict)
                assert isinstance(record.filter, list)


@pytest.mark.integration
class TestGRIDSSIntegration:
    """End-to-end integration tests with GRIDSS structural variant VCF."""

    @pytest.fixture
    def vcf_path(self):
        return FIXTURES_DIR / "gridss_sv.vcf"

    @pytest.fixture
    def header_parser(self, vcf_path):
        vcf = VCF(str(vcf_path))
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)
        return header_parser

    def test_streaming_all_breakends(self, vcf_path):
        """Should stream parse all 192 BND variants."""
        parser = VCFStreamingParser(vcf_path)

        records = []
        for batch in parser.iter_batches():
            records.extend(batch)

        assert parser.variant_count == 192
        assert len(records) == 192

    def test_bnd_alt_format_preserved(self, vcf_path, header_parser):
        """Should preserve BND alt allele format through pipeline."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        bnd_patterns = set()
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                alt = record.alt
                if "[" in alt or "]" in alt:
                    bnd_patterns.add("bracket")
                elif alt.startswith(".") or alt.endswith("."):
                    bnd_patterns.add("single_breakend")

        assert "bracket" in bnd_patterns

    def test_mateid_linkage(self, vcf_path, header_parser):
        """Should preserve MATEID for paired breakends."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        mate_ids = []
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                mateid = record.info.get("MATEID")
                if mateid:
                    mate_ids.append(mateid)

        assert len(mate_ids) >= 100

    def test_gridss_info_fields_preserved(self, vcf_path, header_parser):
        """Should preserve GRIDSS-specific INFO fields."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        gridss_fields_found = set()
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                for field in ["SVTYPE", "CIPOS", "HOMLEN", "EVENT"]:
                    if field in record.info:
                        gridss_fields_found.add(field)

        assert "SVTYPE" in gridss_fields_found
        assert len(gridss_fields_found) >= 2


@pytest.mark.integration
class TestDbSNPIntegration:
    """End-to-end integration tests with dbSNP VCF."""

    @pytest.fixture
    def vcf_path(self):
        return FIXTURES_DIR / "dbsnp_subset.vcf.gz"

    @pytest.fixture
    def header_parser(self, vcf_path):
        vcf = VCF(str(vcf_path))
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)
        return header_parser

    def test_streaming_with_multiallelic_decomposition(self, vcf_path):
        """Should decompose multi-allelics: 2174 variants -> 2216 records."""
        parser = VCFStreamingParser(vcf_path)

        records = []
        for batch in parser.iter_batches():
            records.extend(batch)

        assert parser.variant_count == 2174
        assert len(records) == 2216

    def test_flag_fields_as_booleans(self, vcf_path, header_parser):
        """Should handle Flag fields correctly through pipeline."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        flag_values = []
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                pm = record.info.get("PM")
                if pm is not None:
                    flag_values.append(pm)
                break
            if flag_values:
                break

        assert len(flag_values) >= 1

    def test_rs_ids_preserved(self, vcf_path, header_parser):
        """Should preserve RS IDs from INFO field."""
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(vcf_path))
        rs_count = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                if record.info.get("RS"):
                    rs_count += 1

        assert rs_count >= 2000


@pytest.mark.integration
class TestEmptyVCFIntegration:
    """Integration tests for empty VCF files (header only)."""

    @pytest.fixture
    def vcf_path(self):
        return FIXTURES_DIR / "empty.vcf.gz"

    def test_streaming_empty_file(self, vcf_path):
        """Should handle empty VCF without errors."""
        parser = VCFStreamingParser(vcf_path)

        records = []
        for batch in parser.iter_batches():
            records.extend(batch)

        assert parser.variant_count == 0
        assert len(records) == 0

    def test_header_still_parseable(self, vcf_path):
        """Should parse header even with no variants."""
        vcf = VCF(str(vcf_path))
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        assert header_parser.get_info_field("DP") is not None
        assert header_parser.get_format_field("GT") is not None

    def test_batch_iteration_completes(self, vcf_path):
        """Should complete batch iteration without hanging."""
        parser = VCFStreamingParser(vcf_path, batch_size=100)

        batch_count = 0
        for _batch in parser.iter_batches():
            batch_count += 1

        assert batch_count == 0
