"""Tests for parsing real-world VCF files from variant callers.

These tests use VCF files from nf-core/test-datasets to verify the parser
handles real variant caller output correctly.

Fixtures (variantprioritization branch):
- mutect2_chr22.vcf.gz: GATK Mutect2 somatic variants (162 variants)
- strelka_snvs_chr22.vcf.gz: Strelka2 somatic SNVs (2627 variants)
- strelka_indels_chr22.vcf.gz: Strelka2 somatic indels (140 variants)

Fixtures (modules branch):
- genmod_sv.vcf.gz: SV variants with CSQ, gnomAD AF (57 variants)
- annotated_ranked.vcf.gz: VEP CSQ, CADD, genetic models (113 variants)
- gnomad_subset.vcf.gz: Population-stratified AF fields (3500 variants)
- gvcf_sample.vcf.gz: gVCF with NON_REF, END blocks (130 lines, 136 after decomposition)
- mills_indels.vcf.gz: Classic indel callset (14 variants)
"""

from pathlib import Path

import pytest
from cyvcf2 import VCF

from vcf_pg_loader.models import VariantRecord
from vcf_pg_loader.vcf_parser import VariantParser, VCFHeaderParser, VCFStreamingParser

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestGzippedVCFSupport:
    """Tests for parsing gzipped VCF files."""

    def test_parse_gzipped_mutect2_vcf(self):
        """Should parse gzipped Mutect2 VCF file."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"
        vcf = VCF(str(vcf_path))
        variants = list(vcf)
        assert len(variants) == 162

    def test_parse_gzipped_strelka_snvs(self):
        """Should parse gzipped Strelka SNVs VCF file."""
        vcf_path = FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"
        vcf = VCF(str(vcf_path))
        variants = list(vcf)
        assert len(variants) == 2627

    def test_parse_gzipped_strelka_indels(self):
        """Should parse gzipped Strelka indels VCF file."""
        vcf_path = FIXTURES_DIR / "strelka_indels_chr22.vcf.gz"
        vcf = VCF(str(vcf_path))
        variants = list(vcf)
        assert len(variants) == 140

    def test_streaming_parser_handles_gzip(self):
        """VCFStreamingParser should handle gzipped files."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"
        parser = VCFStreamingParser(vcf_path)

        total_records = 0
        for batch in parser.iter_batches():
            total_records += len(batch)

        assert total_records == 174
        assert parser.variant_count == 162

    def test_header_parsing_from_gzip(self):
        """Should parse header metadata from gzipped VCF."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"
        vcf = VCF(str(vcf_path))

        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        assert "DP" in header_parser._info_fields
        assert "TLOD" in header_parser._info_fields
        assert "GT" in header_parser._format_fields


class TestMutect2VCFParsing:
    """Tests for parsing GATK Mutect2 somatic variant calls."""

    @pytest.fixture
    def mutect2_vcf(self):
        return VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))

    @pytest.fixture
    def mutect2_parser(self, mutect2_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(mutect2_vcf)
        return VariantParser(header_parser), header_parser

    def test_mutect2_samples(self, mutect2_vcf):
        """Should extract tumor and normal sample names."""
        samples = mutect2_vcf.samples
        assert len(samples) == 2
        assert "HCC1395_HCC1395N" in samples or any("HCC1395" in s for s in samples)

    def test_mutect2_number_a_fields(self, mutect2_vcf, mutect2_parser):
        """Should parse Number=A INFO fields correctly."""
        variant_parser, header_parser = mutect2_parser

        mpos_meta = header_parser.get_info_field("MPOS")
        assert mpos_meta is not None
        assert mpos_meta["Number"] == "A"

        tlod_meta = header_parser.get_info_field("TLOD")
        assert tlod_meta is not None
        assert tlod_meta["Number"] == "A"

    def test_mutect2_number_r_fields(self, mutect2_vcf, mutect2_parser):
        """Should parse Number=R INFO fields correctly."""
        _, header_parser = mutect2_parser

        mbq_meta = header_parser.get_info_field("MBQ")
        assert mbq_meta is not None
        assert mbq_meta["Number"] == "R"

        mfrl_meta = header_parser.get_info_field("MFRL")
        assert mfrl_meta is not None
        assert mfrl_meta["Number"] == "R"

    def test_mutect2_complex_filter_parsing(self, mutect2_vcf):
        """Should parse semicolon-delimited FILTER strings."""
        variant_parser = VariantParser()

        for variant in mutect2_vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert isinstance(record.filter, list)
                if variant.FILTER and variant.FILTER != "PASS":
                    assert len(record.filter) >= 1

    def test_mutect2_flag_fields(self, mutect2_vcf, mutect2_parser):
        """Should handle Flag type INFO fields like PON and STR."""
        _, header_parser = mutect2_parser

        pon_meta = header_parser.get_info_field("PON")
        assert pon_meta is not None
        assert pon_meta["Type"] == "Flag"
        assert pon_meta["Number"] == "0"

        str_meta = header_parser.get_info_field("STR")
        assert str_meta is not None
        assert str_meta["Type"] == "Flag"

    def test_mutect2_variant_decomposition(self, mutect2_vcf):
        """Should decompose multi-allelic variants."""
        variant_parser = VariantParser()

        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        total_records = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            total_records += len(records)

        assert total_records >= 162

    def test_mutect2_contigs_parsed(self, mutect2_vcf, mutect2_parser):
        """Should parse contig definitions from header."""
        _, header_parser = mutect2_parser

        assert len(header_parser.contigs) > 0
        assert "chr22" in header_parser.contigs

    def test_mutect2_format_fields(self, mutect2_vcf, mutect2_parser):
        """Should parse FORMAT field definitions."""
        _, header_parser = mutect2_parser

        ad_meta = header_parser.get_format_field("AD")
        assert ad_meta is not None
        assert ad_meta["Number"] == "R"

        af_meta = header_parser.get_format_field("AF")
        assert af_meta is not None
        assert af_meta["Number"] == "A"

        pl_meta = header_parser.get_format_field("PL")
        assert pl_meta is not None
        assert pl_meta["Number"] == "G"


class TestStrelkaVCFParsing:
    """Tests for parsing Strelka2 somatic variant calls."""

    @pytest.fixture
    def strelka_snv_vcf(self):
        return VCF(str(FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"))

    @pytest.fixture
    def strelka_indel_vcf(self):
        return VCF(str(FIXTURES_DIR / "strelka_indels_chr22.vcf.gz"))

    def test_strelka_snv_samples(self, strelka_snv_vcf):
        """Should extract sample names from Strelka SNV VCF."""
        samples = strelka_snv_vcf.samples
        assert len(samples) == 2

    def test_strelka_tier_format_fields(self, strelka_snv_vcf):
        """Should parse Strelka tier-based FORMAT fields (AU, CU, GU, TU)."""
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(strelka_snv_vcf)

        au_meta = header_parser.get_format_field("AU")
        assert au_meta is not None
        assert au_meta["Number"] == "2"
        assert au_meta["Type"] == "Integer"

        cu_meta = header_parser.get_format_field("CU")
        assert cu_meta is not None
        assert cu_meta["Number"] == "2"

    def test_strelka_somatic_flag(self, strelka_snv_vcf):
        """Should detect SOMATIC flag in INFO."""
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(strelka_snv_vcf)

        somatic_meta = header_parser.get_info_field("SOMATIC")
        assert somatic_meta is not None
        assert somatic_meta["Type"] == "Flag"

    def test_strelka_snv_info_fields(self, strelka_snv_vcf):
        """Should parse Strelka SNV-specific INFO fields."""
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(strelka_snv_vcf)

        qss_meta = header_parser.get_info_field("QSS")
        assert qss_meta is not None

        sgt_meta = header_parser.get_info_field("SGT")
        assert sgt_meta is not None

    def test_strelka_indel_info_fields(self, strelka_indel_vcf):
        """Should parse Strelka indel-specific INFO fields."""
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(strelka_indel_vcf)

        ic_meta = header_parser.get_info_field("IC")
        assert ic_meta is not None

        ihp_meta = header_parser.get_info_field("IHP")
        assert ihp_meta is not None

        rc_meta = header_parser.get_info_field("RC")
        assert rc_meta is not None

        ru_meta = header_parser.get_info_field("RU")
        assert ru_meta is not None

    def test_strelka_variant_parsing(self, strelka_snv_vcf):
        """Should parse Strelka variants into VariantRecords."""
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(strelka_snv_vcf)
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"))
        parsed_count = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert isinstance(record, VariantRecord)
                assert record.chrom.startswith("chr")
                assert record.pos > 0
                parsed_count += 1

        assert parsed_count == 2627


class TestEdgeCases:
    """Tests for edge cases in real VCF data."""

    def test_large_deletion(self):
        """Should handle large deletions (81bp deletion in Mutect2)."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        variant_parser = VariantParser()

        large_deletions = []
        for variant in vcf:
            if len(variant.REF) > 50:
                records = variant_parser.parse_variant(variant, [])
                large_deletions.extend(records)

        assert len(large_deletions) >= 1
        for record in large_deletions:
            assert len(record.ref) > 50

    def test_mnv_parsing(self):
        """Should handle MNVs (multi-nucleotide variants like GC->AA)."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        variant_parser = VariantParser()

        mnvs = []
        for variant in vcf:
            if len(variant.REF) > 1 and len(variant.ALT[0]) > 1:
                if len(variant.REF) == len(variant.ALT[0]):
                    records = variant_parser.parse_variant(variant, [])
                    mnvs.extend(records)

        assert len(mnvs) >= 1

    def test_str_variants(self):
        """Should handle STR (short tandem repeat) variants."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))

        str_variants = []
        for variant in vcf:
            info = dict(variant.INFO)
            if info.get("STR"):
                str_variants.append(variant)

        assert len(str_variants) >= 1

        for variant in str_variants:
            info = dict(variant.INFO)
            assert "RU" in info
            assert "RPA" in info

    def test_chr22_boundary_variants(self):
        """Should handle variants across chromosome 22."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        variant_parser = VariantParser()

        positions = []
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                positions.append(record.pos)
                assert record.chrom == "chr22"

        assert min(positions) > 10000000
        assert max(positions) < 51000000

    def test_pass_vs_filtered_variants(self):
        """Should correctly identify PASS vs filtered variants."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        variant_parser = VariantParser()

        pass_count = 0
        filtered_count = 0

        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                if not record.filter or record.filter == []:
                    pass_count += 1
                else:
                    filtered_count += 1

        assert pass_count >= 1
        assert filtered_count >= 1


class TestStreamingPerformance:
    """Tests for streaming parser with real VCF data."""

    def test_stream_large_vcf(self):
        """Should efficiently stream 2627 variants."""
        vcf_path = FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"
        parser = VCFStreamingParser(vcf_path, batch_size=500)

        batch_count = 0
        total_records = 0

        for batch in parser.iter_batches():
            batch_count += 1
            total_records += len(batch)
            assert len(batch) <= 500

        assert total_records == 2627
        assert batch_count == 6

    def test_stream_with_small_batches(self):
        """Should handle small batch sizes correctly."""
        vcf_path = FIXTURES_DIR / "strelka_indels_chr22.vcf.gz"
        parser = VCFStreamingParser(vcf_path, batch_size=10)

        batches = list(parser.iter_batches())
        total_records = sum(len(b) for b in batches)

        assert total_records == 140
        for batch in batches[:-1]:
            assert len(batch) == 10

    def test_streaming_parser_counts(self):
        """Should track variant and record counts correctly."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"
        parser = VCFStreamingParser(vcf_path)

        for _ in parser.iter_batches():
            pass

        assert parser.variant_count == 162
        assert parser.record_count >= 162

    def test_streaming_parser_context_manager(self):
        """Should work as context manager with gzipped files."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"

        with VCFStreamingParser(vcf_path) as parser:
            total = sum(len(batch) for batch in parser.iter_batches())
            assert total == 174
            assert parser.variant_count == 162

        assert parser._vcf is None or parser._closed


class TestRealDataIntegrity:
    """Tests verifying data integrity during parsing."""

    def test_chrom_normalization(self):
        """All chromosomes should be normalized to chr prefix."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        variant_parser = VariantParser()

        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert record.chrom.startswith("chr")
                assert record.chrom == "chr22"

    def test_qual_values(self):
        """QUAL values should be properly parsed or None."""
        vcf = VCF(str(FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"))
        variant_parser = VariantParser()

        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert record.qual is None or isinstance(record.qual, float)

    def test_ref_alt_not_empty(self):
        """REF and ALT should never be empty."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        variant_parser = VariantParser()

        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert len(record.ref) >= 1
                assert len(record.alt) >= 1

    def test_info_dict_populated(self):
        """INFO dict should contain parsed fields."""
        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)
        variant_parser = VariantParser(header_parser)

        vcf = VCF(str(FIXTURES_DIR / "mutect2_chr22.vcf.gz"))
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert "DP" in record.info or "ECNT" in record.info


class TestVEPAnnotationParsing:
    """Tests for parsing VEP/CSQ annotated VCF files."""

    @pytest.fixture
    def annotated_vcf(self):
        return VCF(str(FIXTURES_DIR / "annotated_ranked.vcf.gz"))

    @pytest.fixture
    def annotated_header(self, annotated_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(annotated_vcf)
        return header_parser

    def test_csq_field_definition(self, annotated_header):
        """Should parse CSQ INFO field definition."""
        csq_meta = annotated_header.get_info_field("CSQ")
        assert csq_meta is not None
        assert csq_meta["Type"] == "String"
        assert csq_meta["Number"] == "."

    def test_csq_subfields_extracted(self, annotated_header):
        """Should extract CSQ subfield names from description."""
        csq_fields = annotated_header.csq_fields
        assert len(csq_fields) > 0
        assert "Allele" in csq_fields
        assert "Consequence" in csq_fields
        assert "IMPACT" in csq_fields
        assert "SYMBOL" in csq_fields

    def test_spliceai_fields_in_csq(self, annotated_header):
        """Should have SpliceAI fields in CSQ definition."""
        csq_fields = annotated_header.csq_fields
        spliceai_fields = [f for f in csq_fields if "SpliceAI" in f]
        assert len(spliceai_fields) >= 4

    def test_cadd_fields_parsed(self, annotated_header):
        """Should parse CADD score fields."""
        cadd_raw = annotated_header.get_info_field("cadd_raw")
        assert cadd_raw is not None

        cadd_phred = annotated_header.get_info_field("cadd_phred")
        assert cadd_phred is not None

    def test_parse_annotated_variants(self, annotated_vcf, annotated_header):
        """Should parse all annotated variants."""
        variant_parser = VariantParser(annotated_header)

        vcf = VCF(str(FIXTURES_DIR / "annotated_ranked.vcf.gz"))
        total = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, annotated_header.csq_fields)
            total += len(records)

        assert total >= 113


class TestGeneticModelAnnotations:
    """Tests for parsing genetic model annotations (genmod output)."""

    @pytest.fixture
    def annotated_vcf(self):
        return VCF(str(FIXTURES_DIR / "annotated_ranked.vcf.gz"))

    @pytest.fixture
    def annotated_header(self, annotated_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(annotated_vcf)
        return header_parser

    def test_genetic_models_field(self, annotated_header):
        """Should parse GeneticModels INFO field."""
        gm_meta = annotated_header.get_info_field("GeneticModels")
        assert gm_meta is not None
        assert gm_meta["Type"] == "String"

    def test_compounds_field(self, annotated_header):
        """Should parse Compounds INFO field for compound heterozygous."""
        compounds_meta = annotated_header.get_info_field("Compounds")
        assert compounds_meta is not None

    def test_rank_score_fields(self, annotated_header):
        """Should parse RankScore fields."""
        rank_score = annotated_header.get_info_field("RankScore")
        assert rank_score is not None

        rank_normalized = annotated_header.get_info_field("RankScoreNormalized")
        assert rank_normalized is not None

    def test_genetic_models_in_records(self, annotated_vcf, annotated_header):
        """Should preserve genetic models in parsed records."""
        variant_parser = VariantParser(annotated_header)

        vcf = VCF(str(FIXTURES_DIR / "annotated_ranked.vcf.gz"))
        records_with_models = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                if "GeneticModels" in record.info:
                    records_with_models += 1

        assert records_with_models >= 1


class TestGnomADPopulationFields:
    """Tests for parsing gnomAD population-stratified allele frequencies."""

    @pytest.fixture
    def gnomad_vcf(self):
        return VCF(str(FIXTURES_DIR / "gnomad_subset.vcf.gz"))

    @pytest.fixture
    def gnomad_header(self, gnomad_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(gnomad_vcf)
        return header_parser

    def test_population_ac_fields(self, gnomad_header):
        """Should parse population-specific AC fields."""
        populations = ["afr", "amr", "asj", "eas", "fin", "nfe", "oth"]
        for pop in populations:
            ac_field = gnomad_header.get_info_field(f"AC_{pop}")
            assert ac_field is not None, f"Missing AC_{pop}"
            assert ac_field["Number"] == "A"

    def test_population_af_fields(self, gnomad_header):
        """Should parse population-specific AF fields."""
        populations = ["afr", "amr", "asj", "eas", "fin", "nfe", "oth"]
        for pop in populations:
            af_field = gnomad_header.get_info_field(f"AF_{pop}")
            assert af_field is not None, f"Missing AF_{pop}"
            assert af_field["Number"] == "A"
            assert af_field["Type"] == "Float"

    def test_gnomad_variant_count(self, gnomad_vcf):
        """Should parse all gnomAD variants."""
        variants = list(gnomad_vcf)
        assert len(variants) == 3500

    def test_gnomad_streaming(self):
        """Should stream gnomAD variants efficiently."""
        vcf_path = FIXTURES_DIR / "gnomad_subset.vcf.gz"
        parser = VCFStreamingParser(vcf_path, batch_size=500)

        total = sum(len(batch) for batch in parser.iter_batches())
        assert total == 3500


class TestGVCFParsing:
    """Tests for parsing gVCF files."""

    @pytest.fixture
    def gvcf(self):
        return VCF(str(FIXTURES_DIR / "gvcf_sample.vcf.gz"))

    @pytest.fixture
    def gvcf_header(self, gvcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(gvcf)
        return header_parser

    def test_gvcf_end_field(self, gvcf_header):
        """Should parse END INFO field for reference blocks."""
        end_meta = gvcf_header.get_info_field("END")
        assert end_meta is None or end_meta["Type"] == "Integer"

    def test_gvcf_min_dp_format(self, gvcf_header):
        """Should parse MIN_DP FORMAT field."""
        min_dp = gvcf_header.get_format_field("MIN_DP")
        assert min_dp is not None
        assert min_dp["Type"] == "Integer"

    def test_gvcf_record_count(self, gvcf):
        """Should parse all gVCF records."""
        records = list(gvcf)
        assert len(records) == 130

    def test_gvcf_has_non_ref_allele(self, gvcf):
        """Should handle NON_REF symbolic allele."""
        has_non_ref = False
        for variant in gvcf:
            if "<NON_REF>" in variant.ALT:
                has_non_ref = True
                break
        assert has_non_ref


class TestMillsIndels:
    """Tests for parsing Mills and 1000G indels."""

    def test_mills_variant_count(self):
        """Should parse all Mills indels."""
        vcf = VCF(str(FIXTURES_DIR / "mills_indels.vcf.gz"))
        variants = list(vcf)
        assert len(variants) == 14

    def test_mills_sites_only(self):
        """Mills file should be sites-only (no samples)."""
        vcf = VCF(str(FIXTURES_DIR / "mills_indels.vcf.gz"))
        assert len(vcf.samples) == 0

    def test_mills_all_indels(self):
        """All variants should be indels."""
        vcf = VCF(str(FIXTURES_DIR / "mills_indels.vcf.gz"))
        for variant in vcf:
            ref_len = len(variant.REF)
            alt_len = len(variant.ALT[0]) if variant.ALT else 0
            assert ref_len != alt_len, "Expected indel, got SNV"


class TestSVVariants:
    """Tests for parsing structural variant VCF files."""

    @pytest.fixture
    def sv_vcf(self):
        return VCF(str(FIXTURES_DIR / "genmod_sv.vcf.gz"))

    @pytest.fixture
    def sv_header(self, sv_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(sv_vcf)
        return header_parser

    def test_sv_alt_definitions(self, sv_header):
        """Should parse symbolic ALT definitions."""
        vcf = VCF(str(FIXTURES_DIR / "genmod_sv.vcf.gz"))
        sv_types = set()
        for variant in vcf:
            info = dict(variant.INFO)
            if "SVTYPE" in info:
                sv_types.add(info["SVTYPE"])

        assert len(sv_types) >= 1

    def test_sv_info_fields(self, sv_header):
        """Should parse SV-specific INFO fields."""
        svlen = sv_header.get_info_field("SVLEN")
        assert svlen is not None

        svtype = sv_header.get_info_field("SVTYPE")
        assert svtype is not None

    def test_sv_csq_annotations(self, sv_header):
        """Should have CSQ annotations for SVs."""
        csq_meta = sv_header.get_info_field("CSQ")
        assert csq_meta is not None
        assert len(sv_header.csq_fields) > 0

    def test_sv_variant_parsing(self, sv_vcf, sv_header):
        """Should parse SV variants into records."""
        variant_parser = VariantParser(sv_header)

        vcf = VCF(str(FIXTURES_DIR / "genmod_sv.vcf.gz"))
        total = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, sv_header.csq_fields)
            total += len(records)

        assert total >= 57


class TestEmptyVCF:
    """Tests for handling empty VCF files (header only, no variants)."""

    def test_empty_vcf_parsing(self):
        """Should parse empty VCF without errors."""
        vcf = VCF(str(FIXTURES_DIR / "empty.vcf.gz"))
        variants = list(vcf)
        assert len(variants) == 0

    def test_empty_vcf_header_parseable(self):
        """Should parse header from empty VCF."""
        vcf = VCF(str(FIXTURES_DIR / "empty.vcf.gz"))
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(vcf)

        assert "AC" in header_parser._info_fields
        assert "GT" in header_parser._format_fields

    def test_empty_vcf_streaming(self):
        """Streaming parser should return 0 records for empty VCF."""
        vcf_path = FIXTURES_DIR / "empty.vcf.gz"
        parser = VCFStreamingParser(vcf_path)

        total = sum(len(batch) for batch in parser.iter_batches())
        assert total == 0
        assert parser.variant_count == 0


class TestGRIDSSBreakends:
    """Tests for parsing GRIDSS structural variant VCF with BND format."""

    @pytest.fixture
    def gridss_vcf(self):
        return VCF(str(FIXTURES_DIR / "gridss_sv.vcf"))

    @pytest.fixture
    def gridss_header(self, gridss_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(gridss_vcf)
        return header_parser

    def test_gridss_variant_count(self, gridss_vcf):
        """Should parse all GRIDSS variants."""
        variants = list(gridss_vcf)
        assert len(variants) == 192

    def test_gridss_bnd_alt_formats(self, gridss_vcf):
        """Should handle BND symbolic ALT formats."""
        bnd_formats = set()
        for variant in gridss_vcf:
            for alt in variant.ALT:
                if "[" in alt:
                    bnd_formats.add("bracket_left")
                elif "]" in alt:
                    bnd_formats.add("bracket_right")
                elif alt.endswith("."):
                    bnd_formats.add("single_end_suffix")
                elif alt.startswith("."):
                    bnd_formats.add("single_end_prefix")

        assert len(bnd_formats) >= 2

    def test_gridss_mateid_field(self, gridss_header):
        """Should parse MATEID for paired breakends."""
        mateid_meta = gridss_header.get_info_field("MATEID")
        assert mateid_meta is not None
        assert mateid_meta["Type"] == "String"

    def test_gridss_many_format_fields(self, gridss_header):
        """Should handle many FORMAT fields (45+)."""
        format_count = len(gridss_header._format_fields)
        assert format_count >= 30

    def test_gridss_svtype_bnd(self, gridss_vcf):
        """All variants should be SVTYPE=BND."""
        for variant in gridss_vcf:
            info = dict(variant.INFO)
            assert info.get("SVTYPE") == "BND"

    def test_gridss_variant_parsing(self, gridss_vcf, gridss_header):
        """Should parse GRIDSS variants into records."""
        variant_parser = VariantParser(gridss_header)

        vcf = VCF(str(FIXTURES_DIR / "gridss_sv.vcf"))
        total = 0
        for variant in vcf:
            records = variant_parser.parse_variant(variant, [])
            total += len(records)

        assert total == 192


class TestNonHumanVCF:
    """Tests for parsing non-human organism VCF (SARS-CoV-2)."""

    @pytest.fixture
    def sarscov2_vcf(self):
        return VCF(str(FIXTURES_DIR / "sarscov2.vcf.gz"))

    def test_sarscov2_variant_count(self, sarscov2_vcf):
        """Should parse all SARS-CoV-2 variants."""
        variants = list(sarscov2_vcf)
        assert len(variants) == 9

    def test_nonstandard_contig_names(self, sarscov2_vcf):
        """Should handle non-chromosomal contig names."""
        variant_parser = VariantParser()

        for variant in sarscov2_vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert record.chrom == "chrMT192765.1"

    def test_bcftools_info_fields(self, sarscov2_vcf):
        """Should parse bcftools-specific INFO fields."""
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(sarscov2_vcf)

        vdb_meta = header_parser.get_info_field("VDB")
        assert vdb_meta is not None

        sgb_meta = header_parser.get_info_field("SGB")
        assert sgb_meta is not None


class TestDbSNPVCF:
    """Tests for parsing dbSNP VCF (VCF 4.0 format with many Flag fields)."""

    @pytest.fixture
    def dbsnp_vcf(self):
        return VCF(str(FIXTURES_DIR / "dbsnp_subset.vcf.gz"))

    @pytest.fixture
    def dbsnp_header(self, dbsnp_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(dbsnp_vcf)
        return header_parser

    def test_dbsnp_variant_count(self, dbsnp_vcf):
        """Should parse all dbSNP variants."""
        variants = list(dbsnp_vcf)
        assert len(variants) == 2174

    def test_dbsnp_flag_fields(self, dbsnp_header):
        """Should parse many Flag type INFO fields."""
        flag_fields = [
            "PM", "TPA", "PMC", "S3D", "SLO", "NSF", "NSM", "NSN",
            "REF", "SYN", "U3", "U5", "ASS", "DSS", "INT", "R3", "R5"
        ]
        for field in flag_fields:
            meta = dbsnp_header.get_info_field(field)
            assert meta is not None, f"Missing Flag field: {field}"
            assert meta["Type"] == "Flag"

    def test_dbsnp_rs_ids(self, dbsnp_vcf):
        """Should parse RS dbSNP IDs."""
        has_rs = False
        for variant in dbsnp_vcf:
            info = dict(variant.INFO)
            if "RS" in info:
                has_rs = True
                break
        assert has_rs

    def test_dbsnp_caf_field(self, dbsnp_header):
        """Should parse CAF population frequency strings."""
        caf_meta = dbsnp_header.get_info_field("CAF")
        assert caf_meta is not None
        assert caf_meta["Type"] == "String"

    def test_dbsnp_streaming(self):
        """Should stream dbSNP variants efficiently."""
        vcf_path = FIXTURES_DIR / "dbsnp_subset.vcf.gz"
        parser = VCFStreamingParser(vcf_path, batch_size=500)

        total = sum(len(batch) for batch in parser.iter_batches())
        assert total == 2216


class TestPacBioRepeats:
    """Tests for parsing PacBio PBSV repeat annotation VCF."""

    @pytest.fixture
    def pacbio_vcf(self):
        return VCF(str(FIXTURES_DIR / "pacbio_repeats.vcf.gz"))

    @pytest.fixture
    def pacbio_header(self, pacbio_vcf):
        header_parser = VCFHeaderParser()
        header_parser.parse_from_vcf(pacbio_vcf)
        return header_parser

    def test_pacbio_variant_count(self, pacbio_vcf):
        """Should parse PacBio repeat variant."""
        variants = list(pacbio_vcf)
        assert len(variants) == 1

    def test_pacbio_nonstandard_contig(self, pacbio_vcf):
        """Should handle contig names with colons."""
        variant_parser = VariantParser()

        for variant in pacbio_vcf:
            records = variant_parser.parse_variant(variant, [])
            for record in records:
                assert ":" in record.chrom

    def test_pacbio_svann_field(self, pacbio_header):
        """Should parse SVANN repeat annotation field."""
        svann_meta = pacbio_header.get_info_field("SVANN")
        assert svann_meta is not None

    def test_pacbio_pbsv_filters(self, pacbio_header):
        """Should parse PBSV-specific FILTER definitions."""
        vcf = VCF(str(FIXTURES_DIR / "pacbio_repeats.vcf.gz"))
        for variant in vcf:
            assert variant.FILTER is not None
