"""
Echtvar compatibility tests for vcf-pg-loader.

These tests verify that vcf-pg-loader correctly handles edge cases
identified by echtvar's test suite. Test patterns derived from echtvar
(https://github.com/brentp/echtvar) under MIT License.

See tests/vendored/echtvar/ATTRIBUTION.md for full attribution.
"""

import pytest

from vendored.echtvar import (
    count_variants_by_filter,
    generate_no_chr_prefix_vcf,
    generate_string_vcf_content,
    generate_vcf_content,
    get_expected_filters,
    get_expected_variant_count,
    get_variant_info,
    validate_string_vcf,
)


class TestEchtvarVCFGeneration:
    """Test that VCF generators produce valid output."""

    def test_generate_vcf_content_returns_all_types(self):
        vcfs = generate_vcf_content(mod=2)
        assert "all" in vcfs
        assert "subset0" in vcfs
        assert "subset1" in vcfs

    def test_generated_vcf_has_header(self):
        vcfs = generate_vcf_content(mod=2)
        for name, content in vcfs.items():
            assert content.startswith("##fileformat=VCFv4.2"), f"{name} missing fileformat"
            assert "#CHROM\tPOS\tID\tREF\tALT" in content, f"{name} missing column header"

    def test_generated_vcf_has_variants(self):
        vcfs = generate_vcf_content(mod=2)
        for name, content in vcfs.items():
            data_lines = [ln for ln in content.split("\n") if ln and not ln.startswith("#")]
            assert len(data_lines) > 0, f"{name} has no variants"

    def test_variant_counts_match_expected(self):
        counts = get_expected_variant_count(mod=2)
        vcfs = generate_vcf_content(mod=2)

        for name, content in vcfs.items():
            data_lines = [ln for ln in content.split("\n") if ln and not ln.startswith("#")]
            assert len(data_lines) == counts[name], f"{name} count mismatch"

    def test_no_chr_prefix_conversion(self):
        vcfs = generate_vcf_content(mod=2)
        no_prefix = generate_no_chr_prefix_vcf(vcfs["all"])

        assert "chr1\t" not in no_prefix
        data_lines = [ln for ln in no_prefix.split("\n") if ln and not ln.startswith("#")]
        assert all(ln.startswith("1\t") for ln in data_lines)

    def test_get_variant_info_extracts_fields(self):
        vcfs = generate_vcf_content(mod=2)
        info = get_variant_info(vcfs["all"], 0)

        assert info is not None
        assert info["chrom"] == "chr1"
        assert info["pos"] > 0
        assert info["ref"] in ["A", "C", "G", "T"]
        assert "val" in info["info"]


class TestEchtvarStringVCF:
    """Test string/categorical field VCF generation."""

    def test_string_vcf_generates(self):
        content = generate_string_vcf_content(seed=42)
        assert content.startswith("##fileformat=VCFv4.2")

    def test_string_vcf_has_filter_definitions(self):
        content = generate_string_vcf_content(seed=42)
        assert "##FILTER=<ID=PASS" in content
        assert "##FILTER=<ID=FAIL" in content
        assert "##FILTER=<ID=OTHER" in content

    def test_string_vcf_validation_passes(self):
        content = generate_string_vcf_content(seed=42)
        errors = validate_string_vcf(content)
        assert errors == [], f"Validation errors: {errors}"

    def test_filter_distribution(self):
        content = generate_string_vcf_content(seed=42)
        counts = count_variants_by_filter(content)

        for filt in get_expected_filters():
            assert counts[filt] > 0, f"No variants with FILTER={filt}"


class TestChrPrefixHandling:
    """Test chromosome prefix handling (chr1 vs 1)."""

    @pytest.fixture
    def vcf_with_chr_prefix(self, tmp_path):
        vcfs = generate_vcf_content(mod=2)
        path = tmp_path / "with_chr.vcf"
        path.write_text(vcfs["all"])
        return path

    @pytest.fixture
    def vcf_without_chr_prefix(self, tmp_path):
        vcfs = generate_vcf_content(mod=2)
        no_prefix = generate_no_chr_prefix_vcf(vcfs["all"])
        path = tmp_path / "without_chr.vcf"
        path.write_text(no_prefix)
        return path

    def test_parse_vcf_with_chr_prefix(self, vcf_with_chr_prefix):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(vcf_with_chr_prefix, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert len(records) > 0
        assert all(r.chrom.startswith("chr") for r in records)

    def test_parse_vcf_without_chr_prefix_human_genome_mode(self, vcf_without_chr_prefix):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(vcf_without_chr_prefix, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert len(records) > 0
        assert all(r.chrom.startswith("chr") for r in records)

    def test_parse_vcf_without_chr_prefix_non_human_mode(self, vcf_without_chr_prefix):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(vcf_without_chr_prefix, human_genome=False)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert len(records) > 0
        assert all(r.chrom == "1" for r in records)


class TestLongVariants:
    """Test handling of long REF/ALT alleles (>4 bases combined)."""

    @pytest.fixture
    def vcf_with_long_variants(self, tmp_path):
        vcfs = generate_vcf_content(mod=2)
        path = tmp_path / "long_variants.vcf"
        path.write_text(vcfs["all"])
        return path

    def test_long_ref_alleles_parsed(self, vcf_with_long_variants):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(vcf_with_long_variants, human_genome=False)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        long_ref_records = [r for r in records if len(r.ref) > 10]
        assert len(long_ref_records) > 0, "Should have long REF alleles"

    def test_long_alt_alleles_parsed(self, vcf_with_long_variants):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(vcf_with_long_variants, human_genome=False)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        long_alt_records = [r for r in records if len(r.alt) > 10]
        assert len(long_alt_records) > 0, "Should have long ALT alleles"


class TestFilterExtraction:
    """Test FILTER field extraction."""

    @pytest.fixture
    def vcf_with_filters(self, tmp_path):
        content = generate_string_vcf_content(seed=42)
        path = tmp_path / "filters.vcf"
        path.write_text(content)
        return path

    def test_filter_field_extracted(self, vcf_with_filters):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(vcf_with_filters, human_genome=False)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert len(records) > 0

        pass_records = [r for r in records if len(r.filter) == 0]
        fail_records = [r for r in records if "FAIL" in r.filter]
        other_records = [r for r in records if "OTHER" in r.filter]

        assert len(pass_records) > 0, "Should have PASS filter records (empty filter list)"
        assert len(fail_records) > 0, "Should have FAIL filter records"
        assert len(other_records) > 0, "Should have OTHER filter records"


class TestNumberAFieldExtraction:
    """Test Number=A field extraction for multi-allelic variants."""

    @pytest.fixture
    def multiallelic_vcf(self, tmp_path):
        content = """##fileformat=VCFv4.2
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tC,G,T\t30\tPASS\tAC=10,20,30;AF=0.1,0.2,0.3
chr1\t200\t.\tG\tA,C\t30\tPASS\tAC=5,15;AF=0.05,0.15
"""
        path = tmp_path / "multiallelic.vcf"
        path.write_text(content)
        return path

    def test_number_a_fields_decomposed(self, multiallelic_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(multiallelic_vcf, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert len(records) == 5

        chr1_100_records = [r for r in records if r.pos == 100]
        assert len(chr1_100_records) == 3

        c_record = next(r for r in chr1_100_records if r.alt == "C")
        g_record = next(r for r in chr1_100_records if r.alt == "G")
        t_record = next(r for r in chr1_100_records if r.alt == "T")

        assert c_record.info.get("AC") == 10
        assert g_record.info.get("AC") == 20
        assert t_record.info.get("AC") == 30


class TestInfoFieldTypes:
    """Test various INFO field type handling."""

    @pytest.fixture
    def typed_info_vcf(self, tmp_path):
        content = """##fileformat=VCFv4.2
##INFO=<ID=INT_VAL,Number=1,Type=Integer,Description="Integer value">
##INFO=<ID=FLOAT_VAL,Number=1,Type=Float,Description="Float value">
##INFO=<ID=STR_VAL,Number=.,Type=String,Description="String value">
##INFO=<ID=FLAG_VAL,Number=0,Type=Flag,Description="Flag value">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tC\t30\tPASS\tINT_VAL=42;FLOAT_VAL=0.123;STR_VAL=hello;FLAG_VAL
chr1\t200\t.\tG\tT\t30\tPASS\tINT_VAL=-10;FLOAT_VAL=1.5e-4;STR_VAL=world
"""
        path = tmp_path / "typed_info.vcf"
        path.write_text(content)
        return path

    def test_integer_fields_parsed(self, typed_info_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(typed_info_vcf, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert records[0].info.get("INT_VAL") == 42
        assert records[1].info.get("INT_VAL") == -10

    def test_float_fields_parsed(self, typed_info_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(typed_info_vcf, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert abs(records[0].info.get("FLOAT_VAL") - 0.123) < 0.001
        assert abs(records[1].info.get("FLOAT_VAL") - 1.5e-4) < 0.0001

    def test_string_fields_parsed(self, typed_info_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(typed_info_vcf, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        assert records[0].info.get("STR_VAL") == "hello"
        assert records[1].info.get("STR_VAL") == "world"


class TestBulkVariantParsing:
    """Test parsing large numbers of variants (echtvar big.sh equivalent)."""

    @pytest.fixture
    def large_vcf(self, tmp_path):
        vcfs = generate_vcf_content(mod=2, seed=42)
        path = tmp_path / "large.vcf"
        path.write_text(vcfs["all"])
        return path

    def test_large_vcf_parses_completely(self, large_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(large_vcf, human_genome=False)
        record_count = 0
        for batch in parser.iter_batches():
            record_count += len(batch)
        parser.close()

        expected = get_expected_variant_count(mod=2)["all"]
        assert record_count == expected

    def test_batch_iteration_works(self, large_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(large_vcf, batch_size=100, human_genome=False)
        batch_count = 0
        total_records = 0
        for batch in parser.iter_batches():
            batch_count += 1
            total_records += len(batch)
        parser.close()

        assert batch_count > 1, "Should have multiple batches"
        assert total_records > 0


class TestVariantNormalization:
    """Test variant normalization with echtvar-style variants."""

    @pytest.fixture
    def normalizable_vcf(self, tmp_path):
        content = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tCAA\tCA\t30\tPASS\t.
chr1\t200\t.\tG\tGA\t30\tPASS\t.
chr1\t300\t.\tATG\tA\t30\tPASS\t.
"""
        path = tmp_path / "normalizable.vcf"
        path.write_text(content)
        return path

    def test_variants_normalized(self, normalizable_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(normalizable_vcf, normalize=True, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        normalized_records = [r for r in records if r.normalized]
        assert len(normalized_records) >= 1

    def test_original_coordinates_preserved(self, normalizable_vcf):
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(normalizable_vcf, normalize=True, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        for r in records:
            if r.normalized:
                assert r.original_pos is not None
                assert r.original_ref is not None
                assert r.original_alt is not None
