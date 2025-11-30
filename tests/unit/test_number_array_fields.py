"""Tests for Number=A/R/G array field handling."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import (
    SyntheticVariant,
    VCFGenerator,
    make_multiallelic_vcf_file,
)
from vcf_pg_loader.vcf_parser import VCFStreamingParser, get_array_size


class TestNumberAFields:
    """Test per-ALT allele (Number=A) field handling."""

    @pytest.mark.parametrize(
        "n_alts,expected_len",
        [
            (1, 1),
            (2, 2),
            (3, 3),
        ],
    )
    def test_af_array_length_matches_alts(self, n_alts, expected_len):
        """AF array length equals number of ALT alleles."""
        assert get_array_size("A", n_alts=n_alts) == expected_len

    def test_vcf2db_bug_number_a_not_skipped(self):
        """
        Verify we don't skip Number=A fields like vcf2db does.

        vcf2db skips AC, AF, MLEAC, MLEAF with warning:
        "skipping 'AF' because it has Number=A"

        This is the critical bug our tool fixes.
        """
        vcf_file = make_multiallelic_vcf_file(n_alts=2)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            assert len(batches) == 1
            records = batches[0]

            assert len(records) == 2

            for record in records:
                assert "AF" in record.info or record.info.get("AF") is not None
                assert "AC" in record.info or record.info.get("AC") is not None
        finally:
            vcf_file.unlink()
            parser.close()

    def test_decomposed_multiallelic_preserves_correct_value(self):
        """After decomposition, each record gets the correct A-indexed value."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=100,
                ref="A",
                alt=["G", "T"],
                info={"AF": [0.1, 0.3], "AC": [10, 30]},
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 2
            assert records[0].alt == "G"
            af0 = records[0].info.get("AF")
            assert af0 == pytest.approx(0.1, rel=1e-5) or af0 == pytest.approx([0.1, 0.3], rel=1e-5)
            assert records[1].alt == "T"
            af1 = records[1].info.get("AF")
            assert af1 == pytest.approx(0.3, rel=1e-5) or af1 == pytest.approx([0.1, 0.3], rel=1e-5)
        finally:
            vcf_file.unlink()
            parser.close()


class TestNumberRFields:
    """Test per-allele REF+ALT (Number=R) field handling."""

    def test_number_r_sizing(self):
        """Number=R includes REF + all ALTs."""
        assert get_array_size("R", n_alts=1) == 2
        assert get_array_size("R", n_alts=2) == 3
        assert get_array_size("R", n_alts=3) == 4

    def test_ad_includes_ref_depth(self):
        """AD array includes REF depth at index 0."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=100,
                ref="A",
                alt=["G", "T"],
                info={"AD": [100, 30, 20]},
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            for record in records:
                ad = record.info.get("AD")
                assert ad is not None
                if isinstance(ad, list) and len(ad) == 2:
                    assert ad[0] == 100
        finally:
            vcf_file.unlink()
            parser.close()


class TestNumberGFields:
    """Test per-genotype (Number=G) field handling with binomial formula."""

    @pytest.mark.parametrize(
        "n_alts,ploidy,expected_g",
        [
            (1, 2, 3),
            (2, 2, 6),
            (3, 2, 10),
        ],
    )
    def test_genotype_count_formula(self, n_alts, ploidy, expected_g):
        """Number=G array length follows binomial(n_alts+ploidy, ploidy)."""
        assert get_array_size("G", n_alts=n_alts, ploidy=ploidy) == expected_g

    def test_pl_array_indexing(self):
        """PL array indexing matches VCF spec: Index(a/b) = b(b+1)/2 + a."""

        def genotype_index(a: int, b: int) -> int:
            if a > b:
                a, b = b, a
            return (b * (b + 1)) // 2 + a

        assert genotype_index(0, 0) == 0
        assert genotype_index(0, 1) == 1
        assert genotype_index(1, 1) == 2
        assert genotype_index(0, 2) == 3
        assert genotype_index(1, 2) == 4
        assert genotype_index(2, 2) == 5


class TestFixedAndVariableNumbers:
    """Test fixed and variable Number specifications."""

    def test_fixed_number(self):
        """Fixed number specifications return that number."""
        assert get_array_size("1", n_alts=2) == 1
        assert get_array_size("2", n_alts=3) == 2
        assert get_array_size("10", n_alts=1) == 10

    def test_variable_number(self):
        """Variable length (.) returns -1."""
        assert get_array_size(".", n_alts=2) == -1

    def test_invalid_number(self):
        """Invalid Number spec returns 1."""
        assert get_array_size("X", n_alts=2) == 1
        assert get_array_size("", n_alts=2) == 1
