"""Tests for variant QC metric computation.

TDD tests for PRS-optimized on-load QC metrics including:
- Genotype counting (n_called, n_het, n_hom_ref, n_hom_alt)
- Allele frequency computation (AAF, MAF, MAC)
- Hardy-Weinberg equilibrium p-value (Wigginton et al. 2005)

Reference: Wigginton JE, Cutler DJ, Abecasis GR. A note on exact tests of
Hardy-Weinberg equilibrium. Am J Hum Genet. 2005 May;76(5):887-93.
"""

import math

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


class TestGenotypeCountsBasic:
    """Test compute_genotype_counts with basic inputs."""

    def test_all_homozygous_ref(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/0", "0/0", "0/0", "0/0"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 4
        assert n_het == 0
        assert n_hom_ref == 4
        assert n_hom_alt == 0

    def test_all_homozygous_alt(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["1/1", "1/1", "1/1"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 3
        assert n_het == 0
        assert n_hom_ref == 0
        assert n_hom_alt == 3

    def test_all_heterozygous(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/1", "0/1", "1/0", "0/1"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 4
        assert n_het == 4
        assert n_hom_ref == 0
        assert n_hom_alt == 0

    def test_mixed_genotypes(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/0", "0/1", "1/1", "0/0", "0/1"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 5
        assert n_het == 2
        assert n_hom_ref == 2
        assert n_hom_alt == 1

    def test_empty_list(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes: list[str] = []
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 0
        assert n_het == 0
        assert n_hom_ref == 0
        assert n_hom_alt == 0


class TestGenotypeCountsMissing:
    """Test compute_genotype_counts with missing data patterns."""

    def test_all_missing_unphased(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["./.", "./.", "./."]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 0
        assert n_het == 0
        assert n_hom_ref == 0
        assert n_hom_alt == 0

    def test_all_missing_phased(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = [".|.", ".|.", ".|."]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 0
        assert n_het == 0
        assert n_hom_ref == 0
        assert n_hom_alt == 0

    def test_single_dot_missing(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = [".", ".", "0/0"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 1
        assert n_hom_ref == 1

    def test_mixed_with_missing(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/0", "./.", "0/1", ".|.", "1/1"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 3
        assert n_het == 1
        assert n_hom_ref == 1
        assert n_hom_alt == 1

    def test_partial_missing_ignored(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["./1", "0/.", ".|0", "1|."]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 0


class TestGenotypeCountsPhasing:
    """Test compute_genotype_counts handles phased/unphased genotypes."""

    def test_phased_separator(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0|0", "0|1", "1|1"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 3
        assert n_het == 1
        assert n_hom_ref == 1
        assert n_hom_alt == 1

    def test_mixed_phasing(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/0", "0|1", "1/1", "1|0"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 4
        assert n_het == 2
        assert n_hom_ref == 1
        assert n_hom_alt == 1

    def test_phased_het_order_irrelevant(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0|1", "1|0"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_het == 2


class TestGenotypeCountsHaploid:
    """Test compute_genotype_counts with haploid genotypes (chrY, chrM)."""

    def test_haploid_ref(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0", "0", "0"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 3
        assert n_het == 0
        assert n_hom_ref == 3
        assert n_hom_alt == 0

    def test_haploid_alt(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["1", "1"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 2
        assert n_het == 0
        assert n_hom_ref == 0
        assert n_hom_alt == 2

    def test_haploid_mixed(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0", "1", "0", "1"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 4
        assert n_hom_ref == 2
        assert n_hom_alt == 2


class TestGenotypeCountsMultiallelic:
    """Test compute_genotype_counts with multi-allelic sites."""

    def test_second_alt_homozygous(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["2/2", "2/2"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 2
        assert n_hom_alt == 2

    def test_multiallelic_het(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["1/2", "2/3"]
        n_called, n_het, n_hom_ref, n_hom_alt = compute_genotype_counts(genotypes)

        assert n_called == 2
        assert n_het == 2


class TestAlleleFrequencies:
    """Test compute_allele_frequencies calculations."""

    def test_all_ref(self):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        aaf, maf, mac = compute_allele_frequencies(n_het=0, n_hom_ref=100, n_hom_alt=0)

        assert aaf == 0.0
        assert maf == 0.0
        assert mac == 0

    def test_all_alt(self):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        aaf, maf, mac = compute_allele_frequencies(n_het=0, n_hom_ref=0, n_hom_alt=100)

        assert aaf == 1.0
        assert maf == 0.0
        assert mac == 0

    def test_half_het(self):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        aaf, maf, mac = compute_allele_frequencies(n_het=100, n_hom_ref=0, n_hom_alt=0)

        assert aaf == 0.5
        assert maf == 0.5
        assert mac == 100

    def test_standard_case(self):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        aaf, maf, mac = compute_allele_frequencies(n_het=40, n_hom_ref=50, n_hom_alt=10)
        n_called = 100
        expected_aaf = (2 * 10 + 40) / (2 * n_called)
        expected_maf = min(expected_aaf, 1 - expected_aaf)
        expected_mac = min(2 * 10 + 40, 2 * 50 + 40)

        assert aaf == pytest.approx(expected_aaf)
        assert maf == pytest.approx(expected_maf)
        assert mac == expected_mac

    def test_rare_variant(self):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        aaf, maf, mac = compute_allele_frequencies(n_het=2, n_hom_ref=998, n_hom_alt=0)

        assert aaf == pytest.approx(0.001)
        assert maf == pytest.approx(0.001)
        assert mac == 2

    def test_zero_samples(self):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        aaf, maf, mac = compute_allele_frequencies(n_het=0, n_hom_ref=0, n_hom_alt=0)

        assert math.isnan(aaf) or aaf == 0.0
        assert math.isnan(maf) or maf == 0.0
        assert mac == 0


class TestHWEPValue:
    """Test compute_hwe_pvalue using Wigginton et al. (2005) exact test.

    Reference values from PLINK 2.0 --hardy output.
    """

    def test_perfect_hwe(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=50, n_hom_ref=25, n_hom_alt=25)

        assert p == pytest.approx(1.0, abs=0.01)

    def test_excess_heterozygotes(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=80, n_hom_ref=10, n_hom_alt=10)

        assert p < 0.001

    def test_deficit_heterozygotes(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=10, n_hom_ref=45, n_hom_alt=45)

        assert p < 0.001

    def test_rare_variant_hwe(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=20, n_hom_ref=80, n_hom_alt=0)

        assert 0.0 <= p <= 1.0

    def test_excess_het_rare_variant(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=100, n_hom_ref=400, n_hom_alt=0)

        assert p < 0.05

    def test_all_homozygous_ref(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=0, n_hom_ref=100, n_hom_alt=0)

        assert p == 1.0

    def test_all_homozygous_alt(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=0, n_hom_ref=0, n_hom_alt=100)

        assert p == 1.0

    def test_small_sample(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=2, n_hom_ref=3, n_hom_alt=1)

        assert 0.0 <= p <= 1.0

    def test_zero_samples(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        p = compute_hwe_pvalue(n_het=0, n_hom_ref=0, n_hom_alt=0)

        assert math.isnan(p) or p == 1.0


class TestCallRate:
    """Test call rate computation."""

    def test_call_rate_all_called(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/0", "0/1", "1/1", "0/0"]
        n_called, _, _, _ = compute_genotype_counts(genotypes)
        call_rate = n_called / len(genotypes)

        assert call_rate == 1.0

    def test_call_rate_half_missing(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/0", "./.", "1/1", "./."]
        n_called, _, _, _ = compute_genotype_counts(genotypes)
        call_rate = n_called / len(genotypes)

        assert call_rate == 0.5

    def test_call_rate_all_missing(self):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["./.", "./.", "./."]
        n_called, _, _, _ = compute_genotype_counts(genotypes)
        call_rate = n_called / len(genotypes) if genotypes else 0.0

        assert call_rate == 0.0


class TestPropertyBased:
    """Property-based tests using hypothesis."""

    @given(
        n_hom_ref=st.integers(min_value=0, max_value=1000),
        n_het=st.integers(min_value=0, max_value=1000),
        n_hom_alt=st.integers(min_value=0, max_value=1000),
    )
    @settings(max_examples=100)
    def test_genotype_count_invariant(self, n_hom_ref, n_het, n_hom_alt):
        from vcf_pg_loader.qc.variant_qc import compute_genotype_counts

        genotypes = ["0/0"] * n_hom_ref + ["0/1"] * n_het + ["1/1"] * n_hom_alt

        result_called, result_het, result_hom_ref, result_hom_alt = compute_genotype_counts(
            genotypes
        )

        assert result_called == result_het + result_hom_ref + result_hom_alt
        assert result_hom_ref == n_hom_ref
        assert result_het == n_het
        assert result_hom_alt == n_hom_alt

    @given(
        n_hom_ref=st.integers(min_value=0, max_value=500),
        n_het=st.integers(min_value=0, max_value=500),
        n_hom_alt=st.integers(min_value=0, max_value=500),
    )
    @settings(max_examples=100)
    def test_aaf_bounds(self, n_hom_ref, n_het, n_hom_alt):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        if n_hom_ref + n_het + n_hom_alt == 0:
            return

        aaf, maf, mac = compute_allele_frequencies(n_het, n_hom_ref, n_hom_alt)

        assert 0.0 <= aaf <= 1.0
        assert 0.0 <= maf <= 0.5
        assert mac >= 0

    @given(
        n_hom_ref=st.integers(min_value=0, max_value=100),
        n_het=st.integers(min_value=0, max_value=100),
        n_hom_alt=st.integers(min_value=0, max_value=100),
    )
    @settings(max_examples=50)
    def test_hwe_bounds(self, n_hom_ref, n_het, n_hom_alt):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        if n_hom_ref + n_het + n_hom_alt == 0:
            return

        p = compute_hwe_pvalue(n_het, n_hom_ref, n_hom_alt)

        if not math.isnan(p):
            assert 0.0 <= p <= 1.0

    @given(
        n_hom_ref=st.integers(min_value=1, max_value=100),
        n_het=st.integers(min_value=0, max_value=100),
        n_hom_alt=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=50)
    def test_maf_symmetry(self, n_hom_ref, n_het, n_hom_alt):
        from vcf_pg_loader.qc.variant_qc import compute_allele_frequencies

        aaf1, maf1, _ = compute_allele_frequencies(n_het, n_hom_ref, n_hom_alt)
        aaf2, maf2, _ = compute_allele_frequencies(n_het, n_hom_alt, n_hom_ref)

        assert maf1 == pytest.approx(maf2)
        assert aaf1 == pytest.approx(1.0 - aaf2)
