"""TDD tests for SQL validation functions.

Tests for in-database validation functions:
- hwe_exact_test: Hardy-Weinberg equilibrium exact test (Wigginton et al. 2005)
- af_from_dosages: Allele frequency from dosage array
- n_eff: Effective sample size for case-control
- alleles_match: Allele harmonization check with strand flip support
"""

import math

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
async def pg_pool():
    """Create a test PostgreSQL connection pool using testcontainers."""
    import asyncpg
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:15") as postgres:
        pool = await asyncpg.create_pool(
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            user=postgres.username,
            password=postgres.password,
            database=postgres.dbname,
            min_size=1,
            max_size=5,
        )
        yield pool
        await pool.close()


@pytest.fixture
async def db_with_functions(pg_pool):
    """Set up database with SQL validation functions."""
    from vcf_pg_loader.validation.sql_functions import create_validation_functions

    async with pg_pool.acquire() as conn:
        await create_validation_functions(conn)
    yield pg_pool


class TestHWEExactTestSQL:
    """Test hwe_exact_test SQL function."""

    async def test_function_exists(self, db_with_functions):
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_proc WHERE proname = 'hwe_exact_test'
                )
            """)
            assert result is True

    async def test_perfect_hwe(self, db_with_functions):
        """Test perfect HWE equilibrium returns p ≈ 1.0."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(25, 50, 25)")
            assert p == pytest.approx(1.0, abs=0.01)

    async def test_excess_heterozygotes(self, db_with_functions):
        """Excess heterozygotes should have low p-value."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(10, 80, 10)")
            assert p < 0.001

    async def test_deficit_heterozygotes(self, db_with_functions):
        """Deficit of heterozygotes should have low p-value."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(45, 10, 45)")
            assert p < 0.001

    async def test_all_homozygous_ref(self, db_with_functions):
        """All homozygous ref returns p = 1.0."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(100, 0, 0)")
            assert p == 1.0

    async def test_all_homozygous_alt(self, db_with_functions):
        """All homozygous alt returns p = 1.0."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(0, 0, 100)")
            assert p == 1.0

    async def test_rare_variant(self, db_with_functions):
        """Rare variant with expected het count."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(80, 20, 0)")
            assert 0.0 <= p <= 1.0

    async def test_small_sample(self, db_with_functions):
        """Small sample size returns valid p-value."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(3, 2, 1)")
            assert 0.0 <= p <= 1.0

    async def test_zero_samples_returns_null(self, db_with_functions):
        """Zero samples returns NULL."""
        async with db_with_functions.acquire() as conn:
            p = await conn.fetchval("SELECT hwe_exact_test(0, 0, 0)")
            assert p is None

    async def test_matches_python_implementation(self, db_with_functions):
        """SQL function matches Python reference implementation."""
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue

        test_cases = [
            (50, 40, 10),
            (25, 50, 25),
            (90, 10, 0),
            (40, 40, 20),
        ]

        async with db_with_functions.acquire() as conn:
            for n_aa, n_ab, n_bb in test_cases:
                sql_p = await conn.fetchval("SELECT hwe_exact_test($1, $2, $3)", n_aa, n_ab, n_bb)
                py_p = compute_hwe_pvalue(n_ab, n_aa, n_bb)
                if not math.isnan(py_p):
                    assert sql_p == pytest.approx(
                        py_p, abs=0.001
                    ), f"Mismatch for ({n_aa}, {n_ab}, {n_bb}): SQL={sql_p}, Python={py_p}"


class TestAFFromDosagesSQL:
    """Test af_from_dosages SQL function."""

    async def test_function_exists(self, db_with_functions):
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_proc WHERE proname = 'af_from_dosages'
                )
            """)
            assert result is True

    async def test_all_ref_dosages(self, db_with_functions):
        """All zero dosages (0/0) returns AF = 0."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[0.0, 0.0, 0.0, 0.0])")
            assert af == 0.0

    async def test_all_alt_dosages(self, db_with_functions):
        """All 2.0 dosages (1/1) returns AF = 1.0."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[2.0, 2.0, 2.0, 2.0])")
            assert af == 1.0

    async def test_all_het_dosages(self, db_with_functions):
        """All 1.0 dosages (0/1) returns AF = 0.5."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[1.0, 1.0, 1.0, 1.0])")
            assert af == 0.5

    async def test_mixed_dosages(self, db_with_functions):
        """Mixed dosages returns correct AF."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[0.0, 1.0, 2.0, 1.0])")
            expected = (0.0 + 1.0 + 2.0 + 1.0) / 8.0
            assert af == pytest.approx(expected)

    async def test_with_nulls(self, db_with_functions):
        """NULL values are excluded from calculation."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[0.0, NULL, 2.0, NULL])")
            expected = (0.0 + 2.0) / 4.0
            assert af == pytest.approx(expected)

    async def test_empty_array(self, db_with_functions):
        """Empty array returns NULL."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[]::FLOAT[])")
            assert af is None

    async def test_all_nulls(self, db_with_functions):
        """All NULL values returns NULL."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[NULL, NULL, NULL]::FLOAT[])")
            assert af is None

    async def test_imputation_dosages(self, db_with_functions):
        """Imputation-style fractional dosages work correctly."""
        async with db_with_functions.acquire() as conn:
            af = await conn.fetchval("SELECT af_from_dosages(ARRAY[0.1, 0.9, 1.8, 0.2])")
            expected = (0.1 + 0.9 + 1.8 + 0.2) / 8.0
            assert af == pytest.approx(expected)


class TestNEffSQL:
    """Test n_eff SQL function for effective sample size."""

    async def test_function_exists(self, db_with_functions):
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_proc WHERE proname = 'n_eff'
                )
            """)
            assert result is True

    async def test_balanced_case_control(self, db_with_functions):
        """Balanced case-control (1:1 ratio) returns N_cases + N_controls."""
        async with db_with_functions.acquire() as conn:
            n_eff = await conn.fetchval("SELECT n_eff(1000, 1000)")
            expected = 4.0 * 1000 * 1000 / 2000
            assert n_eff == pytest.approx(expected)

    async def test_unbalanced_case_control(self, db_with_functions):
        """Unbalanced case-control returns correct effective N."""
        async with db_with_functions.acquire() as conn:
            n_eff = await conn.fetchval("SELECT n_eff(500, 2000)")
            expected = 4.0 * 500 * 2000 / 2500
            assert n_eff == pytest.approx(expected)

    async def test_extreme_imbalance(self, db_with_functions):
        """Extreme imbalance (1:100) returns low effective N."""
        async with db_with_functions.acquire() as conn:
            n_eff = await conn.fetchval("SELECT n_eff(100, 10000)")
            expected = 4.0 * 100 * 10000 / 10100
            assert n_eff == pytest.approx(expected)
            assert n_eff < 400

    async def test_zero_cases_returns_null(self, db_with_functions):
        """Zero cases returns NULL."""
        async with db_with_functions.acquire() as conn:
            n_eff = await conn.fetchval("SELECT n_eff(0, 1000)")
            assert n_eff is None

    async def test_zero_controls_returns_null(self, db_with_functions):
        """Zero controls returns NULL."""
        async with db_with_functions.acquire() as conn:
            n_eff = await conn.fetchval("SELECT n_eff(1000, 0)")
            assert n_eff is None

    async def test_both_zero_returns_null(self, db_with_functions):
        """Both zero returns NULL."""
        async with db_with_functions.acquire() as conn:
            n_eff = await conn.fetchval("SELECT n_eff(0, 0)")
            assert n_eff is None

    async def test_small_study(self, db_with_functions):
        """Small study size calculates correctly."""
        async with db_with_functions.acquire() as conn:
            n_eff = await conn.fetchval("SELECT n_eff(50, 50)")
            expected = 4.0 * 50 * 50 / 100
            assert n_eff == pytest.approx(expected)


class TestAllelesMatchSQL:
    """Test alleles_match SQL function for allele harmonization."""

    async def test_function_exists(self, db_with_functions):
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_proc WHERE proname = 'alleles_match'
                )
            """)
            assert result is True

    async def test_exact_match(self, db_with_functions):
        """Exact allele match returns TRUE."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('A', 'G', 'A', 'G')")
            assert result is True

    async def test_swapped_ref_alt(self, db_with_functions):
        """Swapped ref/alt returns TRUE."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('A', 'G', 'G', 'A')")
            assert result is True

    async def test_strand_flip_match(self, db_with_functions):
        """Strand flip (complement) returns TRUE."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('A', 'G', 'T', 'C')")
            assert result is True

    async def test_strand_flip_swapped(self, db_with_functions):
        """Strand flip with swapped ref/alt returns TRUE."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('A', 'G', 'C', 'T')")
            assert result is True

    async def test_no_match(self, db_with_functions):
        """Non-matching alleles return FALSE."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('A', 'G', 'A', 'C')")
            assert result is False

    async def test_case_insensitive(self, db_with_functions):
        """Case-insensitive matching."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('a', 'g', 'A', 'G')")
            assert result is True

    async def test_indel_exact_match(self, db_with_functions):
        """Indel exact match returns TRUE."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('ATG', 'A', 'ATG', 'A')")
            assert result is True

    async def test_indel_swapped(self, db_with_functions):
        """Indel swapped returns TRUE."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('ATG', 'A', 'A', 'ATG')")
            assert result is True

    async def test_ambiguous_snp_a_t(self, db_with_functions):
        """A/T SNP is ambiguous but still matches itself."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('A', 'T', 'A', 'T')")
            assert result is True

    async def test_ambiguous_snp_c_g(self, db_with_functions):
        """C/G SNP is ambiguous but still matches itself."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match('C', 'G', 'C', 'G')")
            assert result is True

    async def test_null_returns_null(self, db_with_functions):
        """NULL input returns NULL."""
        async with db_with_functions.acquire() as conn:
            result = await conn.fetchval("SELECT alleles_match(NULL, 'G', 'A', 'G')")
            assert result is None


class TestPythonReferenceImplementations:
    """Test Python reference implementations match expected values."""

    def test_hwe_exact_test_python_import(self):
        from vcf_pg_loader.validation.sql_functions import hwe_exact_test_python

        p = hwe_exact_test_python(25, 50, 25)
        assert p == pytest.approx(1.0, abs=0.01)

    def test_hwe_exact_test_python_matches_qc_module(self):
        from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue
        from vcf_pg_loader.validation.sql_functions import hwe_exact_test_python

        for n_aa, n_ab, n_bb in [(50, 40, 10), (25, 50, 25), (10, 80, 10)]:
            p1 = hwe_exact_test_python(n_aa, n_ab, n_bb)
            p2 = compute_hwe_pvalue(n_ab, n_aa, n_bb)
            if not math.isnan(p2):
                assert p1 == pytest.approx(p2, abs=0.001)

    def test_af_from_dosages_python(self):
        from vcf_pg_loader.validation.sql_functions import af_from_dosages_python

        assert af_from_dosages_python([0.0, 0.0, 0.0, 0.0]) == 0.0
        assert af_from_dosages_python([2.0, 2.0, 2.0, 2.0]) == 1.0
        assert af_from_dosages_python([1.0, 1.0, 1.0, 1.0]) == 0.5

    def test_n_eff_python(self):
        from vcf_pg_loader.validation.sql_functions import n_eff_python

        assert n_eff_python(1000, 1000) == 2000.0
        assert n_eff_python(500, 2000) == pytest.approx(1600.0)
        assert n_eff_python(0, 1000) is None

    def test_alleles_match_python(self):
        from vcf_pg_loader.validation.sql_functions import alleles_match_python

        assert alleles_match_python("A", "G", "A", "G") is True
        assert alleles_match_python("A", "G", "G", "A") is True
        assert alleles_match_python("A", "G", "T", "C") is True
        assert alleles_match_python("A", "G", "A", "C") is False
