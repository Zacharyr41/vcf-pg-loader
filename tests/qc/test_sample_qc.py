"""Tests for sample-level QC metric computation.

TDD tests for sample QC metrics including:
- Call rate computation
- Het/hom ratio calculation
- Ti/Tv ratio calculation
- Sex inference from X chromosome heterozygosity
- F_inbreeding computation

Reference: Pe'er pipeline filters samples on call rate >99%,
contamination <2.5%, and sex verification.
"""

import math

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


class TestSampleCallRate:
    """Test compute_sample_call_rate with various missing data patterns."""

    def test_all_called(self):
        from vcf_pg_loader.qc.sample_qc import compute_sample_call_rate

        call_rate = compute_sample_call_rate(n_called=1000, n_total=1000)
        assert call_rate == 1.0

    def test_half_missing(self):
        from vcf_pg_loader.qc.sample_qc import compute_sample_call_rate

        call_rate = compute_sample_call_rate(n_called=500, n_total=1000)
        assert call_rate == 0.5

    def test_all_missing(self):
        from vcf_pg_loader.qc.sample_qc import compute_sample_call_rate

        call_rate = compute_sample_call_rate(n_called=0, n_total=1000)
        assert call_rate == 0.0

    def test_zero_total_returns_zero(self):
        from vcf_pg_loader.qc.sample_qc import compute_sample_call_rate

        call_rate = compute_sample_call_rate(n_called=0, n_total=0)
        assert call_rate == 0.0

    def test_high_call_rate_threshold(self):
        from vcf_pg_loader.qc.sample_qc import compute_sample_call_rate

        call_rate = compute_sample_call_rate(n_called=990, n_total=1000)
        assert call_rate == 0.99
        assert call_rate >= 0.99

    def test_just_below_threshold(self):
        from vcf_pg_loader.qc.sample_qc import compute_sample_call_rate

        call_rate = compute_sample_call_rate(n_called=989, n_total=1000)
        assert call_rate == pytest.approx(0.989)
        assert call_rate < 0.99


class TestHetHomRatio:
    """Test compute_het_hom_ratio calculation."""

    def test_equal_het_hom(self):
        from vcf_pg_loader.qc.sample_qc import compute_het_hom_ratio

        ratio = compute_het_hom_ratio(n_het=100, n_hom_var=100)
        assert ratio == 1.0

    def test_double_het(self):
        from vcf_pg_loader.qc.sample_qc import compute_het_hom_ratio

        ratio = compute_het_hom_ratio(n_het=200, n_hom_var=100)
        assert ratio == 2.0

    def test_zero_hom_returns_none(self):
        from vcf_pg_loader.qc.sample_qc import compute_het_hom_ratio

        ratio = compute_het_hom_ratio(n_het=100, n_hom_var=0)
        assert ratio is None

    def test_zero_het(self):
        from vcf_pg_loader.qc.sample_qc import compute_het_hom_ratio

        ratio = compute_het_hom_ratio(n_het=0, n_hom_var=100)
        assert ratio == 0.0

    def test_both_zero_returns_none(self):
        from vcf_pg_loader.qc.sample_qc import compute_het_hom_ratio

        ratio = compute_het_hom_ratio(n_het=0, n_hom_var=0)
        assert ratio is None

    def test_typical_values(self):
        from vcf_pg_loader.qc.sample_qc import compute_het_hom_ratio

        ratio = compute_het_hom_ratio(n_het=150000, n_hom_var=100000)
        assert ratio == pytest.approx(1.5)


class TestClassifyTransitionTransversion:
    """Test classify_transition_transversion for SNPs."""

    def test_a_to_g_transition(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("A", "G") == "transition"

    def test_g_to_a_transition(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("G", "A") == "transition"

    def test_c_to_t_transition(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("C", "T") == "transition"

    def test_t_to_c_transition(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("T", "C") == "transition"

    def test_a_to_c_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("A", "C") == "transversion"

    def test_a_to_t_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("A", "T") == "transversion"

    def test_g_to_c_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("G", "C") == "transversion"

    def test_g_to_t_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("G", "T") == "transversion"

    def test_c_to_a_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("C", "A") == "transversion"

    def test_c_to_g_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("C", "G") == "transversion"

    def test_t_to_a_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("T", "A") == "transversion"

    def test_t_to_g_transversion(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("T", "G") == "transversion"

    def test_indel_returns_none(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("A", "AT") is None
        assert classify_transition_transversion("AT", "A") is None

    def test_same_allele_returns_none(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("A", "A") is None

    def test_lowercase_normalized(self):
        from vcf_pg_loader.qc.sample_qc import classify_transition_transversion

        assert classify_transition_transversion("a", "g") == "transition"


class TestTiTvRatio:
    """Test compute_ti_tv_ratio calculation."""

    def test_typical_ratio(self):
        from vcf_pg_loader.qc.sample_qc import compute_ti_tv_ratio

        ratio = compute_ti_tv_ratio(transitions=200000, transversions=100000)
        assert ratio == 2.0

    def test_equal_ti_tv(self):
        from vcf_pg_loader.qc.sample_qc import compute_ti_tv_ratio

        ratio = compute_ti_tv_ratio(transitions=100, transversions=100)
        assert ratio == 1.0

    def test_zero_transversions_returns_none(self):
        from vcf_pg_loader.qc.sample_qc import compute_ti_tv_ratio

        ratio = compute_ti_tv_ratio(transitions=100, transversions=0)
        assert ratio is None

    def test_zero_transitions(self):
        from vcf_pg_loader.qc.sample_qc import compute_ti_tv_ratio

        ratio = compute_ti_tv_ratio(transitions=0, transversions=100)
        assert ratio == 0.0

    def test_both_zero_returns_none(self):
        from vcf_pg_loader.qc.sample_qc import compute_ti_tv_ratio

        ratio = compute_ti_tv_ratio(transitions=0, transversions=0)
        assert ratio is None

    def test_expected_human_genome_range(self):
        from vcf_pg_loader.qc.sample_qc import compute_ti_tv_ratio

        ratio = compute_ti_tv_ratio(transitions=200000, transversions=100000)
        assert 1.5 <= ratio <= 3.5


class TestSexInference:
    """Test infer_sex_from_x_het for sex determination."""

    def test_low_het_infers_male(self):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        sex = infer_sex_from_x_het(x_het_rate=0.01)
        assert sex == "M"

    def test_high_het_infers_female(self):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        sex = infer_sex_from_x_het(x_het_rate=0.20)
        assert sex == "F"

    def test_borderline_returns_unknown(self):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        sex = infer_sex_from_x_het(x_het_rate=0.10)
        assert sex == "unknown"

    def test_zero_het_male(self):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        sex = infer_sex_from_x_het(x_het_rate=0.0)
        assert sex == "M"

    def test_very_high_het_female(self):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        sex = infer_sex_from_x_het(x_het_rate=0.30)
        assert sex == "F"

    def test_male_threshold_boundary(self):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        assert infer_sex_from_x_het(x_het_rate=0.05) == "M"
        assert infer_sex_from_x_het(x_het_rate=0.06) == "unknown"

    def test_female_threshold_boundary(self):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        assert infer_sex_from_x_het(x_het_rate=0.14) == "unknown"
        assert infer_sex_from_x_het(x_het_rate=0.15) == "F"


class TestFInbreeding:
    """Test compute_f_inbreeding calculation.

    F = 1 - (observed_het / expected_het)

    Positive F indicates excess homozygosity (inbreeding)
    Negative F indicates excess heterozygosity (outbreeding/contamination)
    """

    def test_no_inbreeding(self):
        from vcf_pg_loader.qc.sample_qc import compute_f_inbreeding

        f = compute_f_inbreeding(observed_het=100, expected_het=100)
        assert f == pytest.approx(0.0)

    def test_positive_inbreeding(self):
        from vcf_pg_loader.qc.sample_qc import compute_f_inbreeding

        f = compute_f_inbreeding(observed_het=80, expected_het=100)
        assert f == pytest.approx(0.2)

    def test_negative_f_excess_het(self):
        from vcf_pg_loader.qc.sample_qc import compute_f_inbreeding

        f = compute_f_inbreeding(observed_het=120, expected_het=100)
        assert f == pytest.approx(-0.2)

    def test_zero_expected_returns_nan(self):
        from vcf_pg_loader.qc.sample_qc import compute_f_inbreeding

        f = compute_f_inbreeding(observed_het=100, expected_het=0)
        assert math.isnan(f)

    def test_complete_inbreeding(self):
        from vcf_pg_loader.qc.sample_qc import compute_f_inbreeding

        f = compute_f_inbreeding(observed_het=0, expected_het=100)
        assert f == pytest.approx(1.0)

    def test_typical_human_values(self):
        from vcf_pg_loader.qc.sample_qc import compute_f_inbreeding

        f = compute_f_inbreeding(observed_het=150000, expected_het=155000)
        assert -0.1 < f < 0.1


class TestQCPassLogic:
    """Test the QC pass criteria logic.

    QC pass requires:
    - call_rate >= 0.99
    - contamination_estimate < 0.025 (or NULL)
    - sex_concordant = TRUE (or NULL)
    """

    def test_passes_all_criteria(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        result = evaluate_qc_pass(
            call_rate=0.995,
            contamination_estimate=0.01,
            sex_concordant=True,
        )
        assert result is True

    def test_fails_call_rate(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        result = evaluate_qc_pass(
            call_rate=0.985,
            contamination_estimate=0.01,
            sex_concordant=True,
        )
        assert result is False

    def test_fails_contamination(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        result = evaluate_qc_pass(
            call_rate=0.995,
            contamination_estimate=0.03,
            sex_concordant=True,
        )
        assert result is False

    def test_fails_sex_concordance(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        result = evaluate_qc_pass(
            call_rate=0.995,
            contamination_estimate=0.01,
            sex_concordant=False,
        )
        assert result is False

    def test_null_contamination_passes(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        result = evaluate_qc_pass(
            call_rate=0.995,
            contamination_estimate=None,
            sex_concordant=True,
        )
        assert result is True

    def test_null_sex_concordance_passes(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        result = evaluate_qc_pass(
            call_rate=0.995,
            contamination_estimate=0.01,
            sex_concordant=None,
        )
        assert result is True

    def test_all_nulls_except_call_rate(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        result = evaluate_qc_pass(
            call_rate=0.995,
            contamination_estimate=None,
            sex_concordant=None,
        )
        assert result is True

    def test_boundary_call_rate(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        assert evaluate_qc_pass(call_rate=0.99) is True
        assert evaluate_qc_pass(call_rate=0.9899) is False

    def test_boundary_contamination(self):
        from vcf_pg_loader.qc.sample_qc import evaluate_qc_pass

        assert evaluate_qc_pass(call_rate=0.99, contamination_estimate=0.024) is True
        assert evaluate_qc_pass(call_rate=0.99, contamination_estimate=0.025) is False


class TestPropertyBased:
    """Property-based tests using hypothesis."""

    @given(
        n_called=st.integers(min_value=0, max_value=1000000),
        n_total=st.integers(min_value=1, max_value=1000000),
    )
    @settings(max_examples=100)
    def test_call_rate_bounds(self, n_called, n_total):
        from vcf_pg_loader.qc.sample_qc import compute_sample_call_rate

        if n_called > n_total:
            n_called = n_total

        call_rate = compute_sample_call_rate(n_called, n_total)
        assert 0.0 <= call_rate <= 1.0

    @given(
        n_het=st.integers(min_value=0, max_value=1000000),
        n_hom_var=st.integers(min_value=1, max_value=1000000),
    )
    @settings(max_examples=100)
    def test_het_hom_ratio_non_negative(self, n_het, n_hom_var):
        from vcf_pg_loader.qc.sample_qc import compute_het_hom_ratio

        ratio = compute_het_hom_ratio(n_het, n_hom_var)
        if ratio is not None:
            assert ratio >= 0.0

    @given(
        transitions=st.integers(min_value=0, max_value=1000000),
        transversions=st.integers(min_value=1, max_value=1000000),
    )
    @settings(max_examples=100)
    def test_ti_tv_ratio_non_negative(self, transitions, transversions):
        from vcf_pg_loader.qc.sample_qc import compute_ti_tv_ratio

        ratio = compute_ti_tv_ratio(transitions, transversions)
        if ratio is not None:
            assert ratio >= 0.0

    @given(x_het_rate=st.floats(min_value=0.0, max_value=1.0))
    @settings(max_examples=100)
    def test_sex_inference_valid_output(self, x_het_rate):
        from vcf_pg_loader.qc.sample_qc import infer_sex_from_x_het

        sex = infer_sex_from_x_het(x_het_rate)
        assert sex in ("M", "F", "unknown")

    @given(
        observed=st.floats(min_value=0, max_value=1000000),
        expected=st.floats(min_value=0.001, max_value=1000000),
    )
    @settings(max_examples=100)
    def test_f_inbreeding_bounds(self, observed, expected):
        from vcf_pg_loader.qc.sample_qc import compute_f_inbreeding

        f = compute_f_inbreeding(observed, expected)
        if not math.isnan(f):
            assert f <= 1.0


class TestSampleQCMetricsDataclass:
    """Test SampleQCMetrics dataclass."""

    def test_create_metrics(self):
        from vcf_pg_loader.qc.sample_qc import SampleQCMetrics

        metrics = SampleQCMetrics(
            sample_id="SAMPLE001",
            call_rate=0.995,
            n_called=99500,
            n_snp=100000,
            n_het=45000,
            n_hom_var=35000,
        )
        assert metrics.sample_id == "SAMPLE001"
        assert metrics.call_rate == 0.995

    def test_computed_het_hom_ratio(self):
        from vcf_pg_loader.qc.sample_qc import SampleQCMetrics

        metrics = SampleQCMetrics(
            sample_id="SAMPLE001",
            call_rate=0.995,
            n_called=99500,
            n_snp=100000,
            n_het=45000,
            n_hom_var=30000,
            het_hom_ratio=1.5,
        )
        assert metrics.het_hom_ratio == 1.5

    def test_to_db_row(self):
        from vcf_pg_loader.qc.sample_qc import SampleQCMetrics

        metrics = SampleQCMetrics(
            sample_id="SAMPLE001",
            call_rate=0.995,
            n_called=99500,
            n_snp=100000,
            n_het=45000,
            n_hom_var=30000,
            batch_id=1,
        )
        row = metrics.to_db_row()
        assert row["sample_id"] == "SAMPLE001"
        assert row["call_rate"] == 0.995
        assert row["batch_id"] == 1


class TestSampleQCSchemaManager:
    """Test schema creation and management."""

    @pytest.mark.integration
    async def test_create_sample_qc_table(self, pg_connection):
        from vcf_pg_loader.qc.schema import SampleQCSchemaManager

        manager = SampleQCSchemaManager()
        await manager.create_sample_qc_schema(pg_connection)

        exists = await pg_connection.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'sample_qc'
            )
        """)
        assert exists is True

    @pytest.mark.integration
    async def test_qc_pass_generated_column(self, pg_connection):
        from vcf_pg_loader.qc.schema import SampleQCSchemaManager

        manager = SampleQCSchemaManager()
        await manager.create_sample_qc_schema(pg_connection)

        await pg_connection.execute("""
            INSERT INTO sample_qc (sample_id, call_rate, n_called, n_snp, n_het, n_hom_var)
            VALUES ('PASS_SAMPLE', 0.995, 99500, 100000, 45000, 30000)
        """)

        await pg_connection.execute("""
            INSERT INTO sample_qc (sample_id, call_rate, n_called, n_snp, n_het, n_hom_var)
            VALUES ('FAIL_SAMPLE', 0.985, 98500, 100000, 45000, 30000)
        """)

        pass_result = await pg_connection.fetchval(
            "SELECT qc_pass FROM sample_qc WHERE sample_id = $1", "PASS_SAMPLE"
        )
        fail_result = await pg_connection.fetchval(
            "SELECT qc_pass FROM sample_qc WHERE sample_id = $1", "FAIL_SAMPLE"
        )

        assert pass_result is True
        assert fail_result is False

    @pytest.mark.integration
    async def test_materialized_view_created(self, pg_connection):
        from vcf_pg_loader.qc.schema import SampleQCSchemaManager

        manager = SampleQCSchemaManager()
        await manager.create_sample_qc_schema(pg_connection)

        await pg_connection.execute("""
            INSERT INTO sample_qc (sample_id, call_rate, n_called, n_snp, n_het, n_hom_var, batch_id)
            VALUES ('S1', 0.995, 99500, 100000, 45000, 30000, 1)
        """)

        await manager.refresh_summary_view(pg_connection)

        result = await pg_connection.fetchrow(
            "SELECT * FROM sample_qc_summary WHERE batch_id = $1", 1
        )
        assert result is not None
        assert result["n_samples"] == 1


@pytest.fixture
async def pg_connection():
    """Create a test PostgreSQL connection using testcontainers."""
    pytest.importorskip("testcontainers")
    import asyncpg
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:15") as postgres:
        url = postgres.get_connection_url()
        url = url.replace("postgresql+psycopg2://", "postgresql://")
        conn = await asyncpg.connect(url)
        try:
            yield conn
        finally:
            await conn.close()
