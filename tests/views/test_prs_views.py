"""TDD tests for PRS materialized views.

Tests for materialized views that pre-compute common PRS query patterns:
- prs_candidate_variants: HapMap3 variants passing QC filters
- variant_qc_summary: Aggregate QC counts
- chromosome_variant_counts: Per-chromosome summary for parallel processing
"""

import time
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
async def db_with_variants(pg_pool):
    """Set up database with test variants for view testing."""
    from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
    from vcf_pg_loader.gwas.schema import GWASSchemaManager
    from vcf_pg_loader.schema import SchemaManager

    async with pg_pool.acquire() as conn:
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)
        await schema_mgr.create_indexes(conn)

        popfreq_mgr = PopulationFreqSchemaManager()
        await popfreq_mgr.create_population_frequencies_table(conn)

        gwas_mgr = GWASSchemaManager()
        await gwas_mgr.create_gwas_schema(conn)

        batch_id = uuid.uuid4()

        for row in [
            ("chr1", 100, "A", "G", batch_id, True, 0.95, 0.99, 0.5, 0.15),
            ("chr1", 200, "C", "T", batch_id, True, 0.80, 0.99, 0.1, 0.05),
            ("chr1", 300, "G", "A", batch_id, True, 0.50, 0.99, 0.5, 0.20),
            ("chr1", 400, "T", "C", batch_id, True, 0.95, 0.95, 0.5, 0.10),
            ("chr1", 500, "A", "T", batch_id, False, 0.95, 0.99, 0.5, 0.15),
            ("chr2", 100, "G", "C", batch_id, True, 0.95, 0.99, 0.5, 0.12),
            ("chr2", 200, "C", "G", batch_id, True, 0.95, 0.99, 1e-8, 0.10),
            ("chr2", 300, "A", "G", batch_id, True, 0.95, 0.99, 0.5, 0.005),
            ("chr3", 100, "T", "A", batch_id, True, 0.95, 0.99, 0.5, 0.25),
            ("chr3", 200, "G", "T", batch_id, True, 0.95, 0.99, 0.5, 0.02),
        ]:
            await conn.execute(
                """
                INSERT INTO variants (
                    chrom, pos, pos_range, ref, alt, load_batch_id,
                    in_hapmap3, info_score, call_rate, hwe_p, maf
                ) VALUES ($1, $2::bigint, int8range($2::bigint, $2::bigint+1), $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                *row,
            )

        study_id = await conn.fetchval(
            """
            INSERT INTO studies (study_accession, trait_name, genome_build)
            VALUES ('GCST001', 'Test Trait', 'GRCh38')
            RETURNING study_id
            """
        )

        variant_ids = await conn.fetch(
            "SELECT variant_id FROM variants WHERE chrom = 'chr1' ORDER BY pos LIMIT 3"
        )
        for i, row in enumerate(variant_ids):
            await conn.execute(
                """
                INSERT INTO gwas_summary_stats (variant_id, study_id, effect_allele, beta, standard_error, p_value)
                VALUES ($1, $2, 'A', $3, 0.01, $4)
                """,
                row["variant_id"],
                study_id,
                0.05 * (i + 1),
                1e-5 / (i + 1),
            )

        variant_ids = await conn.fetch(
            "SELECT variant_id FROM variants WHERE chrom = 'chr1' ORDER BY pos LIMIT 2"
        )
        for row in variant_ids:
            await conn.execute(
                """
                INSERT INTO population_frequencies (variant_id, source, population, af)
                VALUES ($1, 'gnomAD_v3', 'NFE', 0.15)
                """,
                row["variant_id"],
            )

    yield pg_pool


class TestPRSCandidateVariantsView:
    """Test prs_candidate_variants materialized view."""

    async def test_create_view(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_matviews WHERE matviewname = 'prs_candidate_variants'
                )
            """)
            assert exists is True

    async def test_view_filters_hapmap3(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            count = await conn.fetchval(
                "SELECT COUNT(*) FROM prs_candidate_variants WHERE in_hapmap3 = FALSE"
            )
            assert count == 0

    async def test_view_filters_info_score(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            count = await conn.fetchval(
                "SELECT COUNT(*) FROM prs_candidate_variants WHERE info_score < 0.6"
            )
            assert count == 0

    async def test_view_filters_call_rate(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            count = await conn.fetchval(
                "SELECT COUNT(*) FROM prs_candidate_variants WHERE call_rate < 0.98"
            )
            assert count == 0

    async def test_view_filters_hwe(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            count = await conn.fetchval(
                "SELECT COUNT(*) FROM prs_candidate_variants WHERE hwe_p <= 1e-6"
            )
            assert count == 0

    async def test_view_filters_maf(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            count = await conn.fetchval(
                "SELECT COUNT(*) FROM prs_candidate_variants WHERE maf < 0.01"
            )
            assert count == 0

    async def test_view_includes_gnomad_af(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            has_gnomad = await conn.fetchval(
                "SELECT COUNT(*) FROM prs_candidate_variants WHERE gnomad_nfe_af IS NOT NULL"
            )
            assert has_gnomad >= 1

    async def test_view_includes_gwas_stats(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            has_beta = await conn.fetchval(
                "SELECT COUNT(*) FROM prs_candidate_variants WHERE beta IS NOT NULL"
            )
            assert has_beta >= 1

    async def test_unique_index_created(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            idx_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_indexes
                    WHERE indexname = 'idx_prs_candidates_pk'
                )
            """)
            assert idx_exists is True

    async def test_position_index_created(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            idx_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_indexes
                    WHERE indexname = 'idx_prs_candidates_pos'
                )
            """)
            assert idx_exists is True

    async def test_unique_index_enables_efficient_lookup(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_candidate_variants_view(conn)

            idx_is_unique = await conn.fetchval("""
                SELECT indisunique FROM pg_index
                JOIN pg_class ON pg_class.oid = pg_index.indexrelid
                WHERE pg_class.relname = 'idx_prs_candidates_pk'
            """)
            assert idx_is_unique is True


class TestVariantQCSummaryView:
    """Test variant_qc_summary materialized view."""

    async def test_create_view(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_matviews WHERE matviewname = 'variant_qc_summary'
                )
            """)
            assert exists is True

    async def test_total_count_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            direct_count = await conn.fetchval("SELECT COUNT(*) FROM variants")
            view_count = await conn.fetchval("SELECT total_variants FROM variant_qc_summary")
            assert view_count == direct_count

    async def test_hapmap3_count_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            direct_count = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE in_hapmap3 = TRUE"
            )
            view_count = await conn.fetchval("SELECT hapmap3_variants FROM variant_qc_summary")
            assert view_count == direct_count

    async def test_high_info_count_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            direct_count = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE info_score >= 0.6"
            )
            view_count = await conn.fetchval("SELECT high_info_variants FROM variant_qc_summary")
            assert view_count == direct_count

    async def test_high_callrate_count_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            direct_count = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE call_rate >= 0.98"
            )
            view_count = await conn.fetchval(
                "SELECT high_callrate_variants FROM variant_qc_summary"
            )
            assert view_count == direct_count

    async def test_hwe_pass_count_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            direct_count = await conn.fetchval("SELECT COUNT(*) FROM variants WHERE hwe_p > 1e-6")
            view_count = await conn.fetchval("SELECT hwe_pass_variants FROM variant_qc_summary")
            assert view_count == direct_count

    async def test_common_variants_count_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            direct_count = await conn.fetchval("SELECT COUNT(*) FROM variants WHERE maf >= 0.01")
            view_count = await conn.fetchval("SELECT common_variants FROM variant_qc_summary")
            assert view_count == direct_count

    async def test_prs_ready_count_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_variant_qc_summary_view(conn)

            direct_count = await conn.fetchval("""
                SELECT COUNT(*) FROM variants
                WHERE in_hapmap3 = TRUE
                  AND info_score >= 0.6
                  AND call_rate >= 0.98
                  AND hwe_p > 1e-6
                  AND maf >= 0.01
            """)
            view_count = await conn.fetchval("SELECT prs_ready_variants FROM variant_qc_summary")
            assert view_count == direct_count


class TestChromosomeVariantCountsView:
    """Test chromosome_variant_counts materialized view."""

    async def test_create_view(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_chromosome_variant_counts_view(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_matviews WHERE matviewname = 'chromosome_variant_counts'
                )
            """)
            assert exists is True

    async def test_chromosome_counts_match(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_chromosome_variant_counts_view(conn)

            direct_chr1 = await conn.fetchval("SELECT COUNT(*) FROM variants WHERE chrom = 'chr1'")
            view_chr1 = await conn.fetchval(
                "SELECT n_variants FROM chromosome_variant_counts WHERE chrom = 'chr1'"
            )
            assert view_chr1 == direct_chr1

    async def test_hapmap3_per_chrom_matches(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_chromosome_variant_counts_view(conn)

            direct_hm3 = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE chrom = 'chr2' AND in_hapmap3 = TRUE"
            )
            view_hm3 = await conn.fetchval(
                "SELECT n_hapmap3 FROM chromosome_variant_counts WHERE chrom = 'chr2'"
            )
            assert view_hm3 == direct_hm3

    async def test_prs_ready_per_chrom(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_chromosome_variant_counts_view(conn)

            direct_prs = await conn.fetchval("""
                SELECT COUNT(*) FROM variants
                WHERE chrom = 'chr1'
                  AND in_hapmap3 = TRUE
                  AND info_score >= 0.6
            """)
            view_prs = await conn.fetchval(
                "SELECT n_prs_ready FROM chromosome_variant_counts WHERE chrom = 'chr1'"
            )
            assert view_prs == direct_prs


class TestViewRefresh:
    """Test materialized view refresh functionality."""

    async def test_refresh_all_views(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_materialized_views(conn)

            timings = await mgr.refresh_prs_views(conn, concurrent=False)

            assert "prs_candidate_variants" in timings
            assert "variant_qc_summary" in timings
            assert "chromosome_variant_counts" in timings
            assert all(t >= 0 for t in timings.values())

    async def test_concurrent_refresh(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_materialized_views(conn)

            timings = await mgr.refresh_prs_views(conn, concurrent=True)

            assert len(timings) == 3

    async def test_refresh_returns_timing(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_materialized_views(conn)

            timings = await mgr.refresh_prs_views(conn, concurrent=False)

            for _view_name, timing in timings.items():
                assert isinstance(timing, float)
                assert timing >= 0

    async def test_refresh_updates_data(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_materialized_views(conn)

            before_count = await conn.fetchval("SELECT total_variants FROM variant_qc_summary")

            batch_id = uuid.uuid4()
            await conn.execute(
                """
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id, in_hapmap3, info_score, call_rate, hwe_p, maf)
                VALUES ('chr1', 999::bigint, int8range(999::bigint, 1000::bigint), 'A', 'G', $1, TRUE, 0.95, 0.99, 0.5, 0.15)
                """,
                batch_id,
            )

            await mgr.refresh_prs_views(conn, concurrent=False)

            after_count = await conn.fetchval("SELECT total_variants FROM variant_qc_summary")
            assert after_count == before_count + 1


class TestViewHelpers:
    """Test helper functions for PRS views."""

    async def test_create_all_views(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import create_prs_materialized_views

        async with db_with_variants.acquire() as conn:
            await create_prs_materialized_views(conn)

            for view_name in [
                "prs_candidate_variants",
                "variant_qc_summary",
                "chromosome_variant_counts",
            ]:
                exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = $1)",
                    view_name,
                )
                assert exists is True, f"View {view_name} not created"

    async def test_refresh_views_function(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import (
            create_prs_materialized_views,
            refresh_prs_views,
        )

        async with db_with_variants.acquire() as conn:
            await create_prs_materialized_views(conn)
            timings = await refresh_prs_views(conn, concurrent=True)

            assert len(timings) == 3

    async def test_verify_views(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()

            exists_before = await mgr.verify_prs_views(conn)
            assert exists_before is False

            await mgr.create_prs_materialized_views(conn)

            exists_after = await mgr.verify_prs_views(conn)
            assert exists_after is True

    async def test_drop_views(self, db_with_variants):
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with db_with_variants.acquire() as conn:
            mgr = PRSViewsManager()
            await mgr.create_prs_materialized_views(conn)

            await mgr.drop_prs_views(conn)

            exists = await mgr.verify_prs_views(conn)
            assert exists is False


class TestPerformance:
    """Performance tests for materialized view refresh."""

    @pytest.mark.slow
    async def test_refresh_with_many_variants(self, pg_pool):
        """Test refresh performance with realistic data volume."""
        from vcf_pg_loader.schema import SchemaManager
        from vcf_pg_loader.views.prs_views import PRSViewsManager

        async with pg_pool.acquire() as conn:
            schema_mgr = SchemaManager(human_genome=True)
            await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)

            batch_id = uuid.uuid4()
            batch_size = 10000

            for i in range(batch_size):
                chrom = f"chr{(i % 22) + 1}"
                pos = i * 100
                await conn.execute(
                    """
                    INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id, in_hapmap3, info_score, call_rate, hwe_p, maf)
                    VALUES ($1, $2::bigint, int8range($2::bigint, $2::bigint+1), $3, $4, $5, $6, $7, $8, $9, $10)
                    """,
                    chrom,
                    pos,
                    "A",
                    "G",
                    batch_id,
                    i % 2 == 0,
                    0.6 + (i % 40) / 100,
                    0.98 + (i % 3) / 100,
                    0.001 + (i % 100) / 1000,
                    0.01 + (i % 50) / 100,
                )

            mgr = PRSViewsManager()
            await mgr.create_prs_materialized_views(conn)

            start = time.time()
            await mgr.refresh_prs_views(conn, concurrent=False)
            total_time = time.time() - start

            assert total_time < 30.0, f"Refresh took {total_time:.1f}s, expected < 30s"


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
