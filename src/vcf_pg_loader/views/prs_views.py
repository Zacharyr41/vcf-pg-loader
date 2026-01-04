"""Materialized views for common PRS query patterns.

Pre-computed views with concurrent refresh enable interactive exploration of PRS results.
Standard PRS filter criteria:
- HapMap3 variants
- High imputation quality (INFO >= 0.6)
- High call rate (>= 98%)
- HWE passing (p > 1e-6)
- Common variants (MAF >= 1%)
"""

import time

import asyncpg


class PRSViewsManager:
    """Manages PostgreSQL materialized views for PRS workflows."""

    async def create_prs_materialized_views(self, conn: asyncpg.Connection) -> None:
        """Create all PRS materialized views with indexes."""
        await self.create_prs_candidate_variants_view(conn)
        await self.create_variant_qc_summary_view(conn)
        await self.create_chromosome_variant_counts_view(conn)

    async def create_prs_candidate_variants_view(self, conn: asyncpg.Connection) -> None:
        """Create prs_candidate_variants materialized view."""
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS prs_candidate_variants CASCADE")

        await conn.execute("""
            CREATE MATERIALIZED VIEW prs_candidate_variants AS
            SELECT
                v.variant_id,
                v.chrom,
                v.pos,
                v.ref,
                v.alt,
                v.rs_id,
                v.info_score,
                v.call_rate,
                v.hwe_p,
                v.maf,
                v.aaf,
                v.in_hapmap3,
                v.ld_block_id,
                v.load_batch_id,
                pf.af AS gnomad_nfe_af,
                ss.beta,
                ss.standard_error,
                ss.p_value
            FROM variants v
            LEFT JOIN population_frequencies pf
                ON v.variant_id = pf.variant_id
                AND pf.population = 'NFE'
                AND pf.source = 'gnomAD_v3'
            LEFT JOIN gwas_summary_stats ss
                ON v.variant_id = ss.variant_id
            WHERE v.in_hapmap3 = TRUE
                AND v.info_score >= 0.6
                AND v.call_rate >= 0.98
                AND v.hwe_p > 1e-6
                AND v.maf >= 0.01
        """)

        await conn.execute("""
            CREATE UNIQUE INDEX idx_prs_candidates_pk
            ON prs_candidate_variants(variant_id)
        """)

        await conn.execute("""
            CREATE INDEX idx_prs_candidates_pos
            ON prs_candidate_variants(chrom, pos)
        """)

    async def create_variant_qc_summary_view(self, conn: asyncpg.Connection) -> None:
        """Create variant_qc_summary materialized view."""
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS variant_qc_summary CASCADE")

        await conn.execute("""
            CREATE MATERIALIZED VIEW variant_qc_summary AS
            SELECT
                1 as id,
                COUNT(*) as total_variants,
                COUNT(*) FILTER (WHERE in_hapmap3 = TRUE) as hapmap3_variants,
                COUNT(*) FILTER (WHERE info_score >= 0.6) as high_info_variants,
                COUNT(*) FILTER (WHERE call_rate >= 0.98) as high_callrate_variants,
                COUNT(*) FILTER (WHERE hwe_p > 1e-6) as hwe_pass_variants,
                COUNT(*) FILTER (WHERE maf >= 0.01) as common_variants,
                COUNT(*) FILTER (
                    WHERE in_hapmap3 = TRUE
                    AND info_score >= 0.6
                    AND call_rate >= 0.98
                    AND hwe_p > 1e-6
                    AND maf >= 0.01
                ) as prs_ready_variants
            FROM variants
        """)

        await conn.execute("""
            CREATE UNIQUE INDEX idx_variant_qc_summary_pk ON variant_qc_summary(id)
        """)

    async def create_chromosome_variant_counts_view(self, conn: asyncpg.Connection) -> None:
        """Create chromosome_variant_counts materialized view."""
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS chromosome_variant_counts CASCADE")

        await conn.execute("""
            CREATE MATERIALIZED VIEW chromosome_variant_counts AS
            SELECT
                chrom,
                COUNT(*) as n_variants,
                COUNT(*) FILTER (WHERE in_hapmap3 = TRUE) as n_hapmap3,
                COUNT(*) FILTER (WHERE in_hapmap3 = TRUE AND info_score >= 0.6) as n_prs_ready
            FROM variants
            GROUP BY chrom
        """)

        await conn.execute("""
            CREATE UNIQUE INDEX idx_chromosome_variant_counts_pk
            ON chromosome_variant_counts(chrom)
        """)

    async def refresh_prs_views(
        self, conn: asyncpg.Connection, concurrent: bool = True
    ) -> dict[str, float]:
        """Refresh all PRS materialized views.

        Args:
            conn: Database connection
            concurrent: If True, use CONCURRENTLY to avoid blocking reads

        Returns:
            Dictionary mapping view name to refresh time in seconds
        """
        views = [
            "prs_candidate_variants",
            "variant_qc_summary",
            "chromosome_variant_counts",
        ]
        timings: dict[str, float] = {}

        concurrently = "CONCURRENTLY" if concurrent else ""

        for view_name in views:
            start = time.time()
            await conn.execute(f"REFRESH MATERIALIZED VIEW {concurrently} {view_name}")
            timings[view_name] = time.time() - start

        return timings

    async def drop_prs_views(self, conn: asyncpg.Connection) -> None:
        """Drop all PRS materialized views."""
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS prs_candidate_variants CASCADE")
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS variant_qc_summary CASCADE")
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS chromosome_variant_counts CASCADE")

    async def verify_prs_views(self, conn: asyncpg.Connection) -> bool:
        """Verify all PRS materialized views exist."""
        views = [
            "prs_candidate_variants",
            "variant_qc_summary",
            "chromosome_variant_counts",
        ]

        for view_name in views:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = $1)",
                view_name,
            )
            if not exists:
                return False

        return True


async def create_prs_materialized_views(conn: asyncpg.Connection) -> None:
    """Create all PRS materialized views.

    Convenience function that creates an instance of PRSViewsManager
    and calls create_prs_materialized_views.

    Args:
        conn: Database connection
    """
    mgr = PRSViewsManager()
    await mgr.create_prs_materialized_views(conn)


async def refresh_prs_views(conn: asyncpg.Connection, concurrent: bool = True) -> dict[str, float]:
    """Refresh all PRS materialized views.

    Convenience function that creates an instance of PRSViewsManager
    and calls refresh_prs_views.

    Args:
        conn: Database connection
        concurrent: If True, use CONCURRENTLY to avoid blocking reads

    Returns:
        Dictionary mapping view name to refresh time in seconds
    """
    mgr = PRSViewsManager()
    return await mgr.refresh_prs_views(conn, concurrent=concurrent)
