"""PostgreSQL schema management for sample-level QC metrics."""

import asyncpg


class SampleQCSchemaManager:
    """Manages PostgreSQL schema for sample QC tables."""

    async def create_sample_qc_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete sample QC schema including table and materialized view."""
        await self.create_sample_qc_table(conn)
        await self.create_sample_qc_indexes(conn)
        await self.create_sample_qc_summary_view(conn)

    async def create_sample_qc_table(self, conn: asyncpg.Connection) -> None:
        """Create the sample_qc table with generated qc_pass column."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sample_qc (
                sample_id VARCHAR(100) PRIMARY KEY,
                call_rate FLOAT,
                n_called INTEGER,
                n_snp INTEGER,
                n_het INTEGER,
                n_hom_var INTEGER,
                het_hom_ratio FLOAT,
                ti_tv_ratio FLOAT,
                n_singleton INTEGER,
                f_inbreeding FLOAT,
                mean_dp FLOAT,
                mean_gq FLOAT,
                sex_inferred VARCHAR(10),
                sex_reported VARCHAR(10),
                sex_concordant BOOLEAN,
                contamination_estimate FLOAT,
                batch_id INTEGER,
                qc_pass BOOLEAN GENERATED ALWAYS AS (
                    call_rate >= 0.99 AND
                    COALESCE(contamination_estimate < 0.025, TRUE) AND
                    COALESCE(sex_concordant, TRUE)
                ) STORED,
                computed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT valid_call_rate CHECK (call_rate >= 0 AND call_rate <= 1),
                CONSTRAINT valid_contamination CHECK (
                    contamination_estimate IS NULL OR
                    (contamination_estimate >= 0 AND contamination_estimate <= 1)
                )
            )
        """)

    async def create_sample_qc_indexes(self, conn: asyncpg.Connection) -> None:
        """Create performance indexes for sample QC queries."""
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sample_qc_pass
            ON sample_qc(qc_pass)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sample_qc_batch
            ON sample_qc(batch_id)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sample_qc_call_rate
            ON sample_qc(call_rate)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sample_qc_batch_pass
            ON sample_qc(batch_id, qc_pass)
        """)

    async def create_sample_qc_summary_view(self, conn: asyncpg.Connection) -> None:
        """Create materialized view for batch-level QC summary."""
        await conn.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS sample_qc_summary AS
            SELECT
                batch_id,
                COUNT(*) as n_samples,
                COUNT(*) FILTER (WHERE qc_pass) as n_pass,
                COUNT(*) FILTER (WHERE NOT qc_pass) as n_fail,
                AVG(call_rate) as mean_call_rate,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY call_rate) as median_call_rate,
                MIN(call_rate) as min_call_rate,
                MAX(call_rate) as max_call_rate,
                AVG(het_hom_ratio) as mean_het_hom_ratio,
                AVG(ti_tv_ratio) as mean_ti_tv_ratio,
                AVG(f_inbreeding) as mean_f_inbreeding,
                COUNT(*) FILTER (WHERE sex_concordant = FALSE) as n_sex_discordant,
                COUNT(*) FILTER (WHERE contamination_estimate >= 0.025) as n_contaminated
            FROM sample_qc
            GROUP BY batch_id
        """)

        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sample_qc_summary_batch
            ON sample_qc_summary(batch_id)
        """)

    async def refresh_summary_view(self, conn: asyncpg.Connection) -> None:
        """Refresh the sample_qc_summary materialized view."""
        await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY sample_qc_summary")

    async def drop_sample_qc_schema(self, conn: asyncpg.Connection) -> None:
        """Drop sample QC schema tables and views."""
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS sample_qc_summary CASCADE")
        await conn.execute("DROP TABLE IF EXISTS sample_qc CASCADE")

    async def verify_sample_qc_schema(self, conn: asyncpg.Connection) -> bool:
        """Verify sample QC schema exists and is properly configured."""
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'sample_qc'
            )
        """)

        view_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM pg_matviews
                WHERE matviewname = 'sample_qc_summary'
            )
        """)

        return table_exists and view_exists

    async def get_sample_qc(self, conn: asyncpg.Connection, sample_id: str) -> dict | None:
        """Get QC metrics for a specific sample."""
        row = await conn.fetchrow(
            "SELECT * FROM sample_qc WHERE sample_id = $1",
            sample_id,
        )
        return dict(row) if row else None

    async def get_batch_summary(self, conn: asyncpg.Connection, batch_id: int) -> dict | None:
        """Get QC summary for a specific batch."""
        row = await conn.fetchrow(
            "SELECT * FROM sample_qc_summary WHERE batch_id = $1",
            batch_id,
        )
        return dict(row) if row else None

    async def get_failing_samples(
        self, conn: asyncpg.Connection, batch_id: int | None = None
    ) -> list[dict]:
        """Get all samples that fail QC criteria."""
        if batch_id is not None:
            rows = await conn.fetch(
                """
                SELECT sample_id, call_rate, contamination_estimate, sex_concordant
                FROM sample_qc
                WHERE qc_pass = FALSE AND batch_id = $1
                ORDER BY call_rate ASC
                """,
                batch_id,
            )
        else:
            rows = await conn.fetch("""
                SELECT sample_id, call_rate, contamination_estimate, sex_concordant
                FROM sample_qc
                WHERE qc_pass = FALSE
                ORDER BY call_rate ASC
            """)
        return [dict(r) for r in rows]

    async def get_qc_stats(self, conn: asyncpg.Connection) -> dict:
        """Get overall QC statistics across all samples."""
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_samples,
                COUNT(*) FILTER (WHERE qc_pass) as passing_samples,
                COUNT(*) FILTER (WHERE NOT qc_pass) as failing_samples,
                AVG(call_rate) as mean_call_rate,
                AVG(het_hom_ratio) as mean_het_hom_ratio,
                AVG(ti_tv_ratio) as mean_ti_tv_ratio
            FROM sample_qc
        """)
        return dict(row) if row else {}

    async def upsert_sample_qc(
        self,
        conn: asyncpg.Connection,
        sample_id: str,
        call_rate: float,
        n_called: int,
        n_snp: int,
        n_het: int,
        n_hom_var: int,
        het_hom_ratio: float | None = None,
        ti_tv_ratio: float | None = None,
        n_singleton: int | None = None,
        f_inbreeding: float | None = None,
        mean_dp: float | None = None,
        mean_gq: float | None = None,
        sex_inferred: str | None = None,
        sex_reported: str | None = None,
        sex_concordant: bool | None = None,
        contamination_estimate: float | None = None,
        batch_id: int | None = None,
    ) -> None:
        """Insert or update sample QC metrics."""
        await conn.execute(
            """
            INSERT INTO sample_qc (
                sample_id, call_rate, n_called, n_snp, n_het, n_hom_var,
                het_hom_ratio, ti_tv_ratio, n_singleton, f_inbreeding,
                mean_dp, mean_gq, sex_inferred, sex_reported, sex_concordant,
                contamination_estimate, batch_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (sample_id) DO UPDATE SET
                call_rate = EXCLUDED.call_rate,
                n_called = EXCLUDED.n_called,
                n_snp = EXCLUDED.n_snp,
                n_het = EXCLUDED.n_het,
                n_hom_var = EXCLUDED.n_hom_var,
                het_hom_ratio = EXCLUDED.het_hom_ratio,
                ti_tv_ratio = EXCLUDED.ti_tv_ratio,
                n_singleton = EXCLUDED.n_singleton,
                f_inbreeding = EXCLUDED.f_inbreeding,
                mean_dp = EXCLUDED.mean_dp,
                mean_gq = EXCLUDED.mean_gq,
                sex_inferred = EXCLUDED.sex_inferred,
                sex_reported = EXCLUDED.sex_reported,
                sex_concordant = EXCLUDED.sex_concordant,
                contamination_estimate = EXCLUDED.contamination_estimate,
                batch_id = EXCLUDED.batch_id,
                computed_at = CURRENT_TIMESTAMP
            """,
            sample_id,
            call_rate,
            n_called,
            n_snp,
            n_het,
            n_hom_var,
            het_hom_ratio,
            ti_tv_ratio,
            n_singleton,
            f_inbreeding,
            mean_dp,
            mean_gq,
            sex_inferred,
            sex_reported,
            sex_concordant,
            contamination_estimate,
            batch_id,
        )
