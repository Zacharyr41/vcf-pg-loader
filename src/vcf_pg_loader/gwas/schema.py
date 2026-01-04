"""PostgreSQL schema management for GWAS summary statistics."""

import asyncpg


class GWASSchemaManager:
    """Manages PostgreSQL schema for GWAS summary statistics tables."""

    async def create_gwas_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete GWAS schema including studies and summary stats tables."""
        await self.create_studies_table(conn)
        await self.create_gwas_summary_stats_table(conn)

    async def create_studies_table(self, conn: asyncpg.Connection) -> None:
        """Create the studies metadata table."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS studies (
                study_id SERIAL PRIMARY KEY,
                study_accession VARCHAR(50) UNIQUE,
                trait_name TEXT,
                trait_ontology_id VARCHAR(50),
                publication_pmid VARCHAR(20),
                sample_size INTEGER,
                n_cases INTEGER,
                n_controls INTEGER,
                genome_build VARCHAR(10),
                analysis_software TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def create_gwas_summary_stats_table(self, conn: asyncpg.Connection) -> None:
        """Create the GWAS summary statistics table per GWAS-SSF standard."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS gwas_summary_stats (
                id SERIAL PRIMARY KEY,
                variant_id BIGINT,
                study_id INTEGER REFERENCES studies(study_id),
                effect_allele VARCHAR(255) NOT NULL,
                other_allele VARCHAR(255),
                beta DOUBLE PRECISION,
                odds_ratio DOUBLE PRECISION,
                standard_error DOUBLE PRECISION,
                p_value DOUBLE PRECISION NOT NULL,
                effect_allele_frequency DOUBLE PRECISION,
                n_total INTEGER,
                n_cases INTEGER,
                info_score DOUBLE PRECISION,
                is_effect_allele_alt BOOLEAN,
                UNIQUE (variant_id, study_id)
            )
        """)

    async def create_gwas_indexes(self, conn: asyncpg.Connection) -> None:
        """Create performance indexes for common GWAS query patterns."""
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gwas_pvalue
            ON gwas_summary_stats (p_value)
            WHERE p_value < 5e-8
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gwas_study_id
            ON gwas_summary_stats (study_id)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gwas_variant_id
            ON gwas_summary_stats (variant_id)
            WHERE variant_id IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gwas_study_pvalue
            ON gwas_summary_stats (study_id, p_value)
        """)

    async def drop_gwas_schema(self, conn: asyncpg.Connection) -> None:
        """Drop GWAS schema tables."""
        await conn.execute("DROP TABLE IF EXISTS gwas_summary_stats CASCADE")
        await conn.execute("DROP TABLE IF EXISTS studies CASCADE")

    async def verify_gwas_schema(self, conn: asyncpg.Connection) -> bool:
        """Verify GWAS schema exists and is properly configured."""
        studies_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'studies'
            )
        """)

        stats_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'gwas_summary_stats'
            )
        """)

        return studies_exists and stats_exists

    async def get_study_by_accession(
        self, conn: asyncpg.Connection, study_accession: str
    ) -> dict | None:
        """Get study record by accession number."""
        row = await conn.fetchrow(
            "SELECT * FROM studies WHERE study_accession = $1",
            study_accession,
        )
        return dict(row) if row else None

    async def create_study(
        self,
        conn: asyncpg.Connection,
        study_accession: str,
        trait_name: str | None = None,
        trait_ontology_id: str | None = None,
        publication_pmid: str | None = None,
        sample_size: int | None = None,
        n_cases: int | None = None,
        n_controls: int | None = None,
        genome_build: str = "GRCh38",
        analysis_software: str | None = None,
    ) -> int:
        """Create a new study record and return the study_id."""
        study_id = await conn.fetchval(
            """
            INSERT INTO studies (
                study_accession, trait_name, trait_ontology_id, publication_pmid,
                sample_size, n_cases, n_controls, genome_build, analysis_software
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING study_id
            """,
            study_accession,
            trait_name,
            trait_ontology_id,
            publication_pmid,
            sample_size,
            n_cases,
            n_controls,
            genome_build,
            analysis_software,
        )
        return study_id

    async def get_stats_count(self, conn: asyncpg.Connection, study_id: int) -> int:
        """Get count of summary statistics for a study."""
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM gwas_summary_stats WHERE study_id = $1",
            study_id,
        )
        return count

    async def get_matched_stats_count(self, conn: asyncpg.Connection, study_id: int) -> int:
        """Get count of matched (variant_id not null) statistics for a study."""
        count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM gwas_summary_stats
            WHERE study_id = $1 AND variant_id IS NOT NULL
            """,
            study_id,
        )
        return count
