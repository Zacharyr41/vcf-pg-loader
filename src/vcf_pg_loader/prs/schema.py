"""PostgreSQL schema management for PRS weights storage."""

import asyncpg


class PRSSchemaManager:
    """Manages PostgreSQL schema for PGS Catalog PRS weights tables."""

    async def create_prs_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete PRS schema including scores and weights tables."""
        await self.create_pgs_scores_table(conn)
        await self.create_prs_weights_table(conn)
        await self.create_prs_indexes(conn)

    async def create_pgs_scores_table(self, conn: asyncpg.Connection) -> None:
        """Create the pgs_scores metadata table."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pgs_scores (
                pgs_id VARCHAR(20) PRIMARY KEY,
                trait_name TEXT,
                trait_ontology_id VARCHAR(50),
                publication_pmid VARCHAR(20),
                n_variants INTEGER,
                genome_build VARCHAR(10),
                weight_type VARCHAR(20),
                reporting_ancestry TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def create_prs_weights_table(self, conn: asyncpg.Connection) -> None:
        """Create the prs_weights table with FK to variants."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS prs_weights (
                id SERIAL PRIMARY KEY,
                variant_id BIGINT,
                pgs_id VARCHAR(20) REFERENCES pgs_scores(pgs_id),
                effect_allele VARCHAR(255) NOT NULL,
                effect_weight DOUBLE PRECISION NOT NULL,
                is_interaction BOOLEAN DEFAULT FALSE,
                is_haplotype BOOLEAN DEFAULT FALSE,
                is_dominant BOOLEAN DEFAULT FALSE,
                is_recessive BOOLEAN DEFAULT FALSE,
                allele_frequency DOUBLE PRECISION,
                locus_name VARCHAR(100),
                chr_name VARCHAR(10),
                chr_position BIGINT,
                rsid VARCHAR(20),
                other_allele VARCHAR(255),
                UNIQUE (variant_id, pgs_id)
            )
        """)

    async def create_prs_indexes(self, conn: asyncpg.Connection) -> None:
        """Create performance indexes for PRS queries."""
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_prs_pgsid
            ON prs_weights(pgs_id)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_prs_variant_id
            ON prs_weights(variant_id)
            WHERE variant_id IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_prs_position
            ON prs_weights(chr_name, chr_position)
            WHERE chr_name IS NOT NULL AND chr_position IS NOT NULL
        """)

    async def drop_prs_schema(self, conn: asyncpg.Connection) -> None:
        """Drop PRS schema tables."""
        await conn.execute("DROP TABLE IF EXISTS prs_weights CASCADE")
        await conn.execute("DROP TABLE IF EXISTS pgs_scores CASCADE")

    async def verify_prs_schema(self, conn: asyncpg.Connection) -> bool:
        """Verify PRS schema exists and is properly configured."""
        scores_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'pgs_scores'
            )
        """)

        weights_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'prs_weights'
            )
        """)

        return scores_exists and weights_exists

    async def get_score_by_id(self, conn: asyncpg.Connection, pgs_id: str) -> dict | None:
        """Get PGS score record by ID."""
        row = await conn.fetchrow(
            "SELECT * FROM pgs_scores WHERE pgs_id = $1",
            pgs_id,
        )
        return dict(row) if row else None

    async def create_score(
        self,
        conn: asyncpg.Connection,
        pgs_id: str,
        trait_name: str | None = None,
        trait_ontology_id: str | None = None,
        publication_pmid: str | None = None,
        n_variants: int | None = None,
        genome_build: str = "GRCh38",
        weight_type: str | None = None,
        reporting_ancestry: str | None = None,
    ) -> str:
        """Create a new PGS score record and return the pgs_id."""
        await conn.execute(
            """
            INSERT INTO pgs_scores (
                pgs_id, trait_name, trait_ontology_id, publication_pmid,
                n_variants, genome_build, weight_type, reporting_ancestry
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (pgs_id) DO UPDATE SET
                trait_name = EXCLUDED.trait_name,
                trait_ontology_id = EXCLUDED.trait_ontology_id,
                publication_pmid = EXCLUDED.publication_pmid,
                n_variants = EXCLUDED.n_variants,
                genome_build = EXCLUDED.genome_build,
                weight_type = EXCLUDED.weight_type,
                reporting_ancestry = EXCLUDED.reporting_ancestry
            """,
            pgs_id,
            trait_name,
            trait_ontology_id,
            publication_pmid,
            n_variants,
            genome_build,
            weight_type,
            reporting_ancestry,
        )
        return pgs_id

    async def get_weights_count(self, conn: asyncpg.Connection, pgs_id: str) -> int:
        """Get count of weights for a PGS score."""
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM prs_weights WHERE pgs_id = $1",
            pgs_id,
        )
        return count

    async def get_matched_weights_count(self, conn: asyncpg.Connection, pgs_id: str) -> int:
        """Get count of matched (variant_id not null) weights for a PGS score."""
        count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM prs_weights
            WHERE pgs_id = $1 AND variant_id IS NOT NULL
            """,
            pgs_id,
        )
        return count

    async def list_scores(self, conn: asyncpg.Connection) -> list[dict]:
        """List all loaded PGS scores with counts."""
        rows = await conn.fetch("""
            SELECT
                s.pgs_id,
                s.trait_name,
                s.genome_build,
                s.weight_type,
                COUNT(w.id) as weight_count,
                COUNT(w.variant_id) as matched_count
            FROM pgs_scores s
            LEFT JOIN prs_weights w ON s.pgs_id = w.pgs_id
            GROUP BY s.pgs_id, s.trait_name, s.genome_build, s.weight_type
            ORDER BY s.pgs_id
        """)
        return [dict(row) for row in rows]
