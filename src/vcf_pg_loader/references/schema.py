"""PostgreSQL schema management for reference panels."""

import asyncpg


class ReferenceSchemaManager:
    """Manages PostgreSQL schema for reference panel tables."""

    async def create_ld_blocks_table(self, conn: asyncpg.Connection) -> None:
        """Create the ld_blocks table for LD block definitions.

        LD blocks from Berisa & Pickrell (2016) are used by PRS-CS and SBayesR
        to partition the genome into largely independent regions.
        """
        await conn.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ld_blocks (
                block_id SERIAL PRIMARY KEY,
                chrom VARCHAR(2) NOT NULL,
                start_pos BIGINT NOT NULL,
                end_pos BIGINT NOT NULL,
                population VARCHAR(10) NOT NULL,
                source VARCHAR(50) DEFAULT 'Berisa_Pickrell_2016',
                genome_build VARCHAR(10) DEFAULT 'GRCh37',
                n_snps_1kg INTEGER,
                UNIQUE (source, population, chrom, start_pos, genome_build)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ldblock_region
            ON ld_blocks USING GIST (chrom, int8range(start_pos, end_pos, '[]'))
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ldblock_population
            ON ld_blocks (population, genome_build)
        """)

    async def add_ld_block_id_column(self, conn: asyncpg.Connection) -> None:
        """Add ld_block_id column to variants table if not exists."""
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = 'variants' AND column_name = 'ld_block_id'
            )
        """)

        if not exists:
            await conn.execute("""
                ALTER TABLE variants ADD COLUMN ld_block_id INTEGER
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_variants_ld_block
                ON variants (ld_block_id)
                WHERE ld_block_id IS NOT NULL
            """)

    async def create_ld_block_summary_view(self, conn: asyncpg.Connection) -> None:
        """Create view for block-level variant aggregation."""
        await conn.execute("""
            CREATE OR REPLACE VIEW variant_ld_block_summary AS
            SELECT
                lb.block_id,
                lb.chrom,
                lb.start_pos,
                lb.end_pos,
                lb.population,
                COUNT(v.variant_id) as n_variants,
                COUNT(v.variant_id) FILTER (WHERE v.in_hapmap3) as n_hapmap3
            FROM ld_blocks lb
            LEFT JOIN variants v ON v.ld_block_id = lb.block_id
            GROUP BY lb.block_id, lb.chrom, lb.start_pos, lb.end_pos, lb.population
        """)

    async def drop_ld_blocks_table(self, conn: asyncpg.Connection) -> None:
        """Drop ld_blocks table."""
        await conn.execute("DROP VIEW IF EXISTS variant_ld_block_summary CASCADE")
        await conn.execute("DROP TABLE IF EXISTS ld_blocks CASCADE")

    async def create_reference_panels_table(self, conn: asyncpg.Connection) -> None:
        """Create the reference_panels table for storing SNP reference sets.

        This table stores reference SNP sets like HapMap3, used for:
        - PRS-CS, LDpred2, and other Bayesian PRS methods
        - LD reference panel filtering
        - Quality control variant selection
        """
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reference_panels (
                panel_name VARCHAR(50) NOT NULL,
                rsid VARCHAR(20),
                chrom VARCHAR(2) NOT NULL,
                position BIGINT NOT NULL,
                a1 VARCHAR(10) NOT NULL,
                a2 VARCHAR(10) NOT NULL,
                PRIMARY KEY (panel_name, chrom, position, a1, a2)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_refpanel_rsid
            ON reference_panels(rsid)
            WHERE rsid IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_refpanel_lookup
            ON reference_panels(panel_name, chrom, position)
        """)

    async def drop_reference_panels_table(self, conn: asyncpg.Connection) -> None:
        """Drop reference_panels table."""
        await conn.execute("DROP TABLE IF EXISTS reference_panels CASCADE")

    async def verify_reference_schema(self, conn: asyncpg.Connection) -> bool:
        """Verify reference_panels table exists."""
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'reference_panels'
            )
        """)
        return exists

    async def get_panel_stats(self, conn: asyncpg.Connection, panel_name: str) -> dict | None:
        """Get statistics for a reference panel."""
        row = await conn.fetchrow(
            """
            SELECT
                panel_name,
                COUNT(*) as variant_count,
                COUNT(DISTINCT chrom) as chrom_count
            FROM reference_panels
            WHERE panel_name = $1
            GROUP BY panel_name
            """,
            panel_name,
        )
        return dict(row) if row else None

    async def list_panels(self, conn: asyncpg.Connection) -> list[dict]:
        """List all loaded reference panels with counts."""
        rows = await conn.fetch("""
            SELECT
                panel_name,
                COUNT(*) as variant_count
            FROM reference_panels
            GROUP BY panel_name
            ORDER BY panel_name
        """)
        return [dict(row) for row in rows]
