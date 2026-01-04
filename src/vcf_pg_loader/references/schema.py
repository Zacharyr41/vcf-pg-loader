"""PostgreSQL schema management for reference panels."""

import asyncpg


class ReferenceSchemaManager:
    """Manages PostgreSQL schema for reference panel tables."""

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
