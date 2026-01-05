"""PostgreSQL schema management for population frequency annotations."""

import asyncpg


class PopulationFreqSchemaManager:
    """Manages PostgreSQL schema for population frequency tables."""

    async def create_population_frequencies_table(self, conn: asyncpg.Connection) -> None:
        """Create the population_frequencies table for normalized frequency storage."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS population_frequencies (
                id SERIAL PRIMARY KEY,
                variant_id BIGINT,
                source VARCHAR(20) NOT NULL,
                population VARCHAR(10) NOT NULL,
                subset VARCHAR(20) DEFAULT 'all',
                ac INTEGER,
                an INTEGER,
                af DOUBLE PRECISION,
                hom_count INTEGER,
                faf_95 DOUBLE PRECISION,
                UNIQUE (variant_id, source, population, subset)
            )
        """)

    async def create_popfreq_indexes(self, conn: asyncpg.Connection) -> None:
        """Create performance indexes for population frequency queries."""
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_popfreq_lookup
            ON population_frequencies(variant_id, population)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_popfreq_af
            ON population_frequencies(af)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_popfreq_source
            ON population_frequencies(source)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_popfreq_rare
            ON population_frequencies(population, af)
            WHERE af < 0.01
        """)

    async def drop_population_frequencies_table(self, conn: asyncpg.Connection) -> None:
        """Drop population_frequencies table."""
        await conn.execute("DROP TABLE IF EXISTS population_frequencies CASCADE")

    async def verify_schema_exists(self, conn: asyncpg.Connection) -> bool:
        """Verify population_frequencies table exists."""
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'population_frequencies'
            )
        """)
        return exists

    async def get_frequency_count(self, conn: asyncpg.Connection) -> int:
        """Get total count of frequency records."""
        count = await conn.fetchval("SELECT COUNT(*) FROM population_frequencies")
        return count

    async def get_sources(self, conn: asyncpg.Connection) -> list[str]:
        """Get distinct frequency sources."""
        rows = await conn.fetch(
            "SELECT DISTINCT source FROM population_frequencies ORDER BY source"
        )
        return [row["source"] for row in rows]
