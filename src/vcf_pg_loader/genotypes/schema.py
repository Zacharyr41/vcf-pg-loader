"""PostgreSQL schema management for genotypes table with partitioning."""

import asyncpg


class GenotypesSchemaManager:
    """Manages PostgreSQL schema for genotypes table."""

    NUM_PARTITIONS = 16

    async def create_genotypes_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete genotypes schema with partitioning and indexes."""
        await self.create_genotypes_table(conn)
        await self.create_partitions(conn)
        await self.create_genotype_indexes(conn)

    async def create_genotypes_table(self, conn: asyncpg.Connection) -> None:
        """Create the genotypes table with hash partitioning by sample_id."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS genotypes (
                variant_id BIGINT NOT NULL,
                sample_id INTEGER NOT NULL REFERENCES samples(sample_id),
                gt VARCHAR(20) NOT NULL,
                phased BOOLEAN DEFAULT FALSE,
                gq SMALLINT,
                dp INTEGER,
                ad INTEGER[],
                dosage FLOAT,
                gp FLOAT[],
                allele_balance REAL,
                passes_adj BOOLEAN GENERATED ALWAYS AS (
                    COALESCE(gq >= 20, TRUE) AND
                    COALESCE(dp >= 10, TRUE) AND
                    (gt NOT IN ('0/1', '0|1', '1|0') OR COALESCE(allele_balance >= 0.2, TRUE))
                ) STORED,
                PRIMARY KEY (variant_id, sample_id),
                CONSTRAINT valid_dosage CHECK (dosage IS NULL OR (dosage >= 0 AND dosage <= 2))
            ) PARTITION BY HASH (sample_id)
        """)

    async def create_partitions(self, conn: asyncpg.Connection) -> None:
        """Create 16 hash partitions for parallel query execution."""
        for i in range(self.NUM_PARTITIONS):
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS genotypes_p{i}
                PARTITION OF genotypes
                FOR VALUES WITH (MODULUS {self.NUM_PARTITIONS}, REMAINDER {i})
            """)

    async def create_genotype_indexes(self, conn: asyncpg.Connection) -> None:
        """Create performance indexes for genotype queries."""
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_genotypes_adj
            ON genotypes(variant_id)
            WHERE passes_adj = TRUE
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_genotypes_dosage
            ON genotypes(variant_id, dosage)
            WHERE dosage IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_genotypes_sample
            ON genotypes(sample_id)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_genotypes_variant
            ON genotypes(variant_id)
        """)

    async def drop_genotypes_schema(self, conn: asyncpg.Connection) -> None:
        """Drop genotypes schema tables."""
        await conn.execute("DROP TABLE IF EXISTS genotypes CASCADE")

    async def verify_genotypes_schema(self, conn: asyncpg.Connection) -> bool:
        """Verify genotypes schema exists and is properly configured."""
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'genotypes'
            )
        """)

        if not table_exists:
            return False

        partition_count = await conn.fetchval("""
            SELECT COUNT(*)
            FROM pg_inherits
            WHERE inhparent = 'genotypes'::regclass
        """)

        return partition_count == self.NUM_PARTITIONS

    async def get_genotype_stats(self, conn: asyncpg.Connection) -> dict:
        """Get genotypes table statistics."""
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_genotypes,
                COUNT(*) FILTER (WHERE passes_adj) as adj_passing,
                COUNT(*) FILTER (WHERE dosage IS NOT NULL) as with_dosage,
                COUNT(DISTINCT sample_id) as unique_samples,
                COUNT(DISTINCT variant_id) as unique_variants
            FROM genotypes
        """)
        return dict(row) if row else {}
