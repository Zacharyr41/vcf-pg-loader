"""PostgreSQL schema management for VCF data."""


import asyncpg

HUMAN_CHROMOSOMES = [
    'chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9',
    'chr10', 'chr11', 'chr12', 'chr13', 'chr14', 'chr15', 'chr16', 'chr17',
    'chr18', 'chr19', 'chr20', 'chr21', 'chr22', 'chrX', 'chrY', 'chrM'
]


class SchemaManager:
    """Manages PostgreSQL schema creation and maintenance."""

    def __init__(self, human_genome: bool = True):
        self.human_genome = human_genome

    async def create_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete database schema."""
        await self.drop_schema(conn)
        await self.create_extensions(conn)
        await self.create_types(conn)
        await self.create_variants_table(conn)
        await self.create_audit_table(conn)
        await self.create_samples_table(conn)

    async def drop_schema(self, conn: asyncpg.Connection) -> None:
        """Drop existing schema tables for clean recreation."""
        await conn.execute("DROP TABLE IF EXISTS samples CASCADE")
        await conn.execute("DROP TABLE IF EXISTS variant_load_audit CASCADE")
        await conn.execute("DROP TABLE IF EXISTS variants CASCADE")

    async def create_extensions(self, conn: asyncpg.Connection) -> None:
        """Create required PostgreSQL extensions."""
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    async def create_types(self, conn: asyncpg.Connection) -> None:
        """Create custom PostgreSQL types."""
        if self.human_genome:
            enum_values = ", ".join(f"'{c}'" for c in HUMAN_CHROMOSOMES)
            await conn.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'chromosome_type') THEN
                        CREATE TYPE chromosome_type AS ENUM ({enum_values});
                    END IF;
                END$$
            """)

    async def create_variants_table(self, conn: asyncpg.Connection) -> None:
        """Create the main variants table with partitioning."""
        if self.human_genome:
            chrom_type = "chromosome_type"
        else:
            chrom_type = "TEXT"

        await conn.execute(f"""
            CREATE TABLE variants (
                variant_id BIGINT GENERATED ALWAYS AS IDENTITY,
                chrom {chrom_type} NOT NULL,
                pos_range int8range NOT NULL,
                pos BIGINT NOT NULL,
                end_pos BIGINT,
                ref TEXT NOT NULL,
                alt TEXT NOT NULL,
                qual REAL,
                filter TEXT[],

                -- Classification (frequently queried)
                variant_type VARCHAR(20),
                rs_id TEXT,

                -- Extracted annotation fields (indexed)
                gene VARCHAR(100),
                transcript VARCHAR(255),
                hgvs_c VARCHAR(255),
                hgvs_p VARCHAR(255),
                consequence VARCHAR(100),
                impact VARCHAR(20),
                is_coding BOOLEAN DEFAULT FALSE,
                is_lof BOOLEAN DEFAULT FALSE,

                -- Population frequencies
                af_gnomad REAL,
                af_gnomad_popmax REAL,
                af_1kg REAL,

                -- Pathogenicity scores
                cadd_phred REAL,
                clinvar_sig VARCHAR(100),
                clinvar_review VARCHAR(50),

                -- Flexible storage for variable annotations
                info JSONB DEFAULT '{{}}'::jsonb,
                vep_annotations JSONB,

                -- Sample tracking
                sample_id VARCHAR(255),

                -- Audit tracking
                load_batch_id UUID NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (chrom, variant_id)
            ) PARTITION BY LIST (chrom)
        """)

        if self.human_genome:
            for chrom in HUMAN_CHROMOSOMES:
                partition_name = f"variants_{chrom.replace('chr', '').lower()}"
                await conn.execute(f"""
                    CREATE TABLE {partition_name} PARTITION OF variants
                    FOR VALUES IN ('{chrom}')
                """)

            await conn.execute("""
                CREATE TABLE variants_other PARTITION OF variants DEFAULT
            """)
        else:
            await conn.execute("""
                CREATE TABLE variants_default PARTITION OF variants DEFAULT
            """)

    async def create_audit_table(self, conn: asyncpg.Connection) -> None:
        """Create the audit trail table."""
        await conn.execute("""
            CREATE TABLE variant_load_audit (
                audit_id BIGSERIAL PRIMARY KEY,
                load_batch_id UUID NOT NULL UNIQUE,
                vcf_file_path TEXT NOT NULL,
                vcf_file_hash CHAR(64) NOT NULL,
                vcf_file_size BIGINT,

                -- Temporal tracking
                load_started_at TIMESTAMPTZ DEFAULT NOW(),
                load_completed_at TIMESTAMPTZ,

                -- Reference and annotation versions
                reference_genome VARCHAR(20) NOT NULL,
                vep_version VARCHAR(20),
                snpeff_version VARCHAR(20),
                clinvar_version DATE,
                gnomad_version VARCHAR(20),

                -- Validation counts
                total_variants_in_file BIGINT,
                variants_loaded BIGINT,
                variants_skipped BIGINT,
                samples_count INTEGER,

                -- Status
                status VARCHAR(20) CHECK (status IN ('started', 'completed', 'failed', 'rolled_back')),
                loaded_by VARCHAR(100),
                error_message TEXT,

                -- Idempotent reload tracking
                is_reload BOOLEAN DEFAULT FALSE,
                previous_load_id UUID
            )
        """)

    async def create_samples_table(self, conn: asyncpg.Connection) -> None:
        """Create the samples table."""
        await conn.execute("""
            CREATE TABLE samples (
                sample_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                external_id VARCHAR(255) NOT NULL UNIQUE,
                family_id VARCHAR(100),
                sex SMALLINT CHECK (sex IN (0, 1, 2)),
                phenotype SMALLINT DEFAULT 0,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

    async def create_indexes(self, conn: asyncpg.Connection) -> None:
        """Create performance indexes."""
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_region
            ON variants USING GiST (chrom, pos_range)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_gene
            ON variants (gene)
            INCLUDE (pos, ref, alt, impact)
            WHERE gene IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_rsid
            ON variants USING HASH (rs_id)
            WHERE rs_id IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_pathogenic
            ON variants (chrom, gene, clinvar_sig)
            WHERE clinvar_sig IN ('Pathogenic', 'Likely_pathogenic')
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_rare
            ON variants (gene, af_gnomad)
            WHERE af_gnomad < 0.01 OR af_gnomad IS NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_info
            ON variants USING GIN (info jsonb_path_ops)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_hgvsp_trgm
            ON variants USING GIN (hgvs_p gin_trgm_ops)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_impact
            ON variants (impact)
            WHERE impact IN ('HIGH', 'MODERATE')
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_consequence
            ON variants (consequence)
            WHERE consequence IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_gene_impact
            ON variants (gene, impact)
            WHERE gene IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variants_transcript
            ON variants (transcript)
            WHERE transcript IS NOT NULL
        """)

    async def drop_indexes(self, conn: asyncpg.Connection) -> list[str]:
        """Drop non-primary key indexes and return their names."""
        indexes = await conn.fetch("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'variants' AND indexname NOT LIKE '%_pkey'
        """)

        dropped_indexes = []
        for idx in indexes:
            await conn.execute(f'DROP INDEX IF EXISTS {idx["indexname"]}')
            dropped_indexes.append(idx["indexname"])

        return dropped_indexes

    async def vacuum_analyze(self, conn: asyncpg.Connection) -> None:
        """Run VACUUM ANALYZE for query planner statistics."""
        await conn.execute('VACUUM ANALYZE variants')
        await conn.execute('VACUUM ANALYZE variant_load_audit')
        await conn.execute('VACUUM ANALYZE samples')
