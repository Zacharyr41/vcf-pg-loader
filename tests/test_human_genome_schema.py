"""Tests for human vs non-human genome schema configuration.

These tests verify that the schema supports both human genomes (with
chromosome enum type) and non-human genomes (with TEXT chromosome column).
They should FAIL until the feature is implemented.
"""

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from vcf_pg_loader.loader import LoadConfig
from vcf_pg_loader.schema import SchemaManager


@pytest.fixture
def postgres_container():
    """Provide a PostgreSQL test container."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture
async def db_connection(postgres_container):
    """Provide an async database connection."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname

    conn = await asyncpg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    yield conn
    await conn.close()


@pytest.fixture
def db_url(postgres_container):
    """Provide database URL for loader."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class TestLoadConfigGenomeType:
    """Tests for genome type configuration."""

    def test_load_config_has_human_genome_flag(self):
        """LoadConfig should have a human_genome parameter."""
        config = LoadConfig(human_genome=True)
        assert config.human_genome is True

    def test_load_config_human_genome_default_true(self):
        """LoadConfig.human_genome should default to True."""
        config = LoadConfig()
        assert config.human_genome is True

    def test_load_config_human_genome_can_be_false(self):
        """LoadConfig.human_genome should be configurable to False."""
        config = LoadConfig(human_genome=False)
        assert config.human_genome is False


class TestSchemaManagerGenomeType:
    """Tests for SchemaManager genome type support."""

    def test_schema_manager_accepts_human_genome_flag(self):
        """SchemaManager should accept human_genome parameter."""
        schema = SchemaManager(human_genome=True)
        assert schema.human_genome is True

        schema_non_human = SchemaManager(human_genome=False)
        assert schema_non_human.human_genome is False

    def test_schema_manager_human_genome_default_true(self):
        """SchemaManager.human_genome should default to True."""
        schema = SchemaManager()
        assert schema.human_genome is True


@pytest.mark.integration
class TestHumanGenomeSchema:
    """Tests for human genome schema with chromosome enum."""

    @pytest.mark.asyncio
    async def test_creates_chromosome_enum_type(self, db_connection):
        """Human genome schema should create chromosome_type enum."""
        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(db_connection)

        types = await db_connection.fetch("""
            SELECT typname FROM pg_type
            WHERE typname = 'chromosome_type'
        """)
        assert len(types) == 1, "chromosome_type enum should be created"

    @pytest.mark.asyncio
    async def test_chromosome_enum_has_human_chromosomes(self, db_connection):
        """chromosome_type enum should have all human chromosomes."""
        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(db_connection)

        enum_values = await db_connection.fetch("""
            SELECT enumlabel FROM pg_enum
            JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
            WHERE typname = 'chromosome_type'
            ORDER BY enumsortorder
        """)
        labels = [row['enumlabel'] for row in enum_values]

        expected_chroms = [
            'chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9',
            'chr10', 'chr11', 'chr12', 'chr13', 'chr14', 'chr15', 'chr16', 'chr17',
            'chr18', 'chr19', 'chr20', 'chr21', 'chr22', 'chrX', 'chrY', 'chrM'
        ]

        for chrom in expected_chroms:
            assert chrom in labels, f"Missing chromosome {chrom} in enum"

    @pytest.mark.asyncio
    async def test_variants_table_uses_enum(self, db_connection):
        """Variants table should use chromosome_type enum for chrom column."""
        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(db_connection)

        column_info = await db_connection.fetchrow("""
            SELECT data_type, udt_name
            FROM information_schema.columns
            WHERE table_name = 'variants' AND column_name = 'chrom'
        """)

        assert column_info['udt_name'] == 'chromosome_type', \
            f"chrom column should use chromosome_type enum, got {column_info['udt_name']}"

    @pytest.mark.asyncio
    async def test_partitions_use_enum_values(self, db_connection):
        """Partitions should be created for each chromosome enum value."""
        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(db_connection)

        partitions = await db_connection.fetch("""
            SELECT tablename FROM pg_tables
            WHERE tablename LIKE 'variants_%' AND tablename != 'variants_other'
        """)
        partition_names = [p['tablename'] for p in partitions]

        assert 'variants_1' in partition_names
        assert 'variants_22' in partition_names
        assert 'variants_x' in partition_names
        assert 'variants_y' in partition_names
        assert 'variants_m' in partition_names

    @pytest.mark.asyncio
    async def test_insert_valid_human_chromosome(self, db_connection):
        """Should allow inserting valid human chromosomes."""
        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(db_connection)

        import uuid
        batch_id = uuid.uuid4()
        await db_connection.execute(
            "INSERT INTO variant_load_audit (load_batch_id, vcf_file_path, vcf_file_hash, reference_genome, status) VALUES ($1, $2, $3, $4, $5)",
            batch_id, '/test.vcf', 'a' * 32, 'GRCh38', 'started'
        )

        await db_connection.execute("""
            INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
            VALUES ('chr1'::chromosome_type, '[100,101)', 100, 'A', 'G', $1)
        """, batch_id)

        count = await db_connection.fetchval("SELECT COUNT(*) FROM variants")
        assert count == 1

    @pytest.mark.asyncio
    async def test_reject_invalid_human_chromosome(self, db_connection):
        """Should reject invalid chromosome values with enum."""
        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(db_connection)

        import uuid
        batch_id = uuid.uuid4()
        await db_connection.execute(
            "INSERT INTO variant_load_audit (load_batch_id, vcf_file_path, vcf_file_hash, reference_genome, status) VALUES ($1, $2, $3, $4, $5)",
            batch_id, '/test.vcf', 'a' * 32, 'GRCh38', 'started'
        )

        with pytest.raises(asyncpg.exceptions.InvalidTextRepresentationError):
            await db_connection.execute("""
                INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                VALUES ('MN908947.3'::chromosome_type, '[100,101)', 100, 'A', 'G', $1)
            """, batch_id)


@pytest.mark.integration
class TestNonHumanGenomeSchema:
    """Tests for non-human genome schema with TEXT chromosome column."""

    @pytest.mark.asyncio
    async def test_no_chromosome_enum_created(self, db_connection):
        """Non-human genome schema should NOT create chromosome_type enum."""
        schema_manager = SchemaManager(human_genome=False)
        await schema_manager.create_schema(db_connection)

        types = await db_connection.fetch("""
            SELECT typname FROM pg_type
            WHERE typname = 'chromosome_type'
        """)
        assert len(types) == 0, "chromosome_type enum should NOT be created for non-human"

    @pytest.mark.asyncio
    async def test_variants_table_uses_text(self, db_connection):
        """Variants table should use TEXT for chrom column in non-human mode."""
        schema_manager = SchemaManager(human_genome=False)
        await schema_manager.create_schema(db_connection)

        column_info = await db_connection.fetchrow("""
            SELECT data_type, udt_name
            FROM information_schema.columns
            WHERE table_name = 'variants' AND column_name = 'chrom'
        """)

        assert column_info['udt_name'] == 'text', \
            f"chrom column should use TEXT type, got {column_info['udt_name']}"

    @pytest.mark.asyncio
    async def test_default_partition_only(self, db_connection):
        """Non-human schema should have minimal or no pre-defined partitions."""
        schema_manager = SchemaManager(human_genome=False)
        await schema_manager.create_schema(db_connection)

        partitions = await db_connection.fetch("""
            SELECT tablename FROM pg_tables
            WHERE tablename LIKE 'variants_%'
        """)

        assert len(partitions) <= 1, "Non-human should have at most default partition"

    @pytest.mark.asyncio
    async def test_insert_arbitrary_chromosome_name(self, db_connection):
        """Should allow inserting any chromosome name for non-human genomes."""
        schema_manager = SchemaManager(human_genome=False)
        await schema_manager.create_schema(db_connection)

        import uuid
        batch_id = uuid.uuid4()
        await db_connection.execute(
            "INSERT INTO variant_load_audit (load_batch_id, vcf_file_path, vcf_file_hash, reference_genome, status) VALUES ($1, $2, $3, $4, $5)",
            batch_id, '/test.vcf', 'a' * 32, 'custom', 'started'
        )

        await db_connection.execute("""
            INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
            VALUES ('MN908947.3', '[100,101)', 100, 'A', 'G', $1)
        """, batch_id)

        count = await db_connection.fetchval("SELECT COUNT(*) FROM variants")
        assert count == 1

        row = await db_connection.fetchrow("SELECT chrom FROM variants")
        assert row['chrom'] == 'MN908947.3'

    @pytest.mark.asyncio
    async def test_insert_non_standard_chromosome(self, db_connection):
        """Should allow inserting non-standard chromosome names (scaffolds, etc)."""
        schema_manager = SchemaManager(human_genome=False)
        await schema_manager.create_schema(db_connection)

        import uuid
        batch_id = uuid.uuid4()
        await db_connection.execute(
            "INSERT INTO variant_load_audit (load_batch_id, vcf_file_path, vcf_file_hash, reference_genome, status) VALUES ($1, $2, $3, $4, $5)",
            batch_id, '/test.vcf', 'a' * 32, 'custom', 'started'
        )

        non_standard_chroms = [
            'scaffold_123',
            'NC_045512.2',
            'chrUn_gl000220',
            'random_contig_1',
        ]

        for i, chrom in enumerate(non_standard_chroms):
            await db_connection.execute("""
                INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                VALUES ($1, '[100,101)', $2, 'A', 'G', $3)
            """, chrom, 100 + i, batch_id)

        count = await db_connection.fetchval("SELECT COUNT(*) FROM variants")
        assert count == len(non_standard_chroms)
