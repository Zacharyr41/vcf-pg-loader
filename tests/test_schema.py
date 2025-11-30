"""Tests for PostgreSQL schema creation and management."""

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

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


@pytest.mark.integration
class TestSchemaManager:
    @pytest.mark.asyncio
    async def test_create_extensions(self, db_connection):
        """Test creating required PostgreSQL extensions."""
        schema_manager = SchemaManager()
        await schema_manager.create_extensions(db_connection)

        extensions = await db_connection.fetch("""
            SELECT extname FROM pg_extension
            WHERE extname IN ('pg_trgm', 'btree_gist')
        """)
        extension_names = [ext['extname'] for ext in extensions]
        assert 'pg_trgm' in extension_names
        assert 'btree_gist' in extension_names

    @pytest.mark.asyncio
    async def test_create_types(self, db_connection):
        """Test creating custom PostgreSQL types (currently a no-op for flexibility)."""
        schema_manager = SchemaManager()
        await schema_manager.create_types(db_connection)

    @pytest.mark.asyncio
    async def test_create_variants_table(self, db_connection):
        """Test creating the main variants table."""
        schema_manager = SchemaManager()
        await schema_manager.create_extensions(db_connection)
        await schema_manager.create_types(db_connection)
        await schema_manager.create_variants_table(db_connection)

        tables = await db_connection.fetch("""
            SELECT tablename FROM pg_tables
            WHERE tablename = 'variants'
        """)
        assert len(tables) == 1

        partitions = await db_connection.fetch("""
            SELECT schemaname, tablename FROM pg_tables
            WHERE tablename LIKE 'variants_%'
        """)
        partition_names = [p['tablename'] for p in partitions]
        assert 'variants_1' in partition_names
        assert 'variants_x' in partition_names
        assert 'variants_other' in partition_names

    @pytest.mark.asyncio
    async def test_create_audit_table(self, db_connection):
        """Test creating the audit trail table."""
        schema_manager = SchemaManager()
        await schema_manager.create_audit_table(db_connection)

        tables = await db_connection.fetch("""
            SELECT tablename FROM pg_tables
            WHERE tablename = 'variant_load_audit'
        """)
        assert len(tables) == 1

        columns = await db_connection.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'variant_load_audit'
            ORDER BY ordinal_position
        """)
        column_names = [col['column_name'] for col in columns]
        assert 'audit_id' in column_names
        assert 'load_batch_id' in column_names
        assert 'vcf_file_path' in column_names
        assert 'vcf_file_hash' in column_names

    @pytest.mark.asyncio
    async def test_create_samples_table(self, db_connection):
        """Test creating the samples table."""
        schema_manager = SchemaManager()
        await schema_manager.create_samples_table(db_connection)

        tables = await db_connection.fetch("""
            SELECT tablename FROM pg_tables
            WHERE tablename = 'samples'
        """)
        assert len(tables) == 1

    @pytest.mark.asyncio
    async def test_create_full_schema(self, db_connection):
        """Test creating all schema components together."""
        schema_manager = SchemaManager()
        await schema_manager.create_schema(db_connection)

        tables = await db_connection.fetch("""
            SELECT tablename FROM pg_tables
            WHERE tablename IN ('variants', 'variant_load_audit', 'samples')
        """)
        table_names = [t['tablename'] for t in tables]
        assert 'variants' in table_names
        assert 'variant_load_audit' in table_names
        assert 'samples' in table_names

    @pytest.mark.asyncio
    async def test_create_indexes(self, db_connection):
        """Test creating performance indexes."""
        schema_manager = SchemaManager()
        await schema_manager.create_schema(db_connection)
        await schema_manager.create_indexes(db_connection)

        indexes = await db_connection.fetch("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'variants'
        """)
        index_names = [idx['indexname'] for idx in indexes]
        assert any('region' in name for name in index_names)
        assert any('gene' in name for name in index_names)
