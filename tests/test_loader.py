"""Tests for VCF loader with asyncpg binary COPY implementation."""

from pathlib import Path
from uuid import UUID

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from vcf_pg_loader.loader import LoadConfig, VCFLoader
from vcf_pg_loader.models import VariantRecord
from vcf_pg_loader.schema import SchemaManager

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def postgres_container():
    """Provide a PostgreSQL test container."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture
async def db_pool(postgres_container):
    """Provide an async database connection pool."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname

    pool = await asyncpg.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        min_size=2,
        max_size=4
    )

    async with pool.acquire() as conn:
        schema_manager = SchemaManager()
        await schema_manager.create_schema(conn)

    yield pool
    await pool.close()


@pytest.fixture
def db_url(postgres_container):
    """Provide database URL for loader."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@pytest.mark.integration
class TestLoadConfig:
    """Tests for LoadConfig dataclass."""

    def test_default_config(self):
        """LoadConfig should have sensible defaults."""
        config = LoadConfig()
        assert config.batch_size == 50_000
        assert config.workers == 8
        assert config.drop_indexes is True
        assert config.normalize is True

    def test_custom_config(self):
        """LoadConfig should accept custom values."""
        config = LoadConfig(batch_size=1000, workers=2, drop_indexes=False)
        assert config.batch_size == 1000
        assert config.workers == 2
        assert config.drop_indexes is False


@pytest.mark.integration
class TestVCFLoader:
    """Tests for the VCFLoader class."""

    @pytest.mark.asyncio
    async def test_loader_init(self, db_url):
        """VCFLoader should initialize with db_url and config."""
        config = LoadConfig(batch_size=100)
        loader = VCFLoader(db_url, config)

        assert loader.db_url == db_url
        assert loader.config.batch_size == 100
        assert loader.pool is None
        assert isinstance(loader.load_batch_id, UUID)

    @pytest.mark.asyncio
    async def test_loader_connect(self, db_url):
        """VCFLoader should establish connection pool."""
        config = LoadConfig(workers=2)
        loader = VCFLoader(db_url, config)

        await loader.connect()

        assert loader.pool is not None
        assert loader.pool.get_size() >= 2

        await loader.close()

    @pytest.mark.asyncio
    async def test_copy_batch(self, db_pool, db_url):
        """VCFLoader should copy a batch of records using binary COPY."""
        config = LoadConfig(batch_size=100)
        loader = VCFLoader(db_url, config)
        await loader.connect()

        records = [
            VariantRecord(
                chrom="chr1",
                pos=12345,
                ref="A",
                alt="G",
                qual=30.0,
                filter=[],
                rs_id=None,
                info={}
            ),
            VariantRecord(
                chrom="chr1",
                pos=12346,
                ref="C",
                alt="T",
                qual=40.0,
                filter=["PASS"],
                rs_id="rs12345",
                info={}
            ),
        ]

        await loader.copy_batch(records)

        async with loader.pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM variants")
            assert count == 2

            row = await conn.fetchrow(
                "SELECT * FROM variants WHERE pos = 12345"
            )
            assert row["chrom"] == "chr1"
            assert row["ref"] == "A"
            assert row["alt"] == "G"
            assert row["load_batch_id"] == loader.load_batch_id

        await loader.close()

    @pytest.mark.asyncio
    async def test_copy_batch_with_annotations(self, db_pool, db_url):
        """VCFLoader should handle records with annotation fields."""
        config = LoadConfig()
        loader = VCFLoader(db_url, config)
        await loader.connect()

        records = [
            VariantRecord(
                chrom="chr17",
                pos=7577121,
                ref="G",
                alt="A",
                qual=100.0,
                filter=["PASS"],
                rs_id="rs28934576",
                info={},
                gene="TP53",
                consequence="missense_variant",
                impact="MODERATE",
                hgvs_c="c.743G>A",
                hgvs_p="p.Arg248Gln",
                af_gnomad=0.00001,
                cadd_phred=35.0,
                clinvar_sig="Pathogenic"
            ),
        ]

        await loader.copy_batch(records)

        async with loader.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM variants WHERE gene = 'TP53'"
            )
            assert row["gene"] == "TP53"
            assert row["consequence"] == "missense_variant"
            assert row["impact"] == "MODERATE"
            assert row["hgvs_c"] == "c.743G>A"
            assert row["hgvs_p"] == "p.Arg248Gln"
            assert abs(row["af_gnomad"] - 0.00001) < 0.000001
            assert abs(row["cadd_phred"] - 35.0) < 0.1
            assert row["clinvar_sig"] == "Pathogenic"

        await loader.close()

    @pytest.mark.asyncio
    async def test_copy_batch_with_null_values(self, db_pool, db_url):
        """VCFLoader should handle records with null annotation fields."""
        config = LoadConfig()
        loader = VCFLoader(db_url, config)
        await loader.connect()

        records = [
            VariantRecord(
                chrom="chr1",
                pos=100,
                ref="A",
                alt="T",
                qual=None,
                filter=[],
                rs_id=None,
                info={}
            ),
        ]

        await loader.copy_batch(records)

        async with loader.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM variants WHERE pos = 100")
            assert row["qual"] is None
            assert row["gene"] is None
            assert row["af_gnomad"] is None

        await loader.close()

    @pytest.mark.asyncio
    async def test_copy_large_batch(self, db_pool, db_url):
        """VCFLoader should handle large batches efficiently."""
        config = LoadConfig(batch_size=10000)
        loader = VCFLoader(db_url, config)
        await loader.connect()

        records = [
            VariantRecord(
                chrom="chr1",
                pos=i,
                ref="A",
                alt="G",
                qual=30.0,
                filter=[],
                rs_id=None,
                info={}
            )
            for i in range(1000, 2000)
        ]

        await loader.copy_batch(records)

        async with loader.pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM variants")
            assert count == 1000

        await loader.close()


@pytest.mark.integration
class TestVCFLoaderLoadVCF:
    """Tests for loading actual VCF files."""

    @pytest.mark.asyncio
    async def test_load_vcf_file(self, db_pool, db_url):
        """VCFLoader should load a complete VCF file."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        config = LoadConfig(batch_size=100, drop_indexes=False)
        loader = VCFLoader(db_url, config)

        result = await loader.load_vcf(vcf_path)

        assert result["variants_loaded"] == 4
        assert "load_batch_id" in result
        assert "file_hash" in result

        async with asyncpg.create_pool(db_url) as pool:
            async with pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM variants")
                assert count == 4

    @pytest.mark.asyncio
    async def test_load_multiallelic_vcf(self, db_pool, db_url):
        """VCFLoader should decompose multi-allelic variants."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        config = LoadConfig(batch_size=100, drop_indexes=False)
        loader = VCFLoader(db_url, config)

        result = await loader.load_vcf(vcf_path)

        assert result["variants_loaded"] == 8

        async with asyncpg.create_pool(db_url) as pool:
            async with pool.acquire() as conn:
                multiallelic_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE pos = 2049437"
                )
                assert multiallelic_count == 7

    @pytest.mark.asyncio
    async def test_load_creates_audit_record(self, db_pool, db_url):
        """VCFLoader should create audit trail entries."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        config = LoadConfig(batch_size=100, drop_indexes=False)
        loader = VCFLoader(db_url, config)

        result = await loader.load_vcf(vcf_path)

        async with asyncpg.create_pool(db_url) as pool:
            async with pool.acquire() as conn:
                audit = await conn.fetchrow(
                    "SELECT * FROM variant_load_audit WHERE load_batch_id = $1",
                    UUID(result["load_batch_id"])
                )
                assert audit is not None
                assert audit["status"] == "completed"
                assert audit["variants_loaded"] == 4
                assert audit["vcf_file_hash"] == result["file_hash"]


@pytest.mark.integration
class TestVCFLoaderIndexManagement:
    """Tests for index management during load."""

    @pytest.mark.asyncio
    async def test_drop_and_recreate_indexes(self, db_pool, db_url):
        """VCFLoader should drop indexes before load and recreate after."""
        async with db_pool.acquire() as conn:
            schema_manager = SchemaManager()
            await schema_manager.create_indexes(conn)

            indexes_before = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'variants' AND indexname NOT LIKE '%_pkey'
            """)
            assert len(indexes_before) > 0

        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        config = LoadConfig(batch_size=100, drop_indexes=True)
        loader = VCFLoader(db_url, config)

        await loader.load_vcf(vcf_path)

        async with asyncpg.create_pool(db_url) as pool:
            async with pool.acquire() as conn:
                indexes_after = await conn.fetch("""
                    SELECT indexname FROM pg_indexes
                    WHERE tablename = 'variants' AND indexname NOT LIKE '%_pkey'
                """)
                assert len(indexes_after) > 0


@pytest.mark.integration
class TestVCFLoaderContextManager:
    """Tests for context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager(self, db_pool, db_url):
        """VCFLoader should support async context manager."""
        config = LoadConfig()

        async with VCFLoader(db_url, config) as loader:
            assert loader.pool is not None

        assert loader.pool is None or loader.pool._closed
