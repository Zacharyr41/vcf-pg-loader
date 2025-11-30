"""Integration tests for normalization in the VCF loader.

These tests verify that normalization works end-to-end through the loader.
They should FAIL until normalization is wired into the pipeline.
"""

from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from vcf_pg_loader.loader import LoadConfig, VCFLoader
from vcf_pg_loader.schema import SchemaManager

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def postgres_container():
    """Provide a PostgreSQL test container."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture
async def db_pool(postgres_container):
    """Provide an async database connection pool with schema."""
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
class TestLoaderNormalization:
    """Integration tests for loader with normalization."""

    @pytest.mark.asyncio
    async def test_loader_respects_normalize_config(self, db_pool, db_url):
        """VCFLoader should use config.normalize flag."""
        vcf_content = '''##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tATG\tAG\t30\tPASS\tDP=50
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(normalize=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            result = await loader.load_vcf(vcf_path)

            assert result["variants_loaded"] == 1

            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT ref, alt FROM variants WHERE load_batch_id = $1
                """, loader.load_batch_id)

                assert row["ref"] == "AT", \
                    f"REF should be normalized to 'AT', got '{row['ref']}'"
                assert row["alt"] == "A", \
                    f"ALT should be normalized to 'A', got '{row['alt']}'"
        finally:
            vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_loader_no_normalize_preserves_original(self, db_pool, db_url):
        """VCFLoader with normalize=False should preserve original alleles."""
        vcf_content = '''##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tATG\tAG\t30\tPASS\tDP=50
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(normalize=False, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            await loader.load_vcf(vcf_path)

            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT ref, alt FROM variants WHERE load_batch_id = $1
                """, loader.load_batch_id)

                assert row["ref"] == "ATG", \
                    f"REF should be preserved as 'ATG', got '{row['ref']}'"
                assert row["alt"] == "AG", \
                    f"ALT should be preserved as 'AG', got '{row['alt']}'"
        finally:
            vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_loader_normalizes_position(self, db_pool, db_url):
        """Normalization should also update the position when needed."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tTAC\tTGC\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(normalize=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            await loader.load_vcf(vcf_path)

            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT pos, ref, alt FROM variants WHERE load_batch_id = $1
                """, loader.load_batch_id)

                assert row["pos"] == 101, \
                    f"Position should be normalized to 101, got {row['pos']}"
                assert row["ref"] == "A", \
                    f"REF should be normalized to 'A', got '{row['ref']}'"
                assert row["alt"] == "G", \
                    f"ALT should be normalized to 'G', got '{row['alt']}'"
        finally:
            vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_loader_normalizes_multiallelic(self, db_pool, db_url):
        """Multi-allelic variants should each be normalized independently."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tATG\tAG,ATCG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(normalize=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            result = await loader.load_vcf(vcf_path)

            assert result["variants_loaded"] == 2

            async with db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT pos, ref, alt FROM variants
                    WHERE load_batch_id = $1
                    ORDER BY alt
                """, loader.load_batch_id)

                assert len(rows) == 2

                row1 = rows[0]
                assert row1["ref"] == "AT"
                assert row1["alt"] == "A"

                row2 = rows[1]
                assert row2["ref"] == "T"
                assert row2["alt"] == "TC"
        finally:
            vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_loader_pos_range_updated_after_normalization(self, db_pool, db_url):
        """pos_range should reflect normalized position and length."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tTAC\tTGC\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(normalize=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            await loader.load_vcf(vcf_path)

            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT pos, pos_range FROM variants WHERE load_batch_id = $1
                """, loader.load_batch_id)

                assert row["pos"] == 101

                pos_range = row["pos_range"]
                assert pos_range.lower == 101, \
                    f"pos_range.lower should be 101, got {pos_range.lower}"
                assert pos_range.upper == 102, \
                    f"pos_range.upper should be 102, got {pos_range.upper}"
        finally:
            vcf_path.unlink()


@pytest.mark.integration
class TestLoaderNormalizationWithFixtures:
    """Integration tests using real VCF fixture files."""

    @pytest.mark.asyncio
    async def test_mills_indels_normalization(self, db_pool, db_url):
        """Mills indels should be normalized when loaded."""
        vcf_path = FIXTURES_DIR / "mills_indels.vcf.gz"
        if not vcf_path.exists():
            pytest.skip("mills_indels.vcf.gz fixture not found")

        config = LoadConfig(normalize=True, drop_indexes=False, batch_size=500)
        loader = VCFLoader(db_url, config)
        await loader.load_vcf(vcf_path)

        async with db_pool.acquire() as conn:
            unnormalized_count = await conn.fetchval("""
                SELECT COUNT(*) FROM variants
                WHERE load_batch_id = $1
                AND (
                    (LENGTH(ref) > 1 AND LENGTH(alt) > 1 AND RIGHT(ref, 1) = RIGHT(alt, 1))
                    OR (LENGTH(ref) > 1 AND LENGTH(alt) > 1 AND LEFT(ref, 1) = LEFT(alt, 1) AND LENGTH(ref) > 2 AND LENGTH(alt) > 2)
                )
            """, loader.load_batch_id)

            assert unnormalized_count == 0, \
                f"Found {unnormalized_count} un-normalized variants after loading with normalize=True"

    @pytest.mark.asyncio
    async def test_multiallelic_vcf_all_normalized(self, db_pool, db_url):
        """All records from multiallelic.vcf should be normalized."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"

        config = LoadConfig(normalize=True, drop_indexes=False)
        loader = VCFLoader(db_url, config)
        await loader.load_vcf(vcf_path)

        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT pos, ref, alt FROM variants
                WHERE load_batch_id = $1
            """, loader.load_batch_id)

            for row in rows:
                ref, alt = row["ref"], row["alt"]
                if len(ref) > 1 and len(alt) > 1:
                    assert ref[-1] != alt[-1], \
                        f"Variant at pos {row['pos']} not normalized: {ref}>{alt} (same trailing base)"
