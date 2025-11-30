"""Integration tests for human vs non-human genome loading.

These tests verify that the loader correctly handles human and non-human
genome configurations end-to-end. They should FAIL until the feature
is implemented.
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
def db_url(postgres_container):
    """Provide database URL for loader."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture
async def db_pool_human(postgres_container):
    """Provide database pool with human genome schema."""
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
        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(conn)

    yield pool
    await pool.close()


@pytest.fixture
async def db_pool_non_human(postgres_container):
    """Provide database pool with non-human genome schema."""
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
        schema_manager = SchemaManager(human_genome=False)
        await schema_manager.create_schema(conn)

    yield pool
    await pool.close()


@pytest.mark.integration
class TestLoaderHumanGenome:
    """Integration tests for loading human genome VCFs."""

    @pytest.mark.asyncio
    async def test_load_human_vcf_with_enum(self, db_pool_human, db_url):
        """Should load human VCF with chromosome enum validation."""
        vcf_path = FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"
        if not vcf_path.exists():
            pytest.skip("strelka_snvs_chr22.vcf.gz fixture not found")

        config = LoadConfig(human_genome=True, drop_indexes=False, batch_size=500)
        loader = VCFLoader(db_url, config)
        result = await loader.load_vcf(vcf_path)

        assert result["variants_loaded"] > 0

        async with db_pool_human.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1",
                loader.load_batch_id
            )
            assert count == result["variants_loaded"]

            chrom = await conn.fetchval(
                "SELECT DISTINCT chrom FROM variants WHERE load_batch_id = $1",
                loader.load_batch_id
            )
            assert chrom == "chr22"

    @pytest.mark.asyncio
    async def test_load_all_human_chromosomes(self, db_pool_human, db_url):
        """Should successfully load variants from all human chromosomes."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t30\tPASS\t.
chr2\t100\t.\tA\tG\t30\tPASS\t.
chr10\t100\t.\tA\tG\t30\tPASS\t.
chr22\t100\t.\tA\tG\t30\tPASS\t.
chrX\t100\t.\tA\tG\t30\tPASS\t.
chrY\t100\t.\tA\tG\t30\tPASS\t.
chrM\t100\t.\tA\tG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(human_genome=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            result = await loader.load_vcf(vcf_path)

            assert result["variants_loaded"] == 7

            async with db_pool_human.acquire() as conn:
                chroms = await conn.fetch(
                    "SELECT DISTINCT chrom FROM variants WHERE load_batch_id = $1 ORDER BY chrom",
                    loader.load_batch_id
                )
                chrom_list = [r['chrom'] for r in chroms]
                assert 'chr1' in chrom_list
                assert 'chr22' in chrom_list
                assert 'chrX' in chrom_list
                assert 'chrM' in chrom_list
        finally:
            vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_human_loader_uses_correct_partition(self, db_pool_human, db_url):
        """Variants should be stored in the correct chromosome partition."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr22\t100\t.\tA\tG\t30\tPASS\t.
chr22\t200\t.\tC\tT\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(human_genome=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            await loader.load_vcf(vcf_path)

            async with db_pool_human.acquire() as conn:
                count_in_partition = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants_22 WHERE load_batch_id = $1",
                    loader.load_batch_id
                )
                assert count_in_partition == 2, \
                    f"Expected 2 variants in variants_22 partition, got {count_in_partition}"
        finally:
            vcf_path.unlink()


@pytest.mark.integration
class TestLoaderNonHumanGenome:
    """Integration tests for loading non-human genome VCFs."""

    @pytest.mark.asyncio
    async def test_load_sarscov2_vcf(self, db_pool_non_human, db_url):
        """Should load SARS-CoV-2 VCF with arbitrary chromosome names."""
        vcf_path = FIXTURES_DIR / "sarscov2.vcf.gz"
        if not vcf_path.exists():
            pytest.skip("sarscov2.vcf.gz fixture not found")

        config = LoadConfig(human_genome=False, drop_indexes=False)
        loader = VCFLoader(db_url, config)
        result = await loader.load_vcf(vcf_path)

        assert result["variants_loaded"] > 0

        async with db_pool_non_human.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1",
                loader.load_batch_id
            )
            assert count == result["variants_loaded"]

    @pytest.mark.asyncio
    async def test_load_arbitrary_chromosome_names(self, db_pool_non_human, db_url):
        """Should load VCF with arbitrary/non-standard chromosome names."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
MN908947.3\t100\t.\tA\tG\t30\tPASS\t.
NC_045512.2\t200\t.\tC\tT\t30\tPASS\t.
scaffold_123\t300\t.\tG\tA\t30\tPASS\t.
random_contig\t400\t.\tT\tC\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(human_genome=False, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            result = await loader.load_vcf(vcf_path)

            assert result["variants_loaded"] == 4

            async with db_pool_non_human.acquire() as conn:
                chroms = await conn.fetch(
                    "SELECT DISTINCT chrom FROM variants WHERE load_batch_id = $1",
                    loader.load_batch_id
                )
                chrom_list = [r['chrom'] for r in chroms]
                assert 'MN908947.3' in chrom_list
                assert 'NC_045512.2' in chrom_list
                assert 'scaffold_123' in chrom_list
                assert 'random_contig' in chrom_list
        finally:
            vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_non_human_no_chromosome_validation(self, db_pool_non_human, db_url):
        """Non-human loader should not validate chromosome names."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
this_is_a_very_long_and_unusual_chromosome_name_that_would_never_work_with_enum\t100\t.\tA\tG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(human_genome=False, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            result = await loader.load_vcf(vcf_path)

            assert result["variants_loaded"] == 1

            async with db_pool_non_human.acquire() as conn:
                chrom = await conn.fetchval(
                    "SELECT chrom FROM variants WHERE load_batch_id = $1",
                    loader.load_batch_id
                )
                assert chrom == "this_is_a_very_long_and_unusual_chromosome_name_that_would_never_work_with_enum"
        finally:
            vcf_path.unlink()


@pytest.mark.integration
class TestLoaderGenomeMismatch:
    """Tests for error handling when genome type doesn't match VCF."""

    @pytest.mark.asyncio
    async def test_human_loader_rejects_unknown_chromosome(self, db_pool_human, db_url):
        """Human loader should reject unknown chromosome names."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
MN908947.3\t100\t.\tA\tG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(human_genome=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)

            with pytest.raises((asyncpg.exceptions.InvalidTextRepresentationError, ValueError)):
                await loader.load_vcf(vcf_path)
        finally:
            vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_human_loader_handles_chr_prefix(self, db_pool_human, db_url):
        """Human loader should handle both 'chr1' and '1' formats."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
1\t100\t.\tA\tG\t30\tPASS\t.
22\t200\t.\tC\tT\t30\tPASS\t.
X\t300\t.\tG\tA\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            config = LoadConfig(human_genome=True, drop_indexes=False)
            loader = VCFLoader(db_url, config)
            result = await loader.load_vcf(vcf_path)

            assert result["variants_loaded"] == 3

            async with db_pool_human.acquire() as conn:
                chroms = await conn.fetch(
                    "SELECT DISTINCT chrom FROM variants WHERE load_batch_id = $1 ORDER BY chrom",
                    loader.load_batch_id
                )
                chrom_list = [r['chrom'] for r in chroms]
                assert 'chr1' in chrom_list
                assert 'chr22' in chrom_list
                assert 'chrX' in chrom_list
        finally:
            vcf_path.unlink()
