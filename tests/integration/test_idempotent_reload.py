"""Integration tests for idempotent reload capability."""

import hashlib
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from vcf_pg_loader.loader import LoadConfig, VCFLoader
from vcf_pg_loader.vcf_parser import VCFStreamingParser

try:
    from testcontainers.postgres import PostgresContainer
    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False


@pytest.mark.integration
class TestIdempotentReloadDetection:
    """Test detection of previously loaded files via MD5."""

    @pytest.fixture
    def sample_vcf(self):
        """Create a sample VCF for testing."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
chr1	300	.	G	A	40	PASS	DP=55	GT	1/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    @pytest.fixture
    def modified_vcf(self):
        """Create a modified VCF (different content, same structure)."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
chr1	300	.	G	A	40	PASS	DP=55	GT	1/1
chr1	400	.	T	C	45	PASS	DP=60	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_md5_computation_consistent(self, sample_vcf):
        """MD5 hash is computed consistently for the same file."""
        md5_1 = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        md5_2 = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        assert md5_1 == md5_2
        assert len(md5_1) == 64

    def test_md5_differs_for_modified_file(self, sample_vcf, modified_vcf):
        """MD5 hash differs when file content changes."""
        md5_original = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        md5_modified = hashlib.sha256(modified_vcf.read_bytes()).hexdigest()
        assert md5_original != md5_modified

    def test_file_size_tracked(self, sample_vcf):
        """File size can be tracked for audit purposes."""
        file_size = sample_vcf.stat().st_size
        assert file_size > 0

    def test_variant_count_for_reload_validation(self, sample_vcf):
        """Variant count can be computed for reload validation."""
        parser = VCFStreamingParser(sample_vcf, human_genome=True)
        try:
            total = 0
            for batch in parser.iter_batches():
                total += len(batch)
            assert total == 3
        finally:
            parser.close()


@pytest.mark.integration
@pytest.mark.skipif(not HAS_TESTCONTAINERS, reason="testcontainers not installed")
class TestIdempotentReloadDatabase:
    """Test idempotent reload with database operations."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        """Create a PostgreSQL container for tests."""
        with PostgresContainer("postgres:15") as postgres:
            yield postgres

    @pytest.fixture
    async def test_db(self, postgres_container):
        """Create isolated test database connection."""
        import asyncpg

        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        conn = await asyncpg.connect(url)

        schema_manager = SchemaManager(human_genome=True)
        await schema_manager.create_schema(conn)

        yield conn

        await conn.close()

    @pytest.fixture
    def sample_vcf(self):
        """Create a sample VCF for testing."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_audit_record_created_on_load(self, test_db, sample_vcf):
        """Audit record is created when loading a VCF."""
        file_hash = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        load_batch_id = uuid4()

        await test_db.execute(
            """
            INSERT INTO variant_load_audit (
                load_batch_id, vcf_file_path, vcf_file_hash,
                vcf_file_size, reference_genome, samples_count, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            load_batch_id,
            str(sample_vcf),
            file_hash,
            sample_vcf.stat().st_size,
            "GRCh38",
            1,
            "started"
        )

        audit = await test_db.fetchrow(
            "SELECT * FROM variant_load_audit WHERE load_batch_id = $1",
            load_batch_id
        )

        assert audit is not None
        assert audit["vcf_file_hash"] == file_hash
        assert audit["status"] == "started"

    @pytest.mark.asyncio
    async def test_detect_duplicate_load_by_md5(self, test_db, sample_vcf):
        """Can detect if file was previously loaded via MD5."""
        file_hash = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        load_batch_id = uuid4()

        await test_db.execute(
            """
            INSERT INTO variant_load_audit (
                load_batch_id, vcf_file_path, vcf_file_hash,
                vcf_file_size, reference_genome, samples_count, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            load_batch_id,
            str(sample_vcf),
            file_hash,
            sample_vcf.stat().st_size,
            "GRCh38",
            1,
            "completed"
        )

        existing = await test_db.fetchrow(
            """
            SELECT load_batch_id, status FROM variant_load_audit
            WHERE vcf_file_hash = $1 AND status = 'completed'
            """,
            file_hash
        )

        assert existing is not None
        assert existing["load_batch_id"] == load_batch_id

    @pytest.mark.asyncio
    async def test_reload_links_to_previous_load(self, test_db, sample_vcf):
        """Reload operation links to previous load via previous_load_id."""
        file_hash = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        original_load_id = uuid4()
        reload_id = uuid4()

        await test_db.execute(
            """
            INSERT INTO variant_load_audit (
                load_batch_id, vcf_file_path, vcf_file_hash,
                vcf_file_size, reference_genome, samples_count, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            original_load_id,
            str(sample_vcf),
            file_hash,
            sample_vcf.stat().st_size,
            "GRCh38",
            1,
            "completed"
        )

        await test_db.execute(
            """
            INSERT INTO variant_load_audit (
                load_batch_id, vcf_file_path, vcf_file_hash,
                vcf_file_size, reference_genome, samples_count, status,
                is_reload, previous_load_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            reload_id,
            str(sample_vcf),
            file_hash,
            sample_vcf.stat().st_size,
            "GRCh38",
            1,
            "started",
            True,
            original_load_id
        )

        reload_audit = await test_db.fetchrow(
            "SELECT * FROM variant_load_audit WHERE load_batch_id = $1",
            reload_id
        )

        assert reload_audit is not None
        assert reload_audit["is_reload"] is True
        assert reload_audit["previous_load_id"] == original_load_id

    @pytest.mark.asyncio
    async def test_audit_status_transitions(self, test_db, sample_vcf):
        """Audit status transitions correctly (started -> completed/failed)."""
        file_hash = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        load_batch_id = uuid4()

        await test_db.execute(
            """
            INSERT INTO variant_load_audit (
                load_batch_id, vcf_file_path, vcf_file_hash,
                vcf_file_size, reference_genome, samples_count, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            load_batch_id,
            str(sample_vcf),
            file_hash,
            sample_vcf.stat().st_size,
            "GRCh38",
            1,
            "started"
        )

        await test_db.execute(
            """
            UPDATE variant_load_audit
            SET status = 'completed',
                variants_loaded = $2,
                load_completed_at = NOW()
            WHERE load_batch_id = $1
            """,
            load_batch_id,
            2
        )

        audit = await test_db.fetchrow(
            "SELECT * FROM variant_load_audit WHERE load_batch_id = $1",
            load_batch_id
        )

        assert audit["status"] == "completed"
        assert audit["variants_loaded"] == 2
        assert audit["load_completed_at"] is not None

    @pytest.mark.asyncio
    async def test_failed_load_records_error(self, test_db, sample_vcf):
        """Failed loads record error message."""
        file_hash = hashlib.sha256(sample_vcf.read_bytes()).hexdigest()
        load_batch_id = uuid4()

        await test_db.execute(
            """
            INSERT INTO variant_load_audit (
                load_batch_id, vcf_file_path, vcf_file_hash,
                vcf_file_size, reference_genome, samples_count, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            load_batch_id,
            str(sample_vcf),
            file_hash,
            sample_vcf.stat().st_size,
            "GRCh38",
            1,
            "started"
        )

        await test_db.execute(
            """
            UPDATE variant_load_audit
            SET status = 'failed',
                error_message = $2,
                load_completed_at = NOW()
            WHERE load_batch_id = $1
            """,
            load_batch_id,
            "Database connection lost"
        )

        audit = await test_db.fetchrow(
            "SELECT * FROM variant_load_audit WHERE load_batch_id = $1",
            load_batch_id
        )

        assert audit["status"] == "failed"
        assert audit["error_message"] == "Database connection lost"


@pytest.mark.integration
class TestIdempotentReloadBehavior:
    """Test the behavior of reload operations."""

    @pytest.fixture
    def vcf_v1(self):
        """Original version of VCF."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    @pytest.fixture
    def vcf_v2(self):
        """Updated version of VCF (additional variant)."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
chr1	300	.	G	A	40	PASS	DP=60	GT	1/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_detect_file_changed(self, vcf_v1, vcf_v2):
        """Can detect when a file has changed via MD5."""
        md5_v1 = hashlib.sha256(vcf_v1.read_bytes()).hexdigest()
        md5_v2 = hashlib.sha256(vcf_v2.read_bytes()).hexdigest()

        assert md5_v1 != md5_v2, "Different content should produce different MD5"

    def test_detect_variant_count_changed(self, vcf_v1, vcf_v2):
        """Can detect variant count changes for validation."""
        def count_variants(vcf_path):
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            try:
                total = 0
                for batch in parser.iter_batches():
                    total += len(batch)
                return total
            finally:
                parser.close()

        count_v1 = count_variants(vcf_v1)
        count_v2 = count_variants(vcf_v2)

        assert count_v1 == 2
        assert count_v2 == 3
        assert count_v2 > count_v1

    def test_identical_content_same_md5(self, vcf_v1):
        """Identical content produces same MD5 regardless of when computed."""
        import time

        md5_1 = hashlib.sha256(vcf_v1.read_bytes()).hexdigest()
        time.sleep(0.01)
        md5_2 = hashlib.sha256(vcf_v1.read_bytes()).hexdigest()

        assert md5_1 == md5_2


@pytest.mark.integration
@pytest.mark.skipif(not HAS_TESTCONTAINERS, reason="testcontainers not installed")
class TestVCFLoaderIdempotentReload:
    """Test actual VCFLoader idempotent reload behavior."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        """Create a PostgreSQL container for tests."""
        with PostgresContainer("postgres:15") as postgres:
            yield postgres

    @pytest.fixture
    def sample_vcf(self):
        """Create a sample VCF for testing."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_loader_skips_duplicate_file(self, postgres_container, sample_vcf):
        """Loader should skip loading if file MD5 matches previous completed load."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=100, normalize=False)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            result1 = await loader.load_vcf(sample_vcf)
            assert result1["variants_loaded"] == 2

        async with VCFLoader(url, config) as loader:
            result2 = await loader.load_vcf(sample_vcf)
            assert result2.get("skipped") is True, "Should skip duplicate file"
            assert result2.get("reason") == "already_loaded"

    @pytest.mark.asyncio
    async def test_loader_force_reload_option(self, postgres_container, sample_vcf):
        """Loader should reload when force_reload=True even if file exists."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=100, normalize=False)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            await loader.load_vcf(sample_vcf)

        async with VCFLoader(url, config) as loader:
            result = await loader.load_vcf(sample_vcf, force_reload=True)
            assert result["variants_loaded"] == 2
            assert result.get("is_reload") is True

    @pytest.mark.asyncio
    async def test_loader_reload_deletes_old_variants(self, postgres_container, sample_vcf):
        """Reload should delete variants from previous load before inserting new ones."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=100, normalize=False)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            await loader.load_vcf(sample_vcf)

            async with loader.pool.acquire() as conn:
                count1 = await conn.fetchval("SELECT COUNT(*) FROM variants")
                assert count1 == 2

        async with VCFLoader(url, config) as loader:
            await loader.load_vcf(sample_vcf, force_reload=True)

            async with loader.pool.acquire() as conn:
                count2 = await conn.fetchval("SELECT COUNT(*) FROM variants")
                assert count2 == 2, "Should have same count after reload (not doubled)"

    @pytest.mark.asyncio
    async def test_loader_check_existing_returns_info(self, postgres_container, sample_vcf):
        """Loader should have method to check if file was previously loaded."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=100, normalize=False)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            existing = await loader.check_existing(sample_vcf)
            assert existing is None, "Should return None for new file"

            await loader.load_vcf(sample_vcf)

        async with VCFLoader(url, config) as loader:
            existing = await loader.check_existing(sample_vcf)
            assert existing is not None, "Should return info for loaded file"
            assert existing["status"] == "completed"
            assert existing["variants_loaded"] == 2
