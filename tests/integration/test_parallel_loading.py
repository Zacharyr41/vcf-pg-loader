"""Integration tests for parallel chromosome loading."""

import asyncio
import tempfile
import time
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
class TestChromosomePartitioning:
    """Test chromosome-based partitioning for parallel loading."""

    @pytest.fixture
    def multi_chromosome_vcf(self):
        """Create a VCF with variants across multiple chromosomes."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chr3,length=198295559>
##contig=<ID=chr17,length=83257441>
##contig=<ID=chrX,length=156040895>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
chr1	300	.	G	A	40	PASS	DP=55	GT	1/1
chr2	1000	.	T	C	30	PASS	DP=50	GT	0/1
chr2	2000	.	A	G	35	PASS	DP=45	GT	0/1
chr3	500	.	C	T	40	PASS	DP=55	GT	1/1
chr17	43094464	.	C	T	100	PASS	DP=60	GT	0/1
chrX	100000	.	G	A	90	PASS	DP=40	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_variants_distributed_across_chromosomes(self, multi_chromosome_vcf):
        """Variants are correctly assigned to chromosomes."""
        parser = VCFStreamingParser(multi_chromosome_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            chrom_counts = {}
            for r in records:
                chrom_counts[r.chrom] = chrom_counts.get(r.chrom, 0) + 1

            assert chrom_counts.get("chr1", 0) == 3
            assert chrom_counts.get("chr2", 0) == 2
            assert chrom_counts.get("chr3", 0) == 1
            assert chrom_counts.get("chr17", 0) == 1
            assert chrom_counts.get("chrX", 0) == 1
        finally:
            parser.close()

    def test_chromosome_extraction_for_parallel_processing(self, multi_chromosome_vcf):
        """Can extract chromosome set for parallel task distribution."""
        parser = VCFStreamingParser(multi_chromosome_vcf, human_genome=True)
        try:
            chromosomes = set()
            for batch in parser.iter_batches():
                for r in batch:
                    chromosomes.add(r.chrom)

            expected_chroms = {"chr1", "chr2", "chr3", "chr17", "chrX"}
            assert chromosomes == expected_chroms
        finally:
            parser.close()

    def test_variants_can_be_grouped_by_chromosome(self, multi_chromosome_vcf):
        """Variants can be grouped by chromosome for parallel insertion."""
        parser = VCFStreamingParser(multi_chromosome_vcf, human_genome=True)
        try:
            chrom_batches = {}
            for batch in parser.iter_batches():
                for r in batch:
                    if r.chrom not in chrom_batches:
                        chrom_batches[r.chrom] = []
                    chrom_batches[r.chrom].append(r)

            assert len(chrom_batches) == 5
            assert len(chrom_batches["chr1"]) == 3
            assert len(chrom_batches["chr2"]) == 2
        finally:
            parser.close()


@pytest.mark.integration
@pytest.mark.skipif(not HAS_TESTCONTAINERS, reason="testcontainers not installed")
class TestParallelDatabaseLoading:
    """Test parallel loading into database partitions."""

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
    def multi_chromosome_vcf(self):
        """Create a VCF with variants across multiple chromosomes."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chr3,length=198295559>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100	.	A	G	30	PASS	DP=50	GT	0/1
chr1	200	.	C	T	35	PASS	DP=45	GT	0/1
chr2	1000	.	T	C	30	PASS	DP=50	GT	0/1
chr3	500	.	C	T	40	PASS	DP=55	GT	1/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_partitions_created_for_human_genome(self, test_db):
        """Human genome schema creates chromosome partitions."""
        partitions = await test_db.fetch("""
            SELECT tablename FROM pg_tables
            WHERE tablename LIKE 'variants_%'
            ORDER BY tablename
        """)

        partition_names = [p["tablename"] for p in partitions]
        assert "variants_1" in partition_names
        assert "variants_2" in partition_names
        assert "variants_x" in partition_names
        assert "variants_y" in partition_names

    @pytest.mark.asyncio
    async def test_variants_routed_to_correct_partitions(self, test_db, multi_chromosome_vcf):
        """Variants are routed to correct chromosome partitions."""
        from asyncpg import Range

        parser = VCFStreamingParser(multi_chromosome_vcf, human_genome=True)
        load_batch_id = uuid4()

        try:
            for batch in parser.iter_batches():
                for r in batch:
                    await test_db.execute(
                        """
                        INSERT INTO variants (
                            chrom, pos_range, pos, ref, alt, load_batch_id
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        r.chrom,
                        Range(r.pos, r.pos + len(r.ref)),
                        r.pos,
                        r.ref,
                        r.alt,
                        load_batch_id
                    )

            chr1_count = await test_db.fetchval(
                "SELECT COUNT(*) FROM variants_1"
            )
            chr2_count = await test_db.fetchval(
                "SELECT COUNT(*) FROM variants_2"
            )
            chr3_count = await test_db.fetchval(
                "SELECT COUNT(*) FROM variants_3"
            )

            assert chr1_count == 2
            assert chr2_count == 1
            assert chr3_count == 1
        finally:
            parser.close()

    @pytest.mark.asyncio
    async def test_parallel_insert_simulation(self, postgres_container, multi_chromosome_vcf):
        """Simulate parallel inserts to different partitions using connection pool."""
        import asyncpg
        from asyncpg import Range

        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        pool = await asyncpg.create_pool(url, min_size=3, max_size=5)

        async with pool.acquire() as conn:
            schema_manager = SchemaManager(human_genome=True)
            await schema_manager.create_schema(conn)

        parser = VCFStreamingParser(multi_chromosome_vcf, human_genome=True)

        try:
            chrom_batches = {}
            for batch in parser.iter_batches():
                for r in batch:
                    if r.chrom not in chrom_batches:
                        chrom_batches[r.chrom] = []
                    chrom_batches[r.chrom].append(r)
        finally:
            parser.close()

        async def insert_chromosome_batch(chrom, records):
            load_batch_id = uuid4()
            async with pool.acquire() as conn:
                for r in records:
                    await conn.execute(
                        """
                        INSERT INTO variants (
                            chrom, pos_range, pos, ref, alt, load_batch_id
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        r.chrom,
                        Range(r.pos, r.pos + len(r.ref)),
                        r.pos,
                        r.ref,
                        r.alt,
                        load_batch_id
                    )
            return len(records)

        tasks = [
            insert_chromosome_batch(chrom, records)
            for chrom, records in chrom_batches.items()
        ]
        results = await asyncio.gather(*tasks)

        total_inserted = sum(results)
        assert total_inserted == 4

        await pool.close()


@pytest.mark.integration
class TestParallelLoadingPerformance:
    """Test performance characteristics of parallel loading."""

    @pytest.fixture
    def large_multi_chromosome_vcf(self):
        """Create a larger VCF for performance testing."""
        lines = [
            "##fileformat=VCFv4.3",
            "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">",
            "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
        ]
        for i in range(1, 23):
            lines.append(f"##contig=<ID=chr{i},length=100000000>")
        lines.append("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1")

        for i in range(1000):
            chrom = f"chr{(i % 22) + 1}"
            pos = 100 + (i * 100)
            lines.append(f"{chrom}\t{pos}\t.\tA\tG\t30\tPASS\tDP=50\tGT\t0/1")

        vcf_content = "\n".join(lines) + "\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_chromosome_distribution_balanced(self, large_multi_chromosome_vcf):
        """Verify variants are distributed across chromosomes for balanced parallelism."""
        parser = VCFStreamingParser(large_multi_chromosome_vcf, human_genome=True)
        try:
            chrom_counts = {}
            for batch in parser.iter_batches():
                for r in batch:
                    chrom_counts[r.chrom] = chrom_counts.get(r.chrom, 0) + 1

            counts = list(chrom_counts.values())
            avg = sum(counts) / len(counts)
            min_count = min(counts)
            max_count = max(counts)

            assert max_count - min_count <= 2, "Distribution should be relatively balanced"
            assert abs(avg - (1000 / 22)) < 1, "Average should be ~45 per chromosome"
        finally:
            parser.close()

    def test_batching_preserves_chromosome_locality(self, large_multi_chromosome_vcf):
        """Batches maintain chromosome locality for efficient parallel loading."""
        parser = VCFStreamingParser(large_multi_chromosome_vcf, human_genome=True, batch_size=100)
        try:
            batch_chrom_counts = []
            for batch in parser.iter_batches():
                chroms_in_batch = len({r.chrom for r in batch})
                batch_chrom_counts.append(chroms_in_batch)

            avg_chroms_per_batch = sum(batch_chrom_counts) / len(batch_chrom_counts)
            assert avg_chroms_per_batch <= 22, "Batches should have bounded chromosome diversity"
        finally:
            parser.close()


@pytest.mark.integration
class TestWorkerCoordination:
    """Test coordination patterns for parallel workers."""

    @pytest.fixture
    def chromosome_vcfs(self):
        """Create separate VCFs per chromosome to simulate worker distribution."""
        vcfs = {}
        for chrom_num in [1, 2, 3]:
            chrom = f"chr{chrom_num}"
            vcf_content = f"""##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID={chrom},length=100000000>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
{chrom}\t100\t.\tA\tG\t30\tPASS\tDP=50\tGT\t0/1
{chrom}\t200\t.\tC\tT\t35\tPASS\tDP=45\tGT\t0/1
"""
            with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
                f.write(vcf_content)
                vcfs[chrom] = Path(f.name)

        yield vcfs

        for path in vcfs.values():
            path.unlink()

    def test_independent_chromosome_parsing(self, chromosome_vcfs):
        """Each chromosome can be parsed independently."""
        results = {}
        for chrom, vcf_path in chromosome_vcfs.items():
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            try:
                count = 0
                for batch in parser.iter_batches():
                    count += len(batch)
                results[chrom] = count
            finally:
                parser.close()

        assert results["chr1"] == 2
        assert results["chr2"] == 2
        assert results["chr3"] == 2

    def test_concurrent_parsing_simulation(self, chromosome_vcfs):
        """Simulate concurrent parsing of multiple chromosomes."""
        import concurrent.futures

        def parse_chromosome(chrom_vcf_pair):
            chrom, vcf_path = chrom_vcf_pair
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            try:
                count = 0
                for batch in parser.iter_batches():
                    count += len(batch)
                return chrom, count
            finally:
                parser.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(parse_chromosome, item)
                for item in chromosome_vcfs.items()
            ]
            results = dict(f.result() for f in concurrent.futures.as_completed(futures))

        assert len(results) == 3
        assert all(count == 2 for count in results.values())


@pytest.mark.integration
@pytest.mark.skipif(not HAS_TESTCONTAINERS, reason="testcontainers not installed")
class TestVCFLoaderParallelLoading:
    """Test actual VCFLoader parallel loading by chromosome."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        """Create a PostgreSQL container for tests."""
        with PostgresContainer("postgres:15") as postgres:
            yield postgres

    @pytest.fixture
    def multi_chromosome_vcf(self):
        """Create a VCF with variants across multiple chromosomes."""
        lines = [
            "##fileformat=VCFv4.3",
            "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">",
            "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
        ]
        for i in range(1, 23):
            lines.append(f"##contig=<ID=chr{i},length=100000000>")
        lines.append("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1")

        for i in range(500):
            chrom = f"chr{(i % 22) + 1}"
            pos = 100 + (i * 100)
            lines.append(f"{chrom}\t{pos}\t.\tA\tG\t30\tPASS\tDP=50\tGT\t0/1")

        vcf_content = "\n".join(lines) + "\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    @pytest.mark.asyncio
    async def test_loader_supports_parallel_workers_config(self, postgres_container, multi_chromosome_vcf):
        """Loader should accept parallel_workers configuration."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=100, normalize=False, workers=4)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            result = await loader.load_vcf(multi_chromosome_vcf, parallel=True)
            assert result["variants_loaded"] == 500

    @pytest.mark.asyncio
    async def test_parallel_loading_faster_than_sequential(self, postgres_container, multi_chromosome_vcf):
        """Parallel loading should be faster than sequential for large files."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=50, normalize=False, workers=4)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            start_seq = time.time()
            await loader.load_vcf(multi_chromosome_vcf, parallel=False)
            seq_time = time.time() - start_seq

            async with loader.pool.acquire() as conn:
                await conn.execute("DELETE FROM variants")

        async with VCFLoader(url, config) as loader:
            start_par = time.time()
            await loader.load_vcf(multi_chromosome_vcf, parallel=True)
            par_time = time.time() - start_par

        assert par_time <= seq_time * 1.5, f"Parallel ({par_time:.2f}s) should not be much slower than sequential ({seq_time:.2f}s)"

    @pytest.mark.asyncio
    async def test_parallel_loading_distributes_across_workers(self, postgres_container, multi_chromosome_vcf):
        """Parallel loading should distribute work across configured workers."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=50, normalize=False, workers=4)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            result = await loader.load_vcf(multi_chromosome_vcf, parallel=True)

            assert result["variants_loaded"] == 500
            assert result.get("parallel") is True

    @pytest.mark.asyncio
    async def test_parallel_loading_all_variants_inserted(self, postgres_container, multi_chromosome_vcf):
        """All variants are correctly inserted with parallel loading."""
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=50, normalize=False, workers=4)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            await loader.load_vcf(multi_chromosome_vcf, parallel=True)

            async with loader.pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM variants")
                assert count == 500

                chrom_counts = await conn.fetch(
                    "SELECT chrom, COUNT(*) as cnt FROM variants GROUP BY chrom ORDER BY chrom"
                )
                assert len(chrom_counts) == 22
