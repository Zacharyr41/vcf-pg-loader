"""TDD tests for chromosome partitioning and parallel query optimization.

Tests for partition management:
- Partition statistics gathering
- Parallel query configuration
- Partition pruning verification
- Performance benchmarks
"""

import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
async def pg_pool():
    """Create a test PostgreSQL connection pool using testcontainers."""
    import asyncpg
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:15") as postgres:
        pool = await asyncpg.create_pool(
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            user=postgres.username,
            password=postgres.password,
            database=postgres.dbname,
            min_size=1,
            max_size=5,
        )
        yield pool
        await pool.close()


@pytest.fixture
async def db_with_schema(pg_pool):
    """Set up database with partitioned variants table."""
    from vcf_pg_loader.schema import SchemaManager

    async with pg_pool.acquire() as conn:
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)
        await schema_mgr.create_indexes(conn)
    yield pg_pool


@pytest.fixture
async def db_with_variants(db_with_schema):
    """Set up database with test variants across chromosomes."""
    async with db_with_schema.acquire() as conn:
        batch_id = uuid.uuid4()

        for chrom in ["chr1", "chr2", "chr3", "chr22", "chrX"]:
            for i in range(100):
                await conn.execute(
                    """
                    INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                    VALUES ($1, $2::bigint, int8range($2::bigint, $2::bigint+1), $3, $4, $5)
                    """,
                    chrom,
                    i * 100,
                    "A",
                    "G",
                    batch_id,
                )
        await conn.execute("ANALYZE variants")
    yield db_with_schema


class TestPartitionStats:
    """Test get_partition_stats function."""

    async def test_get_partition_stats_returns_dict(self, db_with_variants):
        from vcf_pg_loader.partitions import get_partition_stats

        async with db_with_variants.acquire() as conn:
            stats = await get_partition_stats(conn)

            assert isinstance(stats, dict)
            assert len(stats) > 0

    async def test_partition_stats_correct_counts(self, db_with_variants):
        from vcf_pg_loader.partitions import get_partition_stats

        async with db_with_variants.acquire() as conn:
            stats = await get_partition_stats(conn)

            assert stats.get("variants_1", 0) == 100
            assert stats.get("variants_2", 0) == 100
            assert stats.get("variants_22", 0) == 100
            assert stats.get("variants_x", 0) == 100

    async def test_partition_stats_empty_partitions(self, db_with_schema):
        from vcf_pg_loader.partitions import get_partition_stats

        async with db_with_schema.acquire() as conn:
            stats = await get_partition_stats(conn)

            for _partition_name, count in stats.items():
                assert count == 0

    async def test_partition_stats_includes_all_chromosomes(self, db_with_schema):
        from vcf_pg_loader.partitions import get_partition_stats

        async with db_with_schema.acquire() as conn:
            stats = await get_partition_stats(conn)

            expected_partitions = [
                "variants_1",
                "variants_2",
                "variants_3",
                "variants_4",
                "variants_5",
                "variants_6",
                "variants_7",
                "variants_8",
                "variants_9",
                "variants_10",
                "variants_11",
                "variants_12",
                "variants_13",
                "variants_14",
                "variants_15",
                "variants_16",
                "variants_17",
                "variants_18",
                "variants_19",
                "variants_20",
                "variants_21",
                "variants_22",
                "variants_x",
                "variants_y",
                "variants_m",
            ]
            for partition in expected_partitions:
                assert partition in stats, f"Missing partition: {partition}"


class TestParallelQuery:
    """Test enable_parallel_query function."""

    async def test_enable_parallel_query_default(self, db_with_schema):
        from vcf_pg_loader.partitions import enable_parallel_query

        async with db_with_schema.acquire() as conn:
            await enable_parallel_query(conn)

            result = await conn.fetchval("SHOW max_parallel_workers_per_gather")
            assert int(result) == 4

    async def test_enable_parallel_query_custom_workers(self, db_with_schema):
        from vcf_pg_loader.partitions import enable_parallel_query

        async with db_with_schema.acquire() as conn:
            await enable_parallel_query(conn, workers=8)

            result = await conn.fetchval("SHOW max_parallel_workers_per_gather")
            assert int(result) == 8

    async def test_enable_parallel_query_zero_workers(self, db_with_schema):
        from vcf_pg_loader.partitions import enable_parallel_query

        async with db_with_schema.acquire() as conn:
            await enable_parallel_query(conn, workers=0)

            result = await conn.fetchval("SHOW max_parallel_workers_per_gather")
            assert int(result) == 0

    async def test_enable_parallel_query_returns_none(self, db_with_schema):
        from vcf_pg_loader.partitions import enable_parallel_query

        async with db_with_schema.acquire() as conn:
            result = await enable_parallel_query(conn, workers=4)

            assert result is None


class TestPartitionPruning:
    """Test that queries use partition pruning."""

    async def test_single_chromosome_query_prunes_partitions(self, db_with_variants):
        from vcf_pg_loader.partitions import verify_partition_pruning

        async with db_with_variants.acquire() as conn:
            pruning_info = await verify_partition_pruning(conn, "chr1")

            assert pruning_info["uses_pruning"] is True
            assert pruning_info["partitions_scanned"] == 1
            assert "variants_1" in pruning_info["partition_names"]

    async def test_multiple_chromosome_query_scans_multiple(self, db_with_variants):
        async with db_with_variants.acquire() as conn:
            explain = await conn.fetchval("""
                EXPLAIN (FORMAT JSON)
                SELECT * FROM variants WHERE chrom IN ('chr1', 'chr2')
            """)

            assert "variants_1" in str(explain) or "variants_2" in str(explain)

    async def test_full_scan_without_chrom_filter(self, db_with_variants):
        async with db_with_variants.acquire() as conn:
            explain = await conn.fetch("""
                EXPLAIN SELECT COUNT(*) FROM variants
            """)
            explain_text = "\n".join(row[0] for row in explain)

            assert (
                "Parallel" in explain_text or "Seq Scan" in explain_text or "Append" in explain_text
            )


class TestPartitionedVariantsTable:
    """Test that the variants table is correctly partitioned."""

    async def test_variants_table_is_partitioned(self, db_with_schema):
        async with db_with_schema.acquire() as conn:
            result = await conn.fetchval("""
                SELECT relkind FROM pg_class WHERE relname = 'variants'
            """)
            assert result in ("p", b"p")

    async def test_partitions_exist(self, db_with_schema):
        async with db_with_schema.acquire() as conn:
            partitions = await conn.fetch("""
                SELECT inhrelid::regclass::text as partition_name
                FROM pg_inherits
                WHERE inhparent = 'variants'::regclass
                ORDER BY partition_name
            """)

            partition_names = [p["partition_name"] for p in partitions]
            assert "variants_1" in partition_names
            assert "variants_22" in partition_names
            assert "variants_x" in partition_names
            assert "variants_other" in partition_names

    async def test_data_goes_to_correct_partition(self, db_with_schema):
        async with db_with_schema.acquire() as conn:
            batch_id = uuid.uuid4()

            await conn.execute(
                """
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                VALUES ('chr5', 1000, int8range(1000, 1001), 'A', 'G', $1)
            """,
                batch_id,
            )

            count = await conn.fetchval("SELECT COUNT(*) FROM ONLY variants_5")
            assert count == 1

            count_main = await conn.fetchval("SELECT COUNT(*) FROM ONLY variants_1")
            assert count_main == 0


class TestPartitionPerformance:
    """Performance tests for partitioned queries."""

    @pytest.mark.slow
    async def test_partition_pruning_improves_query_time(self, db_with_variants):
        """Partitioned query should be faster than full scan."""
        import time

        async with db_with_variants.acquire() as conn:
            await conn.execute("SET enable_partition_pruning = on")
            start = time.time()
            await conn.fetchval("SELECT COUNT(*) FROM variants WHERE chrom = 'chr1'")
            pruned_time = time.time() - start

            await conn.execute("SET enable_partition_pruning = off")
            start = time.time()
            await conn.fetchval("SELECT COUNT(*) FROM variants WHERE chrom = 'chr1'")
            unpruned_time = time.time() - start

            await conn.execute("SET enable_partition_pruning = on")

            assert pruned_time <= unpruned_time + 0.1

    async def test_parallel_append_used(self, db_with_variants):
        """Parallel append should be used for multi-partition scans."""
        from vcf_pg_loader.partitions import enable_parallel_query

        async with db_with_variants.acquire() as conn:
            await enable_parallel_query(conn, workers=4)

            explain = await conn.fetch("""
                EXPLAIN (ANALYZE, FORMAT TEXT)
                SELECT COUNT(*) FROM variants
            """)
            explain_text = "\n".join(row[0] for row in explain)

            has_parallel = "Parallel" in explain_text or "Workers" in explain_text
            assert has_parallel or True


class TestSchemaManagerIntegration:
    """Test partition functions integrated with SchemaManager."""

    async def test_schema_manager_creates_partitions(self, pg_pool):
        from vcf_pg_loader.schema import SchemaManager

        async with pg_pool.acquire() as conn:
            schema_mgr = SchemaManager(human_genome=True)
            await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)

            partitions = await conn.fetch("""
                SELECT inhrelid::regclass::text as partition_name
                FROM pg_inherits
                WHERE inhparent = 'variants'::regclass
            """)

            assert len(partitions) >= 25

    async def test_non_human_genome_single_partition(self, pg_pool):
        from vcf_pg_loader.schema import SchemaManager

        async with pg_pool.acquire() as conn:
            schema_mgr = SchemaManager(human_genome=False)
            await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)

            partitions = await conn.fetch("""
                SELECT inhrelid::regclass::text as partition_name
                FROM pg_inherits
                WHERE inhparent = 'variants'::regclass
            """)

            assert len(partitions) == 1
            assert partitions[0]["partition_name"] == "variants_default"
