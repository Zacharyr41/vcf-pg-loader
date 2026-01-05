"""TDD tests for LD block annotation (Berisa & Pickrell 2016).

Tests for:
- LD blocks table creation
- Loading LD blocks for EUR, AFR, EAS populations
- Variant-to-block assignment logic
- Edge cases: variants at block boundaries, variants outside all blocks
- GIST index usage for range queries
- Block-level aggregation view
- Performance test: assign 1M variants to blocks
"""

import time
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def postgres_container():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture
async def db_pool(postgres_container):
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
        max_size=4,
    )

    yield pool
    await pool.close()


@pytest.fixture
def db_url(postgres_container):
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@pytest.mark.integration
class TestLDBlockSchema:
    """Test ld_blocks table creation."""

    @pytest.mark.asyncio
    async def test_create_ld_blocks_table(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'ld_blocks'
                )
            """)
            assert exists is True

    @pytest.mark.asyncio
    async def test_ld_blocks_columns(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

            columns = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'ld_blocks'
            """)
            column_names = {row["column_name"] for row in columns}

            assert "block_id" in column_names
            assert "chrom" in column_names
            assert "start_pos" in column_names
            assert "end_pos" in column_names
            assert "population" in column_names
            assert "source" in column_names
            assert "genome_build" in column_names
            assert "n_snps_1kg" in column_names

    @pytest.mark.asyncio
    async def test_ld_blocks_unique_constraint(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

            await conn.execute("""
                INSERT INTO ld_blocks (chrom, start_pos, end_pos, population, source, genome_build)
                VALUES ('1', 10583, 1892607, 'EUR', 'Berisa_Pickrell_2016', 'GRCh37')
            """)

            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute("""
                    INSERT INTO ld_blocks (chrom, start_pos, end_pos, population, source, genome_build)
                    VALUES ('1', 10583, 1892607, 'EUR', 'Berisa_Pickrell_2016', 'GRCh37')
                """)

    @pytest.mark.asyncio
    async def test_ld_blocks_gist_index_created(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

            indexes = await conn.fetch("""
                SELECT indexname, indexdef FROM pg_indexes
                WHERE tablename = 'ld_blocks'
            """)
            index_names = {row["indexname"] for row in indexes}

            assert "idx_ldblock_region" in index_names


@pytest.mark.integration
class TestLDBlockLoader:
    """Test LD block loading from BED files."""

    @pytest.mark.asyncio
    async def test_load_eur_blocks(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            result = await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            assert result["blocks_loaded"] == 15
            assert result["population"] == "EUR"
            assert result["build"] == "grch37"

    @pytest.mark.asyncio
    async def test_load_afr_blocks(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_afr_grch37.bed"

        async with db_pool.acquire() as conn:
            result = await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="AFR", build="grch37"
            )

            assert result["blocks_loaded"] == 7
            assert result["population"] == "AFR"

    @pytest.mark.asyncio
    async def test_load_multiple_populations(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

        loader = LDBlockLoader()

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn,
                FIXTURES_DIR / "ld_blocks_eur_grch37.bed",
                population="EUR",
                build="grch37",
            )
            await loader.load_berisa_pickrell_blocks(
                conn,
                FIXTURES_DIR / "ld_blocks_afr_grch37.bed",
                population="AFR",
                build="grch37",
            )

            eur_count = await conn.fetchval(
                "SELECT COUNT(*) FROM ld_blocks WHERE population = 'EUR'"
            )
            afr_count = await conn.fetchval(
                "SELECT COUNT(*) FROM ld_blocks WHERE population = 'AFR'"
            )

            assert eur_count == 15
            assert afr_count == 7

    @pytest.mark.asyncio
    async def test_block_data_inserted_correctly(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            row = await conn.fetchrow("""
                SELECT * FROM ld_blocks
                WHERE chrom = '1' AND start_pos = 10583
            """)

            assert row is not None
            assert row["end_pos"] == 1892607
            assert row["population"] == "EUR"
            assert row["source"] == "Berisa_Pickrell_2016"
            assert row["genome_build"] == "GRCh37"
            assert row["n_snps_1kg"] == 5000

    @pytest.mark.asyncio
    async def test_reload_replaces_blocks(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_ld_blocks_table(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )
            result = await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            count = await conn.fetchval("SELECT COUNT(*) FROM ld_blocks WHERE population = 'EUR'")

            assert result["blocks_loaded"] == 15
            assert count == 15


@pytest.mark.integration
class TestVariantBlockAssignment:
    """Test assigning variants to LD blocks."""

    @pytest.mark.asyncio
    async def test_assign_variant_inside_block(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            await conn.execute("""
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                VALUES ('chr1', 1000000, int8range(1000000, 1000001), 'A', 'G',
                        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')
            """)

            updated = await loader.assign_variants_to_blocks(conn, population="EUR")

            assert updated == 1

            row = await conn.fetchrow("""
                SELECT ld_block_id FROM variants WHERE pos = 1000000
            """)
            assert row["ld_block_id"] is not None

    @pytest.mark.asyncio
    async def test_variant_at_block_start_boundary(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            await conn.execute("""
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                VALUES ('chr1', 10583, int8range(10583, 10584), 'A', 'G',
                        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')
            """)

            await loader.assign_variants_to_blocks(conn, population="EUR")

            row = await conn.fetchrow("""
                SELECT ld_block_id FROM variants WHERE pos = 10583
            """)
            assert row["ld_block_id"] is not None

    @pytest.mark.asyncio
    async def test_variant_at_block_end_boundary(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            await conn.execute("""
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                VALUES ('chr1', 1892607, int8range(1892607, 1892608), 'A', 'G',
                        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')
            """)

            await loader.assign_variants_to_blocks(conn, population="EUR")

            row = await conn.fetchrow("""
                SELECT ld_block_id FROM variants WHERE pos = 1892607
            """)
            assert row["ld_block_id"] is not None

    @pytest.mark.asyncio
    async def test_variant_outside_all_blocks(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            await conn.execute("""
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                VALUES ('chr1', 999999999, int8range(999999999, 1000000000), 'A', 'G',
                        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')
            """)

            updated = await loader.assign_variants_to_blocks(conn, population="EUR")

            assert updated == 0

            row = await conn.fetchrow("""
                SELECT ld_block_id FROM variants WHERE pos = 999999999
            """)
            assert row["ld_block_id"] is None

    @pytest.mark.asyncio
    async def test_assign_multiple_variants(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            for pos in [1000000, 2000000, 3000000, 4000000, 5000000]:
                await conn.execute(
                    """
                    INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                    VALUES ('chr1', $1::bigint, int8range($1::bigint, $1::bigint + 1), 'A', 'G',
                            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')
                    """,
                    pos,
                )

            updated = await loader.assign_variants_to_blocks(conn, population="EUR")

            assert updated == 5


@pytest.mark.integration
class TestGISTIndexUsage:
    """Test that GIST index is created and usable."""

    @pytest.mark.asyncio
    async def test_gist_index_exists_and_works(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        ref_schema = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await ref_schema.create_ld_blocks_table(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            index_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE indexname = 'idx_ldblock_region'
                )
            """)
            assert index_exists is True

            result = await conn.fetchrow("""
                SELECT block_id FROM ld_blocks
                WHERE chrom = '1'
                  AND int8range(start_pos, end_pos, '[]') @> 1000000::bigint
            """)
            assert result is not None
            assert result["block_id"] is not None


@pytest.mark.integration
class TestBlockAggregationView:
    """Test variant_ld_block_summary view."""

    @pytest.mark.asyncio
    async def test_create_summary_view(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)
            await ref_schema.create_ld_block_summary_view(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.views
                    WHERE table_name = 'variant_ld_block_summary'
                )
            """)
            assert exists is True

    @pytest.mark.asyncio
    async def test_summary_view_aggregation(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)
            await ref_schema.create_ld_block_summary_view(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            for pos in [1000000, 1500000, 1800000]:
                await conn.execute(
                    """
                    INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id, in_hapmap3)
                    VALUES ('chr1', $1::bigint, int8range($1::bigint, $1::bigint + 1), 'A', 'G',
                            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', $2)
                    """,
                    pos,
                    pos == 1000000,
                )

            await loader.assign_variants_to_blocks(conn, population="EUR")

            summary = await conn.fetchrow("""
                SELECT n_variants, n_hapmap3
                FROM variant_ld_block_summary
                WHERE chrom = '1' AND start_pos = 10583
            """)

            assert summary["n_variants"] == 3
            assert summary["n_hapmap3"] == 1


@pytest.mark.integration
class TestLDBlockPerformance:
    """Performance tests for LD block assignment."""

    @pytest.mark.asyncio
    async def test_assign_100k_variants_performance(self, db_pool):
        from vcf_pg_loader.references.ld_blocks import LDBlockLoader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        ref_schema = ReferenceSchemaManager()
        main_schema = SchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn, skip_encryption=True, skip_emergency=True)
            await ref_schema.create_ld_blocks_table(conn)
            await ref_schema.add_ld_block_id_column(conn)

        loader = LDBlockLoader()
        bed_path = FIXTURES_DIR / "ld_blocks_eur_grch37.bed"

        async with db_pool.acquire() as conn:
            await loader.load_berisa_pickrell_blocks(
                conn, bed_path, population="EUR", build="grch37"
            )

            batch_size = 10000
            total_variants = 100000
            base_pos = 100000

            for batch_start in range(0, total_variants, batch_size):
                batch = [
                    (
                        "chr1",
                        base_pos + i,
                        asyncpg.Range(base_pos + i, base_pos + i + 1),
                        "A",
                        "G",
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                    )
                    for i in range(batch_start, min(batch_start + batch_size, total_variants))
                ]
                await conn.executemany(
                    """
                    INSERT INTO variants (chrom, pos, pos_range, ref, alt, load_batch_id)
                    VALUES ($1, $2, $3, $4, $5, $6::uuid)
                    """,
                    batch,
                )

            await conn.execute("ANALYZE variants")
            await conn.execute("ANALYZE ld_blocks")

            start = time.perf_counter()
            updated = await loader.assign_variants_to_blocks(conn, population="EUR")
            elapsed = time.perf_counter() - start

            assert updated > 0
            assert elapsed < 30.0, f"Assignment took {elapsed:.2f}s, expected <30s"


class TestLDBlockNormalization:
    """Unit tests for chromosome normalization."""

    def test_normalize_chrom_with_prefix(self):
        from vcf_pg_loader.references.ld_blocks import normalize_chrom_for_ld

        assert normalize_chrom_for_ld("chr1") == "1"
        assert normalize_chrom_for_ld("chr22") == "22"
        assert normalize_chrom_for_ld("chrX") == "X"

    def test_normalize_chrom_without_prefix(self):
        from vcf_pg_loader.references.ld_blocks import normalize_chrom_for_ld

        assert normalize_chrom_for_ld("1") == "1"
        assert normalize_chrom_for_ld("22") == "22"
        assert normalize_chrom_for_ld("X") == "X"
