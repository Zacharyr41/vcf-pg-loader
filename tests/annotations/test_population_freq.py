"""TDD tests for normalized population frequency storage.

Tests for:
- gnomAD INFO field parsing for all populations
- popmax computation with bottlenecked population exclusion
- Missing population handling
- Bulk import performance
- Population-specific query performance with index usage
"""

import sys
import time
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
sys.path.insert(0, str(FIXTURES_DIR.parent))

from fixtures.gnomad_test_data import (  # noqa: E402
    GNOMAD_INFO_BOTTLENECKED_HIGH,
    GNOMAD_INFO_COMPLETE,
    GNOMAD_INFO_PARTIAL,
    GNOMAD_INFO_RARE,
    GNOMAD_V4_INFO,
    TOPMED_INFO,
    generate_bulk_gnomad_records,
)


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


class TestParseGnomadInfo:
    """Unit tests for parse_gnomad_info function."""

    def test_parse_complete_gnomad_info(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(GNOMAD_INFO_COMPLETE)

        assert len(result) == 7
        assert "AFR" in result
        assert "AMR" in result
        assert "ASJ" in result
        assert "EAS" in result
        assert "FIN" in result
        assert "NFE" in result
        assert "SAS" in result

    def test_parse_afr_population(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(GNOMAD_INFO_COMPLETE)

        afr = result["AFR"]
        assert afr.ac == 800
        assert afr.an == 20000
        assert afr.af == pytest.approx(0.04, rel=1e-6)
        assert afr.hom_count == 32
        assert afr.faf_95 == pytest.approx(0.035, rel=1e-6)

    def test_parse_nfe_population(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(GNOMAD_INFO_COMPLETE)

        nfe = result["NFE"]
        assert nfe.ac == 180
        assert nfe.an == 30000
        assert nfe.af == pytest.approx(0.006, rel=1e-6)
        assert nfe.hom_count == 0
        assert nfe.faf_95 == pytest.approx(0.005, rel=1e-6)

    def test_parse_partial_info_missing_populations(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(GNOMAD_INFO_PARTIAL)

        assert "AFR" in result
        assert "NFE" in result
        assert "AMR" not in result
        assert "ASJ" not in result
        assert "EAS" not in result
        assert "FIN" not in result
        assert "SAS" not in result

    def test_parse_missing_faf95(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(GNOMAD_INFO_PARTIAL)

        assert result["AFR"].faf_95 is None
        assert result["NFE"].faf_95 is None

    def test_parse_missing_hom_count(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(GNOMAD_INFO_PARTIAL)

        assert result["AFR"].hom_count is None
        assert result["NFE"].hom_count is None

    def test_parse_empty_info(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info({})
        assert len(result) == 0

    def test_parse_gnomad_v4_format(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(GNOMAD_V4_INFO, prefix="gnomad_")

        assert "AFR" in result
        assert result["AFR"].ac == 900
        assert result["AFR"].af == pytest.approx(0.03, rel=1e-6)

    def test_parse_topmed_format(self):
        from vcf_pg_loader.annotations.population_freq import parse_gnomad_info

        result = parse_gnomad_info(TOPMED_INFO, prefix="TOPMED_")

        assert "AFR" in result
        assert result["AFR"].ac == 600
        assert result["AFR"].af == pytest.approx(0.015, rel=1e-6)


class TestComputePopmax:
    """Unit tests for compute_popmax function."""

    def test_popmax_excludes_bottlenecked_by_default(self):
        from vcf_pg_loader.annotations.population_freq import (
            compute_popmax,
            parse_gnomad_info,
        )

        frequencies = parse_gnomad_info(GNOMAD_INFO_COMPLETE)
        popmax_af, popmax_pop = compute_popmax(frequencies)

        assert popmax_pop == "AFR"
        assert popmax_af == pytest.approx(0.04, rel=1e-6)

    def test_popmax_includes_bottlenecked_when_disabled(self):
        from vcf_pg_loader.annotations.population_freq import (
            compute_popmax,
            parse_gnomad_info,
        )

        frequencies = parse_gnomad_info(GNOMAD_INFO_BOTTLENECKED_HIGH)
        popmax_af, popmax_pop = compute_popmax(frequencies, exclude_bottlenecked=False)

        assert popmax_pop == "ASJ"
        assert popmax_af == pytest.approx(0.016, rel=1e-6)

    def test_popmax_excludes_asj_fin_by_default(self):
        from vcf_pg_loader.annotations.population_freq import (
            compute_popmax,
            parse_gnomad_info,
        )

        frequencies = parse_gnomad_info(GNOMAD_INFO_BOTTLENECKED_HIGH)
        popmax_af, popmax_pop = compute_popmax(frequencies)

        assert popmax_pop != "ASJ"
        assert popmax_pop != "FIN"
        assert popmax_pop == "AFR"
        assert popmax_af == pytest.approx(0.0005, rel=1e-6)

    def test_popmax_empty_frequencies(self):
        from vcf_pg_loader.annotations.population_freq import compute_popmax

        popmax_af, popmax_pop = compute_popmax({})

        assert popmax_af is None
        assert popmax_pop is None

    def test_popmax_all_zero_frequencies(self):
        from vcf_pg_loader.annotations.population_freq import (
            PopulationFrequency,
            compute_popmax,
        )

        frequencies = {
            "AFR": PopulationFrequency(ac=0, an=10000, af=0.0),
            "NFE": PopulationFrequency(ac=0, an=20000, af=0.0),
        }
        popmax_af, popmax_pop = compute_popmax(frequencies)

        assert popmax_af == 0.0
        assert popmax_pop in ["AFR", "NFE"]

    def test_popmax_single_population(self):
        from vcf_pg_loader.annotations.population_freq import (
            PopulationFrequency,
            compute_popmax,
        )

        frequencies = {
            "AFR": PopulationFrequency(ac=100, an=10000, af=0.01),
        }
        popmax_af, popmax_pop = compute_popmax(frequencies)

        assert popmax_pop == "AFR"
        assert popmax_af == pytest.approx(0.01, rel=1e-6)

    def test_popmax_rare_variant(self):
        from vcf_pg_loader.annotations.population_freq import (
            compute_popmax,
            parse_gnomad_info,
        )

        frequencies = parse_gnomad_info(GNOMAD_INFO_RARE)
        popmax_af, popmax_pop = compute_popmax(frequencies)

        assert popmax_pop == "AFR"
        assert popmax_af == pytest.approx(0.0001, rel=1e-6)


class TestPopulationFrequencyDataclass:
    """Unit tests for PopulationFrequency dataclass."""

    def test_create_with_all_fields(self):
        from vcf_pg_loader.annotations.population_freq import PopulationFrequency

        pf = PopulationFrequency(
            ac=100,
            an=10000,
            af=0.01,
            hom_count=1,
            faf_95=0.008,
        )

        assert pf.ac == 100
        assert pf.an == 10000
        assert pf.af == 0.01
        assert pf.hom_count == 1
        assert pf.faf_95 == 0.008

    def test_create_with_optional_fields_none(self):
        from vcf_pg_loader.annotations.population_freq import PopulationFrequency

        pf = PopulationFrequency(ac=100, an=10000, af=0.01)

        assert pf.hom_count is None
        assert pf.faf_95 is None


@pytest.mark.integration
class TestPopulationFreqSchemaCreation:
    """Integration tests for population_frequencies table creation."""

    @pytest.mark.asyncio
    async def test_create_population_frequencies_table(self, db_pool):
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager

        schema_manager = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_population_frequencies_table(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'population_frequencies'
                )
            """)
            assert exists is True

    @pytest.mark.asyncio
    async def test_population_frequencies_columns(self, db_pool):
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager

        schema_manager = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_population_frequencies_table(conn)

            columns = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'population_frequencies'
            """)
            column_names = {row["column_name"] for row in columns}

            assert "id" in column_names
            assert "variant_id" in column_names
            assert "source" in column_names
            assert "population" in column_names
            assert "subset" in column_names
            assert "ac" in column_names
            assert "an" in column_names
            assert "af" in column_names
            assert "hom_count" in column_names
            assert "faf_95" in column_names

    @pytest.mark.asyncio
    async def test_population_frequencies_unique_constraint(self, db_pool):
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager

        schema_manager = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_population_frequencies_table(conn)

            constraints = await conn.fetch("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_name = 'population_frequencies'
            """)
            constraint_types = {row["constraint_type"] for row in constraints}

            assert "UNIQUE" in constraint_types

    @pytest.mark.asyncio
    async def test_population_frequencies_indexes(self, db_pool):
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager

        schema_manager = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_population_frequencies_table(conn)
            await schema_manager.create_popfreq_indexes(conn)

            indexes = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'population_frequencies'
            """)
            index_names = {row["indexname"] for row in indexes}

            assert "idx_popfreq_lookup" in index_names
            assert "idx_popfreq_af" in index_names


@pytest.mark.integration
class TestPopulationFreqLoader:
    """Integration tests for PopulationFreqLoader."""

    @pytest.mark.asyncio
    async def test_import_frequencies_from_info(self, db_pool):
        from vcf_pg_loader.annotations.population_freq import PopulationFreqLoader
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        popfreq_schema = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await popfreq_schema.create_population_frequencies_table(conn)

            await conn.execute("""
                INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                VALUES ('chr1', '[100, 101)', 100, 'A', 'G', gen_random_uuid())
            """)

        loader = PopulationFreqLoader()

        async with db_pool.acquire() as conn:
            variant_id = await conn.fetchval(
                "SELECT variant_id FROM variants WHERE chrom = 'chr1' AND pos = 100"
            )

            result = await loader.import_variant_frequencies(
                conn=conn,
                variant_id=variant_id,
                info=GNOMAD_INFO_COMPLETE,
                source="gnomAD_v3",
            )

            assert result["frequencies_inserted"] == 7

    @pytest.mark.asyncio
    async def test_popmax_updated_in_variants(self, db_pool):
        from vcf_pg_loader.annotations.population_freq import PopulationFreqLoader
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        popfreq_schema = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await popfreq_schema.create_population_frequencies_table(conn)

            await conn.execute("""
                INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                VALUES ('chr1', '[100, 101)', 100, 'A', 'G', gen_random_uuid())
            """)

        loader = PopulationFreqLoader()

        async with db_pool.acquire() as conn:
            variant_id = await conn.fetchval(
                "SELECT variant_id FROM variants WHERE chrom = 'chr1' AND pos = 100"
            )

            await loader.import_variant_frequencies(
                conn=conn,
                variant_id=variant_id,
                info=GNOMAD_INFO_COMPLETE,
                source="gnomAD_v3",
                update_popmax=True,
            )

            row = await conn.fetchrow(
                "SELECT gnomad_popmax_af, gnomad_popmax_pop FROM variants WHERE variant_id = $1",
                variant_id,
            )

            assert row["gnomad_popmax_af"] == pytest.approx(0.04, rel=1e-6)
            assert row["gnomad_popmax_pop"] == "AFR"

    @pytest.mark.asyncio
    async def test_query_by_population(self, db_pool):
        from vcf_pg_loader.annotations.population_freq import PopulationFreqLoader
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        popfreq_schema = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await popfreq_schema.create_population_frequencies_table(conn)
            await popfreq_schema.create_popfreq_indexes(conn)

            await conn.execute("""
                INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                VALUES ('chr1', '[100, 101)', 100, 'A', 'G', gen_random_uuid())
            """)

        loader = PopulationFreqLoader()

        async with db_pool.acquire() as conn:
            variant_id = await conn.fetchval(
                "SELECT variant_id FROM variants WHERE chrom = 'chr1' AND pos = 100"
            )

            await loader.import_variant_frequencies(
                conn=conn,
                variant_id=variant_id,
                info=GNOMAD_INFO_COMPLETE,
                source="gnomAD_v3",
            )

            afr_rows = await conn.fetch("""
                SELECT * FROM population_frequencies
                WHERE population = 'AFR'
            """)
            assert len(afr_rows) == 1
            assert afr_rows[0]["af"] == pytest.approx(0.04, rel=1e-6)


@pytest.mark.integration
class TestBulkImportPerformance:
    """Performance tests for bulk population frequency import."""

    @pytest.mark.asyncio
    async def test_bulk_import_100k_variants(self, db_pool):
        from vcf_pg_loader.annotations.population_freq import PopulationFreqLoader
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        popfreq_schema = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await popfreq_schema.create_population_frequencies_table(conn)

        records = generate_bulk_gnomad_records(n_variants=1000)

        async with db_pool.acquire() as conn:
            for rec in records:
                pos_range = f"[{rec['pos']}, {rec['pos'] + 1})"
                await conn.execute(
                    f"""
                    INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                    VALUES ($1, '{pos_range}', $2, $3, $4, gen_random_uuid())
                    """,
                    rec["chrom"],
                    rec["pos"],
                    rec["ref"],
                    rec["alt"],
                )

        loader = PopulationFreqLoader(batch_size=1000)

        start = time.perf_counter()

        async with db_pool.acquire() as conn:
            variant_rows = await conn.fetch("SELECT variant_id FROM variants ORDER BY variant_id")

            batch = []
            for vrow, rec in zip(variant_rows, records, strict=True):
                batch.append((vrow["variant_id"], rec["info"]))

                if len(batch) >= 100:
                    await loader.import_batch_frequencies(
                        conn=conn,
                        batch=batch,
                        source="gnomAD_v3",
                    )
                    batch = []

            if batch:
                await loader.import_batch_frequencies(
                    conn=conn,
                    batch=batch,
                    source="gnomAD_v3",
                )

        elapsed = time.perf_counter() - start

        assert elapsed < 30.0

        async with db_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM population_frequencies")
            assert count > 0


@pytest.mark.integration
class TestPopulationSpecificQueries:
    """Test population-specific query performance with indexes."""

    @pytest.mark.asyncio
    async def test_query_rare_variants_by_population(self, db_pool):
        from vcf_pg_loader.annotations.population_freq import PopulationFreqLoader
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        popfreq_schema = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await popfreq_schema.create_population_frequencies_table(conn)
            await popfreq_schema.create_popfreq_indexes(conn)

            await conn.execute("""
                INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                VALUES ('chr1', '[100, 101)', 100, 'A', 'G', gen_random_uuid())
            """)

        loader = PopulationFreqLoader()

        async with db_pool.acquire() as conn:
            variant_id = await conn.fetchval(
                "SELECT variant_id FROM variants WHERE chrom = 'chr1' AND pos = 100"
            )

            await loader.import_variant_frequencies(
                conn=conn,
                variant_id=variant_id,
                info=GNOMAD_INFO_RARE,
                source="gnomAD_v3",
            )

            rare_in_afr = await conn.fetch("""
                SELECT v.chrom, v.pos, pf.af
                FROM variants v
                JOIN population_frequencies pf ON v.variant_id = pf.variant_id
                WHERE pf.population = 'AFR' AND pf.af < 0.001
            """)

            assert len(rare_in_afr) == 1
            assert rare_in_afr[0]["af"] == pytest.approx(0.0001, rel=1e-6)

    @pytest.mark.asyncio
    async def test_index_used_for_population_lookup(self, db_pool):
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        popfreq_schema = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await popfreq_schema.create_population_frequencies_table(conn)
            await popfreq_schema.create_popfreq_indexes(conn)

            explain = await conn.fetch("""
                EXPLAIN (FORMAT JSON)
                SELECT * FROM population_frequencies
                WHERE variant_id = 1 AND population = 'AFR'
            """)

            explain_text = str(explain)
            assert "idx_popfreq_lookup" in explain_text or "Seq Scan" in explain_text


class TestSubsetHandling:
    """Test subset parameter handling (controls, non_neuro, etc)."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_import_with_subset(self, db_pool):
        from vcf_pg_loader.annotations.population_freq import PopulationFreqLoader
        from vcf_pg_loader.annotations.schema import PopulationFreqSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        popfreq_schema = PopulationFreqSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await popfreq_schema.create_population_frequencies_table(conn)

            await conn.execute("""
                INSERT INTO variants (chrom, pos_range, pos, ref, alt, load_batch_id)
                VALUES ('chr1', '[100, 101)', 100, 'A', 'G', gen_random_uuid())
            """)

        loader = PopulationFreqLoader()

        async with db_pool.acquire() as conn:
            variant_id = await conn.fetchval(
                "SELECT variant_id FROM variants WHERE chrom = 'chr1' AND pos = 100"
            )

            await loader.import_variant_frequencies(
                conn=conn,
                variant_id=variant_id,
                info=GNOMAD_INFO_COMPLETE,
                source="gnomAD_v3",
                subset="controls",
            )

            rows = await conn.fetch("""
                SELECT * FROM population_frequencies WHERE subset = 'controls'
            """)
            assert len(rows) == 7
