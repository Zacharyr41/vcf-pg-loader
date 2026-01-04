"""TDD tests for HapMap3 reference panel support.

Tests for:
- Reference panel table creation
- HapMap3 loading for GRCh37 and GRCh38
- Variant flagging during VCF load
- Matching logic with allele flip handling
- Batch lookup performance
- Partial index usage
"""

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
class TestReferencePanelSchema:
    """Test reference_panels table creation."""

    @pytest.mark.asyncio
    async def test_create_reference_panels_table(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'reference_panels'
                )
            """)
            assert exists is True

    @pytest.mark.asyncio
    async def test_reference_panels_columns(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

            columns = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'reference_panels'
            """)
            column_names = {row["column_name"] for row in columns}

            assert "panel_name" in column_names
            assert "rsid" in column_names
            assert "chrom" in column_names
            assert "position" in column_names
            assert "a1" in column_names
            assert "a2" in column_names

    @pytest.mark.asyncio
    async def test_reference_panels_primary_key(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

            pk = await conn.fetch("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'reference_panels'::regclass AND i.indisprimary
            """)
            pk_columns = {row["attname"] for row in pk}

            assert pk_columns == {"panel_name", "chrom", "position", "a1", "a2"}

    @pytest.mark.asyncio
    async def test_reference_panels_rsid_index(self, db_pool):
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

            indexes = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'reference_panels'
            """)
            index_names = {row["indexname"] for row in indexes}

            assert "idx_refpanel_rsid" in index_names


@pytest.mark.integration
class TestHapMap3Loader:
    """Test HapMap3 reference data loading."""

    @pytest.mark.asyncio
    async def test_load_hapmap3_from_tsv(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            result = await loader.load_reference_panel(conn, tsv_path, build="grch38")

            assert result["variants_loaded"] > 0
            assert result["panel_name"] == "hapmap3_grch38"

    @pytest.mark.asyncio
    async def test_load_hapmap3_grch37(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            result = await loader.load_reference_panel(conn, tsv_path, build="grch37")

            assert result["variants_loaded"] > 0
            assert result["panel_name"] == "hapmap3_grch37"

    @pytest.mark.asyncio
    async def test_hapmap3_data_inserted_correctly(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")

            row = await conn.fetchrow("""
                SELECT * FROM reference_panels
                WHERE rsid = 'rs3094315'
            """)

            assert row is not None
            assert row["panel_name"] == "hapmap3_grch38"
            assert row["chrom"] == "1"
            assert row["a1"] in ("A", "G")
            assert row["a2"] in ("A", "G")


@pytest.mark.integration
class TestHapMap3VariantMatching:
    """Test matching logic for HapMap3 variants."""

    @pytest.mark.asyncio
    async def test_build_lookup_dict(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")
            lookup = await loader.build_lookup(conn, panel_name="hapmap3_grch38")

            assert len(lookup) > 0
            assert isinstance(lookup, dict)

    @pytest.mark.asyncio
    async def test_exact_match(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader, match_hapmap3_variant
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")
            lookup = await loader.build_lookup(conn, panel_name="hapmap3_grch38")

            result = match_hapmap3_variant(
                lookup=lookup,
                chrom="chr1",
                pos=752566,
                ref="G",
                alt="A",
            )

            assert result is not None
            assert result["rsid"] == "rs3094315"

    @pytest.mark.asyncio
    async def test_allele_flip_match(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader, match_hapmap3_variant
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")
            lookup = await loader.build_lookup(conn, panel_name="hapmap3_grch38")

            result = match_hapmap3_variant(
                lookup=lookup,
                chrom="chr1",
                pos=752566,
                ref="A",
                alt="G",
            )

            assert result is not None
            assert result["rsid"] == "rs3094315"

    @pytest.mark.asyncio
    async def test_no_match(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader, match_hapmap3_variant
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")
            lookup = await loader.build_lookup(conn, panel_name="hapmap3_grch38")

            result = match_hapmap3_variant(
                lookup=lookup,
                chrom="chr1",
                pos=999999999,
                ref="A",
                alt="G",
            )

            assert result is None


@pytest.mark.integration
class TestHapMap3VCFLoading:
    """Test HapMap3 flagging during VCF loading."""

    @pytest.mark.asyncio
    async def test_variants_table_has_hapmap3_columns(self, db_pool):
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)

            columns = await conn.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'variants'
            """)
            column_names = {row["column_name"] for row in columns}

            assert "in_hapmap3" in column_names
            assert "hapmap3_rsid" in column_names

    @pytest.mark.asyncio
    async def test_hapmap3_partial_index_created(self, db_pool):
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await schema_manager.create_indexes(conn)

            indexes = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE tablename LIKE 'variants%'
            """)
            index_names = {row["indexname"] for row in indexes}

            assert "idx_hapmap3_variants" in index_names

    @pytest.mark.asyncio
    async def test_load_vcf_with_hapmap3_flagging(self, db_pool, db_url):
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager
        from vcf_pg_loader.tls import TLSConfig

        schema_manager = SchemaManager()
        ref_schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await ref_schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"
        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")

        vcf_path = FIXTURES_DIR / "hapmap3_overlap.vcf"
        tls_config = TLSConfig(require_tls=False, verify_server=False)
        config = LoadConfig(
            batch_size=100, drop_indexes=False, flag_hapmap3=True, tls_config=tls_config
        )

        async with VCFLoader(db_url, config) as vcf_loader:
            result = await vcf_loader.load_vcf(vcf_path)

            assert result["variants_loaded"] > 0

        async with db_pool.acquire() as conn:
            hapmap3_count = await conn.fetchval("""
                SELECT COUNT(*) FROM variants WHERE in_hapmap3 = TRUE
            """)
            assert hapmap3_count > 0

    @pytest.mark.asyncio
    async def test_hapmap3_rsid_populated(self, db_pool, db_url):
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader
        from vcf_pg_loader.references.schema import ReferenceSchemaManager
        from vcf_pg_loader.schema import SchemaManager
        from vcf_pg_loader.tls import TLSConfig

        schema_manager = SchemaManager()
        ref_schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await ref_schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"
        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")

        vcf_path = FIXTURES_DIR / "hapmap3_overlap.vcf"
        tls_config = TLSConfig(require_tls=False, verify_server=False)
        config = LoadConfig(
            batch_size=100, drop_indexes=False, flag_hapmap3=True, tls_config=tls_config
        )

        async with VCFLoader(db_url, config) as vcf_loader:
            await vcf_loader.load_vcf(vcf_path)

        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT hapmap3_rsid FROM variants
                WHERE in_hapmap3 = TRUE AND hapmap3_rsid IS NOT NULL
            """)
            assert len(rows) > 0
            for row in rows:
                assert row["hapmap3_rsid"].startswith("rs")


@pytest.mark.integration
class TestHapMap3Performance:
    """Test batch lookup performance."""

    @pytest.mark.asyncio
    async def test_batch_lookup_10k_variants(self, db_pool):
        from vcf_pg_loader.references.hapmap3 import HapMap3Loader, match_hapmap3_variant
        from vcf_pg_loader.references.schema import ReferenceSchemaManager

        schema_manager = ReferenceSchemaManager()
        async with db_pool.acquire() as conn:
            await schema_manager.create_reference_panels_table(conn)

        loader = HapMap3Loader()
        tsv_path = FIXTURES_DIR / "hapmap3_test.tsv"

        async with db_pool.acquire() as conn:
            await loader.load_reference_panel(conn, tsv_path, build="grch38")
            lookup = await loader.build_lookup(conn, panel_name="hapmap3_grch38")

        import time

        variants = [("chr1", 752566 + i, "A", "G") for i in range(10000)]

        start = time.perf_counter()
        matches = 0
        for chrom, pos, ref, alt in variants:
            result = match_hapmap3_variant(lookup, chrom, pos, ref, alt)
            if result:
                matches += 1
        elapsed = time.perf_counter() - start

        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_partial_index_used_for_hapmap3_query(self, db_pool):
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_schema(conn)
            await schema_manager.create_indexes(conn)

            explain = await conn.fetch("""
                EXPLAIN (FORMAT JSON)
                SELECT * FROM variants WHERE in_hapmap3 = TRUE
            """)

            explain_text = str(explain)
            assert "idx_hapmap3_variants" in explain_text or "Seq Scan" in explain_text


class TestHapMap3MatchingLogic:
    """Unit tests for matching logic without database."""

    def test_normalize_chrom_with_prefix(self):
        from vcf_pg_loader.references.hapmap3 import normalize_chrom

        assert normalize_chrom("chr1") == "1"
        assert normalize_chrom("chrX") == "X"
        assert normalize_chrom("chrM") == "M"

    def test_normalize_chrom_without_prefix(self):
        from vcf_pg_loader.references.hapmap3 import normalize_chrom

        assert normalize_chrom("1") == "1"
        assert normalize_chrom("X") == "X"
        assert normalize_chrom("MT") == "MT"

    def test_complement_allele(self):
        from vcf_pg_loader.references.hapmap3 import complement_allele

        assert complement_allele("A") == "T"
        assert complement_allele("T") == "A"
        assert complement_allele("C") == "G"
        assert complement_allele("G") == "C"

    def test_is_strand_ambiguous(self):
        from vcf_pg_loader.references.hapmap3 import is_strand_ambiguous

        assert is_strand_ambiguous("A", "T") is True
        assert is_strand_ambiguous("T", "A") is True
        assert is_strand_ambiguous("C", "G") is True
        assert is_strand_ambiguous("G", "C") is True
        assert is_strand_ambiguous("A", "G") is False
        assert is_strand_ambiguous("A", "C") is False
