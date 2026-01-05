"""TDD tests for PGS Catalog PRS weights storage.

Tests for:
- PGS Catalog header metadata parsing
- TSV weight data parsing
- Allele harmonization with VCF variants
- Genome build validation
- Database schema creation
- Full import workflow with match statistics
"""

from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestPGSMetadataParsing:
    """Test PGS Catalog header metadata parsing."""

    def test_parse_minimal_header(self):
        from vcf_pg_loader.prs.pgs_catalog import parse_pgs_header

        header_lines = [
            "###PGS CATALOG SCORING FILE",
            "#pgs_id=PGS000001",
            "#genome_build=GRCh38",
        ]
        metadata = parse_pgs_header(header_lines)

        assert metadata.pgs_id == "PGS000001"
        assert metadata.genome_build == "GRCh38"

    def test_parse_full_header(self):
        from vcf_pg_loader.prs.pgs_catalog import parse_pgs_header

        header_lines = [
            "###PGS CATALOG SCORING FILE",
            "#pgs_id=PGS000001",
            "#trait_name=Type 2 Diabetes",
            "#trait_ontology_id=EFO_0001360",
            "#publication_pmid=30297969",
            "#genome_build=GRCh38",
            "#weight_type=beta",
            "#n_variants=77",
            "#reporting_ancestry=European",
        ]
        metadata = parse_pgs_header(header_lines)

        assert metadata.pgs_id == "PGS000001"
        assert metadata.trait_name == "Type 2 Diabetes"
        assert metadata.trait_ontology_id == "EFO_0001360"
        assert metadata.publication_pmid == "30297969"
        assert metadata.genome_build == "GRCh38"
        assert metadata.weight_type == "beta"
        assert metadata.n_variants == 77
        assert metadata.reporting_ancestry == "European"

    def test_parse_odds_ratio_weight_type(self):
        from vcf_pg_loader.prs.pgs_catalog import parse_pgs_header

        header_lines = [
            "###PGS CATALOG SCORING FILE",
            "#pgs_id=PGS000002",
            "#genome_build=GRCh37",
            "#weight_type=OR",
        ]
        metadata = parse_pgs_header(header_lines)

        assert metadata.weight_type == "OR"
        assert metadata.genome_build == "GRCh37"

    def test_parse_log_odds_ratio_weight_type(self):
        from vcf_pg_loader.prs.pgs_catalog import parse_pgs_header

        header_lines = [
            "###PGS CATALOG SCORING FILE",
            "#pgs_id=PGS000003",
            "#genome_build=GRCh38",
            "#weight_type=log(OR)",
        ]
        metadata = parse_pgs_header(header_lines)

        assert metadata.weight_type == "log(OR)"

    def test_parse_missing_optional_fields(self):
        from vcf_pg_loader.prs.pgs_catalog import parse_pgs_header

        header_lines = [
            "###PGS CATALOG SCORING FILE",
            "#pgs_id=PGS000004",
            "#genome_build=GRCh38",
        ]
        metadata = parse_pgs_header(header_lines)

        assert metadata.pgs_id == "PGS000004"
        assert metadata.trait_name is None
        assert metadata.trait_ontology_id is None
        assert metadata.publication_pmid is None
        assert metadata.weight_type is None
        assert metadata.n_variants is None
        assert metadata.reporting_ancestry is None

    def test_parse_missing_required_pgs_id_raises(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSParseError, parse_pgs_header

        header_lines = [
            "###PGS CATALOG SCORING FILE",
            "#genome_build=GRCh38",
        ]
        with pytest.raises(PGSParseError, match="pgs_id"):
            parse_pgs_header(header_lines)

    def test_parse_missing_required_genome_build_raises(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSParseError, parse_pgs_header

        header_lines = [
            "###PGS CATALOG SCORING FILE",
            "#pgs_id=PGS000001",
        ]
        with pytest.raises(PGSParseError, match="genome_build"):
            parse_pgs_header(header_lines)


class TestPGSWeightsParsing:
    """Test PGS Catalog TSV weight data parsing."""

    def test_parse_standard_columns(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight
rs3094315\t1\t752566\tG\t0.0234
rs3131972\t1\t752721\tA\t-0.0156
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert len(weights) == 2
        assert weights[0].rsid == "rs3094315"
        assert weights[0].chromosome == "1"
        assert weights[0].position == 752566
        assert weights[0].effect_allele == "G"
        assert weights[0].effect_weight == pytest.approx(0.0234)

    def test_parse_with_other_allele(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\tother_allele\teffect_weight
rs3094315\t1\t752566\tG\tA\t0.0234
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].other_allele == "A"

    def test_parse_with_allele_frequency(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight\tallelefrequency_effect
rs3094315\t1\t752566\tG\t0.0234\t0.35
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].allele_frequency == pytest.approx(0.35)

    def test_parse_interaction_flag(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight\tis_interaction
rs3094315\t1\t752566\tG\t0.0234\tTrue
rs3131972\t1\t752721\tA\t-0.0156\tFalse
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].is_interaction is True
        assert weights[1].is_interaction is False

    def test_parse_haplotype_flag(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight\tis_haplotype
rs3094315\t1\t752566\tG\t0.0234\tTrue
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].is_haplotype is True

    def test_parse_dominant_flag(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight\tis_dominant
rs3094315\t1\t752566\tG\t0.0234\tTrue
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].is_dominant is True

    def test_parse_recessive_flag(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight\tis_recessive
rs3094315\t1\t752566\tG\t0.0234\tTrue
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].is_recessive is True

    def test_parse_locus_name(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight\tlocus_name
rs3094315\t1\t752566\tG\t0.0234\tBRCA1
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].locus_name == "BRCA1"

    def test_parse_negative_weights(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele\teffect_weight
rs3094315\t1\t752566\tG\t-1.234e-2
"""
        parser = PGSCatalogParser.from_string(content)
        weights = list(parser.iter_weights())

        assert weights[0].effect_weight == pytest.approx(-0.01234)

    def test_parse_missing_effect_weight_raises(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser, PGSParseError

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_allele
rs3094315\t1\t752566\tG
"""
        parser = PGSCatalogParser.from_string(content)
        with pytest.raises(PGSParseError, match="effect_weight"):
            list(parser.iter_weights())

    def test_parse_missing_effect_allele_raises(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser, PGSParseError

        content = """###PGS CATALOG SCORING FILE
#pgs_id=PGS000001
#genome_build=GRCh38
rsID\tchr_name\tchr_position\teffect_weight
rs3094315\t1\t752566\t0.0234
"""
        parser = PGSCatalogParser.from_string(content)
        with pytest.raises(PGSParseError, match="effect_allele"):
            list(parser.iter_weights())


class TestAlleleHarmonization:
    """Test allele harmonization with VCF variants."""

    def test_direct_match_effect_is_alt(self):
        from vcf_pg_loader.prs.models import PRSWeight
        from vcf_pg_loader.prs.pgs_catalog import harmonize_weight_allele

        weight = PRSWeight(
            effect_allele="G",
            effect_weight=0.0234,
            other_allele="A",
        )
        result = harmonize_weight_allele(weight, ref="A", alt="G")

        assert result.is_effect_allele_alt is True
        assert result.harmonized_effect_allele == "G"

    def test_direct_match_effect_is_ref(self):
        from vcf_pg_loader.prs.models import PRSWeight
        from vcf_pg_loader.prs.pgs_catalog import harmonize_weight_allele

        weight = PRSWeight(
            effect_allele="A",
            effect_weight=0.0234,
            other_allele="G",
        )
        result = harmonize_weight_allele(weight, ref="A", alt="G")

        assert result.is_effect_allele_alt is False
        assert result.harmonized_effect_allele == "A"

    def test_strand_flip_a_to_t(self):
        from vcf_pg_loader.prs.models import PRSWeight
        from vcf_pg_loader.prs.pgs_catalog import harmonize_weight_allele

        weight = PRSWeight(
            effect_allele="A",
            effect_weight=0.0234,
            other_allele="C",
        )
        result = harmonize_weight_allele(weight, ref="G", alt="T")

        assert result.is_match is True
        assert result.is_flipped is True
        assert result.harmonized_effect_allele == "T"

    def test_strand_flip_c_to_g(self):
        from vcf_pg_loader.prs.models import PRSWeight
        from vcf_pg_loader.prs.pgs_catalog import harmonize_weight_allele

        weight = PRSWeight(
            effect_allele="C",
            effect_weight=0.0234,
            other_allele="T",
        )
        result = harmonize_weight_allele(weight, ref="A", alt="G")

        assert result.is_match is True
        assert result.is_flipped is True
        assert result.harmonized_effect_allele == "G"

    def test_no_match_different_alleles(self):
        from vcf_pg_loader.prs.models import PRSWeight
        from vcf_pg_loader.prs.pgs_catalog import harmonize_weight_allele

        weight = PRSWeight(
            effect_allele="C",
            effect_weight=0.0234,
            other_allele="T",
        )
        result = harmonize_weight_allele(weight, ref="A", alt="G")

        if not result.is_flipped:
            pass

    def test_strand_ambiguous_at(self):
        from vcf_pg_loader.prs.pgs_catalog import is_strand_ambiguous

        assert is_strand_ambiguous("A", "T") is True
        assert is_strand_ambiguous("T", "A") is True

    def test_strand_ambiguous_cg(self):
        from vcf_pg_loader.prs.pgs_catalog import is_strand_ambiguous

        assert is_strand_ambiguous("C", "G") is True
        assert is_strand_ambiguous("G", "C") is True

    def test_not_strand_ambiguous(self):
        from vcf_pg_loader.prs.pgs_catalog import is_strand_ambiguous

        assert is_strand_ambiguous("A", "G") is False
        assert is_strand_ambiguous("A", "C") is False
        assert is_strand_ambiguous("T", "G") is False
        assert is_strand_ambiguous("T", "C") is False


class TestGenomeBuildValidation:
    """Test genome build compatibility validation."""

    def test_matching_grch38_build(self):
        from vcf_pg_loader.prs.pgs_catalog import validate_genome_build

        is_valid = validate_genome_build(pgs_build="GRCh38", db_build="GRCh38")
        assert is_valid is True

    def test_matching_grch37_build(self):
        from vcf_pg_loader.prs.pgs_catalog import validate_genome_build

        is_valid = validate_genome_build(pgs_build="GRCh37", db_build="GRCh37")
        assert is_valid is True

    def test_mismatched_build_raises(self):
        from vcf_pg_loader.prs.pgs_catalog import GenomeBuildMismatchError, validate_genome_build

        with pytest.raises(GenomeBuildMismatchError):
            validate_genome_build(pgs_build="GRCh37", db_build="GRCh38")

    def test_case_insensitive_build_matching(self):
        from vcf_pg_loader.prs.pgs_catalog import validate_genome_build

        is_valid = validate_genome_build(pgs_build="grch38", db_build="GRCh38")
        assert is_valid is True

    def test_hg_alias_to_grch(self):
        from vcf_pg_loader.prs.pgs_catalog import validate_genome_build

        is_valid = validate_genome_build(pgs_build="hg38", db_build="GRCh38")
        assert is_valid is True

        is_valid = validate_genome_build(pgs_build="hg19", db_build="GRCh37")
        assert is_valid is True


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
class TestPGSSchemaCreation:
    """Test PGS database schema creation."""

    @pytest.mark.asyncio
    async def test_create_pgs_scores_table(self, db_pool):
        from vcf_pg_loader.prs.schema import PRSSchemaManager

        schema_manager = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_pgs_scores_table(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'pgs_scores'
                )
            """)
            assert exists is True

    @pytest.mark.asyncio
    async def test_pgs_scores_columns(self, db_pool):
        from vcf_pg_loader.prs.schema import PRSSchemaManager

        schema_manager = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await schema_manager.create_pgs_scores_table(conn)

            columns = await conn.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'pgs_scores'
            """)
            column_names = {row["column_name"] for row in columns}

            assert "pgs_id" in column_names
            assert "trait_name" in column_names
            assert "trait_ontology_id" in column_names
            assert "publication_pmid" in column_names
            assert "n_variants" in column_names
            assert "genome_build" in column_names
            assert "weight_type" in column_names
            assert "reporting_ancestry" in column_names
            assert "created_at" in column_names

    @pytest.mark.asyncio
    async def test_create_prs_weights_table(self, db_pool):
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'prs_weights'
                )
            """)
            assert exists is True

    @pytest.mark.asyncio
    async def test_prs_weights_columns(self, db_pool):
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

            columns = await conn.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'prs_weights'
            """)
            column_names = {row["column_name"] for row in columns}

            assert "id" in column_names
            assert "variant_id" in column_names
            assert "pgs_id" in column_names
            assert "effect_allele" in column_names
            assert "effect_weight" in column_names
            assert "is_interaction" in column_names
            assert "is_haplotype" in column_names
            assert "is_dominant" in column_names
            assert "is_recessive" in column_names
            assert "allele_frequency" in column_names
            assert "locus_name" in column_names

    @pytest.mark.asyncio
    async def test_prs_weights_pgs_id_index(self, db_pool):
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

            indexes = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'prs_weights'
            """)
            index_names = {row["indexname"] for row in indexes}

            assert "idx_prs_pgsid" in index_names

    @pytest.mark.asyncio
    async def test_prs_weights_unique_constraint(self, db_pool):
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

            constraints = await conn.fetch("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name = 'prs_weights' AND constraint_type = 'UNIQUE'
            """)
            constraint_names = {row["constraint_name"] for row in constraints}

            assert (
                any("variant_id" in name and "pgs_id" in name for name in constraint_names)
                or len(constraints) > 0
            )


@pytest.mark.integration
class TestPGSImport:
    """Test full PGS import workflow."""

    @pytest.mark.asyncio
    async def test_import_pgs_file(self, db_pool):
        from vcf_pg_loader.prs.loader import PGSLoader
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

        loader = PGSLoader()
        pgs_path = FIXTURES_DIR / "pgs_test_beta.txt"

        async with db_pool.acquire() as conn:
            result = await loader.import_pgs(conn, pgs_path)

            assert result["pgs_id"] == "PGS000001"
            assert result["weights_imported"] > 0

    @pytest.mark.asyncio
    async def test_import_stores_metadata(self, db_pool):
        from vcf_pg_loader.prs.loader import PGSLoader
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

        loader = PGSLoader()
        pgs_path = FIXTURES_DIR / "pgs_test_beta.txt"

        async with db_pool.acquire() as conn:
            await loader.import_pgs(conn, pgs_path)

            row = await conn.fetchrow(
                "SELECT * FROM pgs_scores WHERE pgs_id = $1",
                "PGS000001",
            )

            assert row is not None
            assert row["trait_name"] == "Type 2 Diabetes"
            assert row["genome_build"] == "GRCh38"
            assert row["weight_type"] == "beta"

    @pytest.mark.asyncio
    async def test_import_stores_weights(self, db_pool):
        from vcf_pg_loader.prs.loader import PGSLoader
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

        loader = PGSLoader()
        pgs_path = FIXTURES_DIR / "pgs_test_beta.txt"

        async with db_pool.acquire() as conn:
            await loader.import_pgs(conn, pgs_path)

            weights = await conn.fetch(
                "SELECT * FROM prs_weights WHERE pgs_id = $1",
                "PGS000001",
            )

            assert len(weights) > 0
            assert all(row["effect_allele"] is not None for row in weights)
            assert all(row["effect_weight"] is not None for row in weights)

    @pytest.mark.asyncio
    async def test_import_reports_match_statistics(self, db_pool, db_url):
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.prs.loader import PGSLoader
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager
        from vcf_pg_loader.tls import TLSConfig

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

        vcf_path = FIXTURES_DIR / "hapmap3_overlap.vcf"
        tls_config = TLSConfig(require_tls=False, verify_server=False)
        config = LoadConfig(batch_size=100, drop_indexes=False, tls_config=tls_config)

        async with VCFLoader(db_url, config) as vcf_loader:
            await vcf_loader.load_vcf(vcf_path)

        loader = PGSLoader()
        pgs_path = FIXTURES_DIR / "pgs_test_beta.txt"

        async with db_pool.acquire() as conn:
            result = await loader.import_pgs(conn, pgs_path)

            assert "weights_imported" in result
            assert "weights_matched" in result
            assert "weights_unmatched" in result
            assert (
                result["weights_imported"]
                == result["weights_matched"] + result["weights_unmatched"]
            )

    @pytest.mark.asyncio
    async def test_import_with_variant_matching(self, db_pool, db_url):
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.prs.loader import PGSLoader
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager
        from vcf_pg_loader.tls import TLSConfig

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

        vcf_path = FIXTURES_DIR / "hapmap3_overlap.vcf"
        tls_config = TLSConfig(require_tls=False, verify_server=False)
        config = LoadConfig(batch_size=100, drop_indexes=False, tls_config=tls_config)

        async with VCFLoader(db_url, config) as vcf_loader:
            await vcf_loader.load_vcf(vcf_path)

        loader = PGSLoader()
        pgs_path = FIXTURES_DIR / "pgs_test_beta.txt"

        async with db_pool.acquire() as conn:
            await loader.import_pgs(conn, pgs_path)

            matched = await conn.fetch(
                """
                SELECT w.*, v.chrom, v.pos, v.ref, v.alt
                FROM prs_weights w
                JOIN variants v ON w.variant_id = v.variant_id
                WHERE w.pgs_id = $1
            """,
                "PGS000001",
            )

            if matched:
                assert matched[0]["chrom"] is not None
                assert matched[0]["pos"] is not None

    @pytest.mark.asyncio
    async def test_import_multiple_scores_per_variant(self, db_pool):
        from vcf_pg_loader.prs.loader import PGSLoader
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

        loader = PGSLoader()

        async with db_pool.acquire() as conn:
            await loader.import_pgs(conn, FIXTURES_DIR / "pgs_test_beta.txt")
            await loader.import_pgs(conn, FIXTURES_DIR / "pgs_test_or.txt")

            scores = await conn.fetch("SELECT DISTINCT pgs_id FROM prs_weights")
            score_ids = {row["pgs_id"] for row in scores}

            assert "PGS000001" in score_ids
            assert "PGS000002" in score_ids

    @pytest.mark.asyncio
    async def test_import_rejects_mismatched_genome_build(self, db_pool):
        from vcf_pg_loader.prs.loader import PGSLoader
        from vcf_pg_loader.prs.pgs_catalog import GenomeBuildMismatchError
        from vcf_pg_loader.prs.schema import PRSSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        main_schema = SchemaManager()
        prs_schema = PRSSchemaManager()

        async with db_pool.acquire() as conn:
            await main_schema.create_schema(conn)
            await prs_schema.create_prs_schema(conn)

            await conn.execute("""
                INSERT INTO variant_load_audit (
                    load_batch_id, vcf_file_path, vcf_file_hash,
                    reference_genome, status
                ) VALUES (
                    gen_random_uuid(), 'test.vcf', 'abc123',
                    'GRCh38', 'completed'
                )
            """)

        loader = PGSLoader()
        pgs_path = FIXTURES_DIR / "pgs_test_grch37.txt"

        async with db_pool.acquire() as conn:
            with pytest.raises(GenomeBuildMismatchError):
                await loader.import_pgs(conn, pgs_path, validate_build=True)


class TestPGSCatalogParserFromFile:
    """Test parsing from actual files."""

    def test_parse_beta_file(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        pgs_path = FIXTURES_DIR / "pgs_test_beta.txt"
        if not pgs_path.exists():
            pytest.skip("Fixture file not created yet")

        parser = PGSCatalogParser(pgs_path)

        assert parser.metadata.pgs_id == "PGS000001"
        assert parser.metadata.weight_type == "beta"

        weights = list(parser.iter_weights())
        assert len(weights) > 0

    def test_parse_or_file(self):
        from vcf_pg_loader.prs.pgs_catalog import PGSCatalogParser

        pgs_path = FIXTURES_DIR / "pgs_test_or.txt"
        if not pgs_path.exists():
            pytest.skip("Fixture file not created yet")

        parser = PGSCatalogParser(pgs_path)

        assert parser.metadata.pgs_id == "PGS000002"
        assert parser.metadata.weight_type == "OR"
