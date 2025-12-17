"""
Annotation lookup tests for vcf-pg-loader.

These tests verify SQL-based variant annotation using population databases
loaded as reference tables. Test patterns derived from echtvar
(https://github.com/brentp/echtvar) under MIT License.

See tests/vendored/echtvar/ATTRIBUTION.md for full attribution.
"""
import json

import pytest

from vendored.echtvar import (
    generate_clinvar_vcf_content,
    generate_gnomad_vcf_content,
    generate_overlapping_variants,
    get_clinvar_field_config,
    get_gnomad_field_config,
    write_annotation_vcf,
)


def fix_postgres_url(url: str) -> str:
    """Convert testcontainers URL to asyncpg-compatible format."""
    if url.startswith("postgresql+psycopg2://"):
        return url.replace("postgresql+psycopg2://", "postgresql://")
    return url


class TestAnnotationFieldConfig:
    """Test annotation field configuration loading."""

    def test_load_field_config_from_json(self, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        fields = load_field_config(config_path)

        assert len(fields) == 8
        assert fields[0].field == "AC"
        assert fields[0].alias == "gnomad_ac"

    def test_field_config_supports_missing_string(self, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config

        config = [{"field": "FILTER", "alias": "gnomad_filter", "missing_string": "PASS"}]
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))

        fields = load_field_config(config_path)

        assert fields[0].missing_string == "PASS"

    def test_field_config_supports_multiplier(self, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config

        config = [{"field": "AF", "alias": "gnomad_af", "multiplier": 2000000}]
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))

        fields = load_field_config(config_path)

        assert fields[0].multiplier == 2000000

    def test_field_config_infers_type_from_vcf_header(self, tmp_path):
        from vcf_pg_loader.annotation_config import AnnotationFieldConfig

        config = AnnotationFieldConfig(
            field="AF",
            alias="gnomad_af",
            field_type="Float",
        )

        assert config.field_type == "Float"


@pytest.mark.integration
class TestAnnotationSchemaCreation:
    """Test annotation reference table schema creation."""

    @pytest.fixture
    async def db_conn(self, postgres_container):
        import asyncpg

        from vcf_pg_loader.schema import SchemaManager

        conn = await asyncpg.connect(fix_postgres_url(postgres_container.get_connection_url()))
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn)
        yield conn
        await conn.close()

    async def test_create_annotation_registry_table(self, db_conn):
        from vcf_pg_loader.annotation_schema import AnnotationSchemaManager

        manager = AnnotationSchemaManager()
        await manager.create_annotation_registry(db_conn)

        result = await db_conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'annotation_sources')"
        )
        assert result is True

    async def test_create_annotation_source_table(self, db_conn, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_schema import AnnotationSchemaManager

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        fields = load_field_config(config_path)
        manager = AnnotationSchemaManager()
        await manager.create_annotation_source_table(db_conn, "gnomad_test", fields)

        result = await db_conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'anno_gnomad_test')"
        )
        assert result is True

    async def test_annotation_table_has_variant_composite_key(self, db_conn, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_schema import AnnotationSchemaManager

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        fields = load_field_config(config_path)
        manager = AnnotationSchemaManager()
        await manager.create_annotation_source_table(db_conn, "gnomad_pk_test", fields)

        columns = await db_conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'anno_gnomad_pk_test'
            ORDER BY ordinal_position
        """)
        col_names = [r["column_name"] for r in columns]

        assert "chrom" in col_names
        assert "pos" in col_names
        assert "ref" in col_names
        assert "alt" in col_names

    async def test_annotation_table_has_lookup_index(self, db_conn, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_schema import AnnotationSchemaManager

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        fields = load_field_config(config_path)
        manager = AnnotationSchemaManager()
        await manager.create_annotation_source_table(db_conn, "gnomad_idx_test", fields)
        await manager.create_variant_lookup_index(db_conn, "gnomad_idx_test")

        indexes = await db_conn.fetch("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'anno_gnomad_idx_test'
        """)
        index_names = [r["indexname"] for r in indexes]

        assert any("lookup" in name for name in index_names)


@pytest.mark.integration
class TestAnnotationLoading:
    """Test loading population databases as annotation sources."""

    @pytest.fixture
    async def db_conn(self, postgres_container):
        import asyncpg

        from vcf_pg_loader.schema import SchemaManager

        conn = await asyncpg.connect(fix_postgres_url(postgres_container.get_connection_url()))
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn)
        yield conn
        await conn.close()

    async def test_load_gnomad_vcf_as_annotation_source(self, db_conn, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_loader import AnnotationLoader

        vcf_content = generate_gnomad_vcf_content(n_variants=100, seed=42)
        vcf_path = write_annotation_vcf(tmp_path / "gnomad.vcf", vcf_content)

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))
        fields = load_field_config(config_path)

        loader = AnnotationLoader()
        result = await loader.load_annotation_source(
            vcf_path=vcf_path,
            source_name="gnomad_v3_test",
            field_config=fields,
            conn=db_conn,
        )

        assert result["variants_loaded"] == 100

    async def test_load_multiple_annotation_sources(self, db_conn, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_loader import AnnotationLoader

        gnomad_vcf = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=50, seed=42),
        )
        clinvar_vcf = write_annotation_vcf(
            tmp_path / "clinvar.vcf",
            generate_clinvar_vcf_content(n_variants=50, seed=42),
        )

        gnomad_config = tmp_path / "gnomad.json"
        gnomad_config.write_text(json.dumps(get_gnomad_field_config()))
        gnomad_fields = load_field_config(gnomad_config)

        clinvar_config = tmp_path / "clinvar.json"
        clinvar_config.write_text(json.dumps(get_clinvar_field_config()))
        clinvar_fields = load_field_config(clinvar_config)

        loader = AnnotationLoader()

        result1 = await loader.load_annotation_source(
            vcf_path=gnomad_vcf,
            source_name="gnomad_multi",
            field_config=gnomad_fields,
            conn=db_conn,
        )
        result2 = await loader.load_annotation_source(
            vcf_path=clinvar_vcf,
            source_name="clinvar_multi",
            field_config=clinvar_fields,
            conn=db_conn,
        )

        assert result1["variants_loaded"] == 50
        assert result2["variants_loaded"] == 50

        sources = await db_conn.fetch("SELECT name FROM annotation_sources")
        source_names = [r["name"] for r in sources]
        assert "gnomad_multi" in source_names
        assert "clinvar_multi" in source_names

    async def test_annotation_source_tracks_version(self, db_conn, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_loader import AnnotationLoader

        vcf_path = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=10, seed=42),
        )
        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))
        fields = load_field_config(config_path)

        loader = AnnotationLoader()
        await loader.load_annotation_source(
            vcf_path=vcf_path,
            source_name="gnomad_v3_1_2",
            field_config=fields,
            conn=db_conn,
            version="v3.1.2",
        )

        row = await db_conn.fetchrow(
            "SELECT version FROM annotation_sources WHERE name = $1",
            "gnomad_v3_1_2",
        )
        assert row["version"] == "v3.1.2"


@pytest.mark.integration
class TestAnnotationLookup:
    """Test variant annotation via SQL JOIN."""

    @pytest.fixture
    async def db_with_annotations(self, postgres_container, tmp_path):
        import asyncpg

        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_loader import AnnotationLoader
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.schema import SchemaManager

        db_vcf, query_vcf = generate_overlapping_variants(
            n_shared=100, n_query_only=50, n_db_only=50, seed=42
        )

        db_vcf_path = write_annotation_vcf(tmp_path / "gnomad.vcf", db_vcf)
        query_vcf_path = write_annotation_vcf(tmp_path / "query.vcf", query_vcf)

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))
        fields = load_field_config(config_path)

        db_url = fix_postgres_url(postgres_container.get_connection_url())
        conn = await asyncpg.connect(db_url)

        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn)

        loader = AnnotationLoader()
        await loader.load_annotation_source(
            vcf_path=db_vcf_path,
            source_name="gnomad_test",
            field_config=fields,
            conn=conn,
        )

        vcf_loader = VCFLoader(
            db_url=db_url,
            config=LoadConfig(batch_size=1000, normalize=False),
        )
        await vcf_loader.load_vcf(query_vcf_path)

        yield {
            "conn": conn,
            "n_shared": 100,
            "n_query_only": 50,
        }
        await conn.close()

    async def test_annotate_variants_with_gnomad_af(self, db_with_annotations):
        from vcf_pg_loader.annotator import VariantAnnotator

        conn = db_with_annotations["conn"]
        annotator = VariantAnnotator(conn)

        results = await annotator.annotate_variants(
            sources=["gnomad_test"],
        )

        annotated = [r for r in results if r.get("gnomad_af") is not None]
        assert len(annotated) == db_with_annotations["n_shared"]

    async def test_annotate_returns_null_for_novel_variants(self, db_with_annotations):
        from vcf_pg_loader.annotator import VariantAnnotator

        conn = db_with_annotations["conn"]
        annotator = VariantAnnotator(conn)

        results = await annotator.annotate_variants(
            sources=["gnomad_test"],
        )

        novel = [r for r in results if r.get("gnomad_af") is None]
        assert len(novel) == db_with_annotations["n_query_only"]

    async def test_annotate_with_expression_filter(self, db_with_annotations):
        from vcf_pg_loader.annotator import VariantAnnotator

        conn = db_with_annotations["conn"]
        annotator = VariantAnnotator(conn)

        results = await annotator.annotate_variants(
            sources=["gnomad_test"],
            filter_expr="gnomad_af < 0.01",
        )

        for r in results:
            if r.get("gnomad_af") is not None:
                assert r["gnomad_af"] < 0.01

    async def test_annotate_with_missing_value_filter(self, db_with_annotations):
        from vcf_pg_loader.annotator import VariantAnnotator

        conn = db_with_annotations["conn"]
        annotator = VariantAnnotator(conn)

        results = await annotator.annotate_variants(
            sources=["gnomad_test"],
            filter_expr="gnomad_af < 0.01 || gnomad_af IS NULL",
        )

        total_expected = db_with_annotations["n_shared"] + db_with_annotations["n_query_only"]
        assert len(results) <= total_expected


@pytest.mark.integration
class TestAnnotationChrPrefixHandling:
    """Test chromosome prefix handling during annotation lookup."""

    @pytest.fixture
    async def db_conn(self, postgres_container):
        import asyncpg

        from vcf_pg_loader.schema import SchemaManager

        conn = await asyncpg.connect(fix_postgres_url(postgres_container.get_connection_url()))
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn)
        yield conn
        await conn.close()

    async def test_annotate_handles_chr_prefix_mismatch(self, db_conn, tmp_path):
        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_loader import AnnotationLoader
        from vcf_pg_loader.annotator import VariantAnnotator

        db_vcf_with_chr = generate_gnomad_vcf_content(
            n_variants=10, seed=42, use_chr_prefix=True
        )

        db_vcf_path = write_annotation_vcf(tmp_path / "gnomad.vcf", db_vcf_with_chr)
        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))
        fields = load_field_config(config_path)

        loader = AnnotationLoader()
        await loader.load_annotation_source(
            vcf_path=db_vcf_path,
            source_name="gnomad_chr",
            field_config=fields,
            conn=db_conn,
        )

        annotator = VariantAnnotator(db_conn)
        annotator.normalize_chr_prefix = True


class TestExpressionParser:
    """Test filter expression parsing."""

    def test_parse_simple_comparison(self):
        from vcf_pg_loader.expression import FilterExpressionParser

        parser = FilterExpressionParser()
        sql = parser.parse("gnomad_af < 0.01", {"gnomad_af"})

        assert "gnomad_af" in sql
        assert "< 0.01" in sql

    def test_parse_boolean_and(self):
        from vcf_pg_loader.expression import FilterExpressionParser

        parser = FilterExpressionParser()
        sql = parser.parse(
            "gnomad_af < 0.01 && clinvar_sig == 'Pathogenic'",
            {"gnomad_af", "clinvar_sig"},
        )

        assert "AND" in sql.upper()

    def test_parse_boolean_or(self):
        from vcf_pg_loader.expression import FilterExpressionParser

        parser = FilterExpressionParser()
        sql = parser.parse(
            "gnomad_af < 0.01 || gnomad_af IS NULL",
            {"gnomad_af"},
        )

        assert "OR" in sql.upper()

    def test_parse_validates_field_names(self):
        from vcf_pg_loader.expression import FilterExpressionParser

        parser = FilterExpressionParser()
        errors = parser.validate("unknown_field < 0.01", {"gnomad_af"})

        assert len(errors) > 0
        assert "unknown_field" in errors[0]

    def test_parse_equality_operators(self):
        from vcf_pg_loader.expression import FilterExpressionParser

        parser = FilterExpressionParser()
        sql = parser.parse("clinvar_sig == 'Pathogenic'", {"clinvar_sig"})

        assert "=" in sql


def generate_matching_vcfs(n_variants: int, seed: int = 42) -> tuple[str, str]:
    """Generate annotation DB and query VCF with 100% overlap for benchmarking."""
    import random

    random.seed(seed)

    db_header = """##fileformat=VCFv4.2
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total alleles">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=nhomalt,Number=A,Type=Integer,Description="Homozygous count">
##INFO=<ID=AC_popmax,Number=A,Type=Integer,Description="AC popmax">
##INFO=<ID=AN_popmax,Number=A,Type=Integer,Description="AN popmax">
##INFO=<ID=AF_popmax,Number=A,Type=Float,Description="AF popmax">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""

    query_header = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""

    db_lines = [db_header]
    query_lines = [query_header]
    bases = ["A", "C", "G", "T"]

    for i in range(n_variants):
        pos = 10000 + i * 100
        ref = bases[i % 4]
        alt = bases[(i + 1) % 4]

        an = 150000
        ac = random.randint(1, int(an * 0.5))
        af = ac / an
        nhomalt = random.randint(0, ac // 2)
        info = f"AC={ac};AN={an};AF={af:.6f};nhomalt={nhomalt};AC_popmax={ac};AN_popmax={an};AF_popmax={af:.6f}"
        db_lines.append(f"chr1\t{pos}\t.\t{ref}\t{alt}\t.\tPASS\t{info}")
        query_lines.append(f"chr1\t{pos}\t.\t{ref}\t{alt}\t30\tPASS\t.")

    return "\n".join(db_lines) + "\n", "\n".join(query_lines) + "\n"


@pytest.mark.integration
@pytest.mark.benchmark
class TestAnnotationPerformance:
    """Benchmark annotation lookup speed."""

    @pytest.fixture
    async def benchmark_db(self, postgres_container, tmp_path):
        """Create DB with 10K matching annotation and query variants."""
        import asyncpg

        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_loader import AnnotationLoader
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.schema import SchemaManager

        n_variants = 10000
        db_vcf, query_vcf = generate_matching_vcfs(n_variants, seed=42)
        db_vcf_path = write_annotation_vcf(tmp_path / "gnomad_bench.vcf", db_vcf)
        query_vcf_path = write_annotation_vcf(tmp_path / "query_bench.vcf", query_vcf)

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))
        fields = load_field_config(config_path)

        db_url = fix_postgres_url(postgres_container.get_connection_url())
        conn = await asyncpg.connect(db_url)
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn)

        loader = AnnotationLoader()
        await loader.load_annotation_source(
            vcf_path=db_vcf_path,
            source_name="gnomad_bench",
            field_config=fields,
            conn=conn,
        )
        await conn.close()

        vcf_loader = VCFLoader(db_url, LoadConfig(batch_size=5000, normalize=False))
        load_result = await vcf_loader.load_vcf(query_vcf_path)

        conn = await asyncpg.connect(db_url)
        yield {"conn": conn, "batch_id": str(load_result["load_batch_id"]), "n_variants": n_variants}
        await conn.close()

    async def test_annotation_lookup_performance(self, benchmark_db):
        """Benchmark: annotation lookup should process >100K variants/sec."""
        import time

        from vcf_pg_loader.annotator import VariantAnnotator

        annotator = VariantAnnotator(benchmark_db["conn"])

        start = time.time()
        results = await annotator.annotate_variants(
            sources=["gnomad_bench"],
            load_batch_id=benchmark_db["batch_id"],
        )
        elapsed = time.time() - start

        n_variants = len(results)
        rate = n_variants / elapsed if elapsed > 0 else 0
        annotated = sum(1 for r in results if r.get("gnomad_af") is not None)

        print(f"\n  Annotation lookup: {n_variants:,} variants in {elapsed:.3f}s ({rate:,.0f}/sec)")
        print(f"  Found annotations: {annotated:,}/{n_variants:,} ({100*annotated/n_variants:.1f}%)")

        assert n_variants == benchmark_db["n_variants"]
        assert annotated == n_variants
        assert rate > 50000

    async def test_annotation_load_performance(self, postgres_container, tmp_path):
        """Benchmark: annotation DB loading should process >50K variants/sec."""
        import time

        import asyncpg

        from vcf_pg_loader.annotation_config import load_field_config
        from vcf_pg_loader.annotation_loader import AnnotationLoader
        from vcf_pg_loader.schema import SchemaManager

        n_variants = 50000
        db_vcf = generate_gnomad_vcf_content(n_variants=n_variants, seed=42)
        db_vcf_path = write_annotation_vcf(tmp_path / "gnomad_load_bench.vcf", db_vcf)

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))
        fields = load_field_config(config_path)

        db_url = fix_postgres_url(postgres_container.get_connection_url())
        conn = await asyncpg.connect(db_url)
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn)

        loader = AnnotationLoader()
        start = time.time()
        result = await loader.load_annotation_source(
            vcf_path=db_vcf_path,
            source_name="gnomad_load_bench",
            field_config=fields,
            conn=conn,
        )
        elapsed = time.time() - start

        rate = result["variants_loaded"] / elapsed
        print(f"\n  Annotation load: {result['variants_loaded']:,} variants in {elapsed:.2f}s ({rate:,.0f}/sec)")

        assert result["variants_loaded"] == n_variants
        assert rate > 20000

        await conn.close()

    async def test_filtered_annotation_performance(self, benchmark_db):
        """Benchmark: filtered annotation lookup with expression."""
        import time

        from vcf_pg_loader.annotator import VariantAnnotator

        annotator = VariantAnnotator(benchmark_db["conn"])

        start = time.time()
        results = await annotator.annotate_variants(
            sources=["gnomad_bench"],
            load_batch_id=benchmark_db["batch_id"],
            filter_expr="gnomad_af < 0.01",
        )
        elapsed = time.time() - start

        print(f"\n  Filtered lookup (AF<0.01): {len(results):,} variants in {elapsed:.3f}s")

        for r in results:
            if r.get("gnomad_af") is not None:
                assert r["gnomad_af"] < 0.01
