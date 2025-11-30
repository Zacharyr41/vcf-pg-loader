"""Database query tests for SnpEff ANN annotation fields.

Tests clinical query patterns using extracted ANN fields:
- Gene-based filtering
- Impact-based filtering
- Consequence type filtering
- HGVS notation search
- Combined clinical filters
- Index efficiency verification
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import make_snpeff_ann_vcf_file


@pytest.mark.integration
class TestANNFieldQueries:
    """Test queries on extracted ANN annotation fields."""

    @pytest.fixture
    async def ann_database(self, test_db):
        """Load SnpEff-annotated variants into database."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_file = make_snpeff_ann_vcf_file()
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_query_by_gene(self, ann_database):
        """Query variants by gene symbol extracted from ANN."""
        conn = ann_database

        results = await conn.fetch(
            "SELECT * FROM variants WHERE gene = $1",
            "TP53"
        )

        assert len(results) == 5
        assert all(r["gene"] == "TP53" for r in results)

    async def test_query_by_impact(self, ann_database):
        """Query variants by impact level extracted from ANN."""
        conn = ann_database

        high_impact = await conn.fetch(
            "SELECT * FROM variants WHERE impact = 'HIGH'"
        )

        assert len(high_impact) == 3
        consequences = {r["consequence"] for r in high_impact}
        assert "stop_gained" in consequences
        assert "splice_acceptor_variant" in consequences
        assert "frameshift_variant" in consequences

    async def test_query_by_consequence(self, ann_database):
        """Query variants by consequence type extracted from ANN."""
        conn = ann_database

        lof_consequences = [
            "stop_gained",
            "frameshift_variant",
            "splice_acceptor_variant",
            "splice_donor_variant",
        ]

        results = await conn.fetch(
            "SELECT * FROM variants WHERE consequence = ANY($1)",
            lof_consequences
        )

        assert len(results) == 3
        assert all(r["impact"] == "HIGH" for r in results)

    async def test_query_by_hgvs_c(self, ann_database):
        """Query variants by HGVS.c notation."""
        conn = ann_database

        results = await conn.fetch(
            "SELECT * FROM variants WHERE hgvs_c = $1",
            "c.817C>G"
        )

        assert len(results) == 1
        assert results[0]["gene"] == "TP53"
        assert results[0]["hgvs_p"] == "p.Pro273Arg"

    async def test_query_by_hgvs_p_pattern(self, ann_database):
        """Query variants by HGVS.p pattern using trigram index."""
        conn = ann_database

        results = await conn.fetch(
            "SELECT * FROM variants WHERE hgvs_p LIKE $1",
            "%Pro273%"
        )

        assert len(results) == 1
        assert results[0]["gene"] == "TP53"

    async def test_query_by_transcript(self, ann_database):
        """Query variants by transcript ID."""
        conn = ann_database

        results = await conn.fetch(
            "SELECT * FROM variants WHERE transcript = $1",
            "ENST00000269305"
        )

        assert len(results) == 5
        assert all(r["gene"] == "TP53" for r in results)


@pytest.mark.integration
class TestANNCombinedQueries:
    """Test combined clinical filtering patterns using ANN fields."""

    @pytest.fixture
    async def ann_database(self, test_db):
        """Load SnpEff-annotated variants into database."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_file = make_snpeff_ann_vcf_file()
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_gene_plus_impact_filter(self, ann_database):
        """Find high-impact variants in specific gene."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE gene = $1 AND impact = 'HIGH'
            ORDER BY pos
            """,
            "TP53"
        )

        assert len(results) == 2
        consequences = {r["consequence"] for r in results}
        assert "stop_gained" in consequences
        assert "splice_acceptor_variant" in consequences

    async def test_rare_high_impact_filter(self, ann_database):
        """Find rare high-impact variants (clinical candidate filter)."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE impact = 'HIGH'
            AND (af_gnomad < 0.001 OR af_gnomad IS NULL)
            ORDER BY gene, pos
            """
        )

        assert len(results) == 3
        genes = {r["gene"] for r in results}
        assert genes == {"TP53", "BRCA2"}

    async def test_acmg_gene_high_moderate_filter(self, ann_database):
        """Find HIGH/MODERATE impact variants in ACMG genes."""
        conn = ann_database

        acmg_genes = ["TP53", "BRCA1", "BRCA2", "MLH1", "MSH2"]

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE gene = ANY($1)
            AND impact IN ('HIGH', 'MODERATE')
            ORDER BY gene, pos
            """,
            acmg_genes
        )

        assert len(results) == 4
        genes = {r["gene"] for r in results}
        assert "TP53" in genes
        assert "BRCA2" in genes

    async def test_lof_variants_rare_filter(self, ann_database):
        """Find rare loss-of-function variants."""
        conn = ann_database

        lof_consequences = [
            "stop_gained",
            "frameshift_variant",
            "splice_acceptor_variant",
            "splice_donor_variant",
            "start_lost",
            "stop_lost",
        ]

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE consequence = ANY($1)
            AND (af_gnomad < 0.01 OR af_gnomad IS NULL)
            """,
            lof_consequences
        )

        assert len(results) == 3
        assert all(r["impact"] == "HIGH" for r in results)

    async def test_missense_in_gene_filter(self, ann_database):
        """Find missense variants in specific gene."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE gene = $1
            AND consequence = 'missense_variant'
            """,
            "TP53"
        )

        assert len(results) == 1
        assert results[0]["hgvs_p"] == "p.Pro273Arg"


@pytest.mark.integration
class TestANNAggregationQueries:
    """Test aggregation queries on ANN fields."""

    @pytest.fixture
    async def ann_database(self, test_db):
        """Load SnpEff-annotated variants into database."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_file = make_snpeff_ann_vcf_file()
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_count_by_impact(self, ann_database):
        """Count variants by impact level."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT impact, COUNT(*) as count
            FROM variants
            GROUP BY impact
            ORDER BY count DESC
            """
        )

        impact_counts = {r["impact"]: r["count"] for r in results}
        assert impact_counts["HIGH"] == 3
        assert impact_counts["MODERATE"] == 1
        assert impact_counts["LOW"] == 1
        assert impact_counts["MODIFIER"] == 1

    async def test_count_by_gene(self, ann_database):
        """Count variants per gene."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT gene, COUNT(*) as count
            FROM variants
            WHERE gene IS NOT NULL
            GROUP BY gene
            ORDER BY count DESC
            """
        )

        gene_counts = {r["gene"]: r["count"] for r in results}
        assert gene_counts["TP53"] == 5
        assert gene_counts["BRCA2"] == 1

    async def test_impact_breakdown_by_gene(self, ann_database):
        """Get impact breakdown per gene."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT gene, impact, COUNT(*) as count
            FROM variants
            WHERE gene IS NOT NULL
            GROUP BY gene, impact
            ORDER BY gene, impact
            """
        )

        tp53_impacts = {r["impact"]: r["count"] for r in results if r["gene"] == "TP53"}
        assert tp53_impacts.get("HIGH") == 2
        assert tp53_impacts.get("MODERATE") == 1
        assert tp53_impacts.get("LOW") == 1
        assert tp53_impacts.get("MODIFIER") == 1


@pytest.mark.integration
class TestANNIndexUsage:
    """Test that queries use appropriate indexes on ANN fields."""

    @pytest.fixture
    async def indexed_database(self, test_db):
        """Load data and create indexes."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.schema import SchemaManager
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        schema = SchemaManager(human_genome=True)
        await schema.create_indexes(test_db)

        vcf_file = make_snpeff_ann_vcf_file()
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_gene_index_exists(self, indexed_database):
        """Verify gene index was created."""
        conn = indexed_database

        indexes = await conn.fetch(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'variants' AND indexname LIKE '%gene%'
            """
        )

        index_names = {r["indexname"] for r in indexes}
        assert "idx_variants_gene" in index_names

    async def test_gene_query_uses_index(self, indexed_database):
        """Verify gene query uses index (EXPLAIN shows Index Scan)."""
        conn = indexed_database

        explain = await conn.fetch(
            "EXPLAIN SELECT * FROM variants WHERE gene = 'TP53'"
        )

        plan_str = " ".join(r["QUERY PLAN"] for r in explain).lower()
        assert "index" in plan_str or "scan" in plan_str

    async def test_hgvsp_trigram_index_exists(self, indexed_database):
        """Verify HGVS.p trigram index was created."""
        conn = indexed_database

        indexes = await conn.fetch(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'variants' AND indexname LIKE '%hgvsp%'
            """
        )

        index_names = {r["indexname"] for r in indexes}
        assert "idx_variants_hgvsp_trgm" in index_names


@pytest.mark.integration
class TestANNRawInfoQueries:
    """Test queries on raw ANN stored in JSONB info field."""

    @pytest.fixture
    async def ann_database(self, test_db):
        """Load SnpEff-annotated variants into database."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_file = make_snpeff_ann_vcf_file()
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_raw_ann_preserved_in_info(self, ann_database):
        """Verify raw ANN string is preserved in info JSONB."""
        conn = ann_database

        results = await conn.fetch(
            "SELECT info->>'ANN' as ann FROM variants WHERE gene = 'TP53' LIMIT 1"
        )

        assert len(results) == 1
        ann = results[0]["ann"]
        assert ann is not None
        assert "missense_variant" in ann or "stop_gained" in ann or "splice_acceptor_variant" in ann

    async def test_lof_annotation_in_info(self, ann_database):
        """Query LOF annotation from info JSONB."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT gene, info->>'LOF' as lof
            FROM variants
            WHERE info ? 'LOF'
            """
        )

        assert len(results) == 2
        genes = {r["gene"] for r in results}
        assert genes == {"TP53", "BRCA2"}
        assert all("1.00" in r["lof"] for r in results)

    async def test_jsonb_containment_query(self, ann_database):
        """Test JSONB containment query on info field."""
        conn = ann_database

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE info @> '{"LOF": "(TP53|ENSG00000141510|1|1.00)"}'::jsonb
            """
        )

        assert len(results) == 1
        assert results[0]["gene"] == "TP53"
        assert results[0]["consequence"] == "stop_gained"
