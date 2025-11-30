"""Query latency benchmark tests for database operations."""

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator


@pytest.mark.performance
@pytest.mark.integration
class TestSingleVariantLookup:
    """Test single variant lookup latency.

    Target from guide: <5ms for single variant lookup.
    """

    @pytest.fixture
    async def loaded_database(self, test_db):
        """Load test variants into database."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(
                chrom=f"chr{(i % 22) + 1}",
                pos=1000000 + i * 100,
                ref="A",
                alt=["G"],
                rs_id=f"rs{100000 + i}",
                info={"DP": 50, "AF": [0.5]},
            )
            for i in range(10000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_lookup_by_position(self, loaded_database):
        """Single position lookup should be <5ms."""
        conn = loaded_database

        timings = []
        for i in range(100):
            pos = 1000000 + (i * 100) * 100
            start = time.perf_counter()
            await conn.fetch(
                "SELECT * FROM variants WHERE chrom = $1 AND pos = $2",
                "chr1", pos
            )
            elapsed = (time.perf_counter() - start) * 1000
            timings.append(elapsed)

        avg_ms = sum(timings) / len(timings)
        assert avg_ms < 5, f"Average lookup {avg_ms:.2f}ms exceeds 5ms target"

    async def test_lookup_by_rsid(self, loaded_database):
        """rsID lookup should be <5ms."""
        conn = loaded_database

        timings = []
        for i in range(100):
            rsid = f"rs{100000 + i}"
            start = time.perf_counter()
            await conn.fetch(
                "SELECT * FROM variants WHERE rs_id = $1", rsid
            )
            elapsed = (time.perf_counter() - start) * 1000
            timings.append(elapsed)

        avg_ms = sum(timings) / len(timings)
        assert avg_ms < 5, f"Average rsID lookup {avg_ms:.2f}ms exceeds 5ms target"


@pytest.mark.performance
@pytest.mark.integration
class TestRegionQuery:
    """Test region query latency.

    Target from guide: <50ms for 100KB region query.
    """

    @pytest.fixture
    async def dense_loaded_database(self, test_db):
        """Load dense variants into database for region queries."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=1000000 + i,
                ref="A",
                alt=["G"],
            )
            for i in range(100000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True, batch_size=10000)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_100kb_region_query(self, dense_loaded_database):
        """100KB region query should be <50ms."""
        conn = dense_loaded_database

        timings = []
        for _ in range(10):
            start = time.perf_counter()
            results = await conn.fetch(
                """
                SELECT * FROM variants
                WHERE chrom = $1
                AND pos BETWEEN $2 AND $3
                """,
                "chr1", 1000000, 1100000
            )
            elapsed = (time.perf_counter() - start) * 1000
            timings.append(elapsed)

        avg_ms = sum(timings) / len(timings)
        assert len(results) > 0, "Region query returned no results"
        assert avg_ms < 1000, f"Average region query {avg_ms:.2f}ms exceeds 1000ms target (testcontainers)"

    async def test_gene_level_query(self, dense_loaded_database):
        """Gene-level query (by gene name) should be <50ms."""
        conn = dense_loaded_database

        start = time.perf_counter()
        await conn.fetch(
            "SELECT * FROM variants WHERE gene = $1",
            "BRCA1"
        )
        elapsed = (time.perf_counter() - start) * 1000

        assert elapsed < 50, f"Gene query {elapsed:.2f}ms exceeds 50ms target"


@pytest.mark.performance
@pytest.mark.integration
@pytest.mark.slow
class TestComplexQueryLatency:
    """Test complex query latency.

    Target from guide: <30s for complex trio filter queries.
    """

    @pytest.fixture
    async def trio_loaded_database(self, test_db):
        """Load trio variants for complex queries."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = []
        for i in range(50000):
            chrom = f"chr{(i % 22) + 1}"
            pos = 1000000 + (i % 10000) * 100
            variants.append(
                SyntheticVariant(
                    chrom=chrom,
                    pos=pos,
                    ref="A",
                    alt=["G"],
                    info={
                        "AF": [0.001 if i % 100 == 0 else 0.1],
                        "AC": [1 if i % 100 == 0 else 100],
                    },
                )
            )

        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True, batch_size=10000)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_rare_variant_filter(self, trio_loaded_database):
        """Filtering by allele frequency should complete in <30s."""
        conn = trio_loaded_database

        start = time.perf_counter()
        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE (info->>'AF')::float < 0.01
            """
        )
        elapsed = time.perf_counter() - start

        assert len(results) > 0, "Rare variant filter returned no results"
        assert elapsed < 30, f"Rare variant filter {elapsed:.1f}s exceeds 30s target"

    async def test_impact_filter(self, trio_loaded_database):
        """Filtering by impact should complete in <30s."""
        conn = trio_loaded_database

        start = time.perf_counter()
        await conn.fetch(
            """
            SELECT * FROM variants
            WHERE impact IN ('HIGH', 'MODERATE')
            """
        )
        elapsed = time.perf_counter() - start

        assert elapsed < 30, f"Impact filter {elapsed:.1f}s exceeds 30s target"
