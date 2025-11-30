"""GIAB benchmark tests for performance and accuracy validation."""

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.nf_core_datasets import (
    GIAB_CHR21_EXPECTATIONS,
    GIAB_FULL_EXPECTATIONS,
    GIABDataManager,
)


@pytest.fixture(scope="module")
def data_manager():
    return GIABDataManager()


@pytest.mark.giab
@pytest.mark.benchmark
class TestGIABChr21ParsingPerformance:
    """Benchmark parsing performance with GIAB chr21 data."""

    @pytest.fixture
    def hg002_chr21(self, data_manager):
        path = data_manager.get_giab_chr21("HG002")
        if path is None or not path.exists():
            pytest.skip("GIAB HG002 chr21 data not available")
        return path

    def test_chr21_parsing_throughput(self, hg002_chr21):
        """Measure parsing throughput for chr21 (~80K variants)."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(hg002_chr21, human_genome=True, batch_size=10000)

        start = time.perf_counter()
        total = 0
        for batch in parser.iter_batches():
            total += len(batch)
        elapsed = time.perf_counter() - start
        parser.close()

        rate = total / elapsed if elapsed > 0 else 0

        assert total > 50_000, f"Expected >50K variants, got {total}"
        assert rate > 10_000, f"Parsing rate {rate:.0f}/sec below 10K/sec target"

        print(f"\nGIAB chr21 parsing: {total:,} variants in {elapsed:.2f}s ({rate:,.0f}/sec)")

    def test_chr21_parsing_with_normalization(self, hg002_chr21):
        """Measure normalization overhead on real data."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser_no_norm = VCFStreamingParser(hg002_chr21, human_genome=True, normalize=False)
        start = time.perf_counter()
        count_no_norm = sum(len(b) for b in parser_no_norm.iter_batches())
        time_no_norm = time.perf_counter() - start
        parser_no_norm.close()

        parser_norm = VCFStreamingParser(hg002_chr21, human_genome=True, normalize=True)
        start = time.perf_counter()
        count_norm = sum(len(b) for b in parser_norm.iter_batches())
        time_norm = time.perf_counter() - start
        parser_norm.close()

        overhead = (time_norm - time_no_norm) / time_no_norm if time_no_norm > 0 else 0

        assert count_no_norm == count_norm
        assert overhead < 0.5, f"Normalization overhead {overhead:.1%} exceeds 50%"

        print(f"\nNormalization overhead: {overhead:.1%}")

    def test_chr21_batch_size_comparison(self, hg002_chr21):
        """Compare different batch sizes on real data."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        results = {}
        for batch_size in [1000, 5000, 10000, 50000]:
            parser = VCFStreamingParser(
                hg002_chr21, human_genome=True, batch_size=batch_size
            )
            start = time.perf_counter()
            total = sum(len(b) for b in parser.iter_batches())
            elapsed = time.perf_counter() - start
            parser.close()

            results[batch_size] = {
                "total": total,
                "time": elapsed,
                "rate": total / elapsed if elapsed > 0 else 0,
            }

        for _bs, r in results.items():
            assert r["total"] == results[1000]["total"]

        print("\nBatch size comparison:")
        for bs, r in results.items():
            print(f"  {bs:,}: {r['rate']:,.0f}/sec ({r['time']:.2f}s)")


@pytest.mark.giab
@pytest.mark.benchmark
class TestGIABChr21Accuracy:
    """Validate parsing accuracy with GIAB chr21 data."""

    @pytest.fixture
    def hg002_chr21(self, data_manager):
        path = data_manager.get_giab_chr21("HG002")
        if path is None or not path.exists():
            pytest.skip("GIAB HG002 chr21 data not available")
        return path

    def test_variant_count_within_expectations(self, hg002_chr21):
        """Total variant count matches expected range."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(hg002_chr21, human_genome=True)
        total = sum(len(b) for b in parser.iter_batches())
        parser.close()

        expected = GIAB_CHR21_EXPECTATIONS["HG002"]["total_variants"]
        assert expected[0] <= total <= expected[1], (
            f"Variant count {total:,} outside expected range {expected[0]:,}-{expected[1]:,}"
        )

    def test_snp_indel_distribution(self, hg002_chr21):
        """SNP/indel ratio matches expected distribution."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(hg002_chr21, human_genome=True)
        snps = 0
        indels = 0

        for batch in parser.iter_batches():
            for record in batch:
                if len(record.ref) == 1 and len(record.alt) == 1:
                    snps += 1
                else:
                    indels += 1
        parser.close()

        total = snps + indels
        snp_ratio = snps / total if total > 0 else 0

        expected_ratio = GIAB_CHR21_EXPECTATIONS["HG002"]["snp_ratio"]
        assert expected_ratio[0] <= snp_ratio <= expected_ratio[1], (
            f"SNP ratio {snp_ratio:.2%} outside expected range "
            f"{expected_ratio[0]:.0%}-{expected_ratio[1]:.0%}"
        )

        print(f"\nSNP/indel distribution: {snps:,} SNPs ({snp_ratio:.1%}), {indels:,} indels")

    def test_no_parsing_errors(self, hg002_chr21):
        """All variants parse without errors."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(hg002_chr21, human_genome=True)
        errors = 0
        total = 0

        for batch in parser.iter_batches():
            for record in batch:
                total += 1
                if record.ref is None or record.alt is None:
                    errors += 1
                if record.chrom is None or record.pos is None:
                    errors += 1
        parser.close()

        assert errors == 0, f"Found {errors} parsing errors in {total:,} variants"

    def test_all_variants_on_chr21(self, hg002_chr21):
        """All variants should be on chr21."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(hg002_chr21, human_genome=True)
        chromosomes = set()

        for batch in parser.iter_batches():
            for record in batch:
                chromosomes.add(record.chrom)
        parser.close()

        assert chromosomes == {"chr21"}, f"Unexpected chromosomes: {chromosomes}"


@pytest.mark.giab
@pytest.mark.giab_full
@pytest.mark.slow
@pytest.mark.benchmark
class TestGIABFullPerformance:
    """Benchmark with full GIAB files (~4M variants each)."""

    @pytest.fixture
    def hg002_full(self, data_manager):
        path = data_manager.get_giab_full("HG002")
        if path is None or not path.exists():
            pytest.skip("GIAB HG002 full benchmark VCF not available")
        return path

    def test_full_vcf_parsing_throughput(self, hg002_full):
        """Measure parsing throughput for full HG002 (~4M variants)."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(hg002_full, human_genome=True, batch_size=50000)

        start = time.perf_counter()
        total = 0
        for batch in parser.iter_batches():
            total += len(batch)
        elapsed = time.perf_counter() - start
        parser.close()

        rate = total / elapsed if elapsed > 0 else 0

        expected = GIAB_FULL_EXPECTATIONS["HG002"]["total_variants"]
        assert expected[0] <= total <= expected[1], f"Variant count {total:,} unexpected"
        assert rate > 20_000, f"Full VCF parsing rate {rate:.0f}/sec below 20K/sec target"

        print(f"\nFull GIAB parsing: {total:,} variants in {elapsed:.1f}s ({rate:,.0f}/sec)")

    def test_full_vcf_memory_efficient(self, hg002_full):
        """Streaming parser should not load entire file into memory."""
        import tracemalloc

        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        tracemalloc.start()

        parser = VCFStreamingParser(hg002_full, human_genome=True, batch_size=10000)
        peak_memory = 0

        for _batch in parser.iter_batches():
            current, peak = tracemalloc.get_traced_memory()
            if current > peak_memory:
                peak_memory = current
        parser.close()

        tracemalloc.stop()

        peak_mb = peak_memory / (1024 * 1024)
        assert peak_mb < 500, f"Peak memory {peak_mb:.0f}MB exceeds 500MB limit"

        print(f"\nPeak memory during parsing: {peak_mb:.0f}MB")


@pytest.mark.giab
@pytest.mark.benchmark
@pytest.mark.integration
class TestGIABDatabaseLoading:
    """Benchmark database loading with GIAB data."""

    @pytest.fixture
    def hg002_chr21(self, data_manager):
        path = data_manager.get_giab_chr21("HG002")
        if path is None or not path.exists():
            pytest.skip("GIAB HG002 chr21 data not available")
        return path

    @pytest.mark.asyncio
    async def test_chr21_loading_throughput(self, postgres_container, hg002_chr21):
        """Measure database loading throughput for chr21."""
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=10000, workers=4, drop_indexes=True)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            start = time.perf_counter()
            result = await loader.load_vcf(hg002_chr21)
            elapsed = time.perf_counter() - start

        total = result["variants_loaded"]
        rate = total / elapsed if elapsed > 0 else 0

        assert total > 50_000, f"Expected >50K variants loaded, got {total}"
        assert rate > 5_000, f"Loading rate {rate:.0f}/sec below 5K/sec target"

        print(f"\nGIAB chr21 loading: {total:,} variants in {elapsed:.1f}s ({rate:,.0f}/sec)")

    @pytest.mark.asyncio
    async def test_chr21_parallel_vs_sequential(self, postgres_container, hg002_chr21):
        """Compare parallel vs sequential loading on real data."""
        from vcf_pg_loader.loader import LoadConfig, VCFLoader
        from vcf_pg_loader.schema import SchemaManager

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        config = LoadConfig(batch_size=10000, workers=4, drop_indexes=True)

        async with VCFLoader(url, config) as loader:
            async with loader.pool.acquire() as conn:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)

            start = time.perf_counter()
            result_seq = await loader.load_vcf(hg002_chr21, parallel=False)
            time_seq = time.perf_counter() - start

            async with loader.pool.acquire() as conn:
                await conn.execute("DELETE FROM variants")

        async with VCFLoader(url, config) as loader:
            start = time.perf_counter()
            result_par = await loader.load_vcf(hg002_chr21, parallel=True, force_reload=True)
            time_par = time.perf_counter() - start

        assert result_seq["variants_loaded"] == result_par["variants_loaded"]

        print(f"\nSequential: {time_seq:.1f}s, Parallel: {time_par:.1f}s")
        print(f"Speedup: {time_seq/time_par:.2f}x" if time_par > 0 else "N/A")


@pytest.mark.giab
@pytest.mark.benchmark
class TestGIABTrioPerformance:
    """Benchmark with full GIAB trio."""

    @pytest.fixture
    def trio_chr21(self, data_manager):
        trio = data_manager.get_giab_trio_chr21()
        if trio is None:
            pytest.skip("GIAB trio chr21 data not available")
        return trio

    def test_trio_parsing_performance(self, trio_chr21):
        """Measure parsing performance across entire trio."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        results = {}
        total_variants = 0

        start_all = time.perf_counter()

        for role, vcf_path in trio_chr21.items():
            parser = VCFStreamingParser(vcf_path, human_genome=True, batch_size=10000)
            start = time.perf_counter()
            count = sum(len(b) for b in parser.iter_batches())
            elapsed = time.perf_counter() - start
            parser.close()

            results[role] = {"count": count, "time": elapsed}
            total_variants += count

        total_time = time.perf_counter() - start_all
        rate = total_variants / total_time if total_time > 0 else 0

        assert total_variants > 150_000, f"Expected >150K total variants, got {total_variants:,}"

        print("\nTrio chr21 parsing:")
        for role, r in results.items():
            print(f"  {role}: {r['count']:,} variants in {r['time']:.2f}s")
        print(f"  Total: {total_variants:,} variants in {total_time:.1f}s ({rate:,.0f}/sec)")
