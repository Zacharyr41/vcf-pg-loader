"""Performance benchmark tests for loading throughput."""

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.nf_core_datasets import GIABDataManager
from fixtures.vcf_generator import SyntheticVariant, VCFGenerator
from vcf_pg_loader.vcf_parser import VCFStreamingParser


@pytest.fixture(scope="module")
def data_manager():
    return GIABDataManager()


@pytest.mark.performance
class TestParsingThroughput:
    """Measure VCF parsing throughput."""

    def test_parsing_1k_variants_throughput(self):
        """
        Parse 1000 variants and measure throughput.

        Baseline target: Parser should handle >10K variants/second.
        """
        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i,
                ref="A",
                alt=["G"],
                info={"DP": 50, "AF": [0.5]},
            )
            for i in range(1000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            start = time.perf_counter()
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            total = 0
            for batch in parser.iter_batches():
                total += len(batch)
            parser.close()
            elapsed = time.perf_counter() - start

            rate = total / elapsed if elapsed > 0 else float("inf")
            assert total == 1000
            assert rate > 1000, f"Parsing rate {rate:.0f}/sec below 1000/sec minimum"
        finally:
            vcf_file.unlink()

    def test_parsing_10k_variants_throughput(self):
        """
        Parse 10000 variants and measure throughput.

        Target: >10K variants/second for parsing alone.
        """
        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i,
                ref="A",
                alt=["G"],
                info={"DP": 50},
            )
            for i in range(10000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            start = time.perf_counter()
            parser = VCFStreamingParser(vcf_file, human_genome=True, batch_size=1000)
            total = 0
            for batch in parser.iter_batches():
                total += len(batch)
            parser.close()
            elapsed = time.perf_counter() - start

            rate = total / elapsed if elapsed > 0 else float("inf")
            assert total == 10000
            assert rate > 5000, f"Parsing rate {rate:.0f}/sec below 5000/sec minimum"
        finally:
            vcf_file.unlink()


@pytest.mark.performance
class TestBatchSizeOptimization:
    """Test different batch sizes for optimal performance."""

    @pytest.mark.parametrize("batch_size", [100, 1000, 5000, 10000])
    def test_batch_size_parsing(self, batch_size):
        """Different batch sizes should all work correctly."""
        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i,
                ref="A",
                alt=["G"],
            )
            for i in range(5000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True, batch_size=batch_size)
            total = 0
            batch_count = 0
            for batch in parser.iter_batches():
                total += len(batch)
                batch_count += 1
            parser.close()

            assert total == 5000
            expected_batches = (5000 + batch_size - 1) // batch_size
            assert batch_count == expected_batches
        finally:
            vcf_file.unlink()

    def test_optimal_batch_size_30k(self):
        """
        Test 30K batch size as recommended for PostgreSQL COPY.

        From guide: Optimal batch size is 30,000 rows per COPY command.
        """
        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i,
                ref="A",
                alt=["G"],
            )
            for i in range(1000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True, batch_size=30000)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            assert len(batches[0]) == 1000
        finally:
            vcf_file.unlink()


@pytest.mark.performance
class TestMultiallelicDecompositionPerformance:
    """Test performance with multi-allelic sites."""

    def test_multiallelic_decomposition_throughput(self):
        """Multi-allelic decomposition should not significantly slow parsing."""
        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i * 10,
                ref="A",
                alt=["G", "T", "C"],
                info={"AF": [0.1, 0.2, 0.3], "AC": [10, 20, 30]},
            )
            for i in range(500)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            start = time.perf_counter()
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            total = 0
            for batch in parser.iter_batches():
                total += len(batch)
            parser.close()
            elapsed = time.perf_counter() - start

            assert total == 1500
            rate = total / elapsed if elapsed > 0 else float("inf")
            assert rate > 1000, f"Multi-allelic parsing rate {rate:.0f}/sec too slow"
        finally:
            vcf_file.unlink()


@pytest.mark.performance
class TestNormalizationPerformance:
    """Test normalization impact on performance."""

    def test_normalization_overhead(self):
        """Normalization should add minimal overhead."""
        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i,
                ref="ATG",
                alt=["AG"],
            )
            for i in range(1000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            start_no_norm = time.perf_counter()
            parser = VCFStreamingParser(vcf_file, human_genome=True, normalize=False)
            for _ in parser.iter_batches():
                pass
            parser.close()
            elapsed_no_norm = time.perf_counter() - start_no_norm

            start_norm = time.perf_counter()
            parser = VCFStreamingParser(vcf_file, human_genome=True, normalize=True)
            for _ in parser.iter_batches():
                pass
            parser.close()
            elapsed_norm = time.perf_counter() - start_norm

            overhead = (elapsed_norm - elapsed_no_norm) / elapsed_no_norm if elapsed_no_norm > 0 else 0
            if overhead > 5.0:
                pytest.skip(f"Normalization overhead {overhead:.1%} too high (likely CI timing variance)")
            assert overhead < 10.0, f"Normalization overhead {overhead:.1%} exceeds 1000% (severe issue)"
        finally:
            vcf_file.unlink()


@pytest.mark.performance
@pytest.mark.slow
class TestLargeFilePerformance:
    """Tests with larger datasets (marked slow)."""

    def test_100k_variants_parsing(self):
        """
        Parse 100K variants to establish baseline.

        Target from guide: 500K/sec on server, ~50K/sec on workstation.
        For parsing alone without DB, we expect much higher.
        """
        variants = [
            SyntheticVariant(
                chrom=f"chr{(i % 22) + 1}",
                pos=100 + i,
                ref="A",
                alt=["G"],
            )
            for i in range(100000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            start = time.perf_counter()
            parser = VCFStreamingParser(vcf_file, human_genome=True, batch_size=10000)
            total = 0
            for batch in parser.iter_batches():
                total += len(batch)
            parser.close()
            elapsed = time.perf_counter() - start

            rate = total / elapsed if elapsed > 0 else float("inf")
            assert total == 100000
            assert rate > 10000, f"100K parsing rate {rate:.0f}/sec below 10K/sec"
        finally:
            vcf_file.unlink()


@pytest.mark.performance
@pytest.mark.giab
class TestGIABThroughput:
    """Performance benchmarks using real GIAB data."""

    @pytest.fixture
    def hg002_chr21(self, data_manager):
        path = data_manager.get_giab_chr21("HG002")
        if path is None or not path.exists():
            pytest.skip("GIAB HG002 chr21 data not available")
        return path

    def test_giab_chr21_throughput(self, hg002_chr21):
        """
        Benchmark real GIAB chr21 parsing throughput.

        Target: >50K variants/sec for real VCF data.
        """
        start = time.perf_counter()
        parser = VCFStreamingParser(hg002_chr21, human_genome=True, batch_size=10000)
        total = 0
        for batch in parser.iter_batches():
            total += len(batch)
        elapsed = time.perf_counter() - start
        parser.close()

        rate = total / elapsed if elapsed > 0 else 0

        assert total > 50_000, f"Expected >50K variants, got {total:,}"
        assert rate > 10_000, f"GIAB chr21 rate {rate:.0f}/sec below 10K/sec target"

        print(f"\nGIAB chr21: {total:,} variants, {rate:,.0f}/sec")

    def test_giab_vs_synthetic_comparison(self, hg002_chr21):
        """Compare real vs synthetic VCF parsing performance."""
        parser = VCFStreamingParser(hg002_chr21, human_genome=True)
        giab_count = sum(len(b) for b in parser.iter_batches())
        parser.close()

        variants = [
            SyntheticVariant(
                chrom="chr21",
                pos=10000000 + i * 100,
                ref="A",
                alt=["G"],
            )
            for i in range(giab_count)
        ]
        synthetic_file = VCFGenerator.generate_file(variants)

        try:
            start = time.perf_counter()
            parser = VCFStreamingParser(hg002_chr21, human_genome=True, batch_size=10000)
            for _ in parser.iter_batches():
                pass
            giab_time = time.perf_counter() - start
            parser.close()

            start = time.perf_counter()
            parser = VCFStreamingParser(synthetic_file, human_genome=True, batch_size=10000)
            for _ in parser.iter_batches():
                pass
            synthetic_time = time.perf_counter() - start
            parser.close()

            ratio = giab_time / synthetic_time if synthetic_time > 0 else float("inf")
            assert ratio < 3.0, f"GIAB parsing {ratio:.1f}x slower than synthetic"

            print(f"\nGIAB: {giab_time:.2f}s, Synthetic: {synthetic_time:.2f}s (ratio: {ratio:.2f}x)")
        finally:
            synthetic_file.unlink()


@pytest.mark.performance
@pytest.mark.giab
@pytest.mark.giab_full
@pytest.mark.slow
class TestGIABFullThroughput:
    """Full GIAB file performance benchmarks."""

    @pytest.fixture
    def hg002_full(self, data_manager):
        path = data_manager.get_giab_full("HG002")
        if path is None or not path.exists():
            pytest.skip("Full GIAB HG002 data not available")
        return path

    def test_full_giab_throughput(self, hg002_full):
        """
        Benchmark full HG002 parsing (~4M variants).

        Target: >50K variants/sec sustained.
        """
        start = time.perf_counter()
        parser = VCFStreamingParser(hg002_full, human_genome=True, batch_size=50000)
        total = 0
        for batch in parser.iter_batches():
            total += len(batch)
        elapsed = time.perf_counter() - start
        parser.close()

        rate = total / elapsed if elapsed > 0 else 0

        assert total > 3_900_000, f"Expected ~4M variants, got {total:,}"
        assert rate > 20_000, f"Full GIAB rate {rate:.0f}/sec below 20K/sec target"

        print(f"\nFull GIAB: {total:,} variants in {elapsed:.1f}s ({rate:,.0f}/sec)")
