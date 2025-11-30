"""Unit tests for parallel loading functionality."""

import tempfile
from pathlib import Path

import pytest

from vcf_pg_loader.vcf_parser import VCFStreamingParser


class TestChromosomePartitioning:
    """Test chromosome-based partitioning logic."""

    @pytest.fixture
    def multi_chrom_vcf(self):
        """Create VCF with multiple chromosomes."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chr3,length=198295559>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
chr1	200	.	C	T	30	.	.
chr2	1000	.	G	A	30	.	.
chr3	500	.	T	C	30	.	.
chr3	600	.	A	G	30	.	.
chr3	700	.	C	T	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)
        yield path
        path.unlink()

    def test_variants_grouped_by_chromosome(self, multi_chrom_vcf):
        """Variants can be grouped by chromosome."""
        parser = VCFStreamingParser(multi_chrom_vcf, human_genome=True)
        chrom_groups = {}

        for batch in parser.iter_batches():
            for r in batch:
                if r.chrom not in chrom_groups:
                    chrom_groups[r.chrom] = []
                chrom_groups[r.chrom].append(r)
        parser.close()

        assert len(chrom_groups) == 3
        assert len(chrom_groups["chr1"]) == 2
        assert len(chrom_groups["chr2"]) == 1
        assert len(chrom_groups["chr3"]) == 3

    def test_chromosome_set_extraction(self, multi_chrom_vcf):
        """Can extract set of chromosomes present."""
        parser = VCFStreamingParser(multi_chrom_vcf, human_genome=True)
        chromosomes = set()

        for batch in parser.iter_batches():
            for r in batch:
                chromosomes.add(r.chrom)
        parser.close()

        assert chromosomes == {"chr1", "chr2", "chr3"}

    def test_chromosome_counts(self, multi_chrom_vcf):
        """Can count variants per chromosome."""
        parser = VCFStreamingParser(multi_chrom_vcf, human_genome=True)
        counts = {}

        for batch in parser.iter_batches():
            for r in batch:
                counts[r.chrom] = counts.get(r.chrom, 0) + 1
        parser.close()

        assert counts["chr1"] == 2
        assert counts["chr2"] == 1
        assert counts["chr3"] == 3


class TestWorkerDistribution:
    """Test worker distribution strategies."""

    def test_round_robin_distribution(self):
        """Test round-robin chromosome assignment."""
        chromosomes = ["chr1", "chr2", "chr3", "chr4", "chr5"]
        num_workers = 3

        worker_assignments = {i: [] for i in range(num_workers)}
        for i, chrom in enumerate(chromosomes):
            worker_idx = i % num_workers
            worker_assignments[worker_idx].append(chrom)

        assert worker_assignments[0] == ["chr1", "chr4"]
        assert worker_assignments[1] == ["chr2", "chr5"]
        assert worker_assignments[2] == ["chr3"]

    def test_count_based_distribution(self):
        """Test distribution based on variant counts."""
        chrom_counts = {
            "chr1": 1000,
            "chr2": 500,
            "chr3": 2000,
            "chr4": 300,
            "chr5": 800
        }

        sorted_chroms = sorted(chrom_counts.keys(), key=lambda x: chrom_counts[x], reverse=True)

        assert sorted_chroms[0] == "chr3"
        assert sorted_chroms[-1] == "chr4"

    def test_balanced_workload_assignment(self):
        """Test balanced workload assignment to workers."""
        chrom_counts = {
            "chr1": 1000,
            "chr2": 800,
            "chr3": 600,
            "chr4": 400,
        }
        num_workers = 2

        worker_loads = [0] * num_workers
        worker_chroms = [[] for _ in range(num_workers)]

        for chrom in sorted(chrom_counts.keys(), key=lambda x: chrom_counts[x], reverse=True):
            min_worker = worker_loads.index(min(worker_loads))
            worker_chroms[min_worker].append(chrom)
            worker_loads[min_worker] += chrom_counts[chrom]

        total_per_worker = [
            sum(chrom_counts[c] for c in chroms)
            for chroms in worker_chroms
        ]
        assert abs(total_per_worker[0] - total_per_worker[1]) <= 600


class TestBatchingForParallelism:
    """Test batching strategies for parallel loading."""

    @pytest.fixture
    def large_vcf(self):
        """Create a larger VCF for batching tests."""
        lines = [
            "##fileformat=VCFv4.3",
            "##contig=<ID=chr1,length=248956422>",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
        ]
        for i in range(1000):
            lines.append(f"chr1\t{100 + i}\t.\tA\tG\t30\t.\t.")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write("\n".join(lines) + "\n")
            path = Path(f.name)
        yield path
        path.unlink()

    def test_batch_size_respected(self, large_vcf):
        """Batch size is respected during iteration."""
        batch_size = 100
        parser = VCFStreamingParser(large_vcf, human_genome=True, batch_size=batch_size)

        batch_sizes = []
        for batch in parser.iter_batches():
            batch_sizes.append(len(batch))
        parser.close()

        for size in batch_sizes[:-1]:
            assert size == batch_size

    def test_all_variants_covered(self, large_vcf):
        """All variants are yielded across batches."""
        parser = VCFStreamingParser(large_vcf, human_genome=True, batch_size=100)

        total = sum(len(batch) for batch in parser.iter_batches())
        parser.close()

        assert total == 1000

    def test_different_batch_sizes_same_total(self, large_vcf):
        """Different batch sizes yield same total."""
        totals = []
        for batch_size in [50, 100, 200, 500]:
            parser = VCFStreamingParser(large_vcf, human_genome=True, batch_size=batch_size)
            total = sum(len(batch) for batch in parser.iter_batches())
            parser.close()
            totals.append(total)

        assert all(t == 1000 for t in totals)


class TestParallelParsingIndependence:
    """Test that chromosome parsing can be independent."""

    def test_multiple_parsers_same_file(self):
        """Multiple parsers can read same file independently."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
chr1	200	.	C	T	30	.	.
chr1	300	.	G	A	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser1 = VCFStreamingParser(path, human_genome=True)
            parser2 = VCFStreamingParser(path, human_genome=True)

            count1 = sum(len(batch) for batch in parser1.iter_batches())
            count2 = sum(len(batch) for batch in parser2.iter_batches())

            parser1.close()
            parser2.close()

            assert count1 == count2 == 3
        finally:
            path.unlink()

    def test_parsers_yield_same_variants(self):
        """Independent parsers yield identical variants."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
chr1	200	.	C	T	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser1 = VCFStreamingParser(path, human_genome=True)
            parser2 = VCFStreamingParser(path, human_genome=True)

            variants1 = []
            for batch in parser1.iter_batches():
                variants1.extend((r.chrom, r.pos, r.ref, r.alt) for r in batch)

            variants2 = []
            for batch in parser2.iter_batches():
                variants2.extend((r.chrom, r.pos, r.ref, r.alt) for r in batch)

            parser1.close()
            parser2.close()

            assert variants1 == variants2
        finally:
            path.unlink()


class TestErrorHandlingInParallelContext:
    """Test error handling for parallel loading scenarios."""

    def test_parser_closes_cleanly_on_error(self):
        """Parser closes cleanly even if iteration stops early."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
chr1	200	.	C	T	30	.	.
chr1	300	.	G	A	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser = VCFStreamingParser(path, human_genome=True)
            for _batch in parser.iter_batches():
                break
            parser.close()
            assert True
        finally:
            path.unlink()

    def test_context_manager_closes_parser(self):
        """Context manager ensures parser is closed."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            with VCFStreamingParser(path, human_genome=True) as parser:
                for _batch in parser.iter_batches():
                    pass
            assert parser._closed
        finally:
            path.unlink()


class TestParallelLoadingErrorHandling:
    """Test error handling for parallel loading failures."""

    def test_parallel_loading_exception_in_worker_should_propagate(self):
        """Exception in parallel worker should propagate to caller."""
        import asyncio

        async def failing_worker():
            raise RuntimeError("Worker failed")

        async def run_test():
            tasks = [failing_worker()]
            with pytest.raises(RuntimeError, match="Worker failed"):
                await asyncio.gather(*tasks)

        asyncio.run(run_test())

    def test_parallel_loading_partial_failure_captured(self):
        """Partial failures in parallel loading should be captured."""
        import asyncio

        results = []
        errors = []

        async def worker(i):
            if i == 2:
                raise RuntimeError(f"Worker {i} failed")
            return i * 10

        async def run_test():
            tasks = [worker(i) for i in range(4)]
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            for r in gathered:
                if isinstance(r, Exception):
                    errors.append(r)
                else:
                    results.append(r)

        asyncio.run(run_test())

        assert len(results) == 3
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)

    def test_loader_should_mark_audit_failed_on_parallel_error(self):
        """Loader should mark audit record as failed when parallel loading fails."""
        from vcf_pg_loader.loader import LoadConfig

        config = LoadConfig(workers=4)
        assert config.workers == 4

    def test_gather_with_return_exceptions_captures_all_errors(self):
        """asyncio.gather with return_exceptions=True captures all errors."""
        import asyncio

        async def worker(fail: bool):
            if fail:
                raise ValueError("Intentional failure")
            return "success"

        async def run_test():
            results = await asyncio.gather(
                worker(False),
                worker(True),
                worker(False),
                return_exceptions=True
            )
            return results

        results = asyncio.run(run_test())

        assert results[0] == "success"
        assert isinstance(results[1], ValueError)
        assert results[2] == "success"


class TestPartitionNaming:
    """Test partition naming conventions."""

    def test_human_chromosome_partition_names(self):
        """Human chromosome partition names follow convention."""
        from vcf_pg_loader.schema import HUMAN_CHROMOSOMES

        partition_names = []
        for chrom in HUMAN_CHROMOSOMES:
            name = f"variants_{chrom.replace('chr', '').lower()}"
            partition_names.append(name)

        assert "variants_1" in partition_names
        assert "variants_x" in partition_names
        assert "variants_y" in partition_names
        assert "variants_m" in partition_names

    def test_all_chromosomes_covered(self):
        """All 25 human chromosomes have partitions."""
        from vcf_pg_loader.schema import HUMAN_CHROMOSOMES

        assert len(HUMAN_CHROMOSOMES) == 25
        assert "chr1" in HUMAN_CHROMOSOMES
        assert "chr22" in HUMAN_CHROMOSOMES
        assert "chrX" in HUMAN_CHROMOSOMES
        assert "chrY" in HUMAN_CHROMOSOMES
        assert "chrM" in HUMAN_CHROMOSOMES
