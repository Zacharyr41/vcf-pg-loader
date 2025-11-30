"""Benchmarking utilities for vcf-pg-loader."""

import asyncio
import gzip
import random
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

CHROMOSOMES = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY", "chrM"]
BASES = ["A", "C", "G", "T"]


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    vcf_path: str
    variant_count: int
    parsing_time: float
    parsing_rate: float
    loading_time: float | None = None
    loading_rate: float | None = None
    batch_size: int = 50000
    normalized: bool = True
    synthetic: bool = False

    def to_dict(self) -> dict:
        result = {
            "vcf_path": self.vcf_path,
            "variant_count": self.variant_count,
            "parsing": {
                "time_seconds": round(self.parsing_time, 3),
                "rate_per_second": round(self.parsing_rate, 0),
            },
            "settings": {
                "batch_size": self.batch_size,
                "normalized": self.normalized,
                "synthetic": self.synthetic,
            },
        }
        if self.loading_time is not None:
            result["loading"] = {
                "time_seconds": round(self.loading_time, 3),
                "rate_per_second": round(self.loading_rate or 0, 0),
            }
        return result


def generate_synthetic_vcf(n_variants: int, output_path: Path | None = None) -> Path:
    """Generate a synthetic VCF file with the specified number of variants.

    Creates realistic-looking variants distributed across chromosomes with
    random positions, refs, and alts.

    Args:
        n_variants: Number of variants to generate.
        output_path: Optional output path. If None, creates a temp file.

    Returns:
        Path to the generated VCF file.
    """
    if output_path is None:
        fd, path_str = tempfile.mkstemp(suffix=".vcf.gz")
        output_path = Path(path_str)
    else:
        output_path = Path(output_path)

    header = """##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chr3,length=198295559>
##contig=<ID=chr4,length=190214555>
##contig=<ID=chr5,length=181538259>
##contig=<ID=chr6,length=170805979>
##contig=<ID=chr7,length=159345973>
##contig=<ID=chr8,length=145138636>
##contig=<ID=chr9,length=138394717>
##contig=<ID=chr10,length=133797422>
##contig=<ID=chr11,length=135086622>
##contig=<ID=chr12,length=133275309>
##contig=<ID=chr13,length=114364328>
##contig=<ID=chr14,length=107043718>
##contig=<ID=chr15,length=101991189>
##contig=<ID=chr16,length=90338345>
##contig=<ID=chr17,length=83257441>
##contig=<ID=chr18,length=80373285>
##contig=<ID=chr19,length=58617616>
##contig=<ID=chr20,length=64444167>
##contig=<ID=chr21,length=46709983>
##contig=<ID=chr22,length=50818468>
##contig=<ID=chrX,length=156040895>
##contig=<ID=chrY,length=57227415>
##contig=<ID=chrM,length=16569>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
"""

    variants_per_chrom = n_variants // len(CHROMOSOMES)
    remainder = n_variants % len(CHROMOSOMES)

    with gzip.open(output_path, "wt") as f:
        f.write(header)

        for i, chrom in enumerate(CHROMOSOMES):
            count = variants_per_chrom + (1 if i < remainder else 0)
            positions = sorted(random.sample(range(10000, 100_000_000), min(count, 99_990_000)))

            for pos in positions:
                ref = random.choice(BASES)
                alt = random.choice([b for b in BASES if b != ref])

                if random.random() < 0.1:
                    ref = ref + "".join(random.choices(BASES, k=random.randint(1, 5)))
                elif random.random() < 0.1:
                    alt = alt + "".join(random.choices(BASES, k=random.randint(1, 5)))

                dp = random.randint(10, 100)
                af = round(random.uniform(0.01, 0.5), 4)
                gt = random.choice(["0/1", "1/1", "0/0"])
                sample_dp = random.randint(5, 50)

                line = f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t{random.randint(20, 100)}\tPASS\tDP={dp};AF={af}\tGT:DP\t{gt}:{sample_dp}\n"
                f.write(line)

    return output_path


def run_parsing_benchmark(
    vcf_path: Path,
    batch_size: int = 50000,
    normalize: bool = True,
    human_genome: bool = True,
) -> tuple[int, float]:
    """Run a parsing-only benchmark.

    Args:
        vcf_path: Path to VCF file.
        batch_size: Batch size for streaming parser.
        normalize: Whether to normalize variants.
        human_genome: Whether to use human genome chromosome handling.

    Returns:
        Tuple of (variant_count, elapsed_time).
    """
    from .vcf_parser import VCFStreamingParser

    parser = VCFStreamingParser(
        vcf_path,
        batch_size=batch_size,
        normalize=normalize,
        human_genome=human_genome,
    )

    start = time.perf_counter()
    total = 0
    for batch in parser.iter_batches():
        total += len(batch)
    elapsed = time.perf_counter() - start
    parser.close()

    return total, elapsed


async def run_loading_benchmark(
    vcf_path: Path,
    db_url: str,
    batch_size: int = 50000,
    normalize: bool = True,
    human_genome: bool = True,
) -> tuple[int, float]:
    """Run a full loading benchmark including database insertion.

    Args:
        vcf_path: Path to VCF file.
        db_url: PostgreSQL connection URL.
        batch_size: Batch size for loading.
        normalize: Whether to normalize variants.
        human_genome: Whether to use human genome mode.

    Returns:
        Tuple of (variant_count, elapsed_time).
    """
    from .loader import LoadConfig, VCFLoader
    from .schema import SchemaManager

    config = LoadConfig(
        batch_size=batch_size,
        normalize=normalize,
        human_genome=human_genome,
        drop_indexes=True,
    )

    async with VCFLoader(db_url, config) as loader:
        async with loader.pool.acquire() as conn:
            schema_manager = SchemaManager(human_genome=human_genome)
            await schema_manager.create_schema(conn)

        start = time.perf_counter()
        result = await loader.load_vcf(vcf_path, force_reload=True)
        elapsed = time.perf_counter() - start

    return result["variants_loaded"], elapsed


def run_benchmark(
    vcf_path: Path | None = None,
    synthetic_count: int | None = None,
    db_url: str | None = None,
    batch_size: int = 50000,
    normalize: bool = True,
    human_genome: bool = True,
) -> BenchmarkResult:
    """Run a complete benchmark.

    Args:
        vcf_path: Path to VCF file. If None and synthetic_count is None,
                  uses a built-in fixture.
        synthetic_count: If provided, generate a synthetic VCF with this many variants.
        db_url: If provided, also benchmark database loading.
        batch_size: Batch size for parsing/loading.
        normalize: Whether to normalize variants.
        human_genome: Whether to use human genome mode.

    Returns:
        BenchmarkResult with timing information.
    """
    synthetic = False
    cleanup_vcf = False

    if synthetic_count is not None:
        vcf_path = generate_synthetic_vcf(synthetic_count)
        synthetic = True
        cleanup_vcf = True
    elif vcf_path is None:
        fixtures_dir = Path(__file__).parent.parent.parent / "tests" / "fixtures"
        vcf_path = fixtures_dir / "strelka_snvs_chr22.vcf.gz"
        if not vcf_path.exists():
            vcf_path = fixtures_dir / "with_annotations.vcf"

    try:
        variant_count, parsing_time = run_parsing_benchmark(
            vcf_path,
            batch_size=batch_size,
            normalize=normalize,
            human_genome=human_genome,
        )
        parsing_rate = variant_count / parsing_time if parsing_time > 0 else 0

        loading_time = None
        loading_rate = None

        if db_url:
            loaded_count, loading_time = asyncio.run(
                run_loading_benchmark(
                    vcf_path,
                    db_url,
                    batch_size=batch_size,
                    normalize=normalize,
                    human_genome=human_genome,
                )
            )
            loading_rate = loaded_count / loading_time if loading_time > 0 else 0

        return BenchmarkResult(
            vcf_path=str(vcf_path),
            variant_count=variant_count,
            parsing_time=parsing_time,
            parsing_rate=parsing_rate,
            loading_time=loading_time,
            loading_rate=loading_rate,
            batch_size=batch_size,
            normalized=normalize,
            synthetic=synthetic,
        )
    finally:
        if cleanup_vcf and vcf_path and vcf_path.exists():
            vcf_path.unlink()
