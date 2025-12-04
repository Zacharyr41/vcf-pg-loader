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

CONSEQUENCE_TYPES = [
    "missense_variant",
    "synonymous_variant",
    "stop_gained",
    "frameshift_variant",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "intron_variant",
    "upstream_gene_variant",
    "downstream_gene_variant",
    "3_prime_UTR_variant",
    "5_prime_UTR_variant",
    "intergenic_variant",
]

IMPACTS = ["HIGH", "MODERATE", "LOW", "MODIFIER"]

BIOTYPES = [
    "protein_coding",
    "processed_transcript",
    "nonsense_mediated_decay",
    "retained_intron",
    "lncRNA",
]

FILTER_VALUES = ["PASS", "LowQual", "LowDepth", "LowEVS", "StrandBias"]

GENE_NAMES = [f"GENE{i}" for i in range(1, 501)]
TRANSCRIPT_IDS = [f"ENST{str(i).zfill(11)}" for i in range(1, 1001)]
GENE_IDS = [f"ENSG{str(i).zfill(11)}" for i in range(1, 501)]

GIAB_PLATFORMS = ["PacBio", "Illumina", "10X", "CG", "Ion"]
GIAB_DATASETS = [
    "CCS15kb_20kb", "HiSeqPE300x", "10XChromiumLR", "HiSeq250x250",
    "HiSeqMatePair", "CGnormal", "IonExome", "SolidSE75bp"
]
GIAB_CALLSETS = [
    "CCS15kb_20kbGATK4", "HiSeqPE300xSentieon", "CCS15kb_20kbDV",
    "10XLRGATK", "HiSeqPE300xfreebayes", "HiSeq250x250Sentieon",
    "HiSeq250x250freebayes", "HiSeqMatePairfreebayes"
]
GIAB_DIFFICULT_REGIONS = [
    "hg38.segdups_sorted_merged", "lowmappabilityall",
    "AJtrio-HG002.hg38.300x.bam.bilkentuniv.072319.dups"
]


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


def _generate_snpeff_annotation(alt: str) -> str:
    """Generate a realistic SnpEff ANN field value."""
    gene_name = random.choice(GENE_NAMES)
    gene_id = random.choice(GENE_IDS)
    transcript_id = random.choice(TRANSCRIPT_IDS)
    consequence = random.choice(CONSEQUENCE_TYPES)
    impact = random.choice(IMPACTS)
    biotype = random.choice(BIOTYPES)

    hgvs_c = f"c.{random.randint(1, 5000)}{random.choice(['A', 'C', 'G', 'T'])}>{alt[0]}"
    hgvs_p = ""
    if consequence in ["missense_variant", "stop_gained", "frameshift_variant"]:
        aa_pos = random.randint(1, 1000)
        hgvs_p = f"p.{random.choice(['Ala', 'Gly', 'Val', 'Leu'])}{aa_pos}{random.choice(['Ser', 'Thr', 'Pro', 'Arg'])}"

    cdna_pos = f"{random.randint(1, 5000)}/{random.randint(5000, 10000)}"
    cds_pos = f"{random.randint(1, 3000)}/{random.randint(3000, 6000)}"
    aa_pos_field = f"{random.randint(1, 1000)}/{random.randint(1000, 2000)}"

    return f"{alt}|{consequence}|{impact}|{gene_name}|{gene_id}|transcript|{transcript_id}|{biotype}|{random.randint(1, 20)}/{random.randint(20, 30)}|{hgvs_c}|{hgvs_p}|{cdna_pos}|{cds_pos}|{aa_pos_field}||"


def _generate_giab_info() -> str:
    """Generate GIAB-style INFO field with platform/callset metadata."""
    n_platforms = random.randint(1, 4)
    platforms = random.sample(GIAB_PLATFORMS, n_platforms)
    n_datasets = random.randint(1, 5)
    datasets = random.sample(GIAB_DATASETS, min(n_datasets, len(GIAB_DATASETS)))
    n_callsets = random.randint(1, 6)
    callsets = random.sample(GIAB_CALLSETS, min(n_callsets, len(GIAB_CALLSETS)))
    missing = random.sample(GIAB_DATASETS, random.randint(2, 5))
    callable_cs = random.choice(callsets) if callsets else "CS_unknown"
    filt_callsets = random.sample(GIAB_CALLSETS, random.randint(1, 4))
    difficult = random.sample(GIAB_DIFFICULT_REGIONS, random.randint(1, 3))

    info_parts = [
        f"platforms={n_platforms}",
        f"platformnames={','.join(platforms)}",
        f"datasets={len(datasets)}",
        f"datasetnames={','.join(datasets)}",
        f"callsets={len(callsets)}",
        f"callsetnames={','.join(callsets)}",
        f"datasetsmissingcall={','.join(missing)}",
        f"callable=CS_{callable_cs}_callable",
        f"filt={','.join(f'CS_{c}_filt' for c in filt_callsets)}",
        f"difficultregion={','.join(difficult)}",
    ]
    return ";".join(info_parts)


def _generate_giab_variant(chrom: str, pos: int) -> str:
    """Generate a GIAB-style variant line with realistic distributions.

    Based on GIAB v4.2.1 HG002 chr21 statistics:
    - 84% SNPs, 16% indels
    - 57% het (0/1), 43% hom-alt (1/1), 0% hom-ref
    - ~1% multiallelic
    - Long INFO fields with platform/callset metadata
    """
    ref = random.choice(BASES)
    alt = random.choice([b for b in BASES if b != ref])

    rand = random.random()
    if rand < 0.01:
        alt2 = random.choice([b for b in BASES if b not in (ref, alt)])
        alt = f"{alt},{alt2}"
    elif rand < 0.16:
        indel_len = random.randint(1, 15)
        if random.random() < 0.5:
            ref = ref + "".join(random.choices(BASES, k=indel_len))
        else:
            alt = alt + "".join(random.choices(BASES, k=indel_len))

    info = _generate_giab_info()

    gt = random.choices(["0/1", "1/1"], weights=[0.57, 0.43])[0]
    dp = random.randint(30, 1000)
    ref_reads = random.randint(0, dp) if gt == "0/1" else 0
    alt_reads = dp - ref_reads
    adall = f"{ref_reads},{alt_reads}"
    ad = f"{ref_reads // 3},{alt_reads // 3}"
    gq = random.randint(90, 400)

    return f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t50\tPASS\t{info}\tGT:PS:DP:ADALL:AD:GQ\t{gt}:.:{dp}:{adall}:{ad}:{gq}\n"


def _generate_realistic_variant(chrom: str, pos: int) -> str:
    """Generate a single realistic variant line."""
    ref = random.choice(BASES)
    alt = random.choice([b for b in BASES if b != ref])

    rand = random.random()
    if rand < 0.05:
        alt2 = random.choice([b for b in BASES if b not in (ref, alt)])
        alt = f"{alt},{alt2}"
    elif rand < 0.25:
        if random.random() < 0.5:
            ref = ref + "".join(random.choices(BASES, k=random.randint(1, 10)))
        else:
            alt = alt + "".join(random.choices(BASES, k=random.randint(1, 10)))

    qual = random.randint(20, 10000)
    filter_val = random.choices(
        FILTER_VALUES,
        weights=[0.7, 0.1, 0.1, 0.05, 0.05]
    )[0]

    dp = random.randint(10, 500)
    ac = random.randint(1, 4)
    an = random.choice([2, 4, 6])
    af = round(ac / an, 4)
    mq = round(random.uniform(30, 60), 2)
    qd = round(random.uniform(1, 40), 2)
    fs = round(random.uniform(0, 60), 3)
    sor = round(random.uniform(0, 10), 3)
    mqranksum = round(random.uniform(-5, 5), 3)
    readposranksum = round(random.uniform(-5, 5), 3)

    info_parts = [
        f"AC={ac}",
        f"AF={af}",
        f"AN={an}",
        f"DP={dp}",
        f"MQ={mq}",
        f"QD={qd}",
        f"FS={fs}",
        f"SOR={sor}",
        f"MQRankSum={mqranksum}",
        f"ReadPosRankSum={readposranksum}",
    ]

    first_alt = alt.split(",")[0]
    if random.random() < 0.7:
        ann = _generate_snpeff_annotation(first_alt)
        info_parts.append(f"ANN={ann}")

    info = ";".join(info_parts)

    gt = random.choice(["0/1", "1/1", "0/0", "1/2"] if "," in alt else ["0/1", "1/1", "0/0"])
    sample_dp = random.randint(10, 200)
    ref_reads = random.randint(0, sample_dp)
    alt_reads = sample_dp - ref_reads
    ad = f"{ref_reads},{alt_reads}"
    gq = random.randint(1, 99)
    pl_ref = 0 if gt == "0/0" else random.randint(100, 3000)
    pl_het = 0 if gt == "0/1" else random.randint(100, 3000)
    pl_hom = 0 if gt == "1/1" else random.randint(100, 3000)
    pl = f"{pl_ref},{pl_het},{pl_hom}"

    return f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t{qual}\t{filter_val}\t{info}\tGT:AD:DP:GQ:PL\t{gt}:{ad}:{sample_dp}:{gq}:{pl}\n"


SIMPLE_HEADER = """##fileformat=VCFv4.2
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

REALISTIC_HEADER = """##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed">
##FILTER=<ID=LowQual,Description="Low quality">
##FILTER=<ID=LowDepth,Description="Low read depth">
##FILTER=<ID=LowEVS,Description="Low empirical variant score">
##FILTER=<ID=StrandBias,Description="Strand bias detected">
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count in genotypes">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total number of alleles in called genotypes">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Approximate read depth">
##INFO=<ID=FS,Number=1,Type=Float,Description="Phred-scaled p-value using Fisher's exact test to detect strand bias">
##INFO=<ID=MQ,Number=1,Type=Float,Description="RMS Mapping Quality">
##INFO=<ID=MQRankSum,Number=1,Type=Float,Description="Z-score From Wilcoxon rank sum test of Alt vs. Ref read mapping qualities">
##INFO=<ID=QD,Number=1,Type=Float,Description="Variant Confidence/Quality by Depth">
##INFO=<ID=ReadPosRankSum,Number=1,Type=Float,Description="Z-score from Wilcoxon rank sum test of Alt vs. Ref read position bias">
##INFO=<ID=SOR,Number=1,Type=Float,Description="Symmetric Odds Ratio of 2x2 contingency table to detect strand bias">
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths for the ref and alt alleles">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Approximate read depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Phred-scaled likelihoods for genotypes">
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

GIAB_HEADER = """##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed">
##INFO=<ID=platforms,Number=1,Type=Integer,Description="Number of platforms with this call">
##INFO=<ID=platformnames,Number=.,Type=String,Description="Names of platforms">
##INFO=<ID=datasets,Number=1,Type=Integer,Description="Number of datasets with this call">
##INFO=<ID=datasetnames,Number=.,Type=String,Description="Names of datasets">
##INFO=<ID=callsets,Number=1,Type=Integer,Description="Number of callsets with this call">
##INFO=<ID=callsetnames,Number=.,Type=String,Description="Names of callsets">
##INFO=<ID=datasetsmissingcall,Number=.,Type=String,Description="Datasets missing this call">
##INFO=<ID=callable,Number=.,Type=String,Description="Callsets with callable region">
##INFO=<ID=filt,Number=.,Type=String,Description="Callsets with filtered call">
##INFO=<ID=difficultregion,Number=.,Type=String,Description="Overlapping difficult regions">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=PS,Number=1,Type=Integer,Description="Phase set">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
##FORMAT=<ID=ADALL,Number=R,Type=Integer,Description="Allelic depths including filtered reads">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype quality">
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
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHG002
"""


def generate_synthetic_vcf(
    n_variants: int,
    output_path: Path | None = None,
    realistic: bool = False,
    giab: bool = False,
) -> Path:
    """Generate a synthetic VCF file with the specified number of variants.

    Creates variants distributed across chromosomes with random positions,
    refs, and alts.

    Args:
        n_variants: Number of variants to generate.
        output_path: Optional output path. If None, creates a temp file.
        realistic: If True, generate realistic VCF with annotations,
            multiple INFO/FORMAT fields, multiallelic sites, and varied filters.
        giab: If True, generate GIAB-style VCF with platform/callset metadata
            and distributions matching GIAB v4.2.1 benchmark data.

    Returns:
        Path to the generated VCF file.
    """
    if output_path is None:
        fd, path_str = tempfile.mkstemp(suffix=".vcf.gz")
        output_path = Path(path_str)
    else:
        output_path = Path(output_path)

    if giab:
        header = GIAB_HEADER
    elif realistic:
        header = REALISTIC_HEADER
    else:
        header = SIMPLE_HEADER

    variants_per_chrom = n_variants // len(CHROMOSOMES)
    remainder = n_variants % len(CHROMOSOMES)

    with gzip.open(output_path, "wt") as f:
        f.write(header)

        for i, chrom in enumerate(CHROMOSOMES):
            count = variants_per_chrom + (1 if i < remainder else 0)
            positions = sorted(random.sample(range(10000, 100_000_000), min(count, 99_990_000)))

            for pos in positions:
                if giab:
                    line = _generate_giab_variant(chrom, pos)
                elif realistic:
                    line = _generate_realistic_variant(chrom, pos)
                else:
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
    realistic: bool = False,
    giab: bool = False,
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
        realistic: If True, generate realistic VCF with annotations.
        giab: If True, generate GIAB-style VCF with platform/callset metadata.

    Returns:
        BenchmarkResult with timing information.
    """
    synthetic = False
    cleanup_vcf = False

    if synthetic_count is not None:
        vcf_path = generate_synthetic_vcf(synthetic_count, realistic=realistic, giab=giab)
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
