"""
Generate mock annotation database VCFs (gnomAD-style) for testing.
See ATTRIBUTION.md for license and copyright information.

Derived from echtvar examples:
- examples/gnomad.v3.1.2.json - Field configuration patterns
- tests/make-string-test-for-issue8.py - Missing value handling
"""
import random
from pathlib import Path

GNOMAD_HEADER = """##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed">
##FILTER=<ID=AC0,Description="Allele count is zero">
##FILTER=<ID=InbreedingCoeff,Description="Inbreeding coefficient < -0.3">
##INFO=<ID=AC,Number=A,Type=Integer,Description="Alternate allele count">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total number of alleles">
##INFO=<ID=AF,Number=A,Type=Float,Description="Alternate allele frequency">
##INFO=<ID=nhomalt,Number=A,Type=Integer,Description="Count of homozygous individuals">
##INFO=<ID=AC_popmax,Number=A,Type=Integer,Description="Allele count in population with max AF">
##INFO=<ID=AN_popmax,Number=A,Type=Integer,Description="Total allele number in population with max AF">
##INFO=<ID=AF_popmax,Number=A,Type=Float,Description="Maximum allele frequency across populations">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=1,length=248956422>
##contig=<ID=2,length=242193529>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""


CLINVAR_HEADER = """##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed">
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="Review status">
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease name">
##INFO=<ID=CLNVC,Number=1,Type=String,Description="Variant type">
##contig=<ID=chr1,length=248956422>
##contig=<ID=1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""


FILTERS = ["PASS", "AC0", "InbreedingCoeff"]
CLNSIG_VALUES = [
    "Pathogenic",
    "Likely_pathogenic",
    "Uncertain_significance",
    "Likely_benign",
    "Benign",
]
CLNREVSTAT_VALUES = [
    "criteria_provided,_single_submitter",
    "criteria_provided,_multiple_submitters,_no_conflicts",
    "reviewed_by_expert_panel",
    "practice_guideline",
]


def generate_gnomad_vcf_content(
    n_variants: int = 1000,
    seed: int = 42,
    use_chr_prefix: bool = True,
    include_rare: bool = True,
    include_common: bool = True,
) -> str:
    """Generate gnomAD-style VCF content.

    Args:
        n_variants: Number of variants to generate
        seed: Random seed for reproducibility
        use_chr_prefix: Use 'chr1' vs '1' naming
        include_rare: Include rare variants (AF < 0.01)
        include_common: Include common variants (AF >= 0.01)

    Returns:
        VCF content as string
    """
    random.seed(seed)
    lines = [GNOMAD_HEADER]

    chroms = ["chr1", "chr2"] if use_chr_prefix else ["1", "2"]
    bases = ["A", "C", "G", "T"]

    for _ in range(n_variants):
        chrom = random.choice(chroms)
        pos = random.randint(10000, 100000000)
        ref = random.choice(bases)
        alt = random.choice([b for b in bases if b != ref])

        an = random.randint(100000, 150000)

        if include_rare and include_common:
            if random.random() < 0.7:
                ac = random.randint(1, int(an * 0.01))
            else:
                ac = random.randint(int(an * 0.01), int(an * 0.5))
        elif include_rare:
            ac = random.randint(1, int(an * 0.01))
        else:
            ac = random.randint(int(an * 0.01), int(an * 0.5))

        af = ac / an
        nhomalt = random.randint(0, max(1, ac // 10))

        ac_popmax = random.randint(1, ac)
        an_popmax = random.randint(an // 10, an // 2)
        af_popmax = min(1.0, ac_popmax / an_popmax)

        filt = random.choice(FILTERS)

        info = (
            f"AC={ac};AN={an};AF={af:.6f};nhomalt={nhomalt};"
            f"AC_popmax={ac_popmax};AN_popmax={an_popmax};AF_popmax={af_popmax:.6f}"
        )

        line = f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t.\t{filt}\t{info}"
        lines.append(line)

    return "\n".join(lines) + "\n"


def generate_clinvar_vcf_content(
    n_variants: int = 500,
    seed: int = 42,
    use_chr_prefix: bool = True,
    pathogenic_fraction: float = 0.3,
) -> str:
    """Generate ClinVar-style VCF content.

    Args:
        n_variants: Number of variants to generate
        seed: Random seed for reproducibility
        use_chr_prefix: Use 'chr1' vs '1' naming
        pathogenic_fraction: Fraction of pathogenic/likely pathogenic variants

    Returns:
        VCF content as string
    """
    random.seed(seed)
    lines = [CLINVAR_HEADER]

    chrom = "chr1" if use_chr_prefix else "1"
    bases = ["A", "C", "G", "T"]

    for idx in range(n_variants):
        pos = random.randint(10000, 100000000)
        ref = random.choice(bases)
        alt = random.choice([b for b in bases if b != ref])

        if random.random() < pathogenic_fraction:
            clnsig = random.choice(["Pathogenic", "Likely_pathogenic"])
        else:
            clnsig = random.choice(CLNSIG_VALUES[2:])

        clnrevstat = random.choice(CLNREVSTAT_VALUES)
        clndn = f"Disease_{idx % 100}"
        clnvc = "single_nucleotide_variant"

        info = f"CLNSIG={clnsig};CLNREVSTAT={clnrevstat};CLNDN={clndn};CLNVC={clnvc}"

        line = f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t.\tPASS\t{info}"
        lines.append(line)

    return "\n".join(lines) + "\n"


def generate_overlapping_variants(
    n_shared: int = 100,
    n_query_only: int = 50,
    n_db_only: int = 50,
    seed: int = 42,
    use_chr_prefix: bool = True,
) -> tuple[str, str]:
    """Generate paired VCFs with controlled overlap for testing annotation.

    Returns:
        Tuple of (database_vcf_content, query_vcf_content)
    """
    random.seed(seed)

    chrom = "chr1" if use_chr_prefix else "1"
    bases = ["A", "C", "G", "T"]

    shared_variants = []
    for i in range(n_shared):
        pos = 10000 + i * 100
        ref = bases[i % 4]
        alt = bases[(i + 1) % 4]
        af = random.uniform(0.0001, 0.1)
        ac = int(af * 100000)
        shared_variants.append((pos, ref, alt, ac, af))

    db_only_variants = []
    for i in range(n_db_only):
        pos = 10000 + (n_shared + i) * 100
        ref = bases[i % 4]
        alt = bases[(i + 2) % 4]
        af = random.uniform(0.0001, 0.1)
        ac = int(af * 100000)
        db_only_variants.append((pos, ref, alt, ac, af))

    query_only_variants = []
    for i in range(n_query_only):
        pos = 10000 + (n_shared + n_db_only + i) * 100
        ref = bases[i % 4]
        alt = bases[(i + 3) % 4]
        query_only_variants.append((pos, ref, alt))

    db_lines = [GNOMAD_HEADER]
    for pos, ref, alt, ac, af in shared_variants + db_only_variants:
        info = f"AC={ac};AN=100000;AF={af:.6f};nhomalt=0"
        db_lines.append(f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t.\tPASS\t{info}")

    query_header = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
##contig=<ID=1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""

    query_lines = [query_header]
    for pos, ref, alt, _, _ in shared_variants:
        query_lines.append(f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t30\tPASS\t.")

    for pos, ref, alt in query_only_variants:
        query_lines.append(f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t30\tPASS\t.")

    return "\n".join(db_lines) + "\n", "\n".join(query_lines) + "\n"


def get_gnomad_field_config() -> list[dict]:
    """Return gnomAD field configuration (echtvar-compatible format)."""
    return [
        {"field": "AC", "alias": "gnomad_ac"},
        {"field": "AN", "alias": "gnomad_an"},
        {"field": "AF", "alias": "gnomad_af"},
        {"field": "nhomalt", "alias": "gnomad_nhomalt"},
        {"field": "AC_popmax", "alias": "gnomad_popmax_ac"},
        {"field": "AN_popmax", "alias": "gnomad_popmax_an"},
        {"field": "AF_popmax", "alias": "gnomad_popmax_af"},
        {"field": "FILTER", "alias": "gnomad_filter", "missing_string": "PASS"},
    ]


def get_clinvar_field_config() -> list[dict]:
    """Return ClinVar field configuration."""
    return [
        {"field": "CLNSIG", "alias": "clinvar_sig"},
        {"field": "CLNREVSTAT", "alias": "clinvar_review"},
        {"field": "CLNDN", "alias": "clinvar_disease"},
        {"field": "CLNVC", "alias": "clinvar_variant_type"},
    ]


def write_annotation_vcf(output_path: Path, content: str) -> Path:
    """Write annotation VCF to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content)
    return output_path
