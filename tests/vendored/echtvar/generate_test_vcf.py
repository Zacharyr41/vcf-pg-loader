"""
VCF test data generation derived from echtvar.
See ATTRIBUTION.md for license and copyright information.

Original: https://github.com/brentp/echtvar/blob/main/tests/make-vcf.py
"""
import itertools
import random
from pathlib import Path

VCF_HEADER_TEMPLATE = """##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed",IDX=0>
##INFO=<ID=AC,Number=A,Type=Integer,Description="Alternate allele count">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total number of alleles">
##INFO=<ID=val{suffix},Number=1,Type=Integer,Description="random value">
##INFO=<ID=nodesc,Number=1,Type=Integer>
##INFO=<ID=nvar,Number=1,Type=Integer,Description="variant index">
##INFO=<ID=str,Number=.,Type=String,Description="string value">
##INFO=<ID=AF,Number=A,Type=Float,Description="Alternate allele frequency">
##contig=<ID=chr1,length=248956422>
##contig=<ID=1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""


STR_VALS = ["YES", "NO", "MAYBE"]
LONG_REFS = [
    "ACCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
    "ACCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCT",
]
LONG_ALTS = ["ACCCCCCCCCCCCCCCCC", "A", "ACCCCCCCCCCCCCCC"]

CHUNK_SWITCHES = [1, 2, 3, 4, 5, 1132, 1133, 1134]


def generate_vcf_content(
    mod: int = 2,
    seed: int = 42,
    include_all: bool = True,
    include_subset0: bool = True,
    include_subset1: bool = True,
) -> dict[str, str]:
    """Generate VCF content for testing.

    Args:
        mod: Modulo value for splitting variants between subsets
        seed: Random seed for reproducibility
        include_all: Whether to generate the "all" VCF
        include_subset0: Whether to generate subset0 VCF
        include_subset1: Whether to generate subset1 VCF

    Returns:
        Dictionary with keys 'all', 'subset0', 'subset1' containing VCF content
    """
    random.seed(seed)

    results = {}
    all_lines = [VCF_HEADER_TEMPLATE.format(suffix="")]
    subset0_lines = [VCF_HEADER_TEMPLATE.format(suffix="0")]
    subset1_lines = [VCF_HEADER_TEMPLATE.format(suffix="1")]

    nvar = 0

    for switch in CHUNK_SWITCHES:
        switch = switch << 20

        for i in range(switch - 32, switch + 32):
            for rlen in range(1, 5):
                for ref in itertools.permutations("ACGT", rlen):
                    ref = "".join(ref)
                    for alen in range(0, 5):
                        for balt in itertools.permutations("ACGT", alen):
                            val = random.randint(0, 10000000)
                            ac = random.randint(1, 3)
                            alt = ref[0] + "".join(balt)
                            str_val = STR_VALS[ac - 1]

                            all_line = (
                                f"chr1\t{i}\t.\t{ref}\t{alt}\t1\tPASS\t"
                                f"val={val};nvar={nvar};AC={ac};str={str_val}"
                            )
                            all_lines.append(all_line)

                            if nvar % mod == 0:
                                subset0_line = (
                                    f"chr1\t{i}\t.\t{ref}\t{alt}\t1\tPASS\t"
                                    f"val0={val};nvar={nvar};AC={ac};str={str_val}"
                                )
                                subset0_lines.append(subset0_line)
                            else:
                                subset1_line = (
                                    f"chr1\t{i}\t.\t{ref}\t{alt}\t1\tPASS\t"
                                    f"val1={val};nvar={nvar};AC={ac};str={str_val}"
                                )
                                subset1_lines.append(subset1_line)
                            nvar += 1

            for ref in LONG_REFS:
                for alt in LONG_ALTS:
                    val = random.randint(0, 10000000)

                    all_line = f"chr1\t{i}\t.\t{ref}\t{alt}\t1\tPASS\tval={val};nvar={nvar}"
                    all_lines.append(all_line)

                    if nvar % mod == 0:
                        subset0_line = f"chr1\t{i}\t.\t{ref}\t{alt}\t1\tPASS\tval0={val};nvar={nvar}"
                        subset0_lines.append(subset0_line)
                    else:
                        subset1_line = f"chr1\t{i}\t.\t{ref}\t{alt}\t1\tPASS\tval1={val};nvar={nvar}"
                        subset1_lines.append(subset1_line)
                    nvar += 1

    if include_all:
        results["all"] = "\n".join(all_lines) + "\n"
    if include_subset0:
        results["subset0"] = "\n".join(subset0_lines) + "\n"
    if include_subset1:
        results["subset1"] = "\n".join(subset1_lines) + "\n"

    return results


def generate_no_chr_prefix_vcf(vcf_content: str) -> str:
    """Generate VCF content without 'chr' prefix (1 instead of chr1)."""
    lines = vcf_content.split("\n")
    result = []
    for line in lines:
        if line.startswith("chr"):
            result.append(line[3:])
        else:
            result.append(line)
    return "\n".join(result)


def write_vcf_files(output_dir: Path, mod: int = 2, seed: int = 42) -> dict[str, Path]:
    """Write VCF test files to disk.

    Args:
        output_dir: Directory to write files to
        mod: Modulo value for subset splitting
        seed: Random seed

    Returns:
        Dictionary mapping file type to Path
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    vcfs = generate_vcf_content(mod=mod, seed=seed)
    paths = {}

    for name, content in vcfs.items():
        path = output_dir / f"generated-{name}.vcf"
        path.write_text(content)
        paths[name] = path

    return paths


def get_expected_variant_count(mod: int = 2) -> dict[str, int]:
    """Calculate expected variant counts for each VCF type."""
    vcfs = generate_vcf_content(mod=mod)

    counts = {}
    for name, content in vcfs.items():
        lines = [ln for ln in content.split("\n") if ln and not ln.startswith("#")]
        counts[name] = len(lines)

    return counts


def get_variant_info(vcf_content: str, variant_idx: int) -> dict | None:
    """Extract INFO fields for a specific variant by index."""
    lines = [ln for ln in vcf_content.split("\n") if ln and not ln.startswith("#")]
    if variant_idx >= len(lines):
        return None

    line = lines[variant_idx]
    parts = line.split("\t")
    if len(parts) < 8:
        return None

    chrom, pos, id_, ref, alt, qual, filt, info_str = parts[:8]

    info = {}
    for item in info_str.split(";"):
        if "=" in item:
            key, value = item.split("=", 1)
            info[key] = value

    return {
        "chrom": chrom,
        "pos": int(pos),
        "id": id_,
        "ref": ref,
        "alt": alt,
        "qual": qual,
        "filter": filt,
        "info": info,
    }
