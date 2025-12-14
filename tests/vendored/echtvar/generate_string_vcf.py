"""
String/categorical field VCF test data generation derived from echtvar.
See ATTRIBUTION.md for license and copyright information.

Original: https://github.com/brentp/echtvar/blob/main/tests/make-string-vcf.py
"""
import itertools
import random
from pathlib import Path

VCF_HEADER = """##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed">
##FILTER=<ID=FAIL,Description="Failed filters">
##FILTER=<ID=OTHER,Description="Other filter">
##INFO=<ID=num,Number=1,Type=Integer,Description="random integer value">
##INFO=<ID=val,Number=.,Type=String,Description="random string value">
##contig=<ID=chr1,length=248956422>
##contig=<ID=1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""


FILTERS = ["PASS", "FAIL", "OTHER"]
CHUNK_SWITCHES = [1, 2, 3, 4, 5, 1132, 1133, 1134]


def generate_string_vcf_content(seed: int | None = None) -> str:
    """Generate VCF content with string/categorical fields.

    Args:
        seed: Random seed for reproducibility (None for random)

    Returns:
        VCF content as string
    """
    if seed is not None:
        random.seed(seed)

    lines = [VCF_HEADER]

    for switch in CHUNK_SWITCHES:
        switch = switch << 20

        for i in range(switch - 12, switch + 12):
            for rlen in range(1, 3):
                for ref in itertools.permutations("ACGT", rlen):
                    ref = "".join(ref)
                    for alen in range(0, 3):
                        for balt in itertools.permutations("ACGT", alen):
                            val = random.randint(0, 100)
                            flt = random.choice(FILTERS)
                            alt = ref[0] + "".join(balt)
                            line = f"chr1\t{i}\t.\t{ref}\t{alt}\t1\t{flt}\tval=s{val};num=3"
                            lines.append(line)

    return "\n".join(lines) + "\n"


def write_string_vcf(output_path: Path, seed: int | None = None) -> Path:
    """Write string test VCF to disk.

    Args:
        output_path: Path to write the VCF
        seed: Random seed

    Returns:
        Path to written file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = generate_string_vcf_content(seed=seed)
    output_path.write_text(content)
    return output_path


def get_expected_filters() -> list[str]:
    """Return list of FILTER values that should be present."""
    return FILTERS


def count_variants_by_filter(vcf_content: str) -> dict[str, int]:
    """Count variants by FILTER value."""
    counts = dict.fromkeys(FILTERS, 0)

    for line in vcf_content.split("\n"):
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 7:
            filt = parts[6]
            if filt in counts:
                counts[filt] += 1

    return counts


def validate_string_vcf(vcf_content: str) -> list[str]:
    """Validate that string VCF has expected structure.

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    lines = vcf_content.split("\n")
    header_lines = [ln for ln in lines if ln.startswith("#")]
    data_lines = [ln for ln in lines if ln and not ln.startswith("#")]

    has_filter_pass = any("##FILTER=<ID=PASS" in ln for ln in header_lines)
    has_filter_fail = any("##FILTER=<ID=FAIL" in ln for ln in header_lines)
    has_info_val = any("##INFO=<ID=val" in ln for ln in header_lines)
    has_info_num = any("##INFO=<ID=num" in ln for ln in header_lines)

    if not has_filter_pass:
        errors.append("Missing FILTER header for PASS")
    if not has_filter_fail:
        errors.append("Missing FILTER header for FAIL")
    if not has_info_val:
        errors.append("Missing INFO header for 'val'")
    if not has_info_num:
        errors.append("Missing INFO header for 'num'")

    if not data_lines:
        errors.append("No variant data lines found")

    for i, line in enumerate(data_lines[:5]):
        parts = line.split("\t")
        if len(parts) < 8:
            errors.append(f"Line {i}: insufficient columns")
            continue

        filt = parts[6]
        if filt not in FILTERS:
            errors.append(f"Line {i}: unexpected FILTER value '{filt}'")

        info = parts[7]
        if "val=" not in info:
            errors.append(f"Line {i}: missing 'val=' in INFO")
        if "num=" not in info:
            errors.append(f"Line {i}: missing 'num=' in INFO")

    return errors
