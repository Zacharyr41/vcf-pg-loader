"""Variant normalization per vt algorithm (Tan et al., 2015)."""

from typing import Protocol


class ReferenceGenome(Protocol):
    """Protocol for reference genome access."""
    def fetch(self, chrom: str, start: int, end: int) -> str:
        """Fetch reference sequence for a region (0-based coordinates)."""
        ...


def _right_trim_or_left_extend(
    alleles: list[str],
    pos: int,
    chrom: str,
    reference_genome: ReferenceGenome | None
) -> tuple[list[str], int]:
    """
    Right trim matching trailing bases, left-extending when an allele becomes empty.

    This implements the first phase of vt normalization: iteratively right-trim
    all alleles when they share the same trailing base, and when any allele
    becomes empty, prepend a reference base to all alleles (left-extend).

    This loop continues until alleles end with different bases and no allele
    is empty, achieving left-alignment through repetitive sequences.
    """
    if len(alleles) <= 1:
        return alleles, pos

    while True:
        to_right_trim = True
        to_left_extend = False

        for allele in alleles:
            if len(allele) == 0:
                to_right_trim = False
                to_left_extend = True
                break
            if allele[-1] != alleles[0][-1]:
                to_right_trim = False

        if pos == 1:
            for allele in alleles:
                if len(allele) == 1:
                    to_right_trim = False
                    break

        if not to_right_trim and not to_left_extend:
            break

        if to_right_trim:
            alleles = [a[:-1] for a in alleles]

        if to_left_extend:
            if reference_genome is None or pos <= 1:
                break
            pos -= 1
            left_base = reference_genome.fetch(chrom, pos - 1, pos).upper()
            alleles = [left_base + a for a in alleles]

    return alleles, pos


def _left_trim(alleles: list[str], pos: int) -> tuple[list[str], int]:
    """
    Left trim matching leading bases for parsimony.

    This implements the second phase of vt normalization: iteratively remove
    the first base from all alleles when they all share the same first base,
    stopping when any allele reaches length 1.
    """
    while True:
        if any(len(a) <= 1 for a in alleles):
            break

        first_bases = {a[0] for a in alleles}
        if len(first_bases) != 1:
            break

        alleles = [a[1:] for a in alleles]
        pos += 1

    return alleles, pos


def normalize_variant(
    chrom: str,
    pos: int,
    ref: str,
    alts: list[str],
    reference_genome: ReferenceGenome | None = None
) -> tuple[int, str, list[str]]:
    """
    Normalize a VCF entry per vt algorithm (Tan et al., 2015).

    Achieves two properties:
    1. Left-alignment: position is leftmost possible
    2. Parsimony: alleles are minimally represented

    The algorithm has two phases:
    1. Right-trim or left-extend: Remove matching trailing bases from all
       alleles. When any allele becomes empty, prepend a reference base
       to all alleles. Continue until alleles end differently.
    2. Left-trim: Remove matching leading bases from all alleles until
       any allele has length 1.

    Args:
        chrom: Chromosome name
        pos: 1-based position
        ref: Reference allele
        alts: List of alternative alleles
        reference_genome: Optional reference for left-extension

    Returns:
        Tuple of (normalized_pos, normalized_ref, normalized_alts)
    """
    if not ref or not alts:
        return pos, ref, alts

    alleles = [ref.upper()] + [a.upper() for a in alts]

    alleles, pos = _right_trim_or_left_extend(alleles, pos, chrom, reference_genome)

    alleles, pos = _left_trim(alleles, pos)

    return pos, alleles[0], alleles[1:]


def is_normalized(ref: str, alts: list[str]) -> bool:
    """
    Quick check if variant is already normalized.

    Uses necessary and sufficient conditions:
    1. Alleles end with different nucleotides
    2. Alleles start differently OR shortest has length 1

    Args:
        ref: Reference allele
        alts: List of alternative alleles

    Returns:
        True if variant appears normalized
    """
    if not ref or not alts:
        return True

    alleles = [ref.upper()] + [a.upper() for a in alts]

    if len({a[-1] for a in alleles if len(a) > 0}) == 1:
        return False

    if min(len(a) for a in alleles) == 1:
        return True

    return len({a[0] for a in alleles}) > 1


def classify_variant(ref: str, alt: str) -> str:
    """
    Classify variant type based on REF and ALT alleles.

    Args:
        ref: Reference allele
        alt: Alternative allele

    Returns:
        Variant type: 'snp', 'indel', 'mnp', or 'sv'
    """
    if alt.startswith('<') and alt.endswith('>'):
        return 'sv'

    if len(ref) == 1 and len(alt) == 1:
        return 'snp'

    if len(ref) != len(alt):
        return 'indel'

    return 'mnp'


def decompose_multiallelic(
    chrom: str,
    pos: int,
    ref: str,
    alts: list[str]
) -> list[tuple[str, int, str, str]]:
    """
    Decompose multi-allelic site into biallelic records.

    Args:
        chrom: Chromosome name
        pos: 1-based position
        ref: Reference allele
        alts: List of alternative alleles

    Returns:
        List of (chrom, pos, ref, alt) tuples for each ALT allele
    """
    return [(chrom, pos, ref, alt) for alt in alts if alt]
