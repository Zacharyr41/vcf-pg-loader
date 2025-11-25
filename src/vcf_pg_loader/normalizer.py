"""Variant normalization per vt algorithm (Tan et al., 2015)."""

from typing import Protocol


class ReferenceGenome(Protocol):
    """Protocol for reference genome access."""
    def fetch(self, chrom: str, start: int, end: int) -> str:
        """Fetch reference sequence for a region (0-based coordinates)."""
        ...


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

    changed = True
    while changed:
        changed = False

        if all(len(a) > 0 for a in alleles):
            last_bases = {a[-1] for a in alleles}
            if len(last_bases) == 1:
                new_alleles = [a[:-1] for a in alleles]

                if any(len(a) == 0 for a in new_alleles):
                    if reference_genome is not None and pos > 1:
                        pos -= 1
                        left_base = reference_genome.fetch(chrom, pos - 1, pos)
                        alleles = [left_base.upper() + a for a in new_alleles]
                        changed = True
                    else:
                        break
                else:
                    alleles = new_alleles
                    changed = True

    while (len({a[0] for a in alleles if len(a) > 0}) == 1 and
           all(len(a) >= 2 for a in alleles)):
        alleles = [a[1:] for a in alleles]
        pos += 1

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
