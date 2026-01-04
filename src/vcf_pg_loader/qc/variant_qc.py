"""Variant QC metric computation for PRS-optimized loading.

Computes population genetics metrics at load time:
- Genotype counts (n_het, n_hom_ref, n_hom_alt)
- Allele frequencies (AAF, MAF, MAC)
- Hardy-Weinberg equilibrium p-value

Reference for HWE exact test:
Wigginton JE, Cutler DJ, Abecasis GR. A note on exact tests of
Hardy-Weinberg equilibrium. Am J Hum Genet. 2005 May;76(5):887-93.
DOI: 10.1086/429864
"""


def compute_genotype_counts(genotypes: list[str]) -> tuple[int, int, int, int]:
    """Compute genotype counts from a list of genotype strings.

    Args:
        genotypes: List of genotype strings (e.g., "0/0", "0|1", "1/1", "./.")

    Returns:
        Tuple of (n_called, n_het, n_hom_ref, n_hom_alt)
    """
    n_het = 0
    n_hom_ref = 0
    n_hom_alt = 0

    for gt in genotypes:
        if gt in (".", "./.", ".|."):
            continue

        if "|" in gt:
            sep = "|"
        elif "/" in gt:
            sep = "/"
        else:
            allele = _parse_allele(gt)
            if allele is None:
                continue
            if allele == 0:
                n_hom_ref += 1
            else:
                n_hom_alt += 1
            continue

        parts = gt.split(sep)
        if len(parts) != 2:
            continue

        a1 = _parse_allele(parts[0])
        a2 = _parse_allele(parts[1])

        if a1 is None or a2 is None:
            continue

        if a1 == a2:
            if a1 == 0:
                n_hom_ref += 1
            else:
                n_hom_alt += 1
        else:
            n_het += 1

    n_called = n_het + n_hom_ref + n_hom_alt
    return n_called, n_het, n_hom_ref, n_hom_alt


def _parse_allele(allele_str: str) -> int | None:
    """Parse allele string to integer, returning None for missing."""
    if allele_str == ".":
        return None
    try:
        return int(allele_str)
    except ValueError:
        return None


def compute_allele_frequencies(
    n_het: int, n_hom_ref: int, n_hom_alt: int
) -> tuple[float, float, int]:
    """Compute allele frequencies from genotype counts.

    Args:
        n_het: Number of heterozygous samples
        n_hom_ref: Number of homozygous reference samples
        n_hom_alt: Number of homozygous alt samples

    Returns:
        Tuple of (aaf, maf, mac) where:
        - aaf: Alternate allele frequency = (2*n_hom_alt + n_het) / (2*n_called)
        - maf: Minor allele frequency = min(aaf, 1 - aaf)
        - mac: Minor allele count = min(AC, AN - AC)
    """
    n_called = n_het + n_hom_ref + n_hom_alt

    if n_called == 0:
        return float("nan"), float("nan"), 0

    an = 2 * n_called
    ac_alt = 2 * n_hom_alt + n_het
    ac_ref = 2 * n_hom_ref + n_het

    aaf = ac_alt / an
    maf = min(aaf, 1 - aaf)
    mac = min(ac_alt, ac_ref)

    return aaf, maf, mac


def compute_hwe_pvalue(n_het: int, n_hom_ref: int, n_hom_alt: int) -> float:
    """Compute Hardy-Weinberg equilibrium p-value using exact test.

    Implements the exact test from Wigginton et al. (2005):
    "A note on exact tests of Hardy-Weinberg equilibrium"
    Am J Hum Genet. 2005 May;76(5):887-93.

    Args:
        n_het: Number of heterozygous samples (observed)
        n_hom_ref: Number of homozygous reference samples
        n_hom_alt: Number of homozygous alt samples

    Returns:
        Two-sided p-value for HWE exact test
    """
    n_called = n_het + n_hom_ref + n_hom_alt

    if n_called == 0:
        return float("nan")

    n_aa = n_hom_ref
    n_ab = n_het
    n_bb = n_hom_alt

    n = n_called
    n_a = 2 * n_aa + n_ab
    n_b = 2 * n_bb + n_ab

    if n_a == 0 or n_b == 0:
        return 1.0

    if n_ab > min(n_a, n_b):
        return float("nan")

    het_probs = _compute_het_probs(n, n_a, n_b)

    if not het_probs:
        return 1.0

    if n_ab >= len(het_probs):
        return 1.0

    p_obs = het_probs[n_ab]

    p_value = 0.0
    for p in het_probs:
        if p <= p_obs + 1e-10:
            p_value += p

    return min(1.0, p_value)


def _compute_het_probs(n: int, n_a: int, n_b: int) -> list[float]:
    """Compute probability distribution for heterozygote counts under HWE.

    Uses the recursive formula from Wigginton et al. (2005).

    Args:
        n: Total number of samples
        n_a: Number of A alleles
        n_b: Number of B alleles

    Returns:
        List of probabilities for each possible heterozygote count
    """
    if n_a + n_b != 2 * n:
        return []

    min_het = abs(n_a - n_b) % 2
    max_het = min(n_a, n_b)

    if max_het < min_het:
        return []

    n_het_values = (max_het - min_het) // 2 + 1
    if n_het_values <= 0:
        return []

    het_probs = [0.0] * (max_het + 1)

    mid = (min_het + max_het) // 2
    if mid % 2 != min_het % 2:
        mid += 1 if mid < max_het else -1

    if mid > max_het or mid < min_het:
        mid = min_het

    het_probs[mid] = 1.0
    total = 1.0

    curr_het = mid
    while curr_het > min_het:
        prev_het = curr_het - 2
        if prev_het < 0:
            break

        n_aa = (n_a - curr_het) // 2
        n_bb = (n_b - curr_het) // 2

        if n_aa <= 0 or n_bb <= 0:
            break

        het_probs[prev_het] = het_probs[curr_het] * curr_het * (curr_het - 1) / (4.0 * n_aa * n_bb)
        total += het_probs[prev_het]
        curr_het = prev_het

    curr_het = mid
    while curr_het < max_het:
        next_het = curr_het + 2
        if next_het > max_het:
            break

        n_aa = (n_a - curr_het) // 2
        n_bb = (n_b - curr_het) // 2

        if n_aa < 0 or n_bb < 0:
            break

        het_probs[next_het] = (
            het_probs[curr_het] * 4.0 * n_aa * n_bb / ((next_het) * (next_het - 1))
        )
        total += het_probs[next_het]
        curr_het = next_het

    if total > 0:
        for i in range(len(het_probs)):
            het_probs[i] /= total

    return het_probs
