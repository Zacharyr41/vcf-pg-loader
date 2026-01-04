"""QC metrics computation for PRS-optimized variant loading."""

from .variant_qc import (
    compute_allele_frequencies,
    compute_genotype_counts,
    compute_hwe_pvalue,
)

__all__ = [
    "compute_genotype_counts",
    "compute_allele_frequencies",
    "compute_hwe_pvalue",
]
