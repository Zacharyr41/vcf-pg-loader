"""Data models for VCF variants."""

from dataclasses import dataclass


@dataclass
class VariantRecord:
    """Represents a single variant record."""

    chrom: str
    pos: int
    ref: str
    alt: str
    qual: float | None
    filter: list[str]
    rs_id: str | None
    info: dict

    # Genomic position info
    end_pos: int | None = None

    # Extracted annotations
    gene: str | None = None
    transcript: str | None = None
    consequence: str | None = None
    impact: str | None = None
    hgvs_c: str | None = None
    hgvs_p: str | None = None

    # Population frequencies
    af_gnomad: float | None = None
    af_gnomad_popmax: float | None = None
    af_1kg: float | None = None

    # Pathogenicity scores
    cadd_phred: float | None = None
    clinvar_sig: str | None = None
    clinvar_review: str | None = None

    # Classification flags
    is_coding: bool = False
    is_lof: bool = False

    # Normalization tracking
    normalized: bool = False
    original_pos: int | None = None
    original_ref: str | None = None
    original_alt: str | None = None

    # Sample identification (for multi-sample VCFs)
    sample_id: str | None = None

    # PRS QC metrics (computed at load time)
    call_rate: float | None = None
    n_het: int | None = None
    n_hom_ref: int | None = None
    n_hom_alt: int | None = None
    aaf: float | None = None
    maf: float | None = None
    mac: int | None = None
    hwe_p: float | None = None

    # Imputation quality metrics
    info_score: float | None = None
    imputation_r2: float | None = None
    is_imputed: bool = False
    is_typed: bool = False
    imputation_source: str | None = None

    @property
    def variant_type(self) -> str:
        """Classify variant type based on REF and ALT alleles."""
        if len(self.ref) == 1 and len(self.alt) == 1:
            return "snp"
        elif len(self.ref) != len(self.alt):
            return "indel"
        else:
            return "mnp"  # Multi-nucleotide polymorphism

    @property
    def pos_range(self) -> str:
        """Return PostgreSQL int8range representation."""
        end = self.end_pos or (self.pos + len(self.ref))
        return f"[{self.pos},{end})"
