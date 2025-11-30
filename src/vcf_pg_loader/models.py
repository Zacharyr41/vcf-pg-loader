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
        return f'[{self.pos},{end})'
