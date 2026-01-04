"""Data models for PGS Catalog PRS weights."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class PGSMetadata:
    """Represents PGS Catalog score metadata from file header."""

    pgs_id: str
    genome_build: str
    trait_name: str | None = None
    trait_ontology_id: str | None = None
    publication_pmid: str | None = None
    weight_type: str | None = None
    n_variants: int | None = None
    reporting_ancestry: str | None = None
    created_at: datetime | None = None


@dataclass
class PRSWeight:
    """Represents a single PRS weight for a variant."""

    effect_allele: str
    effect_weight: float
    chromosome: str | None = None
    position: int | None = None
    rsid: str | None = None
    other_allele: str | None = None
    is_interaction: bool = False
    is_haplotype: bool = False
    is_dominant: bool = False
    is_recessive: bool = False
    allele_frequency: float | None = None
    locus_name: str | None = None
    variant_id: int | None = None
    pgs_id: str | None = None


@dataclass
class HarmonizationResult:
    """Result of allele harmonization."""

    is_match: bool = True
    is_flipped: bool = False
    is_effect_allele_alt: bool | None = None
    harmonized_effect_allele: str | None = None
    harmonized_other_allele: str | None = None
