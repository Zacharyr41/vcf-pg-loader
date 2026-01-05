"""Data models for GWAS summary statistics."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class StudyRecord:
    """Represents a GWAS study record."""

    study_accession: str
    trait_name: str | None = None
    trait_ontology_id: str | None = None
    publication_pmid: str | None = None
    sample_size: int | None = None
    n_cases: int | None = None
    n_controls: int | None = None
    genome_build: str = "GRCh38"
    analysis_software: str | None = None
    study_id: int | None = None
    created_at: datetime | None = None


@dataclass
class GWASSummaryStatRecord:
    """Represents a single GWAS summary statistic record."""

    effect_allele: str
    other_allele: str | None
    p_value: float
    variant_id: int | None = None
    study_id: int | None = None
    beta: float | None = None
    odds_ratio: float | None = None
    standard_error: float | None = None
    effect_allele_frequency: float | None = None
    n_total: int | None = None
    n_cases: int | None = None
    info_score: float | None = None
    is_effect_allele_alt: bool | None = None

    chromosome: str | None = None
    position: int | None = None
    rsid: str | None = None


@dataclass
class HarmonizationResult:
    """Result of allele harmonization."""

    is_match: bool
    is_flipped: bool = False
    is_effect_allele_alt: bool | None = None
    harmonized_effect_allele: str | None = None
    harmonized_other_allele: str | None = None
