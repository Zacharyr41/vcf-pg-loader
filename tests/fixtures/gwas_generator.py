"""GWAS-SSF format fixture generator for testing."""

import tempfile
from dataclasses import dataclass
from pathlib import Path

GWAS_SSF_REQUIRED_COLUMNS = [
    "chromosome",
    "base_pair_location",
    "effect_allele",
    "other_allele",
    "p_value",
]

GWAS_SSF_OPTIONAL_COLUMNS = [
    "variant_id",
    "rsid",
    "beta",
    "odds_ratio",
    "standard_error",
    "effect_allele_frequency",
    "n",
    "n_cases",
    "info",
]


@dataclass
class GWASSummaryStatistic:
    """Represents a single GWAS summary statistic for testing."""

    chromosome: str
    base_pair_location: int
    effect_allele: str
    other_allele: str
    p_value: float
    variant_id: str | None = None
    rsid: str | None = None
    beta: float | None = None
    odds_ratio: float | None = None
    standard_error: float | None = None
    effect_allele_frequency: float | None = None
    n: int | None = None
    n_cases: int | None = None
    info: float | None = None


@dataclass
class GWASStudyMetadata:
    """Metadata for a GWAS study."""

    study_accession: str = "GCST90002357"
    trait_name: str = "Height"
    trait_ontology_id: str = "EFO_0004339"
    publication_pmid: str = "25282103"
    sample_size: int = 253288
    n_cases: int | None = None
    n_controls: int | None = None
    genome_build: str = "GRCh38"
    analysis_software: str = "BOLT-LMM"


class GWASSSFGenerator:
    """Generate GWAS-SSF format TSV files for testing."""

    @classmethod
    def generate(
        cls,
        stats: list[GWASSummaryStatistic],
        include_optional: bool = True,
        columns: list[str] | None = None,
    ) -> str:
        """Generate GWAS-SSF TSV content.

        Args:
            stats: List of summary statistics to include.
            include_optional: Whether to include optional columns with values.
            columns: Explicit column list (overrides auto-detection).

        Returns:
            TSV-formatted string.
        """
        if columns is None:
            columns = list(GWAS_SSF_REQUIRED_COLUMNS)
            if include_optional:
                columns.extend(GWAS_SSF_OPTIONAL_COLUMNS)

        lines = ["\t".join(columns)]

        for stat in stats:
            row = []
            for col in columns:
                value = getattr(stat, col, None)
                if value is None:
                    row.append("")
                else:
                    row.append(str(value))
            lines.append("\t".join(row))

        return "\n".join(lines) + "\n"

    @classmethod
    def generate_file(
        cls,
        stats: list[GWASSummaryStatistic],
        include_optional: bool = True,
        columns: list[str] | None = None,
        suffix: str = ".tsv",
    ) -> Path:
        """Generate a GWAS-SSF TSV file and return the path."""
        content = cls.generate(stats, include_optional, columns)
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
            f.write(content)
            return Path(f.name)


def make_basic_gwas_stats() -> list[GWASSummaryStatistic]:
    """Create basic GWAS statistics for testing."""
    return [
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=12345,
            effect_allele="A",
            other_allele="G",
            p_value=5e-8,
            rsid="rs12345",
            beta=0.05,
            standard_error=0.01,
            effect_allele_frequency=0.3,
            n=100000,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=23456,
            effect_allele="C",
            other_allele="T",
            p_value=1e-6,
            rsid="rs23456",
            beta=-0.03,
            standard_error=0.008,
            effect_allele_frequency=0.45,
            n=100000,
        ),
        GWASSummaryStatistic(
            chromosome="2",
            base_pair_location=34567,
            effect_allele="G",
            other_allele="A",
            p_value=0.001,
            rsid="rs34567",
            beta=0.02,
            standard_error=0.006,
            effect_allele_frequency=0.15,
            n=100000,
        ),
    ]


def make_strand_ambiguous_gwas_stats() -> list[GWASSummaryStatistic]:
    """Create GWAS statistics with strand-ambiguous SNPs (A/T and C/G).

    These require special handling for allele harmonization.
    """
    return [
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=100,
            effect_allele="A",
            other_allele="T",
            p_value=1e-10,
            rsid="rs100",
            beta=0.1,
            standard_error=0.015,
            effect_allele_frequency=0.4,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=200,
            effect_allele="T",
            other_allele="A",
            p_value=1e-8,
            rsid="rs200",
            beta=-0.08,
            standard_error=0.012,
            effect_allele_frequency=0.6,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=300,
            effect_allele="C",
            other_allele="G",
            p_value=1e-6,
            rsid="rs300",
            beta=0.05,
            standard_error=0.01,
            effect_allele_frequency=0.35,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=400,
            effect_allele="G",
            other_allele="C",
            p_value=1e-5,
            rsid="rs400",
            beta=-0.04,
            standard_error=0.009,
            effect_allele_frequency=0.65,
        ),
    ]


def make_effect_allele_orientation_stats() -> list[GWASSummaryStatistic]:
    """Create stats to test is_effect_allele_alt computation.

    Provides variants where effect_allele matches REF vs ALT in VCF context.
    """
    return [
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=1000,
            effect_allele="G",
            other_allele="A",
            p_value=1e-8,
            beta=0.1,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=2000,
            effect_allele="A",
            other_allele="G",
            p_value=1e-7,
            beta=-0.08,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=3000,
            effect_allele="T",
            other_allele="C",
            p_value=1e-6,
            beta=0.05,
        ),
    ]


def make_gwas_with_missing_optional_fields() -> list[GWASSummaryStatistic]:
    """Create stats with various missing optional fields."""
    return [
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=5000,
            effect_allele="A",
            other_allele="G",
            p_value=1e-8,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=6000,
            effect_allele="C",
            other_allele="T",
            p_value=1e-6,
            beta=0.05,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=7000,
            effect_allele="G",
            other_allele="A",
            p_value=0.001,
            rsid="rs7000",
            standard_error=0.01,
        ),
    ]


def make_gwas_for_variant_matching() -> list[GWASSummaryStatistic]:
    """Create stats that correspond to known VCF variants for matching tests.

    These coordinates match the fixtures in with_annotations.vcf.
    """
    return [
        GWASSummaryStatistic(
            chromosome="17",
            base_pair_location=7577121,
            effect_allele="A",
            other_allele="G",
            p_value=1e-20,
            rsid="rs28934576",
            beta=0.5,
            standard_error=0.05,
            effect_allele_frequency=0.001,
        ),
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=12345,
            effect_allele="G",
            other_allele="A",
            p_value=5e-8,
            beta=0.1,
            standard_error=0.02,
        ),
    ]


def make_binary_trait_gwas_stats() -> list[GWASSummaryStatistic]:
    """Create GWAS statistics for binary trait with odds ratios."""
    return [
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=8000,
            effect_allele="A",
            other_allele="G",
            p_value=1e-15,
            odds_ratio=1.25,
            standard_error=0.03,
            effect_allele_frequency=0.35,
            n=50000,
            n_cases=10000,
        ),
        GWASSummaryStatistic(
            chromosome="2",
            base_pair_location=9000,
            effect_allele="C",
            other_allele="T",
            p_value=1e-10,
            odds_ratio=0.85,
            standard_error=0.04,
            effect_allele_frequency=0.22,
            n=50000,
            n_cases=10000,
        ),
    ]


def make_malformed_gwas_tsv() -> str:
    """Create malformed TSV content for error handling tests."""
    return "chromosome\tbase_pair_location\teffect_allele\tother_allele\tp_value\n1\tnot_a_number\tA\tG\t1e-8\n"


def make_missing_required_columns_tsv() -> str:
    """Create TSV missing required columns."""
    return "chromosome\tbeta\np_value\n1\t0.05\t1e-8\n"


def make_gwas_ssf_file_basic() -> Path:
    """Generate a basic GWAS-SSF file for testing."""
    return GWASSSFGenerator.generate_file(make_basic_gwas_stats())


def make_gwas_ssf_file_strand_ambiguous() -> Path:
    """Generate a GWAS-SSF file with strand-ambiguous variants."""
    return GWASSSFGenerator.generate_file(make_strand_ambiguous_gwas_stats())


def make_gwas_ssf_file_for_variant_matching() -> Path:
    """Generate a GWAS-SSF file for variant matching tests."""
    return GWASSSFGenerator.generate_file(make_gwas_for_variant_matching())


def make_gwas_ssf_file_minimal() -> Path:
    """Generate a minimal GWAS-SSF file with only required columns."""
    stats = [
        GWASSummaryStatistic(
            chromosome="1",
            base_pair_location=100,
            effect_allele="A",
            other_allele="G",
            p_value=1e-8,
        ),
    ]
    return GWASSSFGenerator.generate_file(
        stats, include_optional=False, columns=GWAS_SSF_REQUIRED_COLUMNS
    )
