"""Input validation utilities."""

import re


class ValidationError(ValueError):
    """Raised when input validation fails."""

    pass


GCST_PATTERN = re.compile(r"^GCST\d{6,}$", re.IGNORECASE)

GENOME_BUILD_ALIASES = {
    "grch38": "GRCh38",
    "hg38": "GRCh38",
    "grch37": "GRCh37",
    "hg19": "GRCh37",
}


def validate_study_accession(
    value: str | None,
    required: bool = False,
) -> str | None:
    """Validate GWAS Catalog study accession format.

    Study accessions follow the format GCST followed by at least 6 digits.

    Args:
        value: Study accession string
        required: If True, raises error when value is None

    Returns:
        Normalized study accession (uppercase) or None

    Raises:
        ValidationError: If format is invalid
    """
    if value is None:
        if required:
            raise ValidationError("study_accession is required")
        return None

    value = value.strip()
    if not value:
        raise ValidationError(
            "study_accession cannot be empty. Expected format: GCST followed by 6+ digits (e.g., GCST90012345)"
        )

    if not GCST_PATTERN.match(value):
        raise ValidationError(
            f"Invalid study_accession format: '{value}'. "
            f"Expected GCST followed by 6+ digits (e.g., GCST90012345)"
        )

    return value.upper()


def validate_genome_build(
    value: str | None,
    default: str = "GRCh38",
) -> str:
    """Validate and normalize genome build.

    Accepts common aliases: GRCh38/hg38, GRCh37/hg19

    Args:
        value: Genome build string
        default: Default value if None or empty

    Returns:
        Normalized genome build (GRCh38 or GRCh37)

    Raises:
        ValidationError: If build is not recognized
    """
    if value is None or value.strip() == "":
        return default

    normalized = value.strip().lower()

    if normalized in GENOME_BUILD_ALIASES:
        return GENOME_BUILD_ALIASES[normalized]

    valid_values = list(GENOME_BUILD_ALIASES.keys())
    raise ValidationError(
        f"Invalid genome build: '{value}'. " f"Valid values: {', '.join(valid_values)}"
    )
