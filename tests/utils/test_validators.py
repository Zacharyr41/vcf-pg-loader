"""Tests for input validation utilities."""

import pytest

from vcf_pg_loader.utils.validators import (
    ValidationError,
    validate_genome_build,
    validate_study_accession,
)


class TestValidateStudyAccession:
    """Tests for GWAS Catalog study accession validation."""

    def test_valid_gcst_format(self):
        assert validate_study_accession("GCST90012345") == "GCST90012345"
        assert validate_study_accession("GCST000001") == "GCST000001"
        assert validate_study_accession("GCST12345678") == "GCST12345678"

    def test_lowercase_is_normalized(self):
        assert validate_study_accession("gcst90012345") == "GCST90012345"

    def test_none_allowed_if_optional(self):
        assert validate_study_accession(None, required=False) is None

    def test_none_raises_if_required(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_study_accession(None, required=True)
        assert "study_accession is required" in str(exc_info.value)

    def test_invalid_format_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_study_accession("INVALID")
        assert "GCST" in str(exc_info.value)

    def test_empty_string_raises(self):
        with pytest.raises(ValidationError):
            validate_study_accession("")


class TestValidateGenomeBuild:
    """Tests for genome build validation."""

    def test_valid_grch38_formats(self):
        assert validate_genome_build("GRCh38") == "GRCh38"
        assert validate_genome_build("grch38") == "GRCh38"
        assert validate_genome_build("hg38") == "GRCh38"
        assert validate_genome_build("HG38") == "GRCh38"

    def test_valid_grch37_formats(self):
        assert validate_genome_build("GRCh37") == "GRCh37"
        assert validate_genome_build("grch37") == "GRCh37"
        assert validate_genome_build("hg19") == "GRCh37"
        assert validate_genome_build("HG19") == "GRCh37"

    def test_none_uses_default(self):
        assert validate_genome_build(None) == "GRCh38"

    def test_custom_default(self):
        assert validate_genome_build(None, default="GRCh37") == "GRCh37"

    def test_invalid_build_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_genome_build("hg16")
        assert "genome build" in str(exc_info.value).lower()

    def test_empty_string_uses_default(self):
        assert validate_genome_build("") == "GRCh38"
