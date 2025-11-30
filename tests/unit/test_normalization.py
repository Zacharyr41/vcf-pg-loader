"""Tests for variant normalization per vt algorithm."""

import pytest
from vcf_pg_loader.normalizer import (
    normalize_variant,
    is_normalized,
    classify_variant,
    decompose_multiallelic,
)


class TestVTNormalization:
    """Test vt-style left-alignment and parsimony."""

    @pytest.mark.parametrize(
        "pos,ref,alt,exp_pos,exp_ref,exp_alt",
        [
            (10, "A", "G", 10, "A", "G"),
            (10, "GATC", "GTTC", 11, "A", "T"),
            (10, "ATCG", "TTCG", 10, "A", "T"),
            (10, "ACGT", "ACAT", 10, "CG", "CA"),
        ],
    )
    def test_normalization_cases(self, pos, ref, alt, exp_pos, exp_ref, exp_alt):
        """Standard normalization test cases."""
        result_pos, result_ref, result_alts = normalize_variant(
            "chr1", pos, ref, [alt]
        )

        assert result_pos == exp_pos
        assert result_ref == exp_ref
        assert result_alts == [exp_alt]

    def test_snp_unchanged(self):
        """SNPs should remain unchanged."""
        pos, ref, alts = normalize_variant("chr1", 100, "A", ["G"])
        assert pos == 100
        assert ref == "A"
        assert alts == ["G"]

    def test_empty_input(self):
        """Empty REF or ALT returns unchanged."""
        pos, ref, alts = normalize_variant("chr1", 100, "", ["G"])
        assert pos == 100
        assert ref == ""
        assert alts == ["G"]

        pos, ref, alts = normalize_variant("chr1", 100, "A", [])
        assert pos == 100
        assert ref == "A"
        assert alts == []


class TestIsNormalized:
    """Test quick normalization check."""

    def test_normalized_snp(self):
        """SNPs are normalized."""
        assert is_normalized("A", ["G"]) is True
        assert is_normalized("C", ["T"]) is True

    def test_normalized_different_endings(self):
        """Variants with different endings are normalized."""
        assert is_normalized("AT", ["GT"]) is True
        assert is_normalized("AC", ["TG"]) is True

    def test_not_normalized_same_ending(self):
        """Variants with same ending need normalization."""
        assert is_normalized("ATG", ["AG"]) is False
        assert is_normalized("GATC", ["GAC"]) is False

    def test_empty_input(self):
        """Empty inputs are considered normalized."""
        assert is_normalized("", ["G"]) is True
        assert is_normalized("A", []) is True


class TestClassifyVariant:
    """Test variant type classification."""

    def test_snp(self):
        """Single nucleotide polymorphism detection."""
        assert classify_variant("A", "G") == "snp"
        assert classify_variant("C", "T") == "snp"

    def test_indel(self):
        """Insertion/deletion detection."""
        assert classify_variant("A", "AT") == "indel"
        assert classify_variant("AT", "A") == "indel"
        assert classify_variant("ATG", "A") == "indel"

    def test_mnp(self):
        """Multi-nucleotide polymorphism detection."""
        assert classify_variant("AT", "GC") == "mnp"
        assert classify_variant("ATG", "GCA") == "mnp"

    def test_sv(self):
        """Structural variant detection."""
        assert classify_variant("A", "<DEL>") == "sv"
        assert classify_variant("A", "<INS>") == "sv"
        assert classify_variant("A", "<DUP>") == "sv"


class TestDecomposeMultiallelic:
    """Test multi-allelic decomposition."""

    def test_biallelic_unchanged(self):
        """Biallelic sites return single record."""
        result = decompose_multiallelic("chr1", 100, "A", ["G"])
        assert result == [("chr1", 100, "A", "G")]

    def test_multiallelic_decomposed(self):
        """Multi-allelic sites decompose to biallelic."""
        result = decompose_multiallelic("chr1", 100, "A", ["G", "T", "C"])
        assert len(result) == 3
        assert result[0] == ("chr1", 100, "A", "G")
        assert result[1] == ("chr1", 100, "A", "T")
        assert result[2] == ("chr1", 100, "A", "C")

    def test_empty_alts_filtered(self):
        """Empty or None ALTs are filtered out."""
        result = decompose_multiallelic("chr1", 100, "A", ["G", None, "T"])
        assert len(result) == 2
        assert ("chr1", 100, "A", "G") in result
        assert ("chr1", 100, "A", "T") in result
