"""Tests for genotype classification per VCF 4.3 spec and cyvcf2 behavior.

Reference: VCF 4.3 specification defines GT field as allele indices
separated by / (unphased) or | (phased), where 0 = REF, 1 = first ALT, etc.

cyvcf2 gt_types mapping (default):
- 0 = HOM_REF
- 1 = HET
- 2 = UNKNOWN
- 3 = HOM_ALT
"""

import tempfile
from pathlib import Path

import pytest


class TestGenotypeClassificationSpec:
    """Test genotype classification according to VCF 4.3 spec."""

    @pytest.mark.parametrize(
        "gt,expected_category",
        [
            ("0/0", "HOM_REF"),
            ("0|0", "HOM_REF"),
        ],
    )
    def test_homozygous_reference(self, gt, expected_category):
        """0/0 and 0|0 are classified as HOM_REF."""
        assert _classify_gt(gt) == expected_category

    @pytest.mark.parametrize(
        "gt,expected_category",
        [
            ("0/1", "HET"),
            ("1/0", "HET"),
            ("0|1", "HET"),
            ("1|0", "HET"),
            ("0/2", "HET"),
            ("0|2", "HET"),
        ],
    )
    def test_heterozygous(self, gt, expected_category):
        """Heterozygous genotypes with one REF allele."""
        assert _classify_gt(gt) == expected_category

    @pytest.mark.parametrize(
        "gt,expected_category",
        [
            ("1/2", "HET"),
            ("2/1", "HET"),
            ("1|2", "HET"),
            ("2/3", "HET"),
        ],
    )
    def test_multiallelic_heterozygous(self, gt, expected_category):
        """Multi-allelic heterozygotes (1/2, 2/3) are HET, not HOM_ALT."""
        assert _classify_gt(gt) == expected_category

    @pytest.mark.parametrize(
        "gt,expected_category",
        [
            ("1/1", "HOM_ALT"),
            ("1|1", "HOM_ALT"),
            ("2/2", "HOM_ALT"),
            ("2|2", "HOM_ALT"),
            ("3/3", "HOM_ALT"),
        ],
    )
    def test_homozygous_alt(self, gt, expected_category):
        """Homozygous ALT includes 1/1, 2/2, 3/3 (same non-ref allele)."""
        assert _classify_gt(gt) == expected_category

    @pytest.mark.parametrize(
        "gt,expected_category",
        [
            ("./.", "UNKNOWN"),
            (".|.", "UNKNOWN"),
            (".", "UNKNOWN"),
        ],
    )
    def test_missing_genotype(self, gt, expected_category):
        """Missing genotypes are UNKNOWN."""
        assert _classify_gt(gt) == expected_category


class TestPhasingIndependence:
    """Test that phasing (| vs /) does not affect classification."""

    @pytest.mark.parametrize(
        "unphased,phased",
        [
            ("0/0", "0|0"),
            ("0/1", "0|1"),
            ("1/1", "1|1"),
            ("1/2", "1|2"),
            ("./.", ".|."),
        ],
    )
    def test_phasing_does_not_affect_classification(self, unphased, phased):
        """Phased and unphased versions classify identically."""
        assert _classify_gt(unphased) == _classify_gt(phased)


class TestHaploidGenotypes:
    """Test haploid genotype handling (chrY, chrM, male chrX non-PAR)."""

    def test_haploid_ref(self):
        """Haploid 0 is HOM_REF."""
        assert _classify_gt("0") == "HOM_REF"

    def test_haploid_alt(self):
        """Haploid 1 is HOM_ALT."""
        assert _classify_gt("1") == "HOM_ALT"

    def test_haploid_second_alt(self):
        """Haploid 2 (second ALT) is HOM_ALT."""
        assert _classify_gt("2") == "HOM_ALT"

    def test_haploid_missing(self):
        """Haploid missing (.) is UNKNOWN."""
        assert _classify_gt(".") == "UNKNOWN"


class TestPartialMissingGenotypes:
    """Test partially missing genotypes like ./1 and 0/."""

    @pytest.mark.parametrize(
        "gt",
        [
            "./1",
            "1/.",
            "./0",
            "0/.",
            ".|1",
            ".|0",
        ],
    )
    def test_partial_missing_handled(self, gt):
        """Partial missing genotypes return a valid category."""
        result = _classify_gt(gt)
        assert result in ("HET", "UNKNOWN", "HOM_REF", "HOM_ALT")


class TestCyvcf2GtTypesMapping:
    """Test cyvcf2's gt_types integer encoding."""

    def test_cyvcf2_encoding_values(self):
        """Verify cyvcf2 gt_types encoding (default, not gts012 mode)."""
        from cyvcf2 import VCF

        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHOM_REF\tHET\tHOM_ALT\tUNKNOWN
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT\t0/0\t0/1\t1/1\t./.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            vcf = VCF(str(vcf_path))
            variant = next(vcf)

            gt_types = variant.gt_types

            assert gt_types[0] == 0  # HOM_REF
            assert gt_types[1] == 1  # HET
            assert gt_types[2] == 3  # HOM_ALT
            assert gt_types[3] == 2  # UNKNOWN

            vcf.close()
        finally:
            vcf_path.unlink()

    def test_cyvcf2_multiallelic_het(self):
        """Verify 1/2 is classified as HET by cyvcf2."""
        from cyvcf2 import VCF

        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t100\t.\tA\tG,T\t30\tPASS\t.\tGT\t1/2
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            vcf = VCF(str(vcf_path))
            variant = next(vcf)

            assert variant.gt_types[0] == 1  # HET
            vcf.close()
        finally:
            vcf_path.unlink()


class TestStarAlleleGenotype:
    """Test star allele (*) genotype classification."""

    def test_het_with_star_allele(self):
        """0/* should be classified as HET."""
        from cyvcf2 import VCF

        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t100\t.\tA\tG,*\t30\tPASS\t.\tGT\t0/2
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            vcf = VCF(str(vcf_path))
            variant = next(vcf)

            assert variant.gt_types[0] == 1  # HET
            vcf.close()
        finally:
            vcf_path.unlink()


def _classify_gt(gt: str) -> str:
    """Classify genotype string to category.

    This mirrors the classification logic that would be used when
    loading genotypes into the database.
    """
    if gt in (".", "./."):
        return "UNKNOWN"
    if gt in (".|.",):
        return "UNKNOWN"

    sep = "|" if "|" in gt else "/"

    if sep not in gt:
        if gt == ".":
            return "UNKNOWN"
        try:
            allele = int(gt)
            return "HOM_REF" if allele == 0 else "HOM_ALT"
        except ValueError:
            return "UNKNOWN"

    parts = gt.split(sep)

    if len(parts) != 2:
        return "UNKNOWN"

    a1, a2 = parts

    if "." in (a1, a2):
        if a1 == "." and a2 == ".":
            return "UNKNOWN"
        return "HET"

    try:
        allele1 = int(a1)
        allele2 = int(a2)
    except ValueError:
        return "UNKNOWN"

    if allele1 == allele2:
        return "HOM_REF" if allele1 == 0 else "HOM_ALT"

    return "HET"
