"""Tests for variant normalization."""

from vcf_pg_loader.normalizer import (
    classify_variant,
    decompose_multiallelic,
    is_normalized,
    normalize_variant,
)


class MockReferenceGenome:
    """Mock reference genome for testing normalization."""

    def __init__(self, sequence_map: dict):
        self.sequence_map = sequence_map

    def fetch(self, chrom: str, start: int, end: int) -> str:
        """Fetch reference sequence for a region (0-based coordinates)."""
        key = f"{chrom}:{start}-{end}"
        return self.sequence_map.get(key, "N")


class TestNormalizeVariant:
    def test_already_normalized_snp(self):
        """Test that normalized SNP is unchanged."""
        pos, ref, alts = normalize_variant("chr1", 100, "A", ["G"])
        assert pos == 100
        assert ref == "A"
        assert alts == ["G"]

    def test_right_trim_deletion(self):
        """Test right-trimming of identical trailing bases."""
        pos, ref, alts = normalize_variant("chr1", 100, "ATG", ["AG"])
        assert pos == 100
        assert ref == "AT"
        assert alts == ["A"]

    def test_right_trim_insertion(self):
        """Test right-trimming of identical trailing bases in insertion."""
        pos, ref, alts = normalize_variant("chr1", 100, "AG", ["ATG"])
        assert pos == 100
        assert ref == "A"
        assert alts == ["AT"]

    def test_left_trim_parsimony(self):
        """Test left-trimming for parsimony when all alleles have length >= 2."""
        pos, ref, alts = normalize_variant("chr1", 100, "TAC", ["TGC"])
        assert pos == 101
        assert ref == "A"
        assert alts == ["G"]

    def test_no_left_trim_when_min_length_is_one(self):
        """Test that left-trimming stops when any allele has length 1."""
        pos, ref, alts = normalize_variant("chr1", 100, "TA", ["T"])
        assert pos == 100
        assert ref == "TA"
        assert alts == ["T"]

    def test_multiallelic_normalization(self):
        """Test normalization of multi-allelic variants."""
        pos, ref, alts = normalize_variant("chr1", 100, "ATG", ["AG", "ATCG"])
        assert pos == 100
        assert ref == "AT"
        assert alts == ["A", "ATC"]

    def test_complex_indel_with_reference(self):
        """Test complex indel normalization with reference genome.

        ATCGATCG -> ATCG:
        - Right-trim G: ATCGATC -> ATC
        - Right-trim C: ATCGAT -> AT
        - Right-trim T: ATCGA -> A
        - Right-trim A: ATCG -> '' (empty), left-extend with G at pos 99
        - Now GATCG -> G, both end in G, right-trim: GATC -> '' (empty)
        - Left-extend with G at pos 98: GGATC -> G
        - Now end differently (C vs G), done with phase 1
        - Left-trim: both start with G, trim -> pos=99, GATC -> '' ... but alt is empty!
        - Actually alt length is 1, so no left-trim
        Final: pos=98, ref=GGATC, alt=G
        """
        ref_genome = MockReferenceGenome({"chr1:98-99": "G", "chr1:97-98": "G"})
        pos, ref, alts = normalize_variant("chr1", 100, "ATCGATCG", ["ATCG"], ref_genome)
        assert pos == 98
        assert ref == "GGATC"
        assert alts == ["G"]

    def test_complex_indel_no_trimming(self):
        """Test complex indel that cannot be trimmed."""
        pos, ref, alts = normalize_variant("chr1", 100, "ATCGATCG", ["A"])
        assert pos == 100
        assert ref == "ATCGATCG"
        assert alts == ["A"]

    def test_case_insensitivity(self):
        """Test that normalization is case-insensitive."""
        pos, ref, alts = normalize_variant("chr1", 100, "atg", ["Ag"])
        assert pos == 100
        assert ref == "AT"
        assert alts == ["A"]

    def test_with_reference_genome(self):
        """Test normalization with reference genome for left-extension."""
        ref_genome = MockReferenceGenome({"chr1:99-100": "G"})
        pos, ref, alts = normalize_variant("chr1", 101, "AT", ["A"], ref_genome)
        assert pos == 101
        assert ref == "AT"
        assert alts == ["A"]

    def test_deletion_with_reference(self):
        """Test deletion normalization with reference genome.

        AA -> A right-trims to A -> '' (empty), requiring left-extension.
        With reference providing 'G' at position 99, we get pos=99, ref=GA, alt=G.
        """
        ref_genome = MockReferenceGenome({"chr1:98-99": "G"})
        pos, ref, alts = normalize_variant("chr1", 100, "AA", ["A"], ref_genome)
        assert pos == 99
        assert ref == "GA"
        assert alts == ["G"]


class TestIsNormalized:
    def test_normalized_snp(self):
        """Test that SNPs are recognized as normalized."""
        assert is_normalized("A", ["G"]) is True
        assert is_normalized("C", ["T"]) is True

    def test_normalized_indel(self):
        """Test that properly normalized indels are recognized."""
        assert is_normalized("AT", ["A"]) is True
        assert is_normalized("A", ["AT"]) is True

    def test_not_normalized_same_trailing_base(self):
        """Test detection of non-normalized variants with same trailing base."""
        assert is_normalized("ATG", ["AG"]) is False
        assert is_normalized("ATCG", ["AG"]) is False

    def test_not_normalized_same_leading_base(self):
        """Test detection of non-normalized variants with same leading base."""
        assert is_normalized("TAC", ["TGC"]) is False

    def test_empty_alleles(self):
        """Test handling of empty alleles."""
        assert is_normalized("", []) is True
        assert is_normalized("A", []) is True


class TestClassifyVariant:
    def test_snp_classification(self):
        """Test SNP classification."""
        assert classify_variant("A", "G") == "snp"
        assert classify_variant("C", "T") == "snp"
        assert classify_variant("G", "A") == "snp"

    def test_deletion_classification(self):
        """Test deletion classification as indel."""
        assert classify_variant("ATG", "A") == "indel"
        assert classify_variant("ATCGATCG", "A") == "indel"

    def test_insertion_classification(self):
        """Test insertion classification as indel."""
        assert classify_variant("A", "ATG") == "indel"
        assert classify_variant("G", "GCTA") == "indel"

    def test_mnp_classification(self):
        """Test MNP (multi-nucleotide polymorphism) classification."""
        assert classify_variant("AT", "GC") == "mnp"
        assert classify_variant("ATG", "GCA") == "mnp"

    def test_sv_classification(self):
        """Test structural variant classification."""
        assert classify_variant("A", "<DEL>") == "sv"
        assert classify_variant("A", "<INS>") == "sv"
        assert classify_variant("A", "<DUP>") == "sv"
        assert classify_variant("A", "<INV>") == "sv"


class TestDecomposeMultiallelic:
    def test_biallelic_unchanged(self):
        """Test that biallelic sites produce single record."""
        records = decompose_multiallelic("chr1", 100, "A", ["G"])
        assert len(records) == 1
        assert records[0] == ("chr1", 100, "A", "G")

    def test_multiallelic_decomposition(self):
        """Test decomposition of multi-allelic sites."""
        records = decompose_multiallelic("chr1", 100, "A", ["G", "T"])
        assert len(records) == 2
        assert records[0] == ("chr1", 100, "A", "G")
        assert records[1] == ("chr1", 100, "A", "T")

    def test_three_alts_decomposition(self):
        """Test decomposition with three ALT alleles."""
        records = decompose_multiallelic("chr1", 100, "A", ["G", "T", "C"])
        assert len(records) == 3
        assert records[0] == ("chr1", 100, "A", "G")
        assert records[1] == ("chr1", 100, "A", "T")
        assert records[2] == ("chr1", 100, "A", "C")

    def test_empty_alts_filtered(self):
        """Test that empty ALT alleles are filtered out."""
        records = decompose_multiallelic("chr1", 100, "A", ["G", "", "T"])
        assert len(records) == 2
        assert records[0] == ("chr1", 100, "A", "G")
        assert records[1] == ("chr1", 100, "A", "T")

    def test_preserves_chromosome_and_position(self):
        """Test that chromosome and position are preserved in decomposition."""
        records = decompose_multiallelic("chrX", 12345, "ATG", ["A", "ATGC"])
        assert all(r[0] == "chrX" for r in records)
        assert all(r[1] == 12345 for r in records)
        assert all(r[2] == "ATG" for r in records)
