"""Tests for variant matching utilities."""


class TestNormalizeChromosome:
    """Tests for consistent chromosome normalization."""

    def test_strips_chr_prefix(self):
        from vcf_pg_loader.utils.variant_matching import normalize_chromosome

        assert normalize_chromosome("chr1") == "1"
        assert normalize_chromosome("chrX") == "X"
        assert normalize_chromosome("chrY") == "Y"
        assert normalize_chromosome("chrM") == "M"

    def test_preserves_bare_chromosome(self):
        from vcf_pg_loader.utils.variant_matching import normalize_chromosome

        assert normalize_chromosome("1") == "1"
        assert normalize_chromosome("22") == "22"
        assert normalize_chromosome("X") == "X"

    def test_add_chr_prefix_mode(self):
        from vcf_pg_loader.utils.variant_matching import normalize_chromosome

        assert normalize_chromosome("1", add_chr=True) == "chr1"
        assert normalize_chromosome("chr1", add_chr=True) == "chr1"
        assert normalize_chromosome("X", add_chr=True) == "chrX"


class TestMatchVariant:
    """Tests for variant matching logic."""

    def test_match_by_position_and_alleles(self):
        from vcf_pg_loader.utils.variant_matching import match_variant

        variant_lookup = {
            ("1", 100, "A", "G"): 1,
            ("1", 200, "C", "T"): 2,
        }
        rsid_lookup = {}

        result = match_variant(
            chromosome="1",
            position=100,
            effect_allele="G",
            other_allele="A",
            rsid=None,
            variant_lookup=variant_lookup,
            rsid_lookup=rsid_lookup,
        )

        assert result == 1

    def test_match_by_rsid_fallback(self):
        from vcf_pg_loader.utils.variant_matching import match_variant

        variant_lookup = {}
        rsid_lookup = {"rs100": 1, "rs200": 2}

        result = match_variant(
            chromosome="1",
            position=100,
            effect_allele="G",
            other_allele="A",
            rsid="rs100",
            variant_lookup=variant_lookup,
            rsid_lookup=rsid_lookup,
        )

        assert result == 1

    def test_no_match_returns_none(self):
        from vcf_pg_loader.utils.variant_matching import match_variant

        result = match_variant(
            chromosome="1",
            position=100,
            effect_allele="G",
            other_allele="A",
            rsid=None,
            variant_lookup={},
            rsid_lookup={},
        )

        assert result is None

    def test_handles_chr_prefix_normalization(self):
        from vcf_pg_loader.utils.variant_matching import match_variant

        variant_lookup = {
            ("1", 100, "A", "G"): 1,
        }

        result = match_variant(
            chromosome="chr1",
            position=100,
            effect_allele="G",
            other_allele="A",
            rsid=None,
            variant_lookup=variant_lookup,
            rsid_lookup={},
        )

        assert result == 1

    def test_allele_swap_matching(self):
        from vcf_pg_loader.utils.variant_matching import match_variant

        variant_lookup = {
            ("1", 100, "A", "G"): 1,
        }

        result = match_variant(
            chromosome="1",
            position=100,
            effect_allele="A",
            other_allele="G",
            rsid=None,
            variant_lookup=variant_lookup,
            rsid_lookup={},
        )

        assert result == 1
