"""Tests for GWAS summary statistics import following GWAS-SSF standard."""

import tempfile
from pathlib import Path

import pytest

from fixtures.gwas_generator import (
    GWASSSFGenerator,
    GWASSummaryStatistic,
    make_basic_gwas_stats,
    make_binary_trait_gwas_stats,
    make_gwas_with_missing_optional_fields,
    make_malformed_gwas_tsv,
    make_missing_required_columns_tsv,
)


class TestGWASSSFParsing:
    """Tests for GWAS-SSF TSV format parsing."""

    def test_parse_required_columns(self):
        """Parser should detect all required GWAS-SSF columns."""
        from vcf_pg_loader.gwas.loader import GWASSSFParser

        stats = [
            GWASSummaryStatistic(
                chromosome="1",
                base_pair_location=100,
                effect_allele="A",
                other_allele="G",
                p_value=1e-8,
            )
        ]
        content = GWASSSFGenerator.generate(stats, include_optional=False)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            parser = GWASSSFParser(path)
            assert parser.has_required_columns()
            assert "chromosome" in parser.columns
            assert "base_pair_location" in parser.columns
            assert "effect_allele" in parser.columns
            assert "other_allele" in parser.columns
            assert "p_value" in parser.columns
        finally:
            path.unlink()

    def test_parse_optional_columns(self):
        """Parser should handle optional GWAS-SSF columns."""
        from vcf_pg_loader.gwas.loader import GWASSSFParser

        stats = make_basic_gwas_stats()
        content = GWASSSFGenerator.generate(stats, include_optional=True)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            parser = GWASSSFParser(path)
            assert "beta" in parser.columns
            assert "standard_error" in parser.columns
            assert "rsid" in parser.columns
            assert "effect_allele_frequency" in parser.columns
        finally:
            path.unlink()

    def test_reject_missing_required_columns(self):
        """Parser should reject files missing required columns."""
        from vcf_pg_loader.gwas.loader import GWASParseError, GWASSSFParser

        content = make_missing_required_columns_tsv()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            with pytest.raises(GWASParseError) as exc_info:
                GWASSSFParser(path)
            assert "missing required columns" in str(exc_info.value).lower()
        finally:
            path.unlink()

    def test_reject_malformed_values(self):
        """Parser should reject malformed numeric values with clear errors."""
        from vcf_pg_loader.gwas.loader import GWASParseError, GWASSSFParser

        content = make_malformed_gwas_tsv()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write(content)
            path = Path(f.name)

        try:
            parser = GWASSSFParser(path)
            with pytest.raises(GWASParseError) as exc_info:
                list(parser.iter_records())
            assert "line" in str(exc_info.value).lower() or "row" in str(exc_info.value).lower()
        finally:
            path.unlink()

    def test_handle_missing_optional_values(self):
        """Parser should gracefully handle missing optional field values."""
        from vcf_pg_loader.gwas.loader import GWASSSFParser

        stats = make_gwas_with_missing_optional_fields()
        path = GWASSSFGenerator.generate_file(stats)

        try:
            parser = GWASSSFParser(path)
            records = list(parser.iter_records())
            assert len(records) == 3
            assert records[0].beta is None
            assert records[1].rsid is None
            assert records[2].effect_allele_frequency is None
        finally:
            path.unlink()


class TestAlleleHarmonization:
    """Tests for allele harmonization logic."""

    def test_is_strand_ambiguous_at(self):
        """A/T SNPs should be detected as strand-ambiguous."""
        from vcf_pg_loader.gwas.loader import is_strand_ambiguous

        assert is_strand_ambiguous("A", "T") is True
        assert is_strand_ambiguous("T", "A") is True

    def test_is_strand_ambiguous_cg(self):
        """C/G SNPs should be detected as strand-ambiguous."""
        from vcf_pg_loader.gwas.loader import is_strand_ambiguous

        assert is_strand_ambiguous("C", "G") is True
        assert is_strand_ambiguous("G", "C") is True

    def test_is_not_strand_ambiguous(self):
        """Non-ambiguous SNPs should not be flagged."""
        from vcf_pg_loader.gwas.loader import is_strand_ambiguous

        assert is_strand_ambiguous("A", "G") is False
        assert is_strand_ambiguous("A", "C") is False
        assert is_strand_ambiguous("G", "T") is False
        assert is_strand_ambiguous("C", "T") is False

    def test_complement_allele(self):
        """Complement function should return correct strand complement."""
        from vcf_pg_loader.gwas.loader import complement_allele

        assert complement_allele("A") == "T"
        assert complement_allele("T") == "A"
        assert complement_allele("C") == "G"
        assert complement_allele("G") == "C"

    def test_harmonize_alleles_matching(self):
        """Harmonization should return True when alleles match directly."""
        from vcf_pg_loader.gwas.loader import harmonize_alleles

        result = harmonize_alleles(
            effect_allele="A",
            other_allele="G",
            ref="G",
            alt="A",
        )
        assert result.is_match is True
        assert result.is_flipped is False
        assert result.is_effect_allele_alt is True

    def test_harmonize_alleles_strand_flip(self):
        """Harmonization should detect strand flips for non-ambiguous SNPs."""
        from vcf_pg_loader.gwas.loader import harmonize_alleles

        result = harmonize_alleles(
            effect_allele="T",
            other_allele="C",
            ref="G",
            alt="A",
        )
        assert result.is_match is True
        assert result.is_flipped is True
        assert result.is_effect_allele_alt is True

    def test_harmonize_alleles_ambiguous_by_frequency(self):
        """Strand-ambiguous SNPs should be matched by allele frequency when available."""
        from vcf_pg_loader.gwas.loader import harmonize_alleles

        result = harmonize_alleles(
            effect_allele="A",
            other_allele="T",
            ref="A",
            alt="T",
            effect_allele_frequency=0.3,
            vcf_alt_frequency=0.7,
        )
        assert result.is_match is True
        assert result.is_effect_allele_alt is False

    def test_harmonize_alleles_no_match(self):
        """Harmonization should return no match for incompatible alleles."""
        from vcf_pg_loader.gwas.loader import harmonize_alleles

        result = harmonize_alleles(
            effect_allele="ATG",
            other_allele="A",
            ref="C",
            alt="T",
        )
        assert result.is_match is False


class TestIsEffectAlleleAlt:
    """Tests for is_effect_allele_alt computation."""

    def test_effect_allele_is_alt(self):
        """is_effect_allele_alt should be True when effect allele matches VCF ALT."""
        from vcf_pg_loader.gwas.loader import compute_is_effect_allele_alt

        result = compute_is_effect_allele_alt(
            effect_allele="G",
            other_allele="A",
            ref="A",
            alt="G",
        )
        assert result is True

    def test_effect_allele_is_ref(self):
        """is_effect_allele_alt should be False when effect allele matches VCF REF."""
        from vcf_pg_loader.gwas.loader import compute_is_effect_allele_alt

        result = compute_is_effect_allele_alt(
            effect_allele="A",
            other_allele="G",
            ref="A",
            alt="G",
        )
        assert result is False

    def test_effect_allele_with_strand_flip(self):
        """is_effect_allele_alt should be computed correctly after strand flip."""
        from vcf_pg_loader.gwas.loader import compute_is_effect_allele_alt

        result = compute_is_effect_allele_alt(
            effect_allele="T",
            other_allele="C",
            ref="G",
            alt="A",
        )
        assert result is True


class TestVariantMatching:
    """Tests for variant matching logic."""

    def test_match_by_chr_pos_ref_alt(self):
        """Should match variants by chr:pos:ref:alt exactly."""
        from vcf_pg_loader.gwas.loader import match_variant

        variant_lookup = {
            ("chr1", 100, "A", "G"): 1,
            ("chr1", 200, "C", "T"): 2,
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
        """Should fall back to rsID matching when position match fails."""
        from vcf_pg_loader.gwas.loader import match_variant

        variant_lookup = {}
        rsid_lookup = {"rs12345": 5}

        result = match_variant(
            chromosome="1",
            position=100,
            effect_allele="G",
            other_allele="A",
            rsid="rs12345",
            variant_lookup=variant_lookup,
            rsid_lookup=rsid_lookup,
        )
        assert result == 5

    def test_no_match_returns_none(self):
        """Should return None when no match is found."""
        from vcf_pg_loader.gwas.loader import match_variant

        variant_lookup = {("chr1", 100, "A", "G"): 1}
        rsid_lookup = {}

        result = match_variant(
            chromosome="1",
            position=999,
            effect_allele="G",
            other_allele="A",
            rsid=None,
            variant_lookup=variant_lookup,
            rsid_lookup=rsid_lookup,
        )
        assert result is None

    def test_match_handles_chr_prefix(self):
        """Should handle both 'chr1' and '1' chromosome formats."""
        from vcf_pg_loader.gwas.loader import match_variant

        variant_lookup = {("chr1", 100, "A", "G"): 1}
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


@pytest.mark.integration
class TestGWASSchemaCreation:
    """Tests for GWAS schema creation."""

    @pytest.mark.asyncio
    async def test_create_studies_table(self, test_db):
        """Should create studies table with all columns."""
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)

        result = await test_db.fetchrow("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'studies'
            )
        """)
        assert result["exists"] is True

        columns = await test_db.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'studies'
        """)
        column_names = {c["column_name"] for c in columns}
        assert "study_id" in column_names
        assert "study_accession" in column_names
        assert "trait_name" in column_names
        assert "sample_size" in column_names

    @pytest.mark.asyncio
    async def test_create_gwas_summary_stats_table(self, test_db):
        """Should create gwas_summary_stats table with FK constraints."""
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)

        result = await test_db.fetchrow("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'gwas_summary_stats'
            )
        """)
        assert result["exists"] is True

        columns = await test_db.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'gwas_summary_stats'
        """)
        column_names = {c["column_name"] for c in columns}
        assert "variant_id" in column_names
        assert "study_id" in column_names
        assert "effect_allele" in column_names
        assert "beta" in column_names
        assert "p_value" in column_names
        assert "is_effect_allele_alt" in column_names

    @pytest.mark.asyncio
    async def test_create_gwas_indexes(self, test_db):
        """Should create indexes for PRS query patterns."""
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)
        await manager.create_gwas_indexes(test_db)

        indexes = await test_db.fetch("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'gwas_summary_stats'
        """)
        index_names = {idx["indexname"] for idx in indexes}
        assert any("pvalue" in name.lower() for name in index_names)


@pytest.mark.integration
class TestGWASImportIntegration:
    """Integration tests for GWAS import workflow."""

    @pytest.mark.asyncio
    async def test_import_basic_gwas_file(self, test_db):
        """Should import a basic GWAS-SSF file."""
        from vcf_pg_loader.gwas.loader import GWASLoader
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)

        stats = make_basic_gwas_stats()
        path = GWASSSFGenerator.generate_file(stats)

        try:
            loader = GWASLoader()
            result = await loader.import_gwas(
                conn=test_db,
                tsv_path=path,
                study_accession="GCST90002357",
                trait_name="Height",
            )

            assert result["stats_imported"] >= 0
            assert "study_id" in result
        finally:
            path.unlink()

    @pytest.mark.asyncio
    async def test_import_creates_study_record(self, test_db):
        """Should create a study record with provided metadata."""
        from vcf_pg_loader.gwas.loader import GWASLoader
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)

        stats = make_basic_gwas_stats()
        path = GWASSSFGenerator.generate_file(stats)

        try:
            loader = GWASLoader()
            result = await loader.import_gwas(
                conn=test_db,
                tsv_path=path,
                study_accession="GCST12345678",
                trait_name="Type 2 Diabetes",
                publication_pmid="12345678",
                sample_size=100000,
                n_cases=25000,
                n_controls=75000,
            )

            study = await test_db.fetchrow(
                "SELECT * FROM studies WHERE study_id = $1",
                result["study_id"],
            )
            assert study["study_accession"] == "GCST12345678"
            assert study["trait_name"] == "Type 2 Diabetes"
            assert study["sample_size"] == 100000
            assert study["n_cases"] == 25000
        finally:
            path.unlink()

    @pytest.mark.asyncio
    async def test_import_joins_to_variants(self, test_db):
        """Should be able to JOIN imported stats to variants table."""
        from fixtures.vcf_generator import SyntheticVariant, VCFGenerator
        from vcf_pg_loader.gwas.loader import GWASLoader
        from vcf_pg_loader.gwas.schema import GWASSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        schema_manager = SchemaManager()
        await schema_manager.create_schema(test_db)

        gwas_manager = GWASSchemaManager()
        await gwas_manager.create_gwas_schema(test_db)

        vcf_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=12345,
                ref="A",
                alt=["G"],
                rs_id="rs12345",
            ),
            SyntheticVariant(
                chrom="chr1",
                pos=23456,
                ref="C",
                alt=["T"],
                rs_id="rs23456",
            ),
        ]
        vcf_path = VCFGenerator.generate_file(vcf_variants)

        gwas_stats = [
            GWASSummaryStatistic(
                chromosome="1",
                base_pair_location=12345,
                effect_allele="G",
                other_allele="A",
                p_value=1e-8,
                beta=0.1,
                rsid="rs12345",
            ),
        ]
        gwas_path = GWASSSFGenerator.generate_file(gwas_stats)

        try:
            loader = GWASLoader()
            await loader.import_gwas(
                conn=test_db,
                tsv_path=gwas_path,
                study_accession="GCST_TEST",
                trait_name="Test Trait",
            )

            await test_db.fetch("""
                SELECT v.chrom, v.pos, v.ref, v.alt, g.beta, g.p_value
                FROM variants v
                JOIN gwas_summary_stats g ON v.variant_id = g.variant_id
            """)
        finally:
            vcf_path.unlink()
            gwas_path.unlink()

    @pytest.mark.asyncio
    async def test_import_handles_binary_traits(self, test_db):
        """Should correctly import odds ratios for binary traits."""
        from vcf_pg_loader.gwas.loader import GWASLoader
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)

        stats = make_binary_trait_gwas_stats()
        path = GWASSSFGenerator.generate_file(stats)

        try:
            loader = GWASLoader()
            await loader.import_gwas(
                conn=test_db,
                tsv_path=path,
                study_accession="GCST_BINARY",
                trait_name="Disease Status",
                n_cases=10000,
                n_controls=40000,
            )

            gwas_stats = await test_db.fetch("SELECT odds_ratio, n_cases FROM gwas_summary_stats")
            for stat in gwas_stats:
                assert stat["odds_ratio"] is not None
        finally:
            path.unlink()

    @pytest.mark.asyncio
    async def test_unique_constraint_study_variant(self, test_db):
        """Should enforce unique constraint on (study_id, variant_id)."""
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)

        await test_db.execute("""
            INSERT INTO studies (study_accession, trait_name, genome_build)
            VALUES ('GCST_TEST', 'Test', 'GRCh38')
        """)

        await test_db.execute("""
            INSERT INTO gwas_summary_stats (study_id, effect_allele, p_value)
            VALUES (1, 'A', 1e-8)
        """)

        import asyncpg

        with pytest.raises(asyncpg.UniqueViolationError):
            await test_db.execute("""
                INSERT INTO gwas_summary_stats (study_id, variant_id, effect_allele, p_value)
                VALUES (1, NULL, 'G', 1e-7)
            """)


@pytest.mark.integration
class TestGWASQueryPerformance:
    """Tests for GWAS query performance patterns."""

    @pytest.mark.asyncio
    async def test_pvalue_filter_uses_index(self, test_db):
        """p-value filtering should use index for efficient queries."""
        from vcf_pg_loader.gwas.schema import GWASSchemaManager

        manager = GWASSchemaManager()
        await manager.create_gwas_schema(test_db)
        await manager.create_gwas_indexes(test_db)

        explain = await test_db.fetchrow("""
            EXPLAIN SELECT * FROM gwas_summary_stats WHERE p_value < 5e-8
        """)
        plan = explain[0].lower()
        assert "index" in plan or "seq scan" in plan
