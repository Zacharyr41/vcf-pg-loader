"""Database query tests for clinical variant analysis patterns."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator


@pytest.mark.integration
class TestDeNovoQueries:
    """Test de novo variant detection queries.

    De novo variants: present in child, absent in both parents.
    """

    @pytest.fixture
    async def trio_database(self, test_db):
        """Load trio with de novo candidates."""
        from vcf_pg_loader.db_loader import load_variants_with_sample
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        child_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=1000,
                ref="A",
                alt=["G"],
                info={"GT": "0/1"},
            ),
            SyntheticVariant(
                chrom="chr1",
                pos=2000,
                ref="C",
                alt=["T"],
                info={"GT": "0/1"},
            ),
            SyntheticVariant(
                chrom="chr1",
                pos=3000,
                ref="G",
                alt=["A"],
                info={"GT": "0/1"},
            ),
        ]

        father_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=1000,
                ref="A",
                alt=["G"],
                info={"GT": "0/1"},
            ),
        ]

        mother_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=2000,
                ref="C",
                alt=["T"],
                info={"GT": "0/1"},
            ),
        ]

        for sample_name, variants in [
            ("child", child_variants),
            ("father", father_variants),
            ("mother", mother_variants),
        ]:
            vcf_file = VCFGenerator.generate_file(variants)
            try:
                parser = VCFStreamingParser(vcf_file, human_genome=True)
                for batch in parser.iter_batches():
                    await load_variants_with_sample(test_db, batch, sample_name)
                parser.close()
            finally:
                vcf_file.unlink()

        yield test_db

    async def test_find_de_novo_variants(self, trio_database):
        """Find variants in child absent from both parents."""
        conn = trio_database

        results = await conn.fetch(
            """
            SELECT c.chrom, c.pos, c.ref, c.alt
            FROM variants c
            WHERE c.sample_id = 'child'
            AND NOT EXISTS (
                SELECT 1 FROM variants p
                WHERE p.sample_id = 'father'
                AND p.chrom = c.chrom AND p.pos = c.pos
                AND p.ref = c.ref AND p.alt = c.alt
            )
            AND NOT EXISTS (
                SELECT 1 FROM variants p
                WHERE p.sample_id = 'mother'
                AND p.chrom = c.chrom AND p.pos = c.pos
                AND p.ref = c.ref AND p.alt = c.alt
            )
            """
        )

        assert len(results) == 1
        assert results[0]["pos"] == 3000


@pytest.mark.integration
class TestCompoundHeterozygoteQueries:
    """Test compound heterozygote detection.

    Compound het: two different heterozygous variants in the same gene,
    inherited from different parents.
    """

    @pytest.fixture
    async def compound_het_database(self, test_db):
        """Load data with compound het candidates."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(
                chrom="chr17",
                pos=41276044,
                ref="A",
                alt=["G"],
                info={
                    "SYMBOL": "BRCA1",
                    "Consequence": "missense_variant",
                    "IMPACT": "MODERATE",
                },
            ),
            SyntheticVariant(
                chrom="chr17",
                pos=41276100,
                ref="C",
                alt=["T"],
                info={
                    "SYMBOL": "BRCA1",
                    "Consequence": "frameshift_variant",
                    "IMPACT": "HIGH",
                },
            ),
            SyntheticVariant(
                chrom="chr17",
                pos=41277000,
                ref="G",
                alt=["A"],
                info={
                    "SYMBOL": "BRCA2",
                    "Consequence": "synonymous_variant",
                    "IMPACT": "LOW",
                },
            ),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_find_genes_with_multiple_variants(self, compound_het_database):
        """Find genes with multiple heterozygous variants."""
        conn = compound_het_database

        results = await conn.fetch(
            """
            SELECT gene, COUNT(*) as variant_count
            FROM variants
            WHERE gene IS NOT NULL
            GROUP BY gene
            HAVING COUNT(*) >= 2
            """
        )

        genes = {r["gene"] for r in results}
        assert "BRCA1" in genes
        assert "BRCA2" not in genes

    async def test_compound_het_with_impact_filter(self, compound_het_database):
        """Find compound hets with at least one high-impact variant."""
        conn = compound_het_database

        results = await conn.fetch(
            """
            WITH gene_variants AS (
                SELECT gene, chrom, pos, ref, alt, impact
                FROM variants
                WHERE gene IS NOT NULL
            ),
            genes_with_high AS (
                SELECT DISTINCT gene
                FROM gene_variants
                WHERE impact = 'HIGH'
            )
            SELECT gv.*
            FROM gene_variants gv
            INNER JOIN genes_with_high gwh ON gv.gene = gwh.gene
            WHERE gv.gene IN (
                SELECT gene FROM gene_variants
                GROUP BY gene
                HAVING COUNT(*) >= 2
            )
            ORDER BY gv.gene, gv.pos
            """
        )

        assert len(results) == 2
        assert all(r["gene"] == "BRCA1" for r in results)


@pytest.mark.integration
class TestCohortQueries:
    """Test cohort-level analysis queries."""

    @pytest.fixture
    async def cohort_database(self, test_db):
        """Load multi-sample cohort data."""
        from vcf_pg_loader.db_loader import load_variants_with_sample
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        for sample_idx in range(10):
            variants = []
            for i in range(100):
                if i % (sample_idx + 1) == 0:
                    variants.append(
                        SyntheticVariant(
                            chrom="chr1",
                            pos=1000 + i * 10,
                            ref="A",
                            alt=["G"],
                            info={"AF": [0.1]},
                        )
                    )

            if variants:
                vcf_file = VCFGenerator.generate_file(variants)
                try:
                    parser = VCFStreamingParser(vcf_file, human_genome=True)
                    for batch in parser.iter_batches():
                        await load_variants_with_sample(
                            test_db, batch, f"sample_{sample_idx}"
                        )
                    parser.close()
                finally:
                    vcf_file.unlink()

        yield test_db

    async def test_variant_frequency_in_cohort(self, cohort_database):
        """Calculate variant frequency across cohort."""
        conn = cohort_database

        results = await conn.fetch(
            """
            SELECT chrom, pos, ref, alt,
                   COUNT(DISTINCT sample_id) as sample_count,
                   COUNT(DISTINCT sample_id)::float / 10 as cohort_frequency
            FROM variants
            GROUP BY chrom, pos, ref, alt
            ORDER BY sample_count DESC
            LIMIT 10
            """
        )

        assert len(results) > 0
        assert results[0]["sample_count"] == 10

    async def test_rare_variants_in_cohort(self, cohort_database):
        """Find variants present in only 1-2 samples."""
        conn = cohort_database

        results = await conn.fetch(
            """
            SELECT chrom, pos, ref, alt, COUNT(DISTINCT sample_id) as sample_count
            FROM variants
            GROUP BY chrom, pos, ref, alt
            HAVING COUNT(DISTINCT sample_id) <= 2
            """
        )

        assert len(results) > 0
        for r in results:
            assert r["sample_count"] <= 2

    async def test_sample_variant_burden(self, cohort_database):
        """Calculate per-sample variant burden."""
        conn = cohort_database

        results = await conn.fetch(
            """
            SELECT sample_id, COUNT(*) as variant_count
            FROM variants
            GROUP BY sample_id
            ORDER BY variant_count DESC
            """
        )

        assert len(results) == 10
        assert results[0]["variant_count"] == 100


@pytest.mark.integration
class TestClinicalFilterQueries:
    """Test clinical-grade filtering queries."""

    @pytest.fixture
    async def clinical_database(self, test_db):
        """Load variants with clinical annotations."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(
                chrom="chr17",
                pos=41276044,
                ref="A",
                alt=["G"],
                info={
                    "AF": [0.0001],
                    "SYMBOL": "BRCA1",
                    "IMPACT": "HIGH",
                    "Consequence": "frameshift_variant",
                    "gnomAD_AF": 0.00001,
                },
            ),
            SyntheticVariant(
                chrom="chr17",
                pos=41277000,
                ref="C",
                alt=["T"],
                info={
                    "AF": [0.15],
                    "SYMBOL": "BRCA1",
                    "IMPACT": "MODERATE",
                    "Consequence": "missense_variant",
                    "gnomAD_AF": 0.12,
                },
            ),
            SyntheticVariant(
                chrom="chr13",
                pos=32315508,
                ref="G",
                alt=["A"],
                info={
                    "AF": [0.0005],
                    "SYMBOL": "BRCA2",
                    "IMPACT": "HIGH",
                    "Consequence": "stop_gained",
                    "gnomAD_AF": 0.0003,
                },
            ),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()
            yield test_db
        finally:
            vcf_file.unlink()

    async def test_acmg_gene_filter(self, clinical_database):
        """Filter by ACMG recommended genes."""
        conn = clinical_database

        acmg_genes = ["BRCA1", "BRCA2", "MLH1", "MSH2"]

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE gene = ANY($1)
            AND impact IN ('HIGH', 'MODERATE')
            """,
            acmg_genes
        )

        assert len(results) == 3

    async def test_pathogenicity_candidates(self, clinical_database):
        """Find likely pathogenic variants."""
        conn = clinical_database

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE impact = 'HIGH'
            AND (info->>'gnomAD_AF')::float < 0.001
            """
        )

        assert len(results) == 2
        genes = {r["gene"] for r in results}
        assert genes == {"BRCA1", "BRCA2"}

    async def test_lof_variants(self, clinical_database):
        """Find loss-of-function variants."""
        conn = clinical_database

        lof_consequences = [
            "frameshift_variant",
            "stop_gained",
            "splice_acceptor_variant",
            "splice_donor_variant",
        ]

        results = await conn.fetch(
            """
            SELECT * FROM variants
            WHERE consequence = ANY($1)
            """,
            lof_consequences
        )

        assert len(results) == 2
