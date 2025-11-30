"""GIAB validation tests for clinical-grade accuracy."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator


@pytest.mark.validation
@pytest.mark.slow
class TestGIABTrioValidation:
    """Validation using GIAB Ashkenazi trio benchmark data.

    GIAB samples: HG002 (son), HG003 (father), HG004 (mother)
    """

    def test_hg002_parsing_integrity(self, giab_chr21_vcf):
        """HG002 benchmark VCF parses without errors."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(giab_chr21_vcf, human_genome=True)
        try:
            total = 0
            errors = 0
            for batch in parser.iter_batches():
                for record in batch:
                    total += 1
                    if record.ref is None or record.alt is None:
                        errors += 1

            assert total > 50_000, f"Expected >50K variants, got {total}"
            assert errors == 0, f"Found {errors} parsing errors"
        finally:
            parser.close()

    def test_hg002_variant_distribution(self, giab_chr21_vcf):
        """Variant types match expected distribution for chr21."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        parser = VCFStreamingParser(giab_chr21_vcf, human_genome=True)
        try:
            snps = 0
            indels = 0
            for batch in parser.iter_batches():
                for record in batch:
                    if len(record.ref) == 1 and len(record.alt) == 1:
                        snps += 1
                    else:
                        indels += 1

            total = snps + indels
            snp_ratio = snps / total if total > 0 else 0
            assert 0.7 < snp_ratio < 0.95, f"SNP ratio {snp_ratio:.2f} unexpected"
        finally:
            parser.close()


@pytest.mark.validation
class TestMendelianConsistency:
    """Test Mendelian inheritance validation in trios."""

    @pytest.fixture
    def mendelian_trio(self):
        """Create trio with Mendelian-consistent variants."""
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
                info={"GT": "1/1"},
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
            SyntheticVariant(
                chrom="chr1",
                pos=2000,
                ref="C",
                alt=["T"],
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
            SyntheticVariant(
                chrom="chr1",
                pos=3000,
                ref="G",
                alt=["A"],
                info={"GT": "0/1"},
            ),
        ]

        child_vcf = VCFGenerator.generate_file(child_variants)
        father_vcf = VCFGenerator.generate_file(father_variants)
        mother_vcf = VCFGenerator.generate_file(mother_variants)

        yield {"child": child_vcf, "father": father_vcf, "mother": mother_vcf}

        child_vcf.unlink()
        father_vcf.unlink()
        mother_vcf.unlink()

    def test_het_variant_parental_inheritance(self, mendelian_trio):
        """Het variants in child should be present in at least one parent."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        def load_variants(vcf_file):
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            variants = {}
            for batch in parser.iter_batches():
                for r in batch:
                    key = (r.chrom, r.pos, r.ref, r.alt)
                    variants[key] = r.info.get("GT", "0/1")
            parser.close()
            return variants

        child = load_variants(mendelian_trio["child"])
        father = load_variants(mendelian_trio["father"])
        mother = load_variants(mendelian_trio["mother"])

        mendelian_violations = 0
        for key, gt in child.items():
            if gt == "0/1":
                in_father = key in father
                in_mother = key in mother
                if not (in_father or in_mother):
                    mendelian_violations += 1

        assert mendelian_violations == 0, f"Found {mendelian_violations} Mendelian violations"

    def test_hom_alt_requires_both_parents(self, mendelian_trio):
        """Hom-alt variants require variant in both parents (AR)."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        def load_variants(vcf_file):
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            variants = {}
            for batch in parser.iter_batches():
                for r in batch:
                    key = (r.chrom, r.pos, r.ref, r.alt)
                    variants[key] = r.info.get("GT", "0/1")
            parser.close()
            return variants

        child = load_variants(mendelian_trio["child"])
        father = load_variants(mendelian_trio["father"])
        mother = load_variants(mendelian_trio["mother"])

        for key, gt in child.items():
            if gt == "1/1":
                assert key in father, f"Hom-alt {key} missing from father"
                assert key in mother, f"Hom-alt {key} missing from mother"


@pytest.mark.validation
class TestDeNovoDetection:
    """Test de novo variant detection accuracy."""

    @pytest.fixture
    def trio_with_de_novo(self):
        """Create trio with known de novo variant."""
        child_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=1000,
                ref="A",
                alt=["G"],
                info={"GT": "0/1", "DP": 50, "GQ": 99},
            ),
            SyntheticVariant(
                chrom="chr1",
                pos=2000,
                ref="C",
                alt=["T"],
                info={"GT": "0/1", "DP": 45, "GQ": 95},
            ),
        ]

        father_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=2000,
                ref="C",
                alt=["T"],
                info={"GT": "0/1", "DP": 40, "GQ": 90},
            ),
        ]

        mother_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=2000,
                ref="C",
                alt=["T"],
                info={"GT": "0/0", "DP": 35, "GQ": 85},
            ),
        ]

        child_vcf = VCFGenerator.generate_file(child_variants)
        father_vcf = VCFGenerator.generate_file(father_variants)
        mother_vcf = VCFGenerator.generate_file(mother_variants)

        yield {"child": child_vcf, "father": father_vcf, "mother": mother_vcf}

        child_vcf.unlink()
        father_vcf.unlink()
        mother_vcf.unlink()

    def test_identify_de_novo_candidates(self, trio_with_de_novo):
        """De novo candidates are correctly identified."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        def load_variant_set(vcf_file):
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            variants = set()
            for batch in parser.iter_batches():
                for r in batch:
                    variants.add((r.chrom, r.pos, r.ref, r.alt))
            parser.close()
            return variants

        child_set = load_variant_set(trio_with_de_novo["child"])
        father_set = load_variant_set(trio_with_de_novo["father"])
        mother_set = load_variant_set(trio_with_de_novo["mother"])

        de_novo = child_set - (father_set | mother_set)

        assert len(de_novo) == 1
        assert ("chr1", 1000, "A", "G") in de_novo


@pytest.mark.validation
class TestCompoundHetDetection:
    """Test compound heterozygote detection accuracy."""

    @pytest.fixture
    def trio_with_compound_het(self):
        """Create trio with compound het in a gene."""
        child_variants = [
            SyntheticVariant(
                chrom="chr17",
                pos=41276044,
                ref="A",
                alt=["G"],
                info={"GT": "0/1", "SYMBOL": "BRCA1"},
            ),
            SyntheticVariant(
                chrom="chr17",
                pos=41277100,
                ref="C",
                alt=["T"],
                info={"GT": "0/1", "SYMBOL": "BRCA1"},
            ),
        ]

        father_variants = [
            SyntheticVariant(
                chrom="chr17",
                pos=41276044,
                ref="A",
                alt=["G"],
                info={"GT": "0/1", "SYMBOL": "BRCA1"},
            ),
        ]

        mother_variants = [
            SyntheticVariant(
                chrom="chr17",
                pos=41277100,
                ref="C",
                alt=["T"],
                info={"GT": "0/1", "SYMBOL": "BRCA1"},
            ),
        ]

        child_vcf = VCFGenerator.generate_file(child_variants)
        father_vcf = VCFGenerator.generate_file(father_variants)
        mother_vcf = VCFGenerator.generate_file(mother_variants)

        yield {"child": child_vcf, "father": father_vcf, "mother": mother_vcf}

        child_vcf.unlink()
        father_vcf.unlink()
        mother_vcf.unlink()

    def test_identify_compound_het_candidates(self, trio_with_compound_het):
        """Compound het candidates are correctly identified."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        def load_variants_by_gene(vcf_file):
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            genes = {}
            for batch in parser.iter_batches():
                for r in batch:
                    gene = r.info.get("SYMBOL")
                    if gene:
                        if gene not in genes:
                            genes[gene] = []
                        genes[gene].append((r.chrom, r.pos, r.ref, r.alt))
            parser.close()
            return genes

        child_genes = load_variants_by_gene(trio_with_compound_het["child"])
        father_genes = load_variants_by_gene(trio_with_compound_het["father"])
        mother_genes = load_variants_by_gene(trio_with_compound_het["mother"])

        compound_het_genes = []
        for gene, variants in child_genes.items():
            if len(variants) >= 2:
                father_variants = set(father_genes.get(gene, []))
                mother_variants = set(mother_genes.get(gene, []))

                child_set = set(variants)
                from_father = child_set & father_variants
                from_mother = child_set & mother_variants

                if from_father and from_mother and not (from_father & from_mother):
                    compound_het_genes.append(gene)

        assert "BRCA1" in compound_het_genes
