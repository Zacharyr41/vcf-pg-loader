"""Tests for variant record parsing and data structures."""

from vcf_pg_loader.models import VariantRecord
from vcf_pg_loader.vcf_parser import VariantParser


class TestVariantRecord:
    def test_variant_record_creation(self):
        """Test basic VariantRecord creation."""
        record = VariantRecord(
            chrom="chr1",
            pos=100,
            ref="A",
            alt="G",
            qual=30.0,
            filter=["PASS"],
            rs_id="rs123",
            info={"DP": 50}
        )

        assert record.chrom == "chr1"
        assert record.pos == 100
        assert record.ref == "A"
        assert record.alt == "G"
        assert record.qual == 30.0
        assert record.filter == ["PASS"]
        assert record.rs_id == "rs123"
        assert record.info == {"DP": 50}

    def test_variant_record_defaults(self):
        """Test VariantRecord with default values."""
        record = VariantRecord(
            chrom="chr1",
            pos=100,
            ref="A",
            alt="G",
            qual=None,
            filter=[],
            rs_id=None,
            info={}
        )

        assert record.qual is None
        assert record.rs_id is None
        assert record.gene is None
        assert record.consequence is None
        assert record.af_gnomad is None

    def test_variant_type_classification(self):
        """Test variant type classification logic."""
        # SNP
        snp = VariantRecord(chrom="chr1", pos=100, ref="A", alt="G", qual=None, filter=[], rs_id=None, info={})
        assert snp.variant_type == "snp"

        # Deletion
        deletion = VariantRecord(chrom="chr1", pos=100, ref="ATG", alt="A", qual=None, filter=[], rs_id=None, info={})
        assert deletion.variant_type == "indel"

        # Insertion
        insertion = VariantRecord(chrom="chr1", pos=100, ref="A", alt="ATG", qual=None, filter=[], rs_id=None, info={})
        assert insertion.variant_type == "indel"

        # MNV
        mnv = VariantRecord(chrom="chr1", pos=100, ref="AT", alt="GC", qual=None, filter=[], rs_id=None, info={})
        assert mnv.variant_type == "mnp"


class TestVariantParser:
    def test_parse_simple_snp(self):
        """Test parsing a simple SNP variant."""
        # Mock cyvcf2 variant object
        class MockVariant:
            CHROM = "1"
            POS = 100
            REF = "A"
            ALT = ["G"]
            QUAL = 30.0
            FILTER = "PASS"
            ID = "rs123"
            INFO = {"DP": 50, "AC": [1], "AF": [0.5]}

        parser = VariantParser()
        records = parser.parse_variant(MockVariant(), [])

        assert len(records) == 1
        record = records[0]
        assert record.chrom == "chr1"
        assert record.pos == 100
        assert record.ref == "A"
        assert record.alt == "G"
        assert record.qual == 30.0
        assert record.filter == ["PASS"]
        assert record.rs_id == "rs123"

    def test_parse_multiallelic_variant(self):
        """Test parsing a multi-allelic variant (creates multiple records)."""
        class MockVariant:
            CHROM = "1"
            POS = 100
            REF = "A"
            ALT = ["G", "T"]
            QUAL = 30.0
            FILTER = "PASS"
            ID = "rs123"
            INFO = {"DP": 50, "AC": [1, 2], "AF": [0.3, 0.2]}

        parser = VariantParser()
        records = parser.parse_variant(MockVariant(), [])

        assert len(records) == 2
        assert records[0].alt == "G"
        assert records[1].alt == "T"
        assert all(r.chrom == "chr1" for r in records)
        assert all(r.pos == 100 for r in records)

    def test_parse_variant_with_csq(self):
        """Test parsing variant with VEP CSQ annotations."""
        class MockVariant:
            CHROM = "1"
            POS = 100
            REF = "A"
            ALT = ["G"]
            QUAL = 30.0
            FILTER = "PASS"
            ID = "rs123"
            INFO = {
                "CSQ": "G|missense_variant|MODERATE|BRCA1|ENSG00000012048|Transcript|ENST00000012345|protein_coding|2/23||c.181A>G|p.Lys61Glu"
            }

        csq_fields = ['Allele', 'Consequence', 'IMPACT', 'SYMBOL', 'Gene', 'Feature_type', 'Feature', 'BIOTYPE', 'EXON', 'INTRON', 'HGVSc', 'HGVSp']

        parser = VariantParser()
        records = parser.parse_variant(MockVariant(), csq_fields)

        assert len(records) == 1
        record = records[0]
        assert record.gene == "BRCA1"
        assert record.consequence == "missense_variant"
        assert record.impact == "MODERATE"
        assert record.hgvs_c == "c.181A>G"
        assert record.hgvs_p == "p.Lys61Glu"

    def test_parse_variant_missing_values(self):
        """Test parsing variant with missing/null values."""
        class MockVariant:
            CHROM = "1"
            POS = 100
            REF = "A"
            ALT = ["G"]
            QUAL = -1  # cyvcf2 uses -1 for missing QUAL
            FILTER = None
            ID = "."  # cyvcf2 uses "." for missing ID
            INFO = {}

        parser = VariantParser()
        records = parser.parse_variant(MockVariant(), [])

        assert len(records) == 1
        record = records[0]
        assert record.qual is None
        assert record.rs_id is None
        assert record.filter == []

    def test_safe_float_parsing(self):
        """Test safe float parsing utility."""
        parser = VariantParser()

        assert parser._safe_float(1.5) == 1.5
        assert parser._safe_float("1.5") == 1.5
        assert parser._safe_float("1.5e-3") == 0.0015
        assert parser._safe_float(None) is None
        assert parser._safe_float("invalid") is None
        assert parser._safe_float("") is None
