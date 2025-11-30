"""Tests for VCF header parsing functionality."""

import pytest

from vcf_pg_loader.vcf_parser import VCFHeaderParser


class TestInfoFieldParsing:
    """Test INFO field schema extraction from VCF headers."""

    def test_number_1_integer(self):
        """Single-value integer fields map correctly."""
        header = '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert "DP" in fields
        assert fields["DP"]["Number"] == "1"
        assert fields["DP"]["Type"] == "Integer"

    def test_number_a_float(self):
        """Number=A fields are per-ALT allele."""
        header = '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert fields["AF"]["Number"] == "A"
        assert fields["AF"]["Type"] == "Float"

    def test_number_r_integer(self):
        """Number=R fields include REF + ALT alleles."""
        header = '##INFO=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert fields["AD"]["Number"] == "R"
        assert fields["AD"]["Type"] == "Integer"

    def test_number_g(self):
        """Number=G fields are per-genotype."""
        header = '##INFO=<ID=PL,Number=G,Type=Integer,Description="Phred Likelihoods">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert fields["PL"]["Number"] == "G"
        assert fields["PL"]["Type"] == "Integer"

    def test_number_dot(self):
        """Number=. (unbounded) is handled."""
        header = '##INFO=<ID=CSQ,Number=.,Type=String,Description="VEP annotation">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert fields["CSQ"]["Number"] == "."
        assert fields["CSQ"]["Type"] == "String"

    def test_flag_field(self):
        """Flag fields (Number=0) are boolean."""
        header = '##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP member">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert fields["DB"]["Number"] == "0"
        assert fields["DB"]["Type"] == "Flag"

    @pytest.mark.parametrize(
        "description,expected",
        [
            ('Description="A, B, and C"', "A, B, and C"),
            ('Description=""', ""),
            ('Description="Simple"', "Simple"),
        ],
    )
    def test_description_parsing_edge_cases(self, description, expected):
        """Descriptions with special characters parse correctly."""
        header = f"##INFO=<ID=TEST,Number=1,Type=String,{description}>"
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert fields["TEST"]["Description"] == expected

    def test_escaped_quotes_in_description(self):
        """Descriptions with escaped quotes parse correctly per VCF spec."""
        header = r'##INFO=<ID=TEST,Number=1,Type=String,Description="A \"quoted\" value">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert "TEST" in fields
        assert "quoted" in fields["TEST"]["Description"]

    def test_description_with_parentheses_range(self):
        """Descriptions with parenthetical ranges parse correctly (vcf2db edge case)."""
        header = '##INFO=<ID=AF,Number=A,Type=Float,Description="Frequency in range (0,1]">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert "AF" in fields
        assert fields["AF"]["Number"] == "A"
        assert "(0,1]" in fields["AF"]["Description"]


class TestVEPCSQParsing:
    """Test VEP Consequence annotation field parsing."""

    def test_csq_format_extraction(self):
        """Extract CSQ field order from VEP header."""
        header = '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature">'
        parser = VCFHeaderParser()
        csq_fields = parser.parse_csq_header([header])

        assert csq_fields == [
            "Allele",
            "Consequence",
            "IMPACT",
            "SYMBOL",
            "Gene",
            "Feature_type",
            "Feature",
        ]

    def test_sarek_vep_csq_fields(self):
        """Parse CSQ format from actual sarek VEP output."""
        sarek_csq = '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID|CANONICAL|MANE_SELECT|MANE_PLUS_CLINICAL|TSL|APPRIS|CCDS|ENSP|SWISSPROT|TREMBL|UNIPARC|UNIPROT_ISOFORM|SOURCE|GENE_PHENO|SIFT|PolyPhen|DOMAINS|miRNA|AF|AFR_AF|AMR_AF|EAS_AF|EUR_AF|SAS_AF|gnomADe_AF|gnomADe_AFR_AF|gnomADe_AMR_AF|gnomADe_ASJ_AF|gnomADe_EAS_AF|gnomADe_FIN_AF|gnomADe_NFE_AF|gnomADe_OTH_AF|gnomADe_SAS_AF|gnomADg_AF|gnomADg_AFR_AF|gnomADg_AMI_AF|gnomADg_AMR_AF|gnomADg_ASJ_AF|gnomADg_EAS_AF|gnomADg_FIN_AF|gnomADg_MID_AF|gnomADg_NFE_AF|gnomADg_OTH_AF|gnomADg_SAS_AF|MAX_AF|MAX_AF_POPS|CLIN_SIG|SOMATIC|PHENO|PUBMED|VAR_SYNONYMS|MOTIF_NAME|MOTIF_POS|HIGH_INF_POS|MOTIF_SCORE_CHANGE|TRANSCRIPTION_FACTORS|CADD_PHRED|CADD_RAW|SpliceAI_pred_DP_AG|SpliceAI_pred_DP_AL|SpliceAI_pred_DP_DG|SpliceAI_pred_DP_DL|SpliceAI_pred_DS_AG|SpliceAI_pred_DS_AL|SpliceAI_pred_DS_DG|SpliceAI_pred_DS_DL|SpliceAI_pred_SYMBOL|LoF|LoF_filter|LoF_flags|LoF_info">'

        parser = VCFHeaderParser()
        csq_fields = parser.parse_csq_header([sarek_csq])

        assert "SYMBOL" in csq_fields
        assert "Consequence" in csq_fields
        assert "IMPACT" in csq_fields
        assert "CLIN_SIG" in csq_fields
        assert "gnomADe_AF" in csq_fields
        assert "CADD_PHRED" in csq_fields


class TestFormatFieldParsing:
    """Test FORMAT field schema extraction."""

    def test_format_gt(self):
        """Genotype field parses correctly."""
        header = '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">'
        parser = VCFHeaderParser()
        fields = parser.parse_format_fields([header])

        assert fields["GT"]["Number"] == "1"
        assert fields["GT"]["Type"] == "String"

    def test_format_ad_number_r(self):
        """AD with Number=R parses correctly."""
        header = '##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">'
        parser = VCFHeaderParser()
        fields = parser.parse_format_fields([header])

        assert fields["AD"]["Number"] == "R"
        assert fields["AD"]["Type"] == "Integer"

    def test_format_pl_number_g(self):
        """PL with Number=G parses correctly."""
        header = '##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Phred-scaled genotype likelihoods">'
        parser = VCFHeaderParser()
        fields = parser.parse_format_fields([header])

        assert fields["PL"]["Number"] == "G"
        assert fields["PL"]["Type"] == "Integer"


class TestEmptyAndMalformedHeaders:
    """Test handling of empty or malformed headers."""

    def test_empty_header_info(self):
        """Empty INFO header returns empty dict."""
        parser = VCFHeaderParser()
        assert parser.parse_info_fields([]) == {}

    def test_empty_header_format(self):
        """Empty FORMAT header returns empty dict."""
        parser = VCFHeaderParser()
        assert parser.parse_format_fields([]) == {}

    def test_empty_header_csq(self):
        """Empty CSQ header returns empty list."""
        parser = VCFHeaderParser()
        assert parser.parse_csq_header([]) == []

    def test_non_info_lines_ignored(self):
        """Non-INFO header lines are ignored."""
        lines = [
            "##fileformat=VCFv4.3",
            "##FILTER=<ID=PASS,Description=Pass>",
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">',
        ]
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields(lines)

        assert len(fields) == 1
        assert "DP" in fields


class TestFlagFieldConstraints:
    """Test VCF spec constraints for Flag type fields."""

    def test_flag_must_have_number_zero(self):
        """Flag type with Number=0 is valid per VCF spec."""
        header = '##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP member">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert fields["DB"]["Number"] == "0"
        assert fields["DB"]["Type"] == "Flag"

    def test_flag_with_wrong_number_parsed(self):
        """Flag type with Number!=0 is parsed (validation is caller responsibility)."""
        header = '##INFO=<ID=BAD,Number=1,Type=Flag,Description="Invalid flag">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert "BAD" in fields
        assert fields["BAD"]["Number"] == "1"
        assert fields["BAD"]["Type"] == "Flag"

    def test_info_flag_is_valid(self):
        """Flag type in INFO is valid per VCF spec."""
        header = '##INFO=<ID=SOMATIC,Number=0,Type=Flag,Description="Somatic mutation">'
        parser = VCFHeaderParser()
        fields = parser.parse_info_fields([header])

        assert "SOMATIC" in fields
        assert fields["SOMATIC"]["Type"] == "Flag"

    def test_format_flag_parsed(self):
        """Flag type in FORMAT is parsed (spec says it's invalid but tools may produce it)."""
        header = '##FORMAT=<ID=FT,Number=0,Type=Flag,Description="Invalid per spec">'
        parser = VCFHeaderParser()
        fields = parser.parse_format_fields([header])

        assert "FT" in fields
        assert fields["FT"]["Type"] == "Flag"
