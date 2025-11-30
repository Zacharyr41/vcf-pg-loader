"""Unit tests for SnpEff ANN parser."""

import tempfile
from pathlib import Path

import pytest

from vcf_pg_loader.vcf_parser import VariantParser, VCFHeaderParser, VCFStreamingParser


class TestANNHeaderParsing:
    """Test ANN header field parsing."""

    def test_ann_fields_parsed_from_standard_header(self):
        """Standard SnpEff ANN header is parsed correctly."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            ann_fields = parser.header_parser.ann_fields

            assert len(ann_fields) > 0
            assert "Allele" in ann_fields
            assert "Annotation" in ann_fields
            assert "Annotation_Impact" in ann_fields
            assert "Gene_Name" in ann_fields
            parser.close()
        finally:
            vcf_path.unlink()

    def test_ann_fields_default_when_no_description(self):
        """Default ANN fields used when description lacks format spec."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="SnpEff annotations">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            ann_fields = parser.header_parser.ann_fields

            assert len(ann_fields) == len(VCFHeaderParser.ANN_FIELDS)
            parser.close()
        finally:
            vcf_path.unlink()

    def test_no_ann_fields_when_absent(self):
        """No ANN fields when ANN INFO not present."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	DP=50
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            ann_fields = parser.header_parser.ann_fields

            assert len(ann_fields) == 0
            parser.close()
        finally:
            vcf_path.unlink()


class TestANNValueParsing:
    """Test parsing of ANN field values."""

    @pytest.fixture
    def variant_parser(self):
        """Create a VariantParser with header context."""
        return VariantParser(header_parser=None, normalize=False, human_genome=True)

    def test_parse_single_ann_annotation(self, variant_parser):
        """Single ANN annotation is parsed correctly."""
        ann_value = "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Gene_Name") == "TP53"
        assert result.get("Annotation") == "missense_variant"
        assert result.get("Annotation_Impact") == "MODERATE"
        assert result.get("HGVS.c") == "c.817C>G"
        assert result.get("HGVS.p") == "p.Pro273Arg"

    def test_parse_ann_selects_worst_impact(self, variant_parser):
        """Worst impact annotation is selected from multiple."""
        ann_value = (
            "G|downstream_gene_variant|MODIFIER|WRAP53|ENSG00000141499|transcript|ENST00000357449|protein_coding||||||,"
            "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg||||,"
            "G|stop_gained|HIGH|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Trp273Ter||||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Annotation_Impact") == "HIGH"
        assert result.get("Annotation") == "stop_gained"

    def test_parse_ann_filters_by_allele(self, variant_parser):
        """ANN annotation is filtered to match correct allele."""
        ann_value = (
            "T|missense_variant|MODERATE|GENE1|ENSG00000001|transcript|ENST00000001|protein_coding||||||,"
            "G|stop_gained|HIGH|GENE2|ENSG00000002|transcript|ENST00000002|protein_coding||||||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result_t = variant_parser._parse_ann(ann_value, fields, "T")
        result_g = variant_parser._parse_ann(ann_value, fields, "G")

        assert result_t is not None
        assert result_t.get("Gene_Name") == "GENE1"
        assert result_t.get("Annotation_Impact") == "MODERATE"

        assert result_g is not None
        assert result_g.get("Gene_Name") == "GENE2"
        assert result_g.get("Annotation_Impact") == "HIGH"

    def test_parse_ann_handles_missing_fields(self, variant_parser):
        """ANN with fewer fields than expected is handled gracefully."""
        ann_value = "G|missense_variant|MODERATE|TP53"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Allele") == "G"
        assert result.get("Annotation") == "missense_variant"
        assert result.get("Annotation_Impact") == "MODERATE"
        assert result.get("Gene_Name") == "TP53"

    def test_parse_ann_returns_none_for_no_match(self, variant_parser):
        """Returns None when no annotation matches allele."""
        ann_value = "T|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305||||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is None

    def test_parse_ann_handles_empty_fields(self, variant_parser):
        """Empty ANN field values are handled."""
        ann_value = "G|missense_variant|MODERATE|TP53||||||||||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Gene_Name") == "TP53"
        assert result.get("Gene_ID") == ""


class TestANNIntegrationWithParser:
    """Test ANN parsing integration with full VCF parsing."""

    def test_ann_extracts_gene_to_record(self):
        """Gene name from ANN populates record.gene."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr17	7578406	.	C	G	100	PASS	ANN=G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 1
            assert records[0].gene == "TP53"
            assert records[0].consequence == "missense_variant"
            assert records[0].impact == "MODERATE"
        finally:
            vcf_path.unlink()

    def test_ann_extracts_hgvs_to_record(self):
        """HGVS annotations from ANN populate record fields."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr17	7578406	.	C	G	100	PASS	ANN=G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 1
            assert records[0].hgvs_c == "c.817C>G"
            assert records[0].hgvs_p == "p.Pro273Arg"
        finally:
            vcf_path.unlink()

    def test_ann_extracts_transcript_to_record(self):
        """Transcript ID from ANN populates record.transcript."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr17	7578406	.	C	G	100	PASS	ANN=G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 1
            assert records[0].transcript == "ENST00000269305"
        finally:
            vcf_path.unlink()

    def test_csq_takes_precedence_over_ann(self):
        """CSQ annotations take precedence when both present."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature">
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr17	7578406	.	C	G	100	PASS	CSQ=G|stop_gained|HIGH|BRCA1|ENSG00000012048|Transcript|ENST00000357654;ANN=G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 1
            assert records[0].gene == "BRCA1"
            assert records[0].consequence == "stop_gained"
            assert records[0].impact == "HIGH"
        finally:
            vcf_path.unlink()


class TestANNImpactRanking:
    """Test impact ranking in ANN parsing."""

    @pytest.fixture
    def variant_parser(self):
        return VariantParser(header_parser=None, normalize=False, human_genome=True)

    def test_high_impact_selected_over_moderate(self, variant_parser):
        """HIGH impact selected over MODERATE."""
        ann_value = (
            "G|missense_variant|MODERATE|GENE1|ENSG001|transcript|ENST001||||||,"
            "G|stop_gained|HIGH|GENE1|ENSG001|transcript|ENST002||||||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result.get("Annotation_Impact") == "HIGH"

    def test_moderate_impact_selected_over_low(self, variant_parser):
        """MODERATE impact selected over LOW."""
        ann_value = (
            "G|synonymous_variant|LOW|GENE1|ENSG001|transcript|ENST001||||||,"
            "G|missense_variant|MODERATE|GENE1|ENSG001|transcript|ENST002||||||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result.get("Annotation_Impact") == "MODERATE"

    def test_low_impact_selected_over_modifier(self, variant_parser):
        """LOW impact selected over MODIFIER."""
        ann_value = (
            "G|intron_variant|MODIFIER|GENE1|ENSG001|transcript|ENST001||||||,"
            "G|synonymous_variant|LOW|GENE1|ENSG001|transcript|ENST002||||||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result.get("Annotation_Impact") == "LOW"

    def test_modifier_selected_when_only_option(self, variant_parser):
        """MODIFIER selected when it's the only option."""
        ann_value = "G|intron_variant|MODIFIER|GENE1|ENSG001|transcript|ENST001||||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result.get("Annotation_Impact") == "MODIFIER"
