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


class TestANNEdgeCases:
    """Test edge cases from SnpEff documentation and GitHub issues.

    Sources:
    - SnpEff ANN format spec (16-field format)
    - SnpEff GitHub Issues: #158, #218, #122, #255
    - pcingola/SnpEff test files
    """

    @pytest.fixture
    def variant_parser(self):
        return VariantParser(header_parser=None, normalize=False, human_genome=True)

    def test_combined_effects_ampersand_separator(self, variant_parser):
        """Combined effects using & separator are parsed correctly.

        Source: SnpEff ANN format - Multiple effects on same transcript use &
        Example from: compass_artifact guidance doc lines 107-109
        """
        ann_value = "A|splice_donor_variant&intron_variant|HIGH|BRCA1|ENSG00000012048|transcript|ENST00000357654|protein_coding|10/22|c.4096+1G>A|||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        assert "splice_donor_variant" in result.get("Annotation")
        assert "intron_variant" in result.get("Annotation")
        assert result.get("Annotation_Impact") == "HIGH"

    def test_combined_effects_with_plus_sign_legacy(self, variant_parser):
        """Combined effects with + separator (legacy format) handled.

        Source: SnpEff tests/unity/vcf/test_vcf_ann_plus_sign.vcf
        Uses + instead of & in older versions.
        """
        ann_value = "|5_prime_UTR_truncation+exon_loss_variant|MODERATE|GRMZM2G384255|GRMZM2G384255|transcript|GRMZM2G384255_T01|Coding|1/1|c.-6_-1delTTACCC||||||INFO_REALIGN_3_PRIME"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "")

        assert result is not None
        assert "5_prime_UTR_truncation" in result.get("Annotation")
        assert "exon_loss_variant" in result.get("Annotation")

    def test_intergenic_variant_empty_gene_fields(self, variant_parser):
        """Intergenic variants with empty gene fields are handled.

        Source: SnpEff ANN format - Gene fields can be empty for intergenic
        Example from: compass_artifact guidance doc lines 111-114
        """
        ann_value = "A|intergenic_region|MODIFIER|CHR_START-DDX11L1|CHR_START-ENSG00000223972|intergenic_region|CHR_START-ENSG00000223972|||n.2->T|||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        assert result.get("Annotation") == "intergenic_region"
        assert result.get("Annotation_Impact") == "MODIFIER"
        assert "CHR_START" in result.get("Gene_Name", "")

    def test_structural_variant_gene_fusion(self, variant_parser):
        """Structural variant gene fusion with & in gene names is parsed.

        Source: compass_artifact guidance doc lines 116-119
        Gene fusions are represented as GENE1&GENE2.
        """
        ann_value = "<DUP>|gene_fusion|HIGH|FGFR3&TACC3|ENSG00000068078&ENSG00000013810|gene_variant|ENSG00000013810|||||||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "<DUP>")

        assert result is not None
        assert "FGFR3" in result.get("Gene_Name", "")
        assert "TACC3" in result.get("Gene_Name", "")
        assert result.get("Annotation") == "gene_fusion"
        assert result.get("Annotation_Impact") == "HIGH"

    def test_warning_codes_in_field_16(self, variant_parser):
        """Warning codes in field 16 are preserved.

        Source: compass_artifact guidance doc lines 131-138
        Common warnings: WARNING_REF_DOES_NOT_MATCH_GENOME, INFO_REALIGN_3_PRIME, etc.
        """
        ann_value = "T|missense_variant|MODERATE|MSH6|ENSG00000116062|transcript|ENST00000234420|protein_coding|9/9|c.4002-10delT||||||INFO_REALIGN_3_PRIME"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "T")

        assert result is not None
        warning_field = result.get("ERRORS/WARNINGS/INFO", "")
        assert "INFO_REALIGN_3_PRIME" in warning_field

    def test_warning_ref_does_not_match_genome(self, variant_parser):
        """WARNING_REF_DOES_NOT_MATCH_GENOME is captured.

        Source: SnpEff cancer.ann.vcf example
        Critical warning indicating database mismatch.
        """
        ann_value = "G-C|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Leu1?|1/918|1/918|1/305||WARNING_REF_DOES_NOT_MATCH_GENOME"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G-C")

        assert result is not None
        warning_field = result.get("ERRORS/WARNINGS/INFO", "")
        assert "WARNING_REF_DOES_NOT_MATCH_GENOME" in warning_field

    def test_warning_transcript_no_start_codon(self, variant_parser):
        """WARNING_TRANSCRIPT_NO_START_CODON is captured.

        Source: SnpEff test.chr22.ann.filter_missense_any_TRMT2A.vcf
        """
        ann_value = "A|missense_variant|MODERATE|TRMT2A|ENSG00000099899|transcript|ENST00000444845|protein_coding|4/4|c.430C>T|p.Pro144Ser|430/739|430/477|144/158||WARNING_TRANSCRIPT_NO_START_CODON"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        warning_field = result.get("ERRORS/WARNINGS/INFO", "")
        assert "WARNING_TRANSCRIPT_NO_START_CODON" in warning_field

    def test_warning_transcript_incomplete(self, variant_parser):
        """WARNING_TRANSCRIPT_INCOMPLETE is captured.

        Source: SnpEff test files
        """
        ann_value = "A|missense_variant|MODERATE|TRMT2A|ENSG00000099899|transcript|ENST00000444256|protein_coding|3/3|c.382C>T|p.Pro128Ser|384/426|382/424|128/140||WARNING_TRANSCRIPT_INCOMPLETE"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        warning_field = result.get("ERRORS/WARNINGS/INFO", "")
        assert "WARNING_TRANSCRIPT_INCOMPLETE" in warning_field

    def test_many_transcripts_selects_worst_impact(self, variant_parser):
        """With 8+ transcripts, worst impact is selected.

        Source: compass_artifact guidance doc lines 126-129
        Real VCFs often have annotations for many transcripts per variant.
        """
        ann_value = (
            "A|stop_gained|HIGH|NOC2L|ENSG00000188976|transcript|ENST00000327044|protein_coding|7/19|c.706C>T|p.Gln236*|756/2790|706/2250|236/749||,"
            "A|downstream_gene_variant|MODIFIER|NOC2L|ENSG00000188976|transcript|ENST00000487214|processed_transcript||||||4000|,"
            "A|downstream_gene_variant|MODIFIER|NOC2L|ENSG00000188976|transcript|ENST00000469563|processed_transcript||||||3500|,"
            "A|non_coding_exon_variant|MODIFIER|NOC2L|ENSG00000188976|transcript|ENST00000477976|retained_intron|3/5|n.500C>T|||||,"
            "A|intron_variant|MODIFIER|NOC2L|ENSG00000188976|transcript|ENST00000466827|processed_transcript||n.200-50C>T|||||,"
            "A|upstream_gene_variant|MODIFIER|NOC2L|ENSG00000188976|transcript|ENST00000495576|processed_transcript||||||500|,"
            "A|missense_variant|MODERATE|NOC2L|ENSG00000188976|transcript|ENST00000489435|protein_coding|5/12|c.400C>T|p.Pro134Ser||||,"
            "A|synonymous_variant|LOW|NOC2L|ENSG00000188976|transcript|ENST00000496938|protein_coding|3/8|c.300C>T|p.Ala100Ala||||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        assert result.get("Annotation_Impact") == "HIGH"
        assert result.get("Annotation") == "stop_gained"

    def test_compound_allele_cancer_format(self, variant_parser):
        """Compound allele format (G-C) from cancer samples is handled.

        Source: SnpEff cancer.ann.vcf, cancer_pedigree.ann.vcf
        Cancer mode uses compound allele notation like "G-C" for somatic changes.
        """
        ann_value = (
            "G|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Met1?|1/918|1/918|1/305||,"
            "G-C|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Leu1?|1/918|1/918|1/305||WARNING_REF_DOES_NOT_MATCH_GENOME,"
            "C|initiator_codon_variant|LOW|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>C|p.Met1?|1/918|1/918|1/305||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result_g = variant_parser._parse_ann(ann_value, fields, "G")
        result_c = variant_parser._parse_ann(ann_value, fields, "C")

        assert result_g is not None
        assert result_g.get("Annotation_Impact") == "HIGH"

        assert result_c is not None
        assert result_c.get("Annotation") == "initiator_codon_variant"

    def test_empty_allele_field_deletion(self, variant_parser):
        """Deletions with empty Allele field (field 1) are handled.

        Source: SnpEff test.chr22.ann.vcf position 17445640
        Deletions may have empty Allele field.
        """
        ann_value = "|downstream_gene_variant|MODIFIER|GAB4|ENSG00000215568|transcript|ENST00000520505|processed_transcript||n.*170delG|||||1349|"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "")

        assert result is not None
        assert result.get("Annotation") == "downstream_gene_variant"
        assert result.get("Gene_Name") == "GAB4"

    def test_16_field_count_standard(self, variant_parser):
        """Standard ANN annotation has exactly 16 pipe-delimited fields.

        Source: compass_artifact guidance doc line 145
        Every ANN entry must have exactly 16 pipe-delimited subfields.
        """
        ann_value = "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||"
        fields_in_ann = ann_value.split("|")

        assert len(fields_in_ann) == 16

    def test_unknown_effect_type_graceful(self, variant_parser):
        """Unknown effect types don't crash parsing.

        Source: SnpEff GitHub Issue #158
        SnpSift 4.3g cannot parse 'PROTEIN_INTERACTION_LOCUS' from 4.2.
        Parser should handle unknown types gracefully.
        """
        ann_value = "G|PROTEIN_INTERACTION_LOCUS|MODIFIER|GENE1|ENSG001|transcript|ENST001|protein_coding|||||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Annotation") == "PROTEIN_INTERACTION_LOCUS"
        assert result.get("Annotation_Impact") == "MODIFIER"

    def test_intragenic_variant_phantom_annotation(self, variant_parser):
        """Intragenic_variant phantom annotations are handled.

        Source: SnpEff GitHub Issue #218
        When using -onlyTr filter, may get intragenic_variant for overlapping genes.
        """
        ann_value = (
            "C|intron_variant|MODIFIER|MSH6|ENSG00000116062|transcript|ENST00000234420|protein_coding||c.100-50G>C|||||,"
            "C|intragenic_variant|MODIFIER|FBXO11|ENSG00000138081|gene_variant|ENSG00000138081|||n.48033891delA|||||"
        )
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "C")

        assert result is not None

    def test_custom_annotation_format(self, variant_parser):
        """Custom annotations (from SnpEff -interval) are handled.

        Source: SnpEff test.ann.vcf line 6
        Custom annotations use 'custom' as feature type.
        """
        ann_value = "G|custom|MODIFIER|||CUSTOM&my_annotations|MY_ANNOTATION|||||||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Annotation") == "custom"
        assert "my_annotations" in result.get("Feature_Type", "")

    def test_splice_region_with_combined_consequence(self, variant_parser):
        """Splice region combined with other consequence types.

        Source: SnpEff test.chr22.ann.vcf position 17446157
        Example: splice_region_variant&synonymous_variant
        """
        ann_value = "T|splice_region_variant&synonymous_variant|LOW|GAB4|ENSG00000215568|transcript|ENST00000400588|protein_coding|7/10|c.1290C>A|p.Ala430Ala|1398/2630|1290/1725|430/574||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "T")

        assert result is not None
        assert "splice_region_variant" in result.get("Annotation")
        assert "synonymous_variant" in result.get("Annotation")
        assert result.get("Annotation_Impact") == "LOW"

    def test_sequence_feature_annotation(self, variant_parser):
        """Sequence feature annotations (transmembrane_region, etc.) are parsed.

        Source: SnpEff test.chr22.ann.vcf position 17288641
        """
        ann_value = "A|sequence_feature|LOW|XKR3|ENSG00000172967|transmembrane_region:Transmembrane_region|ENST00000331428|protein_coding|2/4|c.323G>T||||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        assert result.get("Annotation") == "sequence_feature"
        assert "transmembrane_region" in result.get("Feature_Type", "")

    def test_nonsense_mediated_decay_transcript(self, variant_parser):
        """Nonsense_mediated_decay transcript biotype is preserved.

        Source: SnpEff test.chr22.ann.vcf
        """
        ann_value = "T|3_prime_UTR_variant|MODIFIER|GAB4|ENSG00000215568|transcript|ENST00000465611|nonsense_mediated_decay|8/9|n.*1681C>T|||||4579|"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "T")

        assert result is not None
        assert result.get("Transcript_BioType") == "nonsense_mediated_decay"

    def test_distance_field_for_upstream_downstream(self, variant_parser):
        """Distance field (field 15) is populated for up/downstream variants.

        Source: SnpEff ANN format spec
        """
        ann_value = "G|upstream_gene_variant|MODIFIER|DDX11L1|ENSG00000223972|transcript|ENST00000456328|processed_transcript||n.-1C>G|||||1400|"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Distance") == "1400"

    def test_rank_field_with_exon_info(self, variant_parser):
        """Rank field (e.g., 10/11) is parsed correctly.

        Source: SnpEff ANN format spec - Rank shows exon/intron number
        """
        ann_value = "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("Rank") == "10/11"

    def test_cdna_cds_aa_position_fields(self, variant_parser):
        """cDNA, CDS, and AA position fields are parsed.

        Source: SnpEff ANN format spec - Fields 12, 13, 14
        """
        ann_value = "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("cDNA.pos/cDNA.length") == "817/2591"
        assert result.get("CDS.pos/CDS.length") == "817/1182"
        assert result.get("AA.pos/AA.length") == "273/393"

    def test_stop_gained_with_star_notation(self, variant_parser):
        """Stop gained with * notation in HGVS.p (p.Trp88*).

        Source: SnpEff test.chr22.ann.vcf position 17073178
        """
        ann_value = "T|stop_gained|HIGH|CCT8L2|ENSG00000198445|transcript|ENST00000359963|protein_coding|1/1|c.263G>A|p.Trp88*|523/2034|263/1674|88/557||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "T")

        assert result is not None
        assert result.get("HGVS.p") == "p.Trp88*"
        assert result.get("Annotation_Impact") == "HIGH"

    def test_stop_lost_with_extension(self, variant_parser):
        """Stop lost with extension notation (p.Ter253Cysext*?).

        Source: SnpEff cancer_pedigree.ann.vcf
        """
        ann_value = "C-A|stop_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.759G>C|p.Ter253Cysext*?|759/918|759/918|253/305||WARNING_REF_DOES_NOT_MATCH_GENOME"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "C-A")

        assert result is not None
        assert "Ter253Cysext" in result.get("HGVS.p", "")

    def test_start_lost_with_question_mark(self, variant_parser):
        """Start lost with ? notation (p.Met1?).

        Source: SnpEff cancer.ann.vcf
        """
        ann_value = "G|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Met1?|1/918|1/918|1/305||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "G")

        assert result is not None
        assert result.get("HGVS.p") == "p.Met1?"

    def test_lof_annotation_captured_in_info(self):
        """LOF (Loss of Function) annotations are captured.

        Source: SnpEff cancer.ann.vcf, cancer_pedigree.ann.vcf
        LOF field format: Gene_Name | Gene_ID | Number_of_transcripts | Percent_affected
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##INFO=<ID=LOF,Number=.,Type=String,Description="Predicted loss of function effects">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	69091	.	A	G	.	PASS	ANN=G|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Met1?|1/918|1/918|1/305||;LOF=(OR4F5|ENSG00000186092|1|1.00)
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=False)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 1
            assert "LOF" in records[0].info
            lof_value = records[0].info.get("LOF")
            assert "OR4F5" in str(lof_value)
        finally:
            vcf_path.unlink()

    def test_nmd_annotation_captured_in_info(self):
        """NMD (Nonsense Mediated Decay) annotations are captured.

        Source: SnpEff documentation
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations">
##INFO=<ID=NMD,Number=.,Type=String,Description="Predicted nonsense mediated decay effects">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	.	PASS	ANN=G|stop_gained|HIGH|GENE1|ENSG001|transcript|ENST001|protein_coding|5/10|c.500C>T|p.Gln167*||||;NMD=(GENE1|ENSG001|3|0.67)
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=False)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 1
            assert "NMD" in records[0].info
        finally:
            vcf_path.unlink()


class TestANNMultiallelicHandling:
    """Test multi-allelic variant handling with ANN annotations."""

    def test_multiallelic_two_alts_separate_annotations(self):
        """Multi-allelic site with two ALTs gets separate annotations per allele.

        Source: compass_artifact guidance doc lines 101-104
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	889455	.	G	A,T	.	.	ANN=A|stop_gained|HIGH|NOC2L|ENSG00000188976|transcript|ENST00000327044|protein_coding|7/19|c.706C>T|p.Gln236*|756/2790|706/2250|236/749||,T|missense_variant|MODERATE|NOC2L|ENSG00000188976|transcript|ENST00000327044|protein_coding|7/19|c.706C>A|p.Gln236Lys|756/2790|706/2250|236/749||
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=False)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 2

            alt_a_record = next((r for r in records if r.alt == "A"), None)
            alt_t_record = next((r for r in records if r.alt == "T"), None)

            assert alt_a_record is not None
            assert alt_a_record.consequence == "stop_gained"
            assert alt_a_record.impact == "HIGH"

            assert alt_t_record is not None
            assert alt_t_record.consequence == "missense_variant"
            assert alt_t_record.impact == "MODERATE"
        finally:
            vcf_path.unlink()

    def test_multiallelic_three_alts_cancer_sample(self):
        """Multi-allelic with 3 ALTs including compound allele (cancer).

        Source: SnpEff cancer.ann.vcf, cancer_pedigree.ann.vcf
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	69091	.	A	C,G	.	PASS	ANN=G|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Met1?|1/918|1/918|1/305||,G-C|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Leu1?|1/918|1/918|1/305||WARNING_REF_DOES_NOT_MATCH_GENOME,C|initiator_codon_variant|LOW|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>C|p.Met1?|1/918|1/918|1/305||
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=False)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 2
            alts = {r.alt for r in records}
            assert alts == {"C", "G"}
        finally:
            vcf_path.unlink()
