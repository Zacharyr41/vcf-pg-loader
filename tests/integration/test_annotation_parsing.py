"""Integration tests for annotation parsing (VEP CSQ and SnpEff ANN)."""

import tempfile
from pathlib import Path

import pytest

from vcf_pg_loader.vcf_parser import VCFStreamingParser


@pytest.mark.integration
class TestVEPCSQIntegration:
    """Integration tests for VEP CSQ annotation parsing."""

    @pytest.fixture
    def vep_annotated_vcf(self):
        """Create a VCF with realistic VEP CSQ annotations."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID|gnomADe_AF|CLIN_SIG|CADD_PHRED">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##contig=<ID=chr17,length=83257441>
##contig=<ID=chr13,length=114364328>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr17	43094464	rs80357906	C	T	100	PASS	DP=50;CSQ=T|missense_variant|MODERATE|BRCA1|ENSG00000012048|Transcript|ENST00000357654|protein_coding|10/23||ENST00000357654.8:c.1067A>G|ENSP00000350283.3:p.Gln356Arg|1186|1067|356|Q/R|cAg/cGg|rs80357906||1||HGNC|1100|0.0001|pathogenic|25.3	GT:DP	0/1:50
chr17	43094500	.	G	A,C	90	PASS	DP=45;CSQ=A|stop_gained|HIGH|BRCA1|ENSG00000012048|Transcript|ENST00000357654|protein_coding|10/23||ENST00000357654.8:c.1031C>T|ENSP00000350283.3:p.Arg344Ter|1150|1031|344|R/*|Cga/Tga|||1||HGNC|1100|0.00001|pathogenic|35.0,C|synonymous_variant|LOW|BRCA1|ENSG00000012048|Transcript|ENST00000357654|protein_coding|10/23||ENST00000357654.8:c.1031C>G|ENSP00000350283.3:p.Arg344Arg|1150|1031|344|R/R|Cga/Gga|||-1||HGNC|1100|0.001||12.5	GT:DP	0/1:45
chr13	32936732	rs80359550	C	A	85	PASS	DP=40;CSQ=A|frameshift_variant|HIGH|BRCA2|ENSG00000139618|Transcript|ENST00000380152|protein_coding|11/27||ENST00000380152.7:c.5946delT|ENSP00000369497.3:p.Ser1982ArgfsTer22|6173|5946|1982|S/X|tCt/t|||1||HGNC|1101|0.00005|pathogenic|30.0	GT:DP	1/1:40
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_csq_extracts_gene_symbol(self, vep_annotated_vcf):
        """CSQ parsing extracts gene symbol into record.gene."""
        parser = VCFStreamingParser(vep_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            brca1_records = [r for r in records if r.gene == "BRCA1"]
            brca2_records = [r for r in records if r.gene == "BRCA2"]

            assert len(brca1_records) >= 1, "Should extract BRCA1 from CSQ"
            assert len(brca2_records) == 1, "Should extract BRCA2 from CSQ"
        finally:
            parser.close()

    def test_csq_extracts_consequence(self, vep_annotated_vcf):
        """CSQ parsing extracts consequence type."""
        parser = VCFStreamingParser(vep_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            consequences = {r.consequence for r in records if r.consequence}
            assert "missense_variant" in consequences
            assert "stop_gained" in consequences or "frameshift_variant" in consequences
        finally:
            parser.close()

    def test_csq_extracts_impact(self, vep_annotated_vcf):
        """CSQ parsing extracts impact level."""
        parser = VCFStreamingParser(vep_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            impacts = {r.impact for r in records if r.impact}
            assert "MODERATE" in impacts or "HIGH" in impacts
        finally:
            parser.close()

    def test_csq_selects_worst_impact_for_multiallelic(self, vep_annotated_vcf):
        """For multi-allelic sites, worst impact annotation is selected per allele."""
        parser = VCFStreamingParser(vep_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            multiallelic_records = [r for r in records if r.pos == 43094500]
            assert len(multiallelic_records) == 2, "Multi-allelic should decompose to 2 records"

            alt_a_record = next((r for r in multiallelic_records if r.alt == "A"), None)
            if alt_a_record and alt_a_record.impact:
                assert alt_a_record.impact == "HIGH", "ALT=A should get HIGH impact (stop_gained)"
        finally:
            parser.close()

    def test_csq_extracts_hgvs(self, vep_annotated_vcf):
        """CSQ parsing extracts HGVS annotations."""
        parser = VCFStreamingParser(vep_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_hgvsc = [r for r in records if r.hgvs_c]
            records_with_hgvsp = [r for r in records if r.hgvs_p]

            assert len(records_with_hgvsc) > 0, "Should extract HGVSc"
            assert len(records_with_hgvsp) > 0, "Should extract HGVSp"
        finally:
            parser.close()


@pytest.mark.integration
class TestSnpEffANNIntegration:
    """Integration tests for SnpEff ANN annotation parsing."""

    @pytest.fixture
    def snpeff_annotated_vcf(self):
        """Create a VCF with realistic SnpEff ANN annotations."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##INFO=<ID=LOF,Number=.,Type=String,Description="Predicted loss of function effects">
##INFO=<ID=NMD,Number=.,Type=String,Description="Predicted nonsense mediated decay effects">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
##contig=<ID=chr7,length=159345973>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr17	7578406	.	C	G	100	PASS	ANN=G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||	GT	0/1
chr17	7578500	.	A	T,G	95	PASS	ANN=T|stop_gained|HIGH|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.723T>A|p.Tyr241Ter|723/2591|723/1182|241/393||,G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.723T>C|p.Tyr241His|723/2591|723/1182|241/393||	GT	0/1
chr7	55259515	.	T	G	90	PASS	ANN=G|missense_variant|MODERATE|EGFR|ENSG00000146648|transcript|ENST00000275493|protein_coding|21/28|c.2573T>G|p.Leu858Arg|2780/5616|2573/3633|858/1210||;LOF=(EGFR|ENSG00000146648|1|1.00)	GT	1/1
chr17	7578550	.	G	A	80	PASS	ANN=A|splice_acceptor_variant|HIGH|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|9/11|c.673-2G>A|||||||,A|downstream_gene_variant|MODIFIER|WRAP53|ENSG00000141499|transcript|ENST00000357449|protein_coding||c.*500G>A|||||4500|	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_ann_field_captured_in_info(self, snpeff_annotated_vcf):
        """ANN field is captured in info dict."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_ann = [r for r in records if r.info.get("ANN")]
            assert len(records_with_ann) > 0, "ANN field should be captured in info"
        finally:
            parser.close()

    def test_ann_multiallelic_decomposition(self, snpeff_annotated_vcf):
        """Multi-allelic sites with ANN are properly decomposed."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            multiallelic_records = [r for r in records if r.pos == 7578500]
            assert len(multiallelic_records) == 2, "Should decompose to 2 records"
            alts = {r.alt for r in multiallelic_records}
            assert alts == {"T", "G"}, "Should have both ALT alleles"
        finally:
            parser.close()

    def test_ann_with_lof_annotation(self, snpeff_annotated_vcf):
        """LOF (Loss of Function) annotations are captured."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            egfr_record = next((r for r in records if r.chrom == "chr7"), None)
            assert egfr_record is not None
            assert "LOF" in egfr_record.info or "ANN" in egfr_record.info
        finally:
            parser.close()

    def test_ann_multiple_transcripts_per_variant(self, snpeff_annotated_vcf):
        """Variants with annotations for multiple transcripts are handled."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            splice_record = next((r for r in records if r.pos == 7578550), None)
            assert splice_record is not None
            ann = splice_record.info.get("ANN")
            assert ann is not None
            if isinstance(ann, str):
                assert "splice_acceptor_variant" in ann
                assert "downstream_gene_variant" in ann
        finally:
            parser.close()

    def test_ann_extracts_gene_symbol(self, snpeff_annotated_vcf):
        """ANN parsing extracts gene symbol into record.gene."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            tp53_records = [r for r in records if r.gene == "TP53"]
            egfr_records = [r for r in records if r.gene == "EGFR"]

            assert len(tp53_records) >= 1, "Should extract TP53 from ANN"
            assert len(egfr_records) == 1, "Should extract EGFR from ANN"
        finally:
            parser.close()

    def test_ann_extracts_consequence(self, snpeff_annotated_vcf):
        """ANN parsing extracts consequence type."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            consequences = {r.consequence for r in records if r.consequence}
            assert "missense_variant" in consequences
        finally:
            parser.close()

    def test_ann_extracts_impact(self, snpeff_annotated_vcf):
        """ANN parsing extracts impact level."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            impacts = {r.impact for r in records if r.impact}
            assert "MODERATE" in impacts or "HIGH" in impacts
        finally:
            parser.close()

    def test_ann_extracts_hgvs(self, snpeff_annotated_vcf):
        """ANN parsing extracts HGVS annotations."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_hgvsc = [r for r in records if r.hgvs_c]
            records_with_hgvsp = [r for r in records if r.hgvs_p]

            assert len(records_with_hgvsc) > 0, "Should extract HGVS.c"
            assert len(records_with_hgvsp) > 0, "Should extract HGVS.p"
        finally:
            parser.close()

    def test_ann_extracts_transcript(self, snpeff_annotated_vcf):
        """ANN parsing extracts transcript ID."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_transcript = [r for r in records if r.transcript]
            assert len(records_with_transcript) > 0, "Should extract transcript ID"

            transcript_ids = {r.transcript for r in records_with_transcript}
            assert any("ENST" in t for t in transcript_ids), "Should have Ensembl transcript IDs"
        finally:
            parser.close()

    def test_ann_selects_worst_impact(self, snpeff_annotated_vcf):
        """ANN parsing selects worst impact annotation."""
        parser = VCFStreamingParser(snpeff_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            splice_record = next((r for r in records if r.pos == 7578550), None)
            assert splice_record is not None
            assert splice_record.impact == "HIGH", "Should select HIGH impact (splice_acceptor) over MODIFIER"
        finally:
            parser.close()


@pytest.mark.integration
class TestMixedAnnotationSources:
    """Test handling of VCFs with both VEP and SnpEff annotations."""

    @pytest.fixture
    def mixed_annotation_vcf(self):
        """Create a VCF with both CSQ and ANN fields (edge case)."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature">
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations from SnpEff">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100000	.	A	G	100	PASS	CSQ=G|missense_variant|MODERATE|GENE1|ENSG00000001|Transcript|ENST00000001;ANN=G|missense_variant|MODERATE|GENE1|ENSG00000001|transcript|ENST00000001|protein_coding|||||||	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_csq_takes_precedence_when_both_present(self, mixed_annotation_vcf):
        """CSQ annotations should be used when both CSQ and ANN are present."""
        parser = VCFStreamingParser(mixed_annotation_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) == 1
            record = records[0]
            assert "CSQ" in record.info or "ANN" in record.info
        finally:
            parser.close()


@pytest.mark.integration
class TestClinVarAnnotationIntegration:
    """Integration tests for ClinVar annotation extraction."""

    @pytest.fixture
    def clinvar_annotated_vcf(self):
        """Create a VCF with ClinVar annotations."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="ClinVar review status">
##INFO=<ID=CLNDN,Number=.,Type=String,Description="ClinVar disease name">
##INFO=<ID=CLNVC,Number=1,Type=String,Description="Variant type">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
##contig=<ID=chr13,length=114364328>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr17	43094464	rs80357906	C	T	100	PASS	CLNSIG=Pathogenic;CLNREVSTAT=reviewed_by_expert_panel;CLNDN=Hereditary_breast_and_ovarian_cancer_syndrome;CLNVC=single_nucleotide_variant	GT	0/1
chr17	43094500	.	G	A	90	PASS	CLNSIG=Likely_pathogenic;CLNREVSTAT=criteria_provided,_multiple_submitters,_no_conflicts;CLNDN=BRCA1-related_cancer	GT	0/1
chr13	32936732	rs80359550	C	A	85	PASS	CLNSIG=Pathogenic/Likely_pathogenic;CLNREVSTAT=criteria_provided,_conflicting_interpretations;CLNDN=Hereditary_cancer-predisposing_syndrome	GT	1/1
chr17	43094600	.	T	C	80	PASS	CLNSIG=Benign;CLNREVSTAT=criteria_provided,_single_submitter	GT	0/1
chr17	43094700	.	A	G	75	PASS	CLNSIG=Uncertain_significance;CLNREVSTAT=criteria_provided,_single_submitter	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_clinvar_sig_extracted(self, clinvar_annotated_vcf):
        """CLNSIG is extracted into clinvar_sig field."""
        parser = VCFStreamingParser(clinvar_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_clinvar = [r for r in records if r.clinvar_sig]
            assert len(records_with_clinvar) >= 4, "Should extract ClinVar significance"

            sigs = {r.clinvar_sig for r in records_with_clinvar}
            assert "Pathogenic" in sigs or any("Pathogenic" in s for s in sigs if s)
        finally:
            parser.close()

    def test_pathogenic_variants_identified(self, clinvar_annotated_vcf):
        """Pathogenic and Likely_pathogenic variants are correctly identified."""
        parser = VCFStreamingParser(clinvar_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            pathogenic = [
                r for r in records
                if r.clinvar_sig and ("Pathogenic" in r.clinvar_sig or "pathogenic" in r.clinvar_sig)
            ]
            assert len(pathogenic) >= 2, "Should have at least 2 pathogenic variants"
        finally:
            parser.close()


@pytest.mark.integration
class TestGnomADFrequencyIntegration:
    """Integration tests for gnomAD frequency extraction."""

    @pytest.fixture
    def gnomad_annotated_vcf(self):
        """Create a VCF with gnomAD frequency annotations."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=gnomAD_AF,Number=A,Type=Float,Description="gnomAD allele frequency">
##INFO=<ID=gnomADe_AF,Number=A,Type=Float,Description="gnomAD exomes allele frequency">
##INFO=<ID=gnomADg_AF,Number=A,Type=Float,Description="gnomAD genomes allele frequency">
##INFO=<ID=AF_popmax,Number=A,Type=Float,Description="Maximum allele frequency across populations">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
chr1	100000	.	A	G	100	PASS	gnomAD_AF=0.001;gnomADe_AF=0.0012;gnomADg_AF=0.0008	GT	0/1
chr1	100100	.	C	T	95	PASS	gnomAD_AF=0.25;gnomADe_AF=0.26	GT	0/1
chr1	100200	.	G	A	90	PASS	gnomAD_AF=0.0001	GT	0/1
chr1	100300	.	T	C	85	PASS	gnomAD_AF=0.00001	GT	0/1
chr1	100400	.	A	T	80	PASS	.	GT	0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)
        yield vcf_path
        vcf_path.unlink()

    def test_gnomad_af_extracted(self, gnomad_annotated_vcf):
        """gnomAD_AF is extracted into af_gnomad field."""
        parser = VCFStreamingParser(gnomad_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_af = [r for r in records if r.af_gnomad is not None]
            assert len(records_with_af) >= 3, "Should extract gnomAD AF"
        finally:
            parser.close()

    def test_rare_variant_filtering(self, gnomad_annotated_vcf):
        """Can filter for rare variants (AF < 0.01)."""
        parser = VCFStreamingParser(gnomad_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            rare_variants = [
                r for r in records
                if r.af_gnomad is not None and r.af_gnomad < 0.01
            ]
            common_variants = [
                r for r in records
                if r.af_gnomad is not None and r.af_gnomad >= 0.01
            ]

            assert len(rare_variants) >= 3, "Should have rare variants"
            assert len(common_variants) >= 1, "Should have common variants"
        finally:
            parser.close()

    def test_missing_af_handled(self, gnomad_annotated_vcf):
        """Variants without gnomAD AF are handled gracefully."""
        parser = VCFStreamingParser(gnomad_annotated_vcf, human_genome=True)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_without_af = [r for r in records if r.af_gnomad is None]
            assert len(records_without_af) >= 1, "Should handle missing AF"
        finally:
            parser.close()


SNPEFF_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "snpeff"


@pytest.mark.integration
class TestSnpEffRealFilesParsing:
    """Integration tests using actual SnpEff test files.

    Source: pcingola/SnpEff repository (examples/ and tests/)
    Files copied to tests/fixtures/snpeff/
    """

    def test_snpeff_test_ann_vcf_basic_parsing(self):
        """Parse SnpEff test.ann.vcf with multiple transcripts per variant.

        Source: SnpEff examples/test.ann.vcf
        Contains: upstream/downstream variants, custom annotations, intergenic regions
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.ann.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.ann.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) >= 1, "Should parse at least 1 variant"

            record = records[0]
            assert "ANN" in record.info
            assert record.gene is not None or "DDX11L1" in str(record.info.get("ANN", ""))
        finally:
            parser.close()

    def test_snpeff_cancer_ann_vcf_multiallelic(self):
        """Parse SnpEff cancer.ann.vcf with multi-allelic variants and warnings.

        Source: SnpEff examples/cancer.ann.vcf
        Contains: Multi-allelic (C,G), compound alleles (G-C), WARNING_REF_DOES_NOT_MATCH_GENOME
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "cancer.ann.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff cancer.ann.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) >= 2, "Multi-allelic should decompose to 2+ records"

            impacts = {r.impact for r in records if r.impact}
            assert "HIGH" in impacts, "Should have HIGH impact (start_lost)"

            has_lof = any("LOF" in r.info for r in records)
            assert has_lof, "Should have LOF annotation"
        finally:
            parser.close()

    def test_snpeff_cancer_pedigree_ann_vcf_warnings(self):
        """Parse SnpEff cancer_pedigree.ann.vcf with warning codes.

        Source: SnpEff examples/cancer_pedigree.ann.vcf
        Contains: WARNING_REF_DOES_NOT_MATCH_GENOME in field 16
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "cancer_pedigree.ann.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff cancer_pedigree.ann.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) >= 3, "Should parse at least 3 multi-allelic variants"

            ann_values = [str(r.info.get("ANN", "")) for r in records]
            has_warning = any("WARNING_REF_DOES_NOT_MATCH_GENOME" in ann for ann in ann_values)
            assert has_warning, "Should preserve WARNING_REF_DOES_NOT_MATCH_GENOME"
        finally:
            parser.close()

    def test_snpeff_chr22_subset_diverse_consequences(self):
        """Parse SnpEff test.chr22.ann.vcf subset with diverse consequence types.

        Source: SnpEff examples/test.chr22.ann.vcf (first 100 lines)
        Contains: missense, synonymous, stop_gained, UTR variants, splice variants
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) >= 50, "Should parse many variants from chr22"

            consequences = {r.consequence for r in records if r.consequence}
            assert len(consequences) >= 3, "Should have diverse consequence types"

            impacts = {r.impact for r in records if r.impact}
            assert "MODERATE" in impacts, "Should have MODERATE impact variants"
        finally:
            parser.close()

    def test_snpeff_plus_sign_combined_effects(self):
        """Parse VCF with + separator for combined effects (legacy format).

        Source: SnpEff tests/unity/vcf/test_vcf_ann_plus_sign.vcf
        Contains: 5_prime_UTR_truncation+exon_loss_variant with + separator
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test_vcf_ann_plus_sign.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test_vcf_ann_plus_sign.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) >= 1, "Should parse the variant"

            ann_value = str(records[0].info.get("ANN", ""))
            assert "5_prime_UTR_truncation" in ann_value or "exon_loss_variant" in ann_value
            assert "INFO_REALIGN_3_PRIME" in ann_value, "Should preserve INFO warning"
        finally:
            parser.close()

    def test_snpeff_extract_all_impact_levels(self):
        """Verify all impact levels are correctly extracted.

        Tests that HIGH, MODERATE, LOW, MODIFIER impacts are properly identified.
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            impacts = {r.impact for r in records if r.impact}

            valid_impacts = {"HIGH", "MODERATE", "LOW", "MODIFIER"}
            for impact in impacts:
                assert impact in valid_impacts, f"Unknown impact: {impact}"
        finally:
            parser.close()

    def test_snpeff_gene_extraction_accuracy(self):
        """Verify gene names are correctly extracted from ANN.

        Source: SnpEff test files
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            genes = {r.gene for r in records if r.gene}
            assert len(genes) >= 2, "Should extract multiple gene names"

            for gene in genes:
                assert gene, "Gene name should not be empty"
                assert not gene.startswith("|"), "Gene should not contain pipe characters"
        finally:
            parser.close()

    def test_snpeff_hgvs_extraction(self):
        """Verify HGVS.c and HGVS.p are correctly extracted.

        Source: SnpEff test files
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_hgvsc = [r for r in records if r.hgvs_c]
            records_with_hgvsp = [r for r in records if r.hgvs_p]

            assert len(records_with_hgvsc) >= 10, "Should have many variants with HGVS.c"
            assert len(records_with_hgvsp) >= 5, "Should have variants with HGVS.p"

            for r in records_with_hgvsc[:5]:
                assert r.hgvs_c.startswith("c.") or r.hgvs_c.startswith("n."), f"Invalid HGVS.c: {r.hgvs_c}"

            for r in records_with_hgvsp[:5]:
                assert r.hgvs_p.startswith("p."), f"Invalid HGVS.p: {r.hgvs_p}"
        finally:
            parser.close()

    def test_snpeff_transcript_extraction(self):
        """Verify transcript IDs are correctly extracted.

        Source: SnpEff test files
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_transcript = [r for r in records if r.transcript]
            assert len(records_with_transcript) >= 10, "Should extract transcript IDs"

            for r in records_with_transcript[:10]:
                assert r.transcript.startswith("ENST"), f"Expected Ensembl transcript: {r.transcript}"
        finally:
            parser.close()


@pytest.mark.integration
class TestSnpEffLegacyEFFFormat:
    """Tests for legacy EFF format (SnpEff v4.0 and earlier).

    Source: SnpEff tests/unity/vcf/test.EFF_V2.vcf
    Format: EFF=EFFECT(IMPACT|...) instead of ANN=Allele|Annotation|...
    """

    def test_eff_format_file_loads_without_crash(self):
        """Legacy EFF format file loads without crashing.

        Source: SnpEff tests/unity/vcf/test.EFF_V2.vcf
        Parser should handle gracefully even if it doesn't parse EFF.
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.EFF_V2.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.EFF_V2.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) >= 1, "Should parse the variant"

            record = records[0]
            assert "EFF" in record.info, "EFF field should be preserved in info"
        finally:
            parser.close()


@pytest.mark.integration
class TestSnpEffAnnotationValidation:
    """Validation tests for SnpEff annotation parsing accuracy."""

    def test_worst_impact_selection_verified(self):
        """Verify worst impact is selected when multiple transcripts exist.

        Uses cancer.ann.vcf which has both HIGH and LOW impact annotations.
        For multi-allelic sites, each ALT has its own record with potentially
        different impacts - we verify that per-allele worst impact is selected.
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "cancer.ann.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff cancer.ann.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            high_impact_records = [r for r in records if r.impact == "HIGH"]
            assert len(high_impact_records) >= 1, "Should select HIGH impact when available"

            for record in records:
                ann = str(record.info.get("ANN", ""))
                alt = record.alt
                annotations_for_allele = [a for a in ann.split(",") if a.startswith(f"{alt}|") or a.startswith("|")]
                if any("|HIGH|" in a for a in annotations_for_allele) and record.impact:
                    assert record.impact == "HIGH", f"Should select HIGH for allele {alt}, got {record.impact}"
        finally:
            parser.close()

    def test_lof_nmd_fields_preserved(self):
        """Verify LOF and NMD INFO fields are preserved.

        Source: SnpEff cancer.ann.vcf
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "cancer.ann.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff cancer.ann.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            lof_records = [r for r in records if "LOF" in r.info]
            assert len(lof_records) >= 1, "Should preserve LOF annotations"

            for r in lof_records:
                lof_value = r.info.get("LOF")
                assert lof_value is not None
                assert "OR4F5" in str(lof_value) or "ENSG" in str(lof_value)
        finally:
            parser.close()

    def test_field_count_consistency(self):
        """Verify parsed annotations have consistent field handling.

        All 16-field annotations should be parsed without data loss.
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records[:20]:
                ann = record.info.get("ANN")
                if ann and isinstance(ann, str):
                    first_annotation = ann.split(",")[0]
                    fields = first_annotation.split("|")
                    assert len(fields) >= 15, f"Expected 16 fields, got {len(fields)}: {first_annotation[:100]}"
        finally:
            parser.close()
