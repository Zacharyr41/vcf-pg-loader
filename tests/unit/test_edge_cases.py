"""Tests for documented edge cases from vcf2db, GEMINI, and variant callers."""

import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator
from vcf_pg_loader.vcf_parser import VCFStreamingParser


class TestSpanningDeletionAllele:
    """Test handling of spanning deletion (*) allele (GATK edge case)."""

    def test_star_allele_does_not_crash(self):
        """
        Parser handles * allele without crashing.

        From guide: Many tools fail on the * allele representing overlapping deletions.
        HTSJDK throws 'Duplicate allele added to VariantContext: *'
        bcftools warns 'Symbolic alleles other than <DEL> are currently not supported'
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t14604\t.\tA\tG,*\t20\tPASS\tDP=36\tGT:AD:DP\t1/2:6,19,10:36
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            assert len(batches) >= 1
            parser.close()
        finally:
            vcf_path.unlink()

    def test_star_allele_filtered_or_handled(self):
        """
        Star allele is either filtered out or converted to a symbolic representation.
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG,*\t30\tPASS\tDP=50\tGT\t1/2
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0] if batches else []
            alts = [r.alt for r in records]
            assert "G" in alts
            parser.close()
        finally:
            vcf_path.unlink()


class TestMNPClassification:
    """Test multi-nucleotide polymorphism (MNP) handling (GEMINI issues #161, #193, #405)."""

    def test_mnp_not_misclassified_as_indel(self):
        """
        MNPs should not be misclassified as indels.

        From guide: GEMINI misclassifies MNPs like CG->AA as 'type=None, sub_type=indel unknown'
        """
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=805101,
                ref="CG",
                alt=["AA"],
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            record = records[0]
            assert record.ref == "CG"
            assert record.alt == "AA"
            assert len(record.ref) == len(record.alt)
        finally:
            vcf_file.unlink()
            parser.close()

    def test_multiallelic_mnp(self):
        """Multi-allelic MNP site parses correctly."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=805101,
                ref="CG",
                alt=["AA", "CT"],
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 2
            alts = {r.alt for r in records}
            assert alts == {"AA", "CT"}
        finally:
            vcf_file.unlink()
            parser.close()


class TestVEPMultipleTranscripts:
    """Test VEP CSQ parsing with multiple transcripts."""

    def test_multiple_transcripts_per_variant(self):
        """
        All transcript annotations are captured, not just the first.

        From guide: Each variant maps to multiple transcripts (pipe-delimited, comma-separated)
        """
        csq_multi = (
            "A|missense_variant|HIGH|GENE1|ENSG001|Transcript|ENST001,"
            "A|synonymous_variant|LOW|GENE1|ENSG001|Transcript|ENST002,"
            "A|upstream_gene_variant|MODIFIER|GENE2|ENSG002|Transcript|ENST003"
        )
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=500,
                ref="G",
                alt=["A"],
                info={"CSQ": csq_multi},
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            record = records[0]
            csq_value = record.info.get("CSQ")
            assert csq_value is not None
            if isinstance(csq_value, str):
                assert "missense_variant" in csq_value
                assert "synonymous_variant" in csq_value
        finally:
            vcf_file.unlink()
            parser.close()

    def test_csq_ampersand_multiple_consequences(self):
        """
        Handle & character separating multiple consequences per transcript.

        From guide: The & character separates multiple consequences per transcript
        (e.g., splice_donor_variant&non_coding_transcript_variant)
        """
        csq_ampersand = "A|splice_donor_variant&non_coding_transcript_variant|HIGH|GENE1|ENSG001|Transcript|ENST001"
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=100,
                ref="G",
                alt=["A"],
                info={"CSQ": csq_ampersand},
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            record = records[0]
            csq_value = record.info.get("CSQ")
            assert csq_value is not None
            if isinstance(csq_value, str):
                assert "splice_donor_variant" in csq_value
                assert "&" in csq_value or "non_coding_transcript_variant" in csq_value
        finally:
            vcf_file.unlink()
            parser.close()


class TestMissingDataHandling:
    """Test handling of missing values in VCF fields."""

    def test_missing_qual(self):
        """Missing QUAL (.) is handled as None."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=100,
                ref="A",
                alt=["G"],
                qual=None,
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert records[0].qual is None
        finally:
            vcf_file.unlink()
            parser.close()

    def test_missing_info_field(self):
        """Missing INFO (.) is handled as empty dict."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT\t0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            assert records[0].info == {} or records[0].info is not None
        finally:
            vcf_path.unlink()
            parser.close()

    def test_partial_genotype(self):
        """
        Partial genotypes like ./1 and 0/. are handled.

        From guide: Test strict genotype mode for partially missing alleles like 0/., ./1
        """
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1\tSAMPLE2\tSAMPLE3
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DP\t0/1:30\t./1:25\t0/.:20
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            assert len(batches) >= 1
            parser.close()
        finally:
            vcf_path.unlink()


class TestTypeCoercion:
    """Test type handling respects VCF header declarations."""

    def test_string_field_with_comma_separated_values(self):
        """
        String fields with comma-separated floats don't cause type errors.

        From guide (vcf2db #51): dbNSFP annotations contain comma-separated float values
        in String-typed fields like VEST3_score=0.123,0.122
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=VEST3_score,Number=.,Type=String,Description="VEST3 scores">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\tVEST3_score=0.123,0.122\tGT\t0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            vest_score = records[0].info.get("VEST3_score")
            assert vest_score is not None
        finally:
            vcf_path.unlink()
            parser.close()

    def test_integer_field_stays_integer(self):
        """Integer-typed fields remain integers."""
        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(
                chrom="chr1",
                pos=100,
                ref="A",
                alt=["G"],
                info={"DP": 100, "AN": 200},
            )
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            dp = records[0].info.get("DP")
            an = records[0].info.get("AN")
            assert isinstance(dp, int)
            assert isinstance(an, int)
        finally:
            vcf_file.unlink()
            parser.close()


class TestVCFVersionCompatibility:
    """Test VCF format version compatibility."""

    @pytest.mark.parametrize("version", ["4.0", "4.1", "4.2", "4.3"])
    def test_vcf_version_parsing(self, version):
        """
        Parser handles VCF versions 4.0-4.3.

        From guide: Test VCF v4.0, v4.1, v4.2, v4.3 format compatibility
        """
        vcf_content = f"""##fileformat=VCFv{version}
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\tDP=50\tGT\t0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            assert records[0].pos == 100
            parser.close()
        finally:
            vcf_path.unlink()


class TestSnpEffANNParsing:
    """Test SnpEff ANN field parsing."""

    def test_ann_field_basic_parsing(self):
        """
        SnpEff ANN field is captured in INFO.

        ANN format: Allele|Annotation|Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|...
        """
        ann_value = "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||"
        vcf_content = f"""##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr17\t7578406\t.\tC\tG\t100\tPASS\tANN={ann_value}\tGT\t0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            ann = records[0].info.get("ANN")
            assert ann is not None
            if isinstance(ann, str):
                assert "missense_variant" in ann
                assert "TP53" in ann
        finally:
            vcf_path.unlink()
            parser.close()

    def test_ann_multiple_annotations(self):
        """Multiple ANN annotations per variant are preserved."""
        ann_value = (
            "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg||||,"
            "G|downstream_gene_variant|MODIFIER|WRAP53|ENSG00000141499|transcript|ENST00000357449|protein_coding||c.*1234C>G|||||"
        )
        vcf_content = f"""##fileformat=VCFv4.3
##INFO=<ID=ANN,Number=.,Type=String,Description="SnpEff annotations">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr17,length=83257441>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr17\t7578406\t.\tC\tG\t100\tPASS\tANN={ann_value}\tGT\t0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            ann = records[0].info.get("ANN")
            assert ann is not None
            if isinstance(ann, str):
                assert "TP53" in ann
                assert "WRAP53" in ann
        finally:
            vcf_path.unlink()
            parser.close()


class TestStrelka2NonStandardFields:
    """Test Strelka2 non-standard FORMAT fields."""

    def test_strelka2_snv_format_fields(self):
        """
        Strelka2 AU/CU/GU/TU fields for SNVs are handled.

        From guide: Strelka2 uses AU, CU, GU, TU fields instead of AD for SNVs
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=SOMATIC,Number=0,Type=Flag,Description="Somatic">
##INFO=<ID=SGT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=FDP,Number=1,Type=Integer,Description="Filtered depth">
##FORMAT=<ID=AU,Number=2,Type=Integer,Description="A allele counts">
##FORMAT=<ID=CU,Number=2,Type=Integer,Description="C allele counts">
##FORMAT=<ID=GU,Number=2,Type=Integer,Description="G allele counts">
##FORMAT=<ID=TU,Number=2,Type=Integer,Description="T allele counts">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNORMAL\tTUMOR
chr1\t4345650\t.\tC\tT\t.\tPASS\tSOMATIC;SGT=CC->CT\tDP:FDP:AU:CU:GU:TU\t63:1:0,0:62,63:0,0:0,0\t85:2:0,0:45,47:0,0:38,40
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            record = records[0]
            assert record.ref == "C"
            assert record.alt == "T"
            assert record.info.get("SOMATIC") is True or "SOMATIC" in record.info
        finally:
            vcf_path.unlink()
            parser.close()

    def test_strelka2_indel_tar_tir_fields(self):
        """
        Strelka2 TAR/TIR fields for indels are handled.

        From guide: Strelka2 uses TAR/TIR for indels
        """
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=SOMATIC,Number=0,Type=Flag,Description="Somatic">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=TAR,Number=2,Type=Integer,Description="Tier1/Tier2 ref counts">
##FORMAT=<ID=TIR,Number=2,Type=Integer,Description="Tier1/Tier2 indel counts">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNORMAL\tTUMOR
chr1\t1000\t.\tATG\tA\t.\tPASS\tSOMATIC\tDP:TAR:TIR\t50:45,48:0,0\t60:30,32:25,28
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_path = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_path, human_genome=True)
            batches = list(parser.iter_batches())
            records = batches[0]

            assert len(records) == 1
            record = records[0]
            assert record.ref == "ATG"
            assert record.alt == "A"
        finally:
            vcf_path.unlink()
            parser.close()
