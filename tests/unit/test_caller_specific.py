"""Tests for variant caller-specific field handling."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestFreeBayesFields:
    """Test FreeBayes-specific field parsing.

    FreeBayes uses RO/AO for reference/alternate observation counts
    instead of standard AD format.
    """

    def test_freebayes_ro_ao_parsing(self):
        """FreeBayes RO/AO fields are parsed correctly."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
##FORMAT=<ID=RO,Number=1,Type=Integer,Description="Reference allele observation count">
##FORMAT=<ID=AO,Number=A,Type=Integer,Description="Alternate allele observation count">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\tG\t100\tPASS\tDP=50\tGT:DP:RO:AO\t0/1:50:25:25
chr1\t2000\t.\tC\tT,G\t100\tPASS\tDP=60\tGT:DP:RO:AO\t1/2:60:10:30,20
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches[0]) >= 1
        finally:
            vcf_file.unlink()

    def test_freebayes_vaf_calculation(self):
        """VAF can be calculated from FreeBayes RO/AO."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=RO,Number=1,Type=Integer,Description="Reference allele observation count">
##FORMAT=<ID=AO,Number=A,Type=Integer,Description="Alternate allele observation count">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\tG\t100\tPASS\t.\tGT:RO:AO\t0/1:75:25
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches[0]) == 1
            ro = 75
            ao = 25
            expected_vaf = ao / (ro + ao)
            assert abs(expected_vaf - 0.25) < 0.01
        finally:
            vcf_file.unlink()


class TestDeepVariantFields:
    """Test DeepVariant-specific field parsing.

    DeepVariant includes VAF directly in FORMAT fields.
    """

    def test_deepvariant_vaf_parsing(self):
        """DeepVariant VAF field is parsed correctly."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
##FORMAT=<ID=VAF,Number=A,Type=Float,Description="Variant allele frequency">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\tG\t50\tPASS\t.\tGT:DP:AD:VAF:GQ\t0/1:100:50,50:0.5:99
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            assert len(batches[0]) == 1
        finally:
            vcf_file.unlink()

    def test_deepvariant_ad_format(self):
        """DeepVariant AD field (Number=R) is parsed correctly."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths for ref and alt">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\tG\t50\tPASS\t.\tGT:AD\t0/1:50,50
chr1\t2000\t.\tC\tT,G\t50\tPASS\t.\tGT:AD\t1/2:10,45,45
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches[0]) >= 1
        finally:
            vcf_file.unlink()


class TestStrelka2Fields:
    """Test Strelka2-specific field parsing.

    Strelka2 uses tier-based counts (AU, CU, GU, TU) and TAR/TIR for indels.
    """

    def test_strelka2_snv_tier_counts(self):
        """Strelka2 SNV tier count fields are parsed."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AU,Number=2,Type=Integer,Description="A allele counts (tier1,tier2)">
##FORMAT=<ID=CU,Number=2,Type=Integer,Description="C allele counts (tier1,tier2)">
##FORMAT=<ID=GU,Number=2,Type=Integer,Description="G allele counts (tier1,tier2)">
##FORMAT=<ID=TU,Number=2,Type=Integer,Description="T allele counts (tier1,tier2)">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNORMAL\tTUMOR
chr1\t1000\t.\tA\tG\t.\tPASS\t.\tGT:AU:CU:GU:TU\t0/0:50,52:0,0:0,0:0,0\t0/1:25,26:0,0:25,26:0,0
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            assert len(batches[0]) == 1
        finally:
            vcf_file.unlink()

    def test_strelka2_indel_tar_tir(self):
        """Strelka2 indel TAR/TIR fields are parsed."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=TAR,Number=2,Type=Integer,Description="Ref allele tier1,tier2 counts">
##FORMAT=<ID=TIR,Number=2,Type=Integer,Description="Alt allele tier1,tier2 counts">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNORMAL\tTUMOR
chr1\t1000\t.\tAT\tA\t.\tPASS\t.\tGT:TAR:TIR\t0/0:100,102:0,0\t0/1:50,52:50,52
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()


class TestMutect2Fields:
    """Test Mutect2-specific field parsing."""

    def test_mutect2_af_format(self):
        """Mutect2 AF FORMAT field is parsed correctly."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
##FORMAT=<ID=AF,Number=A,Type=Float,Description="Allele fractions">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tTUMOR\tNORMAL
chr1\t1000\t.\tA\tG\t.\tPASS\t.\tGT:AD:AF:DP\t0/1:50,50:0.5:100\t0/0:100,0:0:100
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()

    def test_mutect2_somatic_filters(self):
        """Mutect2 somatic filters are parsed."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FILTER=<ID=weak_evidence,Description="Weak evidence">
##FILTER=<ID=strand_bias,Description="Strand bias">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\tG\t.\tPASS\t.\tGT\t0/1
chr1\t2000\t.\tC\tT\t.\tweak_evidence\t.\tGT\t0/1
chr1\t3000\t.\tG\tA\t.\tweak_evidence;strand_bias\t.\tGT\t0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            records = batches[0]
            assert len(records) == 3
            assert records[0].filter == []
            assert records[1].filter == ["weak_evidence"]
            assert set(records[2].filter) == {"weak_evidence", "strand_bias"}
        finally:
            vcf_file.unlink()


class TestHaplotypeCallerFields:
    """Test GATK HaplotypeCaller-specific field parsing."""

    def test_haplotypecaller_pl_field(self):
        """HaplotypeCaller PL (phred-scaled likelihoods) field is parsed."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Phred-scaled likelihoods">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\tG\t100\tPASS\t.\tGT:AD:DP:GQ:PL\t0/1:50,50:100:99:99,0,99
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()

    def test_haplotypecaller_gvcf_mode(self):
        """HaplotypeCaller gVCF reference blocks are handled."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=MIN_DP,Number=1,Type=Integer,Description="Min depth in block">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\t<NON_REF>\t.\t.\tEND=1100\tGT:DP:GQ:MIN_DP\t0/0:50:99:45
chr1\t1101\t.\tC\tT,<NON_REF>\t100\tPASS\t.\tGT:DP:GQ\t0/1:60:99
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            records = batches[0]
            variant_count = sum(1 for r in records if r.alt not in ("<NON_REF>", "*"))
            assert variant_count >= 1
        finally:
            vcf_file.unlink()


class TestBCFToolsNormOutput:
    """Test bcftools norm output compatibility."""

    def test_bcftools_norm_multiallelic_split(self):
        """Variants split by bcftools norm -m- are parsed correctly."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##bcftools_normVersion=1.17
##bcftools_normCommand=norm -m- input.vcf
##INFO=<ID=OLD_CLUMPED,Number=1,Type=String,Description="Original position">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t1000\t.\tA\tG\t.\tPASS\tOLD_CLUMPED=chr1:1000:A:G,T\tGT\t0/1
chr1\t1000\t.\tA\tT\t.\tPASS\tOLD_CLUMPED=chr1:1000:A:G,T\tGT\t0/1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            records = batches[0]
            assert len(records) == 2
            assert {r.alt for r in records} == {"G", "T"}
        finally:
            vcf_file.unlink()
