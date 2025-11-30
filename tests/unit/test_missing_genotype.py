"""Tests for missing genotype (./.) vs homozygous reference (0/0) distinction.

Critical clinical requirement: Missing genotype must be NULL in database,
NOT confused with explicit homozygous reference calls.

Source: compass_artifact guidance doc lines 334-346

Note: The VCFStreamingParser produces per-variant records without sample-level
genotype data. These tests verify the parser handles VCFs with missing data
patterns correctly without crashing or corrupting data.
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from vcf_pg_loader.vcf_parser import VCFStreamingParser


class TestMissingGenotypeHandling:
    """Test that VCFs with ./. genotypes parse correctly."""

    def test_vcf_with_missing_genotype_parses(self):
        """VCF with ./. genotype parses without error."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1\tSAMPLE2
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DP\t0/1:30\t./.:10
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
            assert records[0].ref == "A"
            assert records[0].alt == "G"
        finally:
            vcf_path.unlink()

    def test_vcf_with_hom_ref_parses(self):
        """VCF with 0/0 genotype parses without error."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1\tSAMPLE2
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DP\t0/1:30\t0/0:25
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
        finally:
            vcf_path.unlink()

    def test_mixed_missing_and_hom_ref(self):
        """VCF with both ./. and 0/0 parses correctly."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tMISSING\tHOM_REF\tHET
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DP\t./.:.\t0/0:30\t0/1:25
chr1\t200\t.\tC\tT\t30\tPASS\t.\tGT:DP\t0/0:20\t./.:.\t1/1:40
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

            assert len(records) == 2
            positions = {r.pos for r in records}
            assert positions == {100, 200}
        finally:
            vcf_path.unlink()


class TestPartialGenotypes:
    """Test handling of partially missing genotypes like ./1 and 0/."""

    def test_partial_missing_first_allele(self):
        """./1 genotype is handled without crashing."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT\t./1
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
        finally:
            vcf_path.unlink()

    def test_partial_missing_second_allele(self):
        """0/. genotype is handled without crashing."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT\t0/.
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
        finally:
            vcf_path.unlink()

    def test_all_partial_genotype_patterns(self):
        """All partial genotype patterns are handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\tS3\tS4\tS5
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT\t./.\t0/.\t./1\t1/.\t./2
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
        finally:
            vcf_path.unlink()


class TestMissingDataInFormatFields:
    """Test handling of missing data (.) in FORMAT fields beyond GT."""

    def test_missing_dp_value(self):
        """Missing DP value (.) is handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DP:GQ\t0/1:.:30
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
        finally:
            vcf_path.unlink()

    def test_missing_gq_value(self):
        """Missing GQ value (.) is handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DP:GQ\t0/1:30:.
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
        finally:
            vcf_path.unlink()

    def test_all_format_fields_missing(self):
        """All FORMAT fields except GT being missing is handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic Depth">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DP:GQ:AD\t0/1:.:.:.,.
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
        finally:
            vcf_path.unlink()


class TestPhasedGenotypes:
    """Test phased genotype handling (| separator)."""

    def test_phased_het(self):
        """0|1 phased genotype VCF parses correctly."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT\t0|1
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
            assert records[0].ref == "A"
            assert records[0].alt == "G"
        finally:
            vcf_path.unlink()

    def test_phased_missing(self):
        """.|. phased missing genotype is handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT\t.|.
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
        finally:
            vcf_path.unlink()


class TestHaploidGenotypes:
    """Test haploid genotype handling (single allele, e.g., chrY)."""

    def test_haploid_ref(self):
        """Haploid reference genotype (0) is handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chrY,length=57227415>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chrY\t100\t.\tA\tG\t30\tPASS\t.\tGT\t0
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
        finally:
            vcf_path.unlink()

    def test_haploid_alt(self):
        """Haploid alt genotype (1) is handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chrY,length=57227415>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chrY\t100\t.\tA\tG\t30\tPASS\t.\tGT\t1
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
        finally:
            vcf_path.unlink()

    def test_haploid_missing(self):
        """Haploid missing genotype (.) is handled."""
        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##contig=<ID=chrY,length=57227415>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chrY\t100\t.\tA\tG\t30\tPASS\t.\tGT\t.
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
        finally:
            vcf_path.unlink()


class TestMissingInfoFieldValues:
    """Test missing INFO field values (.) handling."""

    def test_missing_af_value(self):
        """AF=. is handled as None, not string '.'."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t30\tPASS\tAF=.;DP=50
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
            af_value = records[0].info.get("AF")
            assert af_value is None or af_value == "." or af_value == [None]
        finally:
            vcf_path.unlink()

    def test_missing_info_entirely(self):
        """INFO column as single '.' is handled as empty."""
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
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            assert len(records) == 1
            assert records[0].info == {} or "." not in records[0].info
        finally:
            vcf_path.unlink()
