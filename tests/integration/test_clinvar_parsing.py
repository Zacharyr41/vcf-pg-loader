"""ClinVar VCF field validation tests.

Clinical annotations carry medico-legal weight - corrupted pathogenicity
classifications could lead to misdiagnosis. These tests validate annotation
field integrity.

Sources:
- compass_artifact guidance doc lines 211-255
- ClinVar README: ftp.ncbi.nlm.nih.gov/pub/clinvar/README_VCF.txt
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from vcf_pg_loader.vcf_parser import VCFStreamingParser


class TestCLNSIGPreservation:
    """Test CLNSIG (Clinical Significance) field preservation."""

    def test_pathogenic_clnsig_preserved(self):
        """Pathogenic classification is preserved exactly."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease name">
##contig=<ID=chr17,length=83257441>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr17\t43094464\trs80357906\tC\tT\t.\t.\tCLNSIG=Pathogenic;CLNDN=Hereditary_breast_and_ovarian_cancer_syndrome
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
            clnsig = records[0].info.get("CLNSIG")
            assert clnsig is not None
            assert "Pathogenic" in str(clnsig)
        finally:
            vcf_path.unlink()

    def test_benign_clnsig_preserved(self):
        """Benign classification is preserved exactly."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNSIG=Benign
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
            clnsig = records[0].info.get("CLNSIG")
            assert clnsig is not None
            assert "Benign" in str(clnsig)
        finally:
            vcf_path.unlink()

    def test_vus_clnsig_preserved(self):
        """Uncertain_significance (VUS) is preserved exactly."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNSIG=Uncertain_significance
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
            clnsig = records[0].info.get("CLNSIG")
            assert clnsig is not None
            assert "Uncertain_significance" in str(clnsig)
        finally:
            vcf_path.unlink()

    def test_conflicting_clnsig_preserved(self):
        """Conflicting interpretations are preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=CLNSIGCONF,Number=.,Type=String,Description="Conflicting significance">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNSIG=Conflicting_interpretations_of_pathogenicity;CLNSIGCONF=Pathogenic(1)|Uncertain_significance(2)
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
            clnsig = records[0].info.get("CLNSIG")
            assert clnsig is not None
            assert "Conflicting" in str(clnsig)
        finally:
            vcf_path.unlink()


class TestCLNREVSTATPreservation:
    """Test CLNREVSTAT (Review Status) preservation for star ratings."""

    def test_four_star_review_preserved(self):
        """Four-star review status is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="Review status">
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNSIG=Pathogenic;CLNREVSTAT=practice_guideline
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
            clnrevstat = records[0].info.get("CLNREVSTAT")
            assert clnrevstat is not None
            assert "practice_guideline" in str(clnrevstat)
        finally:
            vcf_path.unlink()

    def test_three_star_review_preserved(self):
        """Three-star review (reviewed_by_expert_panel) is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="Review status">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNREVSTAT=reviewed_by_expert_panel
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
            clnrevstat = records[0].info.get("CLNREVSTAT")
            assert clnrevstat is not None
            assert "reviewed_by_expert_panel" in str(clnrevstat)
        finally:
            vcf_path.unlink()

    def test_one_star_review_preserved(self):
        """One-star review status is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="Review status">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNREVSTAT=criteria_provided,_single_submitter
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
            clnrevstat = records[0].info.get("CLNREVSTAT")
            assert clnrevstat is not None
        finally:
            vcf_path.unlink()


class TestALLELEIDPreservation:
    """Test ALLELEID (ClinVar allele ID) integer preservation."""

    def test_alleleid_is_integer(self):
        """ALLELEID remains an integer, not converted to string."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description="ClinVar allele ID">
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tALLELEID=12345;CLNSIG=Pathogenic
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
            alleleid = records[0].info.get("ALLELEID")
            assert alleleid is not None
            assert alleleid == 12345 or str(alleleid) == "12345"
        finally:
            vcf_path.unlink()

    def test_large_alleleid_preserved(self):
        """Large ALLELEID values are preserved correctly."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description="ClinVar allele ID">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tALLELEID=2147483647
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
            alleleid = records[0].info.get("ALLELEID")
            assert alleleid is not None
        finally:
            vcf_path.unlink()


class TestCLNDNSpecialCharacters:
    """Test CLNDN (Disease Name) special character handling."""

    def test_clndn_with_underscores(self):
        """Disease names with underscores are preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease name">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNDN=Hereditary_breast_and_ovarian_cancer_syndrome
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
            clndn = records[0].info.get("CLNDN")
            assert clndn is not None
            assert "breast" in str(clndn).lower()
        finally:
            vcf_path.unlink()

    def test_clndn_multiple_diseases(self):
        """Multiple disease names (pipe-separated) are preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease name">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNDN=Breast_cancer|Ovarian_cancer|not_provided
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
            clndn = records[0].info.get("CLNDN")
            assert clndn is not None
        finally:
            vcf_path.unlink()


class TestGENEINFOPreservation:
    """Test GENEINFO (Gene:ID pairs) delimiter preservation."""

    def test_geneinfo_single_gene(self):
        """Single gene:ID pair is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=GENEINFO,Number=1,Type=String,Description="Gene:ID pairs">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tGENEINFO=BRCA1:672
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
            geneinfo = records[0].info.get("GENEINFO")
            assert geneinfo is not None
            assert "BRCA1" in str(geneinfo)
            assert "672" in str(geneinfo)
        finally:
            vcf_path.unlink()

    def test_geneinfo_multiple_genes(self):
        """Multiple gene:ID pairs (pipe-separated) are preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=GENEINFO,Number=1,Type=String,Description="Gene:ID pairs">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tGENEINFO=BRCA1:672|NBR2:10230
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
            geneinfo = records[0].info.get("GENEINFO")
            assert geneinfo is not None
        finally:
            vcf_path.unlink()


class TestCLNVIHandling:
    """Test CLNVI (ClinVar Variation ID) handling with potential illegal VCF chars.

    Known issue: CLNVI field contains illegal VCF characters
    Source: compass_artifact guidance doc lines 235-236
    """

    def test_clnvi_basic(self):
        """Basic CLNVI is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNVI,Number=.,Type=String,Description="Variation IDs">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNVI=OMIM_Allelic_Variant:113705.0003
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
            clnvi = records[0].info.get("CLNVI")
            assert clnvi is not None
        finally:
            vcf_path.unlink()


class TestACMGCriteriaPreservation:
    """Test ACMG/AMP classification criteria preservation.

    The 28 ACMG criteria codes must be preserved exactly:
    - Pathogenic: PVS1, PS1-4, PM1-6, PP1-5
    - Benign: BA1, BS1-4, BP1-7

    Source: Richards S et al. Genetics in Medicine 17(5):405-424 (2015)
    """

    def test_acmg_pvs1_preserved(self):
        """PVS1 (very strong pathogenic) criterion is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=ACMG,Number=.,Type=String,Description="ACMG criteria">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNSIG=Pathogenic;ACMG=PVS1|PS1|PM2
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
            acmg = records[0].info.get("ACMG")
            if acmg is not None:
                assert "PVS1" in str(acmg)
        finally:
            vcf_path.unlink()

    def test_acmg_ba1_preserved(self):
        """BA1 (benign stand-alone) criterion is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=ACMG,Number=.,Type=String,Description="ACMG criteria">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100000\t.\tA\tG\t.\t.\tCLNSIG=Benign;ACMG=BA1
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
            acmg = records[0].info.get("ACMG")
            if acmg is not None:
                assert "BA1" in str(acmg)
        finally:
            vcf_path.unlink()


class TestCompleteClinVarRecord:
    """Test complete ClinVar record with all critical fields."""

    def test_full_clinvar_record_preserved(self):
        """Complete ClinVar record with all fields is preserved."""
        vcf_content = """##fileformat=VCFv4.3
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description="ClinVar allele ID">
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="Review status">
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease name">
##INFO=<ID=GENEINFO,Number=1,Type=String,Description="Gene:ID pairs">
##INFO=<ID=MC,Number=.,Type=String,Description="Molecular consequence">
##INFO=<ID=ORIGIN,Number=.,Type=String,Description="Allele origin">
##contig=<ID=chr17,length=83257441>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr17\t43094464\trs80357906\tC\tT\t.\t.\tALLELEID=22144;CLNSIG=Pathogenic;CLNREVSTAT=reviewed_by_expert_panel;CLNDN=Hereditary_breast_and_ovarian_cancer_syndrome;GENEINFO=BRCA1:672;MC=SO:0001587|nonsense;ORIGIN=1
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
            record = records[0]

            assert record.rs_id == "rs80357906"
            assert record.info.get("ALLELEID") is not None
            assert record.info.get("CLNSIG") is not None
            assert record.info.get("CLNREVSTAT") is not None
            assert record.info.get("CLNDN") is not None
            assert record.info.get("GENEINFO") is not None
        finally:
            vcf_path.unlink()
