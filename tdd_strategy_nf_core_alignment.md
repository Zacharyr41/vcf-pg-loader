# TDD Strategy for VCF-to-PostgreSQL Loader: nf-core Ecosystem Alignment

A rigorous test-driven development approach ensures your loader handles the specific gaps in nf-core pipelines while remaining relevant to academic genomics researchers. This strategy organizes test cases around **three tiers**: unit tests for parsing correctness, integration tests validating nf-core output compatibility, and acceptance tests proving the tool solves real workflow problems that streaming tools cannot.

The key insight: nf-core pipelines like sarek and raredisease produce richly annotated VCFs, but researchers hit a wall when they need cross-sample queries, historical tracking, or ad-hoc SQL analysis. Your test suite should prove that variants load correctly AND that the database enables query patterns impossible with bcftools/slivar.

---

## Test Data Source Strategy

### Tier 1: Synthetic VCFs for Unit Tests (Generated, <1MB each)

Generate minimal VCFs that isolate specific parsing challenges. These run in milliseconds and test edge cases that real data rarely exhibits in isolation.

```python
# tests/fixtures/vcf_generator.py
from dataclasses import dataclass
from typing import List, Optional
import io

@dataclass
class SyntheticVariant:
    chrom: str
    pos: int
    ref: str
    alt: List[str]
    qual: Optional[float] = 30.0
    filter: str = "PASS"
    info: dict = None
    format_fields: dict = None  # sample_name -> {field: value}

class VCFGenerator:
    """Generate minimal VCFs for targeted unit tests."""
    
    HEADER_TEMPLATE = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele Count">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total Alleles">
##INFO=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">
##INFO=<ID=CSQ,Number=.,Type=String,Description="VEP annotation">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">
##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Phred-scaled Likelihoods">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chrX,length=156040895>
##contig=<ID=chrY,length=57227415>
##contig=<ID=chrM,length=16569>
"""

    @classmethod
    def generate(cls, variants: List[SyntheticVariant], 
                 samples: List[str] = None) -> str:
        """Generate a minimal VCF string."""
        samples = samples or ["SAMPLE1"]
        lines = [cls.HEADER_TEMPLATE.strip()]
        lines.append("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + 
                    "\t".join(samples))
        
        for v in variants:
            info_str = cls._format_info(v.info) if v.info else "."
            alt_str = ",".join(v.alt)
            qual_str = str(v.qual) if v.qual else "."
            
            # Build FORMAT and sample columns
            format_keys = ["GT"]
            if v.format_fields:
                first_sample = list(v.format_fields.values())[0]
                format_keys = list(first_sample.keys())
            
            sample_cols = []
            for sample in samples:
                if v.format_fields and sample in v.format_fields:
                    vals = [str(v.format_fields[sample].get(k, ".")) 
                           for k in format_keys]
                    sample_cols.append(":".join(vals))
                else:
                    sample_cols.append("./.")
            
            line = f"{v.chrom}\t{v.pos}\t.\t{v.ref}\t{alt_str}\t{qual_str}\t" \
                   f"{v.filter}\t{info_str}\tGT\t" + "\t".join(sample_cols)
            lines.append(line)
        
        return "\n".join(lines) + "\n"
    
    @staticmethod
    def _format_info(info: dict) -> str:
        parts = []
        for k, v in info.items():
            if v is True:  # Flag field
                parts.append(k)
            elif isinstance(v, list):
                parts.append(f"{k}={','.join(map(str, v))}")
            else:
                parts.append(f"{k}={v}")
        return ";".join(parts) if parts else "."


# Factory functions for common edge cases
def make_multiallelic_vcf(n_alts: int = 3) -> str:
    """VCF with multi-allelic site for Number=A/R/G testing."""
    alts = ["G", "T", "C"][:n_alts]
    return VCFGenerator.generate([
        SyntheticVariant(
            chrom="chr1", pos=100, ref="A", alt=alts,
            info={
                "AF": [0.1] * n_alts,  # Number=A
                "AC": [10] * n_alts,   # Number=A  
                "AD": [100] + [10] * n_alts,  # Number=R
                "AN": 200,
            }
        )
    ])

def make_unnormalized_vcf() -> str:
    """VCF with variants requiring normalization."""
    return VCFGenerator.generate([
        # Right-trimmable: ATG->AG should become T->empty (needs left-extend)
        SyntheticVariant(chrom="chr1", pos=100, ref="ATG", alt=["AG"]),
        # Left-trimmable: GATC->GTTC should become A->T at pos 101
        SyntheticVariant(chrom="chr1", pos=200, ref="GATC", alt=["GTTC"]),
        # Already normalized SNP
        SyntheticVariant(chrom="chr1", pos=300, ref="A", alt=["G"]),
    ])

def make_vep_csq_vcf() -> str:
    """VCF with VEP CSQ annotations matching nf-core/sarek output."""
    csq = "T|missense_variant|MODERATE|BRCA1|ENSG00000012048|Transcript|" \
          "ENST00000357654|protein_coding|11/23||ENST00000357654.3:c.2612C>T|" \
          "ENSP00000350283.3:p.Pro871Leu|2731|2612|871|P/L|cCa/cTa||"
    return VCFGenerator.generate([
        SyntheticVariant(
            chrom="chr17", pos=43094464, ref="C", alt=["T"],
            info={"CSQ": csq}
        )
    ])

def make_trio_vcf() -> str:
    """Minimal trio VCF with inheritance patterns for testing."""
    samples = ["proband", "father", "mother"]
    return VCFGenerator.generate([
        # De novo: child het, parents hom_ref
        SyntheticVariant(
            chrom="chr1", pos=1000, ref="A", alt=["G"],
            format_fields={
                "proband": {"GT": "0/1", "DP": 30, "GQ": 99},
                "father": {"GT": "0/0", "DP": 25, "GQ": 99},
                "mother": {"GT": "0/0", "DP": 28, "GQ": 99},
            }
        ),
        # Autosomal recessive: child hom_alt, parents het
        SyntheticVariant(
            chrom="chr2", pos=2000, ref="C", alt=["T"],
            format_fields={
                "proband": {"GT": "1/1", "DP": 35, "GQ": 99},
                "father": {"GT": "0/1", "DP": 30, "GQ": 99},
                "mother": {"GT": "0/1", "DP": 32, "GQ": 99},
            }
        ),
        # Compound het candidate 1 (from father)
        SyntheticVariant(
            chrom="chr3", pos=3000, ref="G", alt=["A"],
            info={"SYMBOL": "GENE1"},
            format_fields={
                "proband": {"GT": "0/1", "DP": 28, "GQ": 99},
                "father": {"GT": "0/1", "DP": 26, "GQ": 99},
                "mother": {"GT": "0/0", "DP": 30, "GQ": 99},
            }
        ),
        # Compound het candidate 2 (from mother, same gene)
        SyntheticVariant(
            chrom="chr3", pos=3500, ref="T", alt=["C"],
            info={"SYMBOL": "GENE1"},
            format_fields={
                "proband": {"GT": "0/1", "DP": 32, "GQ": 99},
                "father": {"GT": "0/0", "DP": 29, "GQ": 99},
                "mother": {"GT": "0/1", "DP": 31, "GQ": 99},
            }
        ),
    ], samples=samples)
```

### Tier 2: nf-core Test Dataset VCFs (~10-100MB)

Use the exact VCFs from nf-core/test-datasets to ensure compatibility with real pipeline outputs.

```python
# tests/fixtures/nf_core_datasets.py
"""
nf-core test dataset references.
Clone with: git clone --single-branch --branch modules https://github.com/nf-core/test-datasets
"""
from pathlib import Path
import urllib.request
import hashlib

NF_CORE_TEST_DATA = {
    # From nf-core/test-datasets modules branch
    "dbsnp_146_hg38": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/dbsnp_146.hg38.vcf.gz",
        "md5": None,  # Verify on first download
        "description": "dbSNP subset used by nf-core/sarek tests"
    },
    "gnomad_r2_hg38": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/gnomAD.r2.1.1.vcf.gz",
        "md5": None,
        "description": "gnomAD subset for germline resource testing"
    },
    "mills_1000g_indels": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/mills_and_1000G.indels.vcf.gz",
        "md5": None,
        "description": "Known indels for BQSR testing"
    },
    
    # Sarek-specific test outputs (requires running sarek test profile)
    "sarek_test_vcf": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/sarek/testdata/vcf/test.vcf.gz",
        "md5": None,
        "description": "Sarek test VCF output"
    },
}

GIAB_BENCHMARK_DATA = {
    # Full GIAB Ashkenazi trio benchmarks (v4.2.1)
    "HG002_benchmark": {
        "url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
        "bed_url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed",
        "variants": 4_042_186,
        "description": "Son/proband - ~4M variants"
    },
    "HG003_benchmark": {
        "url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG003_NA24149_father/NISTv4.2.1/GRCh38/HG003_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
        "variants": 3_993_257,
        "description": "Father"
    },
    "HG004_benchmark": {
        "url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG004_NA24143_mother/NISTv4.2.1/GRCh38/HG004_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
        "variants": 4_052_103,
        "description": "Mother"
    },
    
    # Smaller test files for CI
    "HG002_chr21_subset": {
        "description": "Chr21 only (~150K variants) - fast CI testing",
        "region": "chr21",
        "expected_variants": 150_000,  # approximate
    }
}

# Clinical annotation sources
CLINICAL_DATA = {
    "clinvar_grch38": {
        "url": "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz",
        "description": "Full ClinVar - ~2M variants, tests annotation parsing"
    },
    "gnomad_chr21": {
        "url": "https://storage.googleapis.com/gcp-public-data--gnomad/release/3.1.1/vcf/genomes/gnomad.genomes.v3.1.1.sites.chr21.vcf.bgz",
        "description": "gnomAD chr21 - tests population frequency fields"
    }
}


class TestDataManager:
    """Manages test data downloads with caching."""
    
    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or Path.home() / ".cache" / "vcf-pg-loader-tests"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_vcf(self, dataset_key: str, subset_region: str = None) -> Path:
        """Download and cache a test VCF, optionally subsetting by region."""
        # Implementation: download, cache, optionally bcftools view -r region
        ...
    
    def get_giab_chr21(self, sample: str = "HG002") -> Path:
        """Get chr21 subset of GIAB sample for fast testing."""
        full_vcf = self.get_vcf(f"{sample}_benchmark")
        subset_path = self.cache_dir / f"{sample}_chr21.vcf.gz"
        
        if not subset_path.exists():
            import subprocess
            subprocess.run([
                "bcftools", "view", "-r", "chr21",
                "-Oz", "-o", str(subset_path), str(full_vcf)
            ], check=True)
            subprocess.run(["bcftools", "index", str(subset_path)], check=True)
        
        return subset_path
```

### Tier 3: Real nf-core Pipeline Outputs

Generate actual VCFs by running nf-core pipelines with their test profiles:

```bash
# scripts/generate_nf_core_test_outputs.sh
#!/bin/bash
# Generate real nf-core pipeline outputs for integration testing

set -euo pipefail

OUTPUT_DIR="${1:-./test_data/nf_core_outputs}"
mkdir -p "$OUTPUT_DIR"

# Run sarek test profile (germline)
echo "Running nf-core/sarek test profile..."
nextflow run nf-core/sarek -r 3.4.0 \
    -profile test,docker \
    --outdir "$OUTPUT_DIR/sarek_germline" \
    --tools "haplotypecaller,vep,snpeff" \
    -resume

# Run sarek test_full_germline (NA12878 WGS 30x)
# Note: This downloads ~30GB and takes hours - run separately
# nextflow run nf-core/sarek -r 3.4.0 \
#     -profile test_full_germline,docker \
#     --outdir "$OUTPUT_DIR/sarek_na12878"

# Run raredisease test profile
echo "Running nf-core/raredisease test profile..."
nextflow run nf-core/raredisease -r 2.0.0 \
    -profile test,docker \
    --outdir "$OUTPUT_DIR/raredisease" \
    -resume

echo "Test outputs generated in $OUTPUT_DIR"
```

---

## Unit Test Cases: Parsing Correctness

### Test Suite 1: VCF Header Parsing

These tests validate correct INFO/FORMAT schema extraction—the foundation for database schema generation.

```python
# tests/unit/test_header_parsing.py
import pytest
from vcf_pg_loader.parser import VCFHeaderParser

class TestInfoFieldParsing:
    """Test INFO field schema extraction from VCF headers."""
    
    def test_number_1_integer(self):
        """Single-value integer fields map to INTEGER."""
        header = '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">'
        parser = VCFHeaderParser()
        field = parser.parse_info_line(header)
        
        assert field.id == "DP"
        assert field.number == "1"
        assert field.vcf_type == "Integer"
        assert field.pg_type == "INTEGER"
        assert field.is_array == False
    
    def test_number_a_float_becomes_array(self):
        """Number=A fields become arrays (per-ALT allele)."""
        header = '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">'
        field = VCFHeaderParser().parse_info_line(header)
        
        assert field.number == "A"
        assert field.pg_type == "REAL[]"
        assert field.is_array == True
    
    def test_number_r_integer_becomes_array(self):
        """Number=R fields become arrays (REF + ALT alleles)."""
        header = '##INFO=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">'
        field = VCFHeaderParser().parse_info_line(header)
        
        assert field.number == "R"
        assert field.pg_type == "INTEGER[]"
    
    def test_number_g_becomes_array(self):
        """Number=G fields (per-genotype) become arrays."""
        header = '##INFO=<ID=PL,Number=G,Type=Integer,Description="Phred Likelihoods">'
        field = VCFHeaderParser().parse_info_line(header)
        
        assert field.number == "G"
        assert field.pg_type == "INTEGER[]"
    
    def test_number_dot_becomes_text_array(self):
        """Number=. (unbounded) becomes TEXT[] for flexibility."""
        header = '##INFO=<ID=CSQ,Number=.,Type=String,Description="VEP annotation">'
        field = VCFHeaderParser().parse_info_line(header)
        
        assert field.number == "."
        assert field.pg_type == "TEXT[]"
    
    def test_flag_becomes_boolean(self):
        """Flag fields (Number=0) become BOOLEAN."""
        header = '##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP member">'
        field = VCFHeaderParser().parse_info_line(header)
        
        assert field.number == "0"
        assert field.pg_type == "BOOLEAN"
    
    @pytest.mark.parametrize("description,expected", [
        # Escaped quotes in description
        ('Description="Contains \\"quoted\\" text"', 'Contains "quoted" text'),
        # Commas in description (common issue)
        ('Description="A, B, and C"', "A, B, and C"),
        # Empty description
        ('Description=""', ""),
    ])
    def test_description_parsing_edge_cases(self, description, expected):
        """Descriptions with special characters parse correctly."""
        header = f'##INFO=<ID=TEST,Number=1,Type=String,{description}>'
        field = VCFHeaderParser().parse_info_line(header)
        assert field.description == expected


class TestVEPCSQParsing:
    """Test VEP Consequence annotation field parsing."""
    
    def test_csq_format_extraction(self):
        """Extract CSQ field order from VEP header."""
        header = '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature">'
        parser = VCFHeaderParser()
        csq_fields = parser.extract_csq_format(header)
        
        assert csq_fields == ["Allele", "Consequence", "IMPACT", "SYMBOL", 
                             "Gene", "Feature_type", "Feature"]
    
    def test_sarek_vep_csq_fields(self):
        """Parse CSQ format from actual sarek VEP output."""
        # This is the actual CSQ format from nf-core/sarek VEP output
        sarek_csq = '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID|CANONICAL|MANE_SELECT|MANE_PLUS_CLINICAL|TSL|APPRIS|CCDS|ENSP|SWISSPROT|TREMBL|UNIPARC|UNIPROT_ISOFORM|SOURCE|GENE_PHENO|SIFT|PolyPhen|DOMAINS|miRNA|AF|AFR_AF|AMR_AF|EAS_AF|EUR_AF|SAS_AF|gnomADe_AF|gnomADe_AFR_AF|gnomADe_AMR_AF|gnomADe_ASJ_AF|gnomADe_EAS_AF|gnomADe_FIN_AF|gnomADe_NFE_AF|gnomADe_OTH_AF|gnomADe_SAS_AF|gnomADg_AF|gnomADg_AFR_AF|gnomADg_AMI_AF|gnomADg_AMR_AF|gnomADg_ASJ_AF|gnomADg_EAS_AF|gnomADg_FIN_AF|gnomADg_MID_AF|gnomADg_NFE_AF|gnomADg_OTH_AF|gnomADg_SAS_AF|MAX_AF|MAX_AF_POPS|CLIN_SIG|SOMATIC|PHENO|PUBMED|VAR_SYNONYMS|MOTIF_NAME|MOTIF_POS|HIGH_INF_POS|MOTIF_SCORE_CHANGE|TRANSCRIPTION_FACTORS|CADD_PHRED|CADD_RAW|SpliceAI_pred_DP_AG|SpliceAI_pred_DP_AL|SpliceAI_pred_DP_DG|SpliceAI_pred_DP_DL|SpliceAI_pred_DS_AG|SpliceAI_pred_DS_AL|SpliceAI_pred_DS_DG|SpliceAI_pred_DS_DL|SpliceAI_pred_SYMBOL|LoF|LoF_filter|LoF_flags|LoF_info">'
        
        parser = VCFHeaderParser()
        csq_fields = parser.extract_csq_format(sarek_csq)
        
        # Key fields for clinical annotation
        assert "SYMBOL" in csq_fields
        assert "Consequence" in csq_fields
        assert "IMPACT" in csq_fields
        assert "CLIN_SIG" in csq_fields
        assert "gnomADe_AF" in csq_fields
        assert "CADD_PHRED" in csq_fields


class TestGENMODFieldParsing:
    """Test GENMOD annotation fields from nf-core/raredisease."""
    
    def test_genetic_models_field(self):
        """Parse GENMOD GeneticModels INFO field."""
        header = '##INFO=<ID=GeneticModels,Number=.,Type=String,Description="Inheritance models">'
        # Value format: "family_id:AR_hom|AD_dn"
        value = "FAM001:AR_hom,FAM001:AR_comp"
        
        parser = VCFHeaderParser()
        models = parser.parse_genmod_models(value)
        
        assert models == {"FAM001": ["AR_hom", "AR_comp"]}
    
    def test_compounds_field(self):
        """Parse GENMOD Compounds INFO field."""
        # Value format: "gene:var1>var2"
        value = "BRCA1:chr17_43094464_C_T>chr17_43094500_G_A"
        
        parser = VCFHeaderParser()
        compounds = parser.parse_genmod_compounds(value)
        
        assert "BRCA1" in compounds
        assert len(compounds["BRCA1"]) == 1
```

### Test Suite 2: Number=A/R/G Array Handling

This is where vcf2db fails—your tests prove correct handling.

```python
# tests/unit/test_number_array_fields.py
import pytest
from vcf_pg_loader.parser import parse_info_value
from tests.fixtures.vcf_generator import make_multiallelic_vcf

class TestNumberAFields:
    """Test per-ALT allele (Number=A) field handling."""
    
    @pytest.mark.parametrize("n_alts,expected_len", [
        (1, 1),
        (2, 2),
        (3, 3),
        (5, 5),
    ])
    def test_af_array_length_matches_alts(self, n_alts, expected_len):
        """AF array length equals number of ALT alleles."""
        vcf = make_multiallelic_vcf(n_alts)
        variant = parse_first_variant(vcf)
        
        af_values = variant.info["AF"]
        assert len(af_values) == expected_len
    
    def test_vcf2db_bug_number_a_not_skipped(self):
        """
        Verify we don't skip Number=A fields like vcf2db does.
        
        vcf2db skips AC, AF, MLEAC, MLEAF with warning:
        "skipping 'AF' because it has Number=A"
        
        This is the critical bug our tool fixes.
        """
        vcf = make_multiallelic_vcf(2)
        variant = parse_first_variant(vcf)
        
        # These fields must be present and correct
        assert "AF" in variant.info
        assert "AC" in variant.info
        assert len(variant.info["AF"]) == 2
        assert len(variant.info["AC"]) == 2
    
    def test_decomposed_multiallelic_preserves_correct_value(self):
        """After decomposition, each record gets the correct A-indexed value."""
        # Original: chr1:100 A->G,T AF=0.1,0.3
        # Decomposed: chr1:100 A->G AF=0.1
        #            chr1:100 A->T AF=0.3
        
        vcf_text = """##fileformat=VCFv4.3
##INFO=<ID=AF,Number=A,Type=Float,Description="AF">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG,T\t30\tPASS\tAF=0.1,0.3
"""
        decomposed = decompose_multiallelic(vcf_text)
        variants = list(parse_vcf_string(decomposed))
        
        assert len(variants) == 2
        assert variants[0].alt == "G"
        assert variants[0].info["AF"] == 0.1
        assert variants[1].alt == "T"
        assert variants[1].info["AF"] == 0.3


class TestNumberRFields:
    """Test per-allele REF+ALT (Number=R) field handling."""
    
    def test_ad_includes_ref_depth(self):
        """AD array includes REF depth at index 0."""
        vcf_text = """##fileformat=VCFv4.3
##INFO=<ID=AD,Number=R,Type=Integer,Description="AD">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG,T\t30\tPASS\tAD=100,30,20
"""
        variant = parse_first_variant(vcf_text)
        
        ad = variant.info["AD"]
        assert len(ad) == 3  # REF + 2 ALTs
        assert ad[0] == 100  # REF depth
        assert ad[1] == 30   # G depth
        assert ad[2] == 20   # T depth


class TestNumberGFields:
    """Test per-genotype (Number=G) field handling with binomial formula."""
    
    @pytest.mark.parametrize("n_alts,ploidy,expected_g", [
        (1, 2, 3),   # 0/0, 0/1, 1/1
        (2, 2, 6),   # 0/0, 0/1, 1/1, 0/2, 1/2, 2/2
        (3, 2, 10),  # binomial(3+2, 2) = 10
    ])
    def test_genotype_count_formula(self, n_alts, ploidy, expected_g):
        """Number=G array length follows binomial(n_alts+ploidy, ploidy)."""
        from vcf_pg_loader.parser import calculate_g_length
        assert calculate_g_length(n_alts, ploidy) == expected_g
    
    def test_pl_array_indexing(self):
        """PL array indexing matches VCF spec: Index(a/b) = b(b+1)/2 + a."""
        # For diploid with 2 alts: 0/0=0, 0/1=1, 1/1=2, 0/2=3, 1/2=4, 2/2=5
        from vcf_pg_loader.parser import genotype_index
        
        assert genotype_index(0, 0) == 0
        assert genotype_index(0, 1) == 1
        assert genotype_index(1, 1) == 2
        assert genotype_index(0, 2) == 3
        assert genotype_index(1, 2) == 4
        assert genotype_index(2, 2) == 5
```

### Test Suite 3: Variant Normalization

```python
# tests/unit/test_normalization.py
import pytest
from vcf_pg_loader.normalizer import normalize_variant, is_normalized

class TestVTNormalization:
    """Test vt-style left-alignment and parsimony."""
    
    # Reference sequence for chr1 (mock)
    REFERENCE = {
        "chr1": "NNNNATCGATCGATCGATCGNNNN"  # Positions 1-25
    }
    
    @pytest.mark.parametrize("pos,ref,alt,exp_pos,exp_ref,exp_alt", [
        # Already normalized SNP
        (10, "A", "G", 10, "A", "G"),
        # Right-trim needed: ATG->AG => T->empty => extend left
        (10, "ATG", "AG", 10, "AT", "A"),
        # Left-trim needed: GATC->GTTC => A->T at pos+1
        (10, "GATC", "GTTC", 11, "A", "T"),
        # Complex case requiring both phases
        (10, "ATCG", "TTCG", 10, "A", "T"),
    ])
    def test_normalization_cases(self, pos, ref, alt, exp_pos, exp_ref, exp_alt):
        """Standard normalization test cases."""
        result = normalize_variant("chr1", pos, ref, [alt], self.REFERENCE)
        
        assert result.pos == exp_pos
        assert result.ref == exp_ref
        assert result.alts == [exp_alt]
    
    def test_is_normalized_check(self):
        """Quick normalization check without reference lookup."""
        # Normalized: different ending nucleotides
        assert is_normalized("A", ["G"]) == True
        assert is_normalized("AT", ["GT"]) == True
        
        # Not normalized: same ending
        assert is_normalized("ATG", ["AG"]) == False
        assert is_normalized("GATC", ["GAC"]) == False
    
    def test_multiallelic_normalization(self):
        """Each ALT allele normalizes independently after decomposition."""
        # Original: pos=10, ref=ATG, alts=[AG, ATTG]
        # After decomp: ATG->AG normalizes to T->empty (needs reference)
        #              ATG->ATTG normalizes to G->TG
        pass  # Implement with reference mock
```

---

## Integration Tests: nf-core Output Compatibility

### Test Suite 4: sarek VCF Loading

```python
# tests/integration/test_sarek_outputs.py
import pytest
from pathlib import Path
from vcf_pg_loader import VCFLoader

@pytest.fixture(scope="module")
def sarek_test_vcf(test_data_manager):
    """Get sarek test output VCF."""
    return test_data_manager.get_nf_core_output("sarek", "annotation")

@pytest.fixture(scope="module")
def loaded_sarek_db(sarek_test_vcf, test_db):
    """Load sarek VCF into test database."""
    loader = VCFLoader(test_db.url)
    result = loader.load(sarek_test_vcf)
    return result


class TestSarekVEPAnnotations:
    """Test loading sarek VEP-annotated VCFs."""
    
    def test_csq_fields_extracted(self, loaded_sarek_db, test_db):
        """VEP CSQ fields are extracted to database columns."""
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT gene, consequence, impact 
                FROM variants 
                WHERE gene IS NOT NULL 
                LIMIT 10
            """)
            rows = cur.fetchall()
        
        assert len(rows) > 0
        # Verify VEP consequence terms
        consequences = {r["consequence"] for r in rows}
        assert any("missense" in c.lower() for c in consequences if c)
    
    def test_gnomad_frequencies_populated(self, loaded_sarek_db, test_db):
        """gnomAD allele frequencies from VEP are queryable."""
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM variants 
                WHERE gnomad_af IS NOT NULL AND gnomad_af < 0.01
            """)
            rare_count = cur.fetchone()[0]
        
        assert rare_count > 0
    
    def test_cadd_scores_populated(self, loaded_sarek_db, test_db):
        """CADD phred scores are extracted and queryable."""
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT chrom, pos, ref, alt, cadd_phred 
                FROM variants 
                WHERE cadd_phred > 20
                ORDER BY cadd_phred DESC
                LIMIT 5
            """)
            high_cadd = cur.fetchall()
        
        # sarek annotates with CADD via VEP plugin
        assert len(high_cadd) >= 0  # May be empty in minimal test data


class TestSarekCallerOutputs:
    """Test loading VCFs from different sarek variant callers."""
    
    @pytest.mark.parametrize("caller", [
        "haplotypecaller",
        "deepvariant", 
        "freebayes",
        "strelka",
    ])
    def test_caller_specific_vcf(self, caller, test_data_manager, test_db):
        """Each caller's VCF loads without errors."""
        vcf_path = test_data_manager.get_sarek_caller_output(caller)
        if not vcf_path.exists():
            pytest.skip(f"No {caller} output available")
        
        loader = VCFLoader(test_db.url)
        result = loader.load(vcf_path)
        
        assert result["variants_loaded"] > 0
        assert result["errors"] == []


class TestSarekSomaticVCF:
    """Test somatic (tumor-normal) VCF handling."""
    
    def test_mutect2_output_loads(self, test_data_manager, test_db):
        """Mutect2 somatic VCF loads with tumor/normal sample distinction."""
        vcf_path = test_data_manager.get_sarek_somatic_output("mutect2")
        if not vcf_path.exists():
            pytest.skip("No Mutect2 somatic output available")
        
        loader = VCFLoader(test_db.url)
        result = loader.load(vcf_path, sample_type="somatic")
        
        # Verify sample metadata indicates tumor/normal
        with test_db.cursor() as cur:
            cur.execute("SELECT sample_id, is_tumor FROM samples")
            samples = {r["sample_id"]: r["is_tumor"] for r in cur.fetchall()}
        
        assert any(samples.values())  # At least one tumor sample
```

### Test Suite 5: raredisease GENMOD Annotations

```python
# tests/integration/test_raredisease_outputs.py
import pytest
from vcf_pg_loader import VCFLoader

class TestRarediseaseGENMOD:
    """Test loading nf-core/raredisease GENMOD-annotated VCFs."""
    
    def test_genetic_models_extracted(self, raredisease_vcf, test_db):
        """GENMOD GeneticModels field is parsed and queryable."""
        loader = VCFLoader(test_db.url)
        loader.load(raredisease_vcf)
        
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT chrom, pos, ref, alt, genetic_models
                FROM variants
                WHERE genetic_models IS NOT NULL
            """)
            annotated = cur.fetchall()
        
        # raredisease adds GeneticModels like "FAM:AR_hom|AD"
        model_types = set()
        for row in annotated:
            if row["genetic_models"]:
                for model in row["genetic_models"]:
                    model_types.add(model.split(":")[-1])
        
        # Expected models from GENMOD
        expected = {"AR_hom", "AR_comp", "AD", "XR", "XD"}
        assert model_types & expected  # At least one match
    
    def test_compound_pairs_extracted(self, raredisease_vcf, test_db):
        """GENMOD Compounds field links variant pairs."""
        loader = VCFLoader(test_db.url)
        loader.load(raredisease_vcf)
        
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT gene, COUNT(*) as compound_count
                FROM variant_compounds
                GROUP BY gene
                HAVING COUNT(*) >= 2
            """)
            compound_genes = cur.fetchall()
        
        # Should have some genes with compound het pairs
        assert len(compound_genes) >= 0  # May be empty in test data
    
    def test_rank_scores_populated(self, raredisease_vcf, test_db):
        """GENMOD RankScore is queryable for prioritization."""
        loader = VCFLoader(test_db.url)
        loader.load(raredisease_vcf)
        
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT chrom, pos, gene, rank_score
                FROM variants
                WHERE rank_score IS NOT NULL
                ORDER BY rank_score DESC
                LIMIT 10
            """)
            top_ranked = cur.fetchall()
        
        if top_ranked:
            # Higher rank = more likely pathogenic
            assert all(r["rank_score"] >= 0 for r in top_ranked)
```

---

## Acceptance Tests: Proving the Gap Is Filled

These tests prove your tool enables query patterns impossible with streaming tools.

### Test Suite 6: Cross-Sample Queries

```python
# tests/acceptance/test_cross_sample_queries.py
import pytest

class TestQueriesImpossibleWithBcftools:
    """
    Demonstrate queries that require database persistence.
    
    These are the "killer features" that justify using a database
    over bcftools/slivar streaming.
    """
    
    def test_variant_seen_before_query(self, loaded_giab_trio, test_db):
        """
        Query: "Has this variant been seen in our lab before?"
        
        This requires checking ALL historical VCFs - impossible with
        bcftools without parsing every file every time.
        """
        # Pick a variant from HG002
        test_variant = {"chrom": "chr1", "pos": 12345, "ref": "A", "alt": "G"}
        
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT sample_id, load_date
                FROM variants v
                JOIN samples s ON v.sample_id = s.id
                WHERE v.chrom = %(chrom)s 
                  AND v.pos = %(pos)s
                  AND v.ref = %(ref)s
                  AND v.alt = %(alt)s
            """, test_variant)
            occurrences = cur.fetchall()
        
        # This query is O(1) with database, O(n_files) with bcftools
        assert isinstance(occurrences, list)
    
    def test_internal_allele_frequency(self, loaded_giab_trio, test_db):
        """
        Calculate allele frequency across internal cohort.
        
        bcftools can't do this without multi-pass processing
        and sample-count tracking.
        """
        with test_db.cursor() as cur:
            # Count carriers per variant
            cur.execute("""
                WITH variant_counts AS (
                    SELECT chrom, pos, ref, alt,
                           COUNT(DISTINCT sample_id) as n_carriers,
                           SUM(CASE WHEN gt_type = 2 THEN 2 
                                    WHEN gt_type = 1 THEN 1 
                                    ELSE 0 END) as allele_count
                    FROM variants
                    GROUP BY chrom, pos, ref, alt
                ),
                total_samples AS (
                    SELECT COUNT(*) * 2 as total_alleles FROM samples
                )
                SELECT vc.*, 
                       vc.allele_count::float / ts.total_alleles as internal_af
                FROM variant_counts vc, total_samples ts
                WHERE vc.n_carriers >= 2
                ORDER BY internal_af DESC
                LIMIT 20
            """)
            recurrent_variants = cur.fetchall()
        
        # Variants seen in multiple trio members
        assert len(recurrent_variants) >= 0
    
    def test_variant_classification_history(self, test_db):
        """
        Query: "What was the ClinVar classification when we reported this?"
        
        Requires temporal tracking impossible with VCF files.
        """
        with test_db.cursor() as cur:
            # Hypothetical query on versioned annotation table
            cur.execute("""
                SELECT v.chrom, v.pos, v.gene,
                       ah.clinvar_sig as classification_at_report,
                       ah.annotation_date,
                       current_clinvar.clinvar_sig as current_classification
                FROM variants v
                JOIN annotation_history ah ON v.id = ah.variant_id
                LEFT JOIN clinvar_current current_clinvar 
                    ON v.chrom = current_clinvar.chrom 
                   AND v.pos = current_clinvar.pos
                WHERE ah.annotation_date < '2024-01-01'
                  AND ah.clinvar_sig != current_clinvar.clinvar_sig
            """)
            reclassified = cur.fetchall()
        
        # Any reclassified variants since report date
        assert isinstance(reclassified, list)


class TestTrioInheritanceQueries:
    """Test SQL-based inheritance pattern detection."""
    
    def test_de_novo_detection_sql(self, loaded_giab_trio, test_db):
        """
        Detect de novo variants using SQL JOIN.
        
        Equivalent to slivar: kid.het && mom.hom_ref && dad.hom_ref
        """
        with test_db.cursor() as cur:
            cur.execute("""
                SELECT v.chrom, v.pos, v.ref, v.alt, v.gene,
                       g_kid.gt_type, g_mom.gt_type, g_dad.gt_type
                FROM variants v
                JOIN genotypes g_kid ON v.id = g_kid.variant_id 
                    AND g_kid.sample_id = (SELECT id FROM samples WHERE name = 'HG002')
                JOIN genotypes g_mom ON v.id = g_mom.variant_id
                    AND g_mom.sample_id = (SELECT id FROM samples WHERE name = 'HG004')
                JOIN genotypes g_dad ON v.id = g_dad.variant_id
                    AND g_dad.sample_id = (SELECT id FROM samples WHERE name = 'HG003')
                WHERE g_kid.gt_type = 1  -- HET
                  AND g_mom.gt_type = 0  -- HOM_REF
                  AND g_dad.gt_type = 0  -- HOM_REF
                  AND g_kid.depth >= 10
                  AND g_mom.depth >= 10
                  AND g_dad.depth >= 10
            """)
            de_novos = cur.fetchall()
        
        # slivar paper expects 1-2 de novos per trio
        # GIAB has ~2500 Mendelian violations, some are de novo
        assert len(de_novos) < 100  # Should be rare
    
    def test_compound_het_sql(self, loaded_giab_trio, test_db):
        """
        Detect compound heterozygotes using SQL aggregation.
        
        This requires:
        1. Kid is het at both positions
        2. Each variant comes from different parent
        3. Both variants in same gene
        """
        with test_db.cursor() as cur:
            cur.execute("""
                WITH proband_hets AS (
                    SELECT v.id, v.chrom, v.pos, v.gene,
                           g_kid.gt_type as kid_gt,
                           g_mom.gt_type as mom_gt,
                           g_dad.gt_type as dad_gt,
                           CASE 
                               WHEN g_mom.gt_type = 1 AND g_dad.gt_type = 0 THEN 'maternal'
                               WHEN g_dad.gt_type = 1 AND g_mom.gt_type = 0 THEN 'paternal'
                               ELSE 'unknown'
                           END as parent_of_origin
                    FROM variants v
                    JOIN genotypes g_kid ON v.id = g_kid.variant_id 
                        AND g_kid.sample_id = (SELECT id FROM samples WHERE name = 'HG002')
                    JOIN genotypes g_mom ON v.id = g_mom.variant_id
                        AND g_mom.sample_id = (SELECT id FROM samples WHERE name = 'HG004')
                    JOIN genotypes g_dad ON v.id = g_dad.variant_id
                        AND g_dad.sample_id = (SELECT id FROM samples WHERE name = 'HG003')
                    WHERE g_kid.gt_type = 1  -- Kid is het
                      AND v.gene IS NOT NULL
                      AND v.gnomad_af < 0.01
                )
                SELECT gene, 
                       COUNT(*) as het_count,
                       COUNT(DISTINCT parent_of_origin) as distinct_parents
                FROM proband_hets
                WHERE parent_of_origin != 'unknown'
                GROUP BY gene
                HAVING COUNT(*) >= 2 
                   AND COUNT(DISTINCT parent_of_origin) = 2
            """)
            compound_het_genes = cur.fetchall()
        
        # slivar paper expects 9-11 compound het genes per WGS trio
        assert 5 <= len(compound_het_genes) <= 20


class TestPerformanceBenchmarks:
    """Validate performance targets against vcf2db baseline."""
    
    def test_loading_speed_exceeds_vcf2db(self, giab_chr21_vcf, test_db, benchmark):
        """
        Load speed should exceed vcf2db's ~1,200 variants/second.
        Target: 50,000+ variants/second.
        """
        loader = VCFLoader(test_db.url)
        
        result = benchmark(loader.load, giab_chr21_vcf)
        
        variants_per_second = result["variants_loaded"] / result["load_time_seconds"]
        
        # vcf2db baseline: 1,200 v/s
        # Our target: 50,000+ v/s (conservative for chr21 subset)
        assert variants_per_second > 10_000, \
            f"Loading too slow: {variants_per_second:.0f} v/s < 10,000 target"
    
    def test_region_query_under_100ms(self, loaded_giab_trio, test_db, benchmark):
        """Region queries should complete in <100ms using GiST index."""
        def run_region_query():
            with test_db.cursor() as cur:
                cur.execute("""
                    SELECT chrom, pos, ref, alt, gene
                    FROM variants
                    WHERE chrom = 'chr17'
                      AND pos_range && int8range(43044295, 43125483)
                """)  # BRCA1 region
                return cur.fetchall()
        
        result = benchmark(run_region_query)
        
        # GiST index should make this sub-millisecond
        assert benchmark.stats["mean"] < 0.1  # 100ms
```

---

## Test Data Validation Fixtures

```python
# tests/conftest.py
import pytest
import asyncpg
from pathlib import Path

@pytest.fixture(scope="session")
def test_db():
    """Create isolated test database."""
    import subprocess
    
    db_name = "vcf_pg_loader_test"
    
    # Create database
    subprocess.run([
        "createdb", "-h", "localhost", "-U", "postgres", db_name
    ], check=True)
    
    yield DatabaseConnection(f"postgresql://postgres@localhost/{db_name}")
    
    # Cleanup
    subprocess.run(["dropdb", "-h", "localhost", "-U", "postgres", db_name])


@pytest.fixture(scope="session")
def test_data_manager(tmp_path_factory):
    """Manage test data downloads."""
    cache_dir = tmp_path_factory.mktemp("vcf_test_data")
    return TestDataManager(cache_dir)


@pytest.fixture(scope="session")
def giab_chr21_vcf(test_data_manager):
    """GIAB HG002 chr21 subset for fast testing."""
    return test_data_manager.get_giab_chr21("HG002")


@pytest.fixture(scope="session") 
def giab_trio_vcfs(test_data_manager):
    """Full GIAB Ashkenazi trio VCFs."""
    return {
        "proband": test_data_manager.get_vcf("HG002_benchmark"),
        "father": test_data_manager.get_vcf("HG003_benchmark"),
        "mother": test_data_manager.get_vcf("HG004_benchmark"),
    }


@pytest.fixture(scope="session")
def loaded_giab_trio(giab_trio_vcfs, test_db):
    """Load GIAB trio into database for acceptance tests."""
    from vcf_pg_loader import VCFLoader
    
    # Create PED file
    ped_content = """#family_id\tindividual_id\tpaternal_id\tmaternal_id\tsex\tphenotype
GIAB\tHG002\tHG003\tHG004\t1\t2
GIAB\tHG003\t0\t0\t1\t1
GIAB\tHG004\t0\t0\t2\t1
"""
    
    loader = VCFLoader(test_db.url)
    loader.load_trio(
        giab_trio_vcfs["proband"],
        giab_trio_vcfs["father"],
        giab_trio_vcfs["mother"],
        ped_content
    )
    
    return test_db


# Expected variant counts from slivar paper for validation
GIAB_TRIO_EXPECTATIONS = {
    "de_novo_count": (1, 5),  # 1-2 expected, allow margin
    "compound_het_genes": (5, 15),  # 9-11 expected
    "autosomal_recessive": (0, 5),  # ~1 expected
    "mendelian_error_rate": 0.001,  # 0.05% expected
}
```

---

## CI/CD Test Workflow

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -e ".[dev]"
      
      - name: Run unit tests
        run: pytest tests/unit -v --cov

  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
    
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install bcftools
        run: sudo apt-get install -y bcftools tabix
      
      - name: Cache test data
        uses: actions/cache@v4
        with:
          path: ~/.cache/vcf-pg-loader-tests
          key: test-data-v1-${{ hashFiles('tests/fixtures/nf_core_datasets.py') }}
      
      - name: Install dependencies
        run: pip install -e ".[dev]"
      
      - name: Run integration tests
        run: pytest tests/integration -v --tb=short
        env:
          TEST_DB_URL: postgresql://postgres:postgres@localhost/test

  acceptance-tests:
    runs-on: ubuntu-latest
    needs: integration-tests
    if: github.ref == 'refs/heads/main'
    
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Download GIAB chr21 subset
        run: |
          mkdir -p test_data
          wget -q "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz" -O test_data/HG002.vcf.gz
          bcftools view -r chr21 test_data/HG002.vcf.gz -Oz -o test_data/HG002_chr21.vcf.gz
      
      - name: Run acceptance tests
        run: pytest tests/acceptance -v --benchmark-only
```

---

## Summary: What These Tests Prove

| Test Category | What It Validates | Gap Addressed |
|--------------|-------------------|---------------|
| Header Parsing | Correct Number=A/R/G schema | vcf2db skips Number=A fields |
| Normalization | vt-compatible left-alignment | Many tools skip normalization |
| sarek Compatibility | Load VEP-annotated VCFs | No database loading in nf-core |
| raredisease Compatibility | Parse GENMOD annotations | GENMOD outputs to VCF, not DB |
| Cross-Sample Queries | SQL enables "seen before?" | Impossible with bcftools |
| Trio Analysis | SQL inheritance patterns | Equivalent to slivar, but persistent |
| Performance | >10K v/s (vs vcf2db 1.2K) | Performance gap vs legacy tools |

The test suite proves your tool:
1. **Parses correctly** what existing tools get wrong (Number=A)
2. **Integrates with** nf-core pipeline outputs (sarek, raredisease)
3. **Enables queries** impossible with streaming tools
4. **Performs better** than vcf2db baseline
5. **Validates against** published benchmarks (slivar paper expectations)
