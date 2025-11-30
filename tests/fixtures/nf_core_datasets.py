"""nf-core test dataset references and management."""

from pathlib import Path
import hashlib
import subprocess
import urllib.request

NF_CORE_TEST_DATA = {
    "dbsnp_146_hg38": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/dbsnp_146.hg38.vcf.gz",
        "md5": None,
        "description": "dbSNP subset used by nf-core/sarek tests",
    },
    "gnomad_r2_hg38": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/gnomAD.r2.1.1.vcf.gz",
        "md5": None,
        "description": "gnomAD subset for germline resource testing",
    },
    "mills_1000g_indels": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/mills_and_1000G.indels.vcf.gz",
        "md5": None,
        "description": "Known indels for BQSR testing",
    },
    "sarek_test_vcf": {
        "url": "https://raw.githubusercontent.com/nf-core/test-datasets/sarek/testdata/vcf/test.vcf.gz",
        "md5": None,
        "description": "Sarek test VCF output",
    },
}

GIAB_BENCHMARK_DATA = {
    "HG002_benchmark": {
        "url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
        "bed_url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed",
        "variants": 4_042_186,
        "description": "Son/proband - ~4M variants",
    },
    "HG003_benchmark": {
        "url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG003_NA24149_father/NISTv4.2.1/GRCh38/HG003_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
        "variants": 3_993_257,
        "description": "Father",
    },
    "HG004_benchmark": {
        "url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG004_NA24143_mother/NISTv4.2.1/GRCh38/HG004_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
        "variants": 4_052_103,
        "description": "Mother",
    },
    "HG002_chr21_subset": {
        "description": "Chr21 only (~150K variants) - fast CI testing",
        "region": "chr21",
        "expected_variants": 150_000,
    },
}

CLINICAL_DATA = {
    "clinvar_grch38": {
        "url": "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz",
        "description": "Full ClinVar - ~2M variants, tests annotation parsing",
    },
    "gnomad_chr21": {
        "url": "https://storage.googleapis.com/gcp-public-data--gnomad/release/3.1.1/vcf/genomes/gnomad.genomes.v3.1.1.sites.chr21.vcf.bgz",
        "description": "gnomAD chr21 - tests population frequency fields",
    },
}

GIAB_TRIO_EXPECTATIONS = {
    "de_novo_count": (1, 5),
    "compound_het_genes": (5, 15),
    "autosomal_recessive": (0, 5),
    "mendelian_error_rate": 0.001,
}


class TestDataManager:
    """Manages test data downloads with caching."""

    def __init__(self, cache_dir: Path | None = None):
        self.cache_dir = cache_dir or Path.home() / ".cache" / "vcf-pg-loader-tests"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_vcf(self, dataset_key: str, subset_region: str | None = None) -> Path:
        """Download and cache a test VCF, optionally subsetting by region."""
        all_data = {**NF_CORE_TEST_DATA, **GIAB_BENCHMARK_DATA, **CLINICAL_DATA}
        if dataset_key not in all_data:
            raise ValueError(f"Unknown dataset: {dataset_key}")

        dataset = all_data[dataset_key]
        if "url" not in dataset:
            raise ValueError(f"Dataset {dataset_key} has no URL")

        url = dataset["url"]
        filename = url.split("/")[-1]
        cached_path = self.cache_dir / filename

        if not cached_path.exists():
            self._download_file(url, cached_path)

        if subset_region:
            subset_path = self.cache_dir / f"{cached_path.stem}_{subset_region}.vcf.gz"
            if not subset_path.exists():
                self._subset_vcf(cached_path, subset_path, subset_region)
            return subset_path

        return cached_path

    def get_giab_chr21(self, sample: str = "HG002") -> Path:
        """Get chr21 subset of GIAB sample for fast testing."""
        try:
            full_vcf = self.get_vcf(f"{sample}_benchmark")
        except Exception:
            return None

        subset_path = self.cache_dir / f"{sample}_chr21.vcf.gz"

        if not subset_path.exists():
            self._subset_vcf(full_vcf, subset_path, "chr21")

        return subset_path

    def get_nf_core_output(self, pipeline: str, output_type: str) -> Path | None:
        """Get output from nf-core pipeline test run."""
        test_output_dir = self.cache_dir / "nf_core_outputs" / pipeline
        if not test_output_dir.exists():
            return None

        vcf_patterns = {
            "annotation": "*.ann.vcf.gz",
            "variants": "*.vcf.gz",
        }

        pattern = vcf_patterns.get(output_type, "*.vcf.gz")
        vcf_files = list(test_output_dir.glob(f"**/{pattern}"))
        return vcf_files[0] if vcf_files else None

    def get_sarek_caller_output(self, caller: str) -> Path | None:
        """Get VCF from specific sarek variant caller."""
        sarek_dir = self.cache_dir / "nf_core_outputs" / "sarek"
        if not sarek_dir.exists():
            return None

        caller_patterns = {
            "haplotypecaller": "*haplotypecaller*.vcf.gz",
            "deepvariant": "*deepvariant*.vcf.gz",
            "freebayes": "*freebayes*.vcf.gz",
            "strelka": "*strelka*.vcf.gz",
        }

        pattern = caller_patterns.get(caller, "*.vcf.gz")
        vcf_files = list(sarek_dir.glob(f"**/{pattern}"))
        return vcf_files[0] if vcf_files else None

    def get_sarek_somatic_output(self, caller: str) -> Path | None:
        """Get somatic VCF from sarek."""
        sarek_dir = self.cache_dir / "nf_core_outputs" / "sarek_somatic"
        if not sarek_dir.exists():
            return None

        vcf_files = list(sarek_dir.glob(f"**/*{caller}*.vcf.gz"))
        return vcf_files[0] if vcf_files else None

    def _download_file(self, url: str, dest: Path) -> None:
        """Download a file from URL to destination."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(url, dest)

    def _subset_vcf(self, input_vcf: Path, output_vcf: Path, region: str) -> None:
        """Subset VCF to a specific region using bcftools."""
        subprocess.run(
            [
                "bcftools",
                "view",
                "-r",
                region,
                "-Oz",
                "-o",
                str(output_vcf),
                str(input_vcf),
            ],
            check=True,
        )
        subprocess.run(["bcftools", "index", str(output_vcf)], check=True)

    def compute_md5(self, filepath: Path) -> str:
        """Compute MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
