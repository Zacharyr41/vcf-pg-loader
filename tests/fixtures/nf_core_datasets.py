"""nf-core test dataset references and management."""

import hashlib
import os
import shutil
import subprocess
import urllib.request
from pathlib import Path

NF_CORE_TEST_DATA_BASE_URL = "https://raw.githubusercontent.com/nf-core/test-datasets/modules/data"

NF_CORE_TEST_DATA = {
    "dbsnp_146_hg38": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/genome/vcf/dbsnp_146.hg38.vcf.gz",
        "local_path": "genomics/homo_sapiens/genome/vcf/dbsnp_146.hg38.vcf.gz",
        "description": "dbSNP subset used by nf-core/sarek tests",
    },
    "gnomad_r2_hg38": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/genome/vcf/gnomAD.r2.1.1.vcf.gz",
        "local_path": "genomics/homo_sapiens/genome/vcf/gnomAD.r2.1.1.vcf.gz",
        "description": "gnomAD subset for germline resource testing",
    },
    "mills_1000g_indels": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/genome/vcf/mills_and_1000G.indels.vcf.gz",
        "local_path": "genomics/homo_sapiens/genome/vcf/mills_and_1000G.indels.vcf.gz",
        "description": "Known indels for BQSR testing",
    },
    "haplotypecaller_vcf": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/illumina/gatk/haplotypecaller_calls/test_haplotc.vcf.gz",
        "local_path": "genomics/homo_sapiens/illumina/gatk/haplotypecaller_calls/test_haplotc.vcf.gz",
        "description": "HaplotypeCaller output VCF",
    },
    "haplotypecaller_ann_vcf": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/illumina/gatk/haplotypecaller_calls/test_haplotc.ann.vcf.gz",
        "local_path": "genomics/homo_sapiens/illumina/gatk/haplotypecaller_calls/test_haplotc.ann.vcf.gz",
        "description": "HaplotypeCaller annotated VCF (SnpEff/VEP)",
    },
    "mutect2_vcf": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/illumina/gatk/paired_mutect2_calls/test_test2_paired_mutect2_calls.vcf.gz",
        "local_path": "genomics/homo_sapiens/illumina/gatk/paired_mutect2_calls/test_test2_paired_mutect2_calls.vcf.gz",
        "description": "Mutect2 somatic VCF",
    },
    "mutect2_filtered_vcf": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/illumina/gatk/paired_mutect2_calls/test_test2_paired_filtered_mutect2_calls.vcf.gz",
        "local_path": "genomics/homo_sapiens/illumina/gatk/paired_mutect2_calls/test_test2_paired_filtered_mutect2_calls.vcf.gz",
        "description": "Mutect2 filtered somatic VCF",
    },
    "genmod_vcf": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/illumina/vcf/genmod.vcf.gz",
        "local_path": "genomics/homo_sapiens/illumina/vcf/genmod.vcf.gz",
        "description": "GENMOD annotated VCF (raredisease)",
    },
    "na12878_giab_chr22": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/illumina/vcf/NA12878_GIAB.chr22.vcf.gz",
        "local_path": "genomics/homo_sapiens/illumina/vcf/NA12878_GIAB.chr22.vcf.gz",
        "description": "NA12878 GIAB chr22 benchmark",
    },
    "na12878_giab_chr21_22": {
        "url": f"{NF_CORE_TEST_DATA_BASE_URL}/genomics/homo_sapiens/illumina/vcf/NA12878_GIAB.chr21_22.vcf.gz",
        "local_path": "genomics/homo_sapiens/illumina/vcf/NA12878_GIAB.chr21_22.vcf.gz",
        "description": "NA12878 GIAB chr21-22 benchmark",
    },
}

GIAB_BENCHMARK_DATA = {
    "HG002_benchmark": {
        "url": "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
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
}

GIAB_CHR21_EXPECTATIONS = {
    "HG002": {
        "total_variants": (50_000, 65_000),
        "snps": (40_000, 55_000),
        "indels": (7_000, 15_000),
        "snp_ratio": (0.80, 0.90),
    },
    "HG003": {
        "total_variants": (50_000, 65_000),
        "snps": (40_000, 55_000),
        "indels": (7_000, 15_000),
        "snp_ratio": (0.80, 0.90),
    },
    "HG004": {
        "total_variants": (50_000, 65_000),
        "snps": (40_000, 55_000),
        "indels": (7_000, 15_000),
        "snp_ratio": (0.80, 0.90),
    },
}
"""
GIAB chr21 expectations derived from actual GIAB v4.2.1 benchmark data:

HG002 chr21 raw VCF: 55,210 variant lines
HG002 chr21 after multi-allelic decomposition: 55,812 variants
  - SNPs: ~46,500 (83%)
  - Indels: ~9,300 (17%)

These values were verified using:
  bcftools view -H HG002_chr21.vcf.gz | wc -l  # raw count
  bcftools norm -m -any ... | wc -l            # decomposed count

The ranges (50K-65K) allow for minor version differences in GIAB releases.
"""

GIAB_FULL_EXPECTATIONS = {
    "HG002": {
        "total_variants": (4_000_000, 4_100_000),
        "snp_ratio": (0.80, 0.88),
    },
    "HG003": {
        "total_variants": (3_950_000, 4_050_000),
        "snp_ratio": (0.80, 0.88),
    },
    "HG004": {
        "total_variants": (4_000_000, 4_100_000),
        "snp_ratio": (0.80, 0.88),
    },
}

GIAB_TRIO_EXPECTATIONS = {
    "de_novo_count": (1, 5),
    "compound_het_genes": (5, 15),
    "autosomal_recessive": (0, 5),
    "mendelian_error_rate": 0.001,
}

GIAB_V421_PUBLICATION_VALUES = {
    "total_variants_in_benchmark_regions": 4_968_730,
    "mendelian_violations": 2_502,
    "mendelian_violation_rate": 0.0005,
    "putative_de_novo_snvs": 1_110,
    "putative_de_novo_indels": 213,
    "putative_de_novo_total": 1_323,
    "benchmark_regions_coverage_gbp": 2.54,
    "expected_ti_tv_ratio": (2.0, 2.2),
    "expected_het_hom_ratio": (1.8, 2.2),
}
"""
Published GIAB v4.2.1 GRCh38 values from:
- Wagner J et al. Cell Genomics 2(5):100128 (2022). DOI: 10.1016/j.xgen.2022.100128
- Zook JM et al. Nature Biotechnology 37:561-566 (2019). DOI: 10.1038/s41587-019-0074-6

Slivar expected variant yields per trio (WGS ~30x, DeepVariant):
- De novo: 1.4 mean (0-5 range acceptable)
- Autosomal recessive: 0.8 mean (0-3 range acceptable)
- Compound heterozygotes: 9.2 genes (5-15 acceptable)
- X-linked recessive: 1.7 mean (0-5 range acceptable)

Source: Pedersen BS et al. npj Genomic Medicine 6:60 (2021). DOI: 10.1038/s41525-021-00227-3
"""


ADDITIONAL_TEST_DATA = {
    "strelka_snvs": {
        "local_paths": [
            "test_data/HCC1395T_vs_HCC1395N.strelka.somatic_snvs_chr22.vcf.gz",
        ],
        "description": "Strelka2 somatic SNVs",
    },
    "strelka_indels": {
        "local_paths": [
            "test_data/HCC1395T_vs_HCC1395N.strelka.somatic_indels_chr22.vcf.gz",
        ],
        "description": "Strelka2 somatic indels",
    },
}


def find_local_test_datasets() -> Path | None:
    """Find local nf-core/test-datasets clone."""
    search_paths = [
        Path(os.environ.get("NF_CORE_TEST_DATASETS", "")),
        Path.home() / "Code" / "test-datasets",
        Path.home() / "Code" / "other-test-data" / "test-datasets",
        Path.home() / "nf-core" / "test-datasets",
        Path("/data/test-datasets"),
        Path.cwd().parent / "test-datasets",
    ]

    for p in search_paths:
        if p.exists() and (p / "data" / "genomics").exists():
            return p / "data"

    return None


def find_giab_cache() -> Path | None:
    """Find GIAB cache directory."""
    cache_dir = Path.home() / ".cache" / "vcf-pg-loader-tests" / "giab"
    if cache_dir.exists():
        return cache_dir
    return None


class GIABDataManager:
    """Manages test data downloads with caching."""

    def __init__(self, cache_dir: Path | None = None):
        self.cache_dir = cache_dir or Path.home() / ".cache" / "vcf-pg-loader-tests"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.local_test_datasets = find_local_test_datasets()

    def get_vcf(self, dataset_key: str, subset_region: str | None = None) -> Path:
        """Get a test VCF, using local clone if available, otherwise download."""
        all_data = {**NF_CORE_TEST_DATA, **GIAB_BENCHMARK_DATA}
        if dataset_key not in all_data:
            raise ValueError(f"Unknown dataset: {dataset_key}")

        dataset = all_data[dataset_key]

        if self.local_test_datasets and "local_path" in dataset:
            local_path = self.local_test_datasets / dataset["local_path"]
            if local_path.exists():
                if subset_region:
                    return self._subset_vcf_safe(local_path, subset_region)
                return local_path

        if "url" not in dataset:
            raise ValueError(f"Dataset {dataset_key} has no URL and no local path")

        url = dataset["url"]
        filename = url.split("/")[-1]
        cached_path = self.cache_dir / filename

        if not cached_path.exists():
            self._download_file(url, cached_path)

        if subset_region:
            return self._subset_vcf_safe(cached_path, subset_region)

        return cached_path

    def get_giab_chr21(self, sample: str = "HG002") -> Path | None:
        """Get chr21 subset of GIAB sample for fast testing."""
        giab_cache = find_giab_cache()
        if giab_cache:
            chr21_path = giab_cache / f"{sample}_chr21.vcf.gz"
            if chr21_path.exists():
                return chr21_path

        try:
            full_vcf = self.get_vcf(f"{sample}_benchmark")
        except Exception:
            return None

        subset_path = self.cache_dir / f"{sample}_chr21.vcf.gz"

        if not subset_path.exists():
            if not self._has_bcftools():
                return None
            self._subset_vcf(full_vcf, subset_path, "chr21")

        return subset_path

    def get_giab_full(self, sample: str = "HG002") -> Path | None:
        """Get full GIAB benchmark VCF for a sample."""
        giab_cache = find_giab_cache()
        if giab_cache:
            full_path = giab_cache / f"{sample}_benchmark.vcf.gz"
            if full_path.exists():
                return full_path

        try:
            return self.get_vcf(f"{sample}_benchmark")
        except Exception:
            return None

    def get_giab_trio(self) -> dict[str, Path] | None:
        """Get all three GIAB Ashkenazi trio VCFs."""
        trio = {}
        for sample, role in [("HG002", "proband"), ("HG003", "father"), ("HG004", "mother")]:
            vcf = self.get_giab_full(sample)
            if vcf is None:
                return None
            trio[role] = vcf
        return trio

    def get_giab_trio_chr21(self) -> dict[str, Path] | None:
        """Get chr21 subsets for all three GIAB Ashkenazi trio members."""
        trio = {}
        for sample, role in [("HG002", "proband"), ("HG003", "father"), ("HG004", "mother")]:
            vcf = self.get_giab_chr21(sample)
            if vcf is None:
                return None
            trio[role] = vcf
        return trio

    def get_nf_core_output(self, pipeline: str, output_type: str) -> Path | None:
        """Get output from nf-core pipeline test run or pre-generated test data."""
        if pipeline == "sarek":
            if output_type == "annotation":
                try:
                    return self.get_vcf("haplotypecaller_ann_vcf")
                except Exception:
                    pass
            elif output_type == "variants":
                try:
                    return self.get_vcf("haplotypecaller_vcf")
                except Exception:
                    pass

        if pipeline == "raredisease":
            if output_type in ("annotation", "variants"):
                try:
                    return self.get_vcf("genmod_vcf")
                except Exception:
                    pass

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
        caller_datasets = {
            "haplotypecaller": "haplotypecaller_vcf",
            "mutect2": "mutect2_filtered_vcf",
        }

        if caller in caller_datasets:
            try:
                return self.get_vcf(caller_datasets[caller])
            except Exception:
                pass

        if caller == "strelka":
            strelka_vcf = self._find_additional_test_data("strelka_snvs")
            if strelka_vcf:
                return strelka_vcf

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

    def _find_additional_test_data(self, key: str) -> Path | None:
        """Find additional test data from local repos."""
        if key not in ADDITIONAL_TEST_DATA:
            return None

        search_dirs = [
            Path.home() / "Code" / "test-datasets",
            Path.home() / "Code" / "other-test-data" / "test-datasets",
            Path.cwd().parent / "test-datasets",
        ]

        for base_dir in search_dirs:
            if not base_dir.exists():
                continue
            for local_path in ADDITIONAL_TEST_DATA[key]["local_paths"]:
                full_path = base_dir / local_path
                if full_path.exists():
                    return full_path

        return None

    def get_sarek_somatic_output(self, caller: str) -> Path | None:
        """Get somatic VCF from sarek."""
        if caller == "mutect2":
            try:
                return self.get_vcf("mutect2_filtered_vcf")
            except Exception:
                pass

        sarek_dir = self.cache_dir / "nf_core_outputs" / "sarek_somatic"
        if not sarek_dir.exists():
            return None

        vcf_files = list(sarek_dir.glob(f"**/*{caller}*.vcf.gz"))
        return vcf_files[0] if vcf_files else None

    def _has_bcftools(self) -> bool:
        """Check if bcftools is available."""
        return shutil.which("bcftools") is not None

    def _download_file(self, url: str, dest: Path) -> None:
        """Download a file from URL to destination."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(url, dest)
        if dest.suffix == ".gz" and self._has_bcftools():
            subprocess.run(["bcftools", "index", str(dest)], check=False)

    def _subset_vcf_safe(self, input_vcf: Path, region: str) -> Path:
        """Subset VCF if bcftools available, otherwise return original."""
        if not self._has_bcftools():
            return input_vcf

        subset_path = self.cache_dir / f"{input_vcf.stem}_{region}.vcf.gz"
        if not subset_path.exists():
            self._subset_vcf(input_vcf, subset_path, region)
        return subset_path

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

    def list_available_datasets(self) -> dict[str, bool]:
        """List all datasets and their availability."""
        result = {}
        all_data = {**NF_CORE_TEST_DATA, **GIAB_BENCHMARK_DATA}

        for key, dataset in all_data.items():
            available = False

            if self.local_test_datasets and "local_path" in dataset:
                local_path = self.local_test_datasets / dataset["local_path"]
                available = local_path.exists()

            if not available and "url" in dataset:
                cached_path = self.cache_dir / dataset["url"].split("/")[-1]
                available = cached_path.exists()

            result[key] = available

        return result
