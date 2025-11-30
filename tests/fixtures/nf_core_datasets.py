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

GIAB_TRIO_EXPECTATIONS = {
    "de_novo_count": (1, 5),
    "compound_het_genes": (5, 15),
    "autosomal_recessive": (0, 5),
    "mendelian_error_rate": 0.001,
}


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


class TestDataManager:
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
