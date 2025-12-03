"""Tests for benchmark CLI command and utilities."""

import gzip
import re
from pathlib import Path

from typer.testing import CliRunner

from vcf_pg_loader.benchmark import (
    BenchmarkResult,
    generate_synthetic_vcf,
    run_parsing_benchmark,
)
from vcf_pg_loader.cli import app

runner = CliRunner()
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestGenerateSyntheticVCF:
    """Tests for synthetic VCF generation."""

    def test_generates_correct_variant_count(self):
        """Should generate approximately the requested number of variants."""
        vcf_path = generate_synthetic_vcf(1000)
        try:
            count, _ = run_parsing_benchmark(vcf_path, normalize=False)
            assert 950 <= count <= 1050
        finally:
            vcf_path.unlink()

    def test_generates_gzipped_vcf(self):
        """Should generate a gzipped VCF file."""
        vcf_path = generate_synthetic_vcf(100)
        try:
            assert vcf_path.suffix == ".gz"
            assert vcf_path.exists()
        finally:
            vcf_path.unlink()

    def test_vcf_has_valid_header(self):
        """Generated VCF should have a valid header."""
        vcf_path = generate_synthetic_vcf(10)
        try:
            with gzip.open(vcf_path, "rt") as f:
                first_line = f.readline()
                assert first_line.startswith("##fileformat=VCF")
        finally:
            vcf_path.unlink()


class TestRealisticSyntheticVCF:
    """Tests for realistic synthetic VCF generation that mimics real variant callers."""

    def test_has_snpeff_ann_field_in_header(self):
        """Generated VCF should include SnpEff ANN field definition in header."""
        vcf_path = generate_synthetic_vcf(100, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                header = f.read()
            assert "##INFO=<ID=ANN," in header
            assert "Functional annotations" in header
        finally:
            vcf_path.unlink()

    def test_has_multiple_info_fields_in_header(self):
        """Generated VCF should have realistic INFO fields like real callers."""
        vcf_path = generate_synthetic_vcf(100, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                header = f.read()
            required_fields = ["AC", "AN", "MQ", "QD", "FS", "SOR"]
            for field in required_fields:
                assert f"##INFO=<ID={field}," in header, f"Missing INFO field: {field}"
        finally:
            vcf_path.unlink()

    def test_has_multiple_format_fields_in_header(self):
        """Generated VCF should have realistic FORMAT fields."""
        vcf_path = generate_synthetic_vcf(100, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                header = f.read()
            required_fields = ["GT", "DP", "AD", "GQ", "PL"]
            for field in required_fields:
                assert f"##FORMAT=<ID={field}," in header, f"Missing FORMAT field: {field}"
        finally:
            vcf_path.unlink()

    def test_variants_have_annotations(self):
        """At least some variants should have ANN annotations."""
        vcf_path = generate_synthetic_vcf(100, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                content = f.read()
            variant_lines = [line for line in content.split("\n") if line and not line.startswith("#")]
            ann_count = sum(1 for line in variant_lines if "ANN=" in line)
            assert ann_count >= len(variant_lines) * 0.5, "At least 50% of variants should have ANN"
        finally:
            vcf_path.unlink()

    def test_variants_have_realistic_info_fields(self):
        """Variants should have multiple INFO fields like real data."""
        vcf_path = generate_synthetic_vcf(100, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                content = f.read()
            variant_lines = [line for line in content.split("\n") if line and not line.startswith("#")]
            for line in variant_lines[:10]:
                fields = line.split("\t")
                info = fields[7]
                info_keys = [field.split("=")[0] for field in info.split(";") if "=" in field]
                assert len(info_keys) >= 5, f"Expected at least 5 INFO fields, got {len(info_keys)}"
        finally:
            vcf_path.unlink()

    def test_has_multiallelic_variants(self):
        """Realistic VCF should include some multiallelic variants."""
        vcf_path = generate_synthetic_vcf(500, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                content = f.read()
            variant_lines = [line for line in content.split("\n") if line and not line.startswith("#")]
            multiallelic_count = sum(1 for line in variant_lines if "," in line.split("\t")[4])
            assert multiallelic_count >= 10, f"Expected at least 10 multiallelic variants, got {multiallelic_count}"
        finally:
            vcf_path.unlink()

    def test_has_realistic_indel_distribution(self):
        """Should have a realistic mix of SNVs and indels (not just simple SNVs)."""
        vcf_path = generate_synthetic_vcf(500, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                content = f.read()
            variant_lines = [line for line in content.split("\n") if line and not line.startswith("#")]
            indel_count = 0
            for line in variant_lines:
                fields = line.split("\t")
                ref, alt = fields[3], fields[4]
                alts = alt.split(",")
                if any(len(ref) != len(a) for a in alts):
                    indel_count += 1
            indel_pct = indel_count / len(variant_lines) * 100
            assert 10 <= indel_pct <= 40, f"Indel percentage {indel_pct:.1f}% outside expected range 10-40%"
        finally:
            vcf_path.unlink()

    def test_has_realistic_format_sample_data(self):
        """Sample data should have realistic FORMAT fields (AD, GQ, PL)."""
        vcf_path = generate_synthetic_vcf(100, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                content = f.read()
            variant_lines = [line for line in content.split("\n") if line and not line.startswith("#")]
            for line in variant_lines[:10]:
                fields = line.split("\t")
                format_str = fields[8]
                format_fields = format_str.split(":")
                assert "GT" in format_fields
                assert "AD" in format_fields, "Missing AD in FORMAT"
                assert "GQ" in format_fields, "Missing GQ in FORMAT"
        finally:
            vcf_path.unlink()

    def test_has_mix_of_filter_values(self):
        """Should have both PASS and filtered variants like real data."""
        vcf_path = generate_synthetic_vcf(500, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                content = f.read()
            variant_lines = [line for line in content.split("\n") if line and not line.startswith("#")]
            pass_count = sum(1 for line in variant_lines if line.split("\t")[6] == "PASS")
            filtered_count = len(variant_lines) - pass_count
            assert pass_count > 0, "Should have some PASS variants"
            assert filtered_count > 0, "Should have some filtered variants"
        finally:
            vcf_path.unlink()

    def test_annotation_has_proper_format(self):
        """ANN field should follow SnpEff format with pipe-separated values."""
        vcf_path = generate_synthetic_vcf(100, realistic=True)
        try:
            with gzip.open(vcf_path, "rt") as f:
                content = f.read()
            variant_lines = [line for line in content.split("\n") if line and not line.startswith("#")]
            ann_pattern = re.compile(r"ANN=([^;]+)")
            found_valid_ann = False
            for line in variant_lines:
                match = ann_pattern.search(line)
                if match:
                    ann_value = match.group(1)
                    parts = ann_value.split("|")
                    assert len(parts) >= 10, f"ANN should have at least 10 pipe-separated fields, got {len(parts)}"
                    found_valid_ann = True
                    break
            assert found_valid_ann, "Should find at least one valid ANN field"
        finally:
            vcf_path.unlink()


class TestRunParsingBenchmark:
    """Tests for parsing benchmark."""

    def test_returns_variant_count_and_time(self):
        """Should return variant count and elapsed time."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        count, elapsed = run_parsing_benchmark(vcf_path)
        assert count == 4
        assert elapsed > 0
        assert elapsed < 5

    def test_respects_batch_size(self):
        """Should work with different batch sizes."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        count1, _ = run_parsing_benchmark(vcf_path, batch_size=1)
        count2, _ = run_parsing_benchmark(vcf_path, batch_size=100)
        assert count1 == count2


class TestBenchmarkResult:
    """Tests for BenchmarkResult dataclass."""

    def test_to_dict_includes_parsing_info(self):
        """to_dict should include parsing information."""
        result = BenchmarkResult(
            vcf_path="/test.vcf",
            variant_count=1000,
            parsing_time=1.0,
            parsing_rate=1000.0,
        )
        d = result.to_dict()
        assert d["variant_count"] == 1000
        assert d["parsing"]["time_seconds"] == 1.0
        assert d["parsing"]["rate_per_second"] == 1000.0

    def test_to_dict_includes_loading_info_when_present(self):
        """to_dict should include loading info when available."""
        result = BenchmarkResult(
            vcf_path="/test.vcf",
            variant_count=1000,
            parsing_time=1.0,
            parsing_rate=1000.0,
            loading_time=2.0,
            loading_rate=500.0,
        )
        d = result.to_dict()
        assert "loading" in d
        assert d["loading"]["time_seconds"] == 2.0
        assert d["loading"]["rate_per_second"] == 500.0

    def test_to_dict_excludes_loading_when_not_present(self):
        """to_dict should not include loading when not performed."""
        result = BenchmarkResult(
            vcf_path="/test.vcf",
            variant_count=1000,
            parsing_time=1.0,
            parsing_rate=1000.0,
        )
        d = result.to_dict()
        assert "loading" not in d


class TestBenchmarkCLI:
    """Tests for benchmark CLI command."""

    def test_benchmark_help(self):
        """Benchmark command should show help."""
        result = runner.invoke(app, ["benchmark", "--help"])
        assert result.exit_code == 0
        assert "benchmark" in result.stdout.lower()

    def test_benchmark_with_fixture(self):
        """Benchmark should work with built-in fixture."""
        result = runner.invoke(app, ["benchmark"])
        assert result.exit_code == 0
        assert "Parsing:" in result.stdout
        assert "/sec" in result.stdout

    def test_benchmark_synthetic(self):
        """Benchmark should work with synthetic data."""
        result = runner.invoke(app, ["benchmark", "--synthetic", "1000"])
        assert result.exit_code == 0
        assert "synthetic" in result.stdout.lower()
        assert "1,000" in result.stdout or "1000" in result.stdout

    def test_benchmark_json_output(self):
        """Benchmark should output valid JSON with --json flag."""
        import json

        result = runner.invoke(app, ["benchmark", "--synthetic", "100", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "variant_count" in data
        assert "parsing" in data

    def test_benchmark_quiet_mode(self):
        """Benchmark quiet mode should show minimal output."""
        result = runner.invoke(app, ["benchmark", "--quiet"])
        assert result.exit_code == 0
        assert "Benchmark Results" not in result.stdout
        assert "Parsing:" in result.stdout

    def test_benchmark_missing_vcf_file(self):
        """Benchmark should error for missing VCF file."""
        result = runner.invoke(app, ["benchmark", "--vcf", "/nonexistent.vcf"])
        assert result.exit_code == 1
        assert "not found" in result.stdout.lower()
