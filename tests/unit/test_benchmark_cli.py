"""Tests for benchmark CLI command and utilities."""

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
        import gzip

        vcf_path = generate_synthetic_vcf(10)
        try:
            with gzip.open(vcf_path, "rt") as f:
                first_line = f.readline()
                assert first_line.startswith("##fileformat=VCF")
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
