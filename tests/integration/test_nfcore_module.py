"""
Integration tests for nf-core vcfpgloader/load module.

These tests verify:
1. The module exists in nf-core/modules repository
2. The module can be installed via nf-core tools
3. The stub test passes with correct outputs
"""

import json
import subprocess
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def nfcore_module_dir():
    """Get path to local nf-core module for testing."""
    module_dir = (
        Path(__file__).parent.parent.parent / "nf-core" / "modules" / "vcfpgloader" / "load"
    )
    if not module_dir.exists():
        pytest.skip("Local nf-core module not found")
    return module_dir


class TestNfcoreModuleExists:
    """Verify module exists in nf-core/modules repository."""

    def test_module_exists_in_nfcore_repo(self):
        """Check that vcfpgloader/load exists in nf-core/modules."""
        import urllib.request

        url = (
            "https://api.github.com/repos/nf-core/modules/contents/modules/nf-core/vcfpgloader/load"
        )
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                data = json.loads(response.read().decode())
                files = [f["name"] for f in data]
                assert "main.nf" in files
                assert "meta.yml" in files
                assert "environment.yml" in files
        except urllib.error.URLError:
            pytest.skip("Cannot reach GitHub API")

    def test_module_has_required_files(self, nfcore_module_dir):
        """Verify local module has all required files."""
        required_files = ["main.nf", "meta.yml", "environment.yml"]
        for filename in required_files:
            assert (nfcore_module_dir / filename).exists(), f"Missing {filename}"

    def test_meta_yml_valid(self, nfcore_module_dir):
        """Verify meta.yml has required fields."""
        import yaml

        meta_path = nfcore_module_dir / "meta.yml"
        with open(meta_path) as f:
            meta = yaml.safe_load(f)

        assert meta["name"] == "vcfpgloader_load"
        assert "input" in meta
        assert "output" in meta
        assert "tools" in meta


class TestNfcoreModuleStub:
    """Test the module stub functionality."""

    @pytest.fixture
    def test_workflow_dir(self, nfcore_module_dir):
        """Create a temporary directory with a test workflow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            workflow = tmpdir / "main.nf"
            workflow.write_text(f"""
nextflow.enable.dsl=2

include {{ VCFPGLOADER_LOAD }} from '{nfcore_module_dir}/main.nf'

workflow {{
    ch_input = Channel.of([
        [ id: 'test_sample' ],
        file('test.vcf.gz'),
        file('test.vcf.gz.tbi'),
        'localhost',
        5432,
        'testdb',
        'postgres',
        'public'
    ])

    VCFPGLOADER_LOAD(ch_input)
}}
""")

            config = tmpdir / "nextflow.config"
            config.write_text("""
docker.enabled = true
env.PGPASSWORD = 'test'

process {
    withName: 'VCFPGLOADER_LOAD' {
        ext.batch_size = '1000'
    }
}
""")

            (tmpdir / "test.vcf.gz").touch()
            (tmpdir / "test.vcf.gz.tbi").touch()

            yield tmpdir

    @pytest.mark.integration
    @pytest.mark.skip(reason="nf-test run in CI workflow, skip locally due to Java version issues")
    def test_stub_produces_expected_outputs(self, nfcore_module_dir):
        """Run stub test and verify outputs."""
        tests_dir = nfcore_module_dir / "tests"
        if not tests_dir.exists():
            pytest.skip("Tests directory not found")

        result = subprocess.run(
            ["nf-test", "test", "tests/main.nf.test", "--tag", "stub"],
            cwd=nfcore_module_dir,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if "nf-test: command not found" in result.stderr:
            pytest.skip("nf-test not installed")

        assert result.returncode == 0, f"nf-test failed:\n{result.stdout}\n{result.stderr}"


class TestNfcoreDocumentation:
    """Verify documentation matches actual module interface."""

    def test_readme_example_matches_module(self, nfcore_module_dir):
        """Check that README example uses correct process name and inputs."""
        readme_path = Path(__file__).parent.parent.parent / "README.md"
        readme = readme_path.read_text()

        assert "VCFPGLOADER_LOAD" in readme, "README should use correct process name"
        assert (
            "nf-core modules install vcfpgloader/load" in readme
        ), "README should show install command"
        assert (
            "modules/nf-core/vcfpgloader/load" in readme
        ), "README should show correct include path"

    def test_readme_inputs_match_meta(self, nfcore_module_dir):
        """Verify README documents all inputs from meta.yml."""
        readme_path = Path(__file__).parent.parent.parent / "README.md"
        readme = readme_path.read_text()

        expected_inputs = ["db_host", "db_port", "db_name", "db_user", "db_schema"]
        for input_name in expected_inputs:
            assert input_name in readme, f"README missing documentation for input: {input_name}"

    def test_readme_outputs_match_meta(self, nfcore_module_dir):
        """Verify README documents all outputs from meta.yml."""
        import yaml

        readme_path = Path(__file__).parent.parent.parent / "README.md"
        readme = readme_path.read_text()

        meta_path = nfcore_module_dir / "meta.yml"
        with open(meta_path) as f:
            meta = yaml.safe_load(f)

        for output_name in meta.get("output", {}).keys():
            if output_name.startswith("versions"):
                continue
            assert (
                output_name in readme.lower()
            ), f"README missing documentation for output: {output_name}"


class TestMainNfSyntax:
    """Validate main.nf process definition."""

    def test_process_has_required_directives(self, nfcore_module_dir):
        """Check that main.nf has required nf-core directives."""
        main_nf = (nfcore_module_dir / "main.nf").read_text()

        assert "process VCFPGLOADER_LOAD" in main_nf
        assert "conda" in main_nf
        assert "container" in main_nf
        assert "input:" in main_nf
        assert "output:" in main_nf
        assert "script:" in main_nf
        assert "stub:" in main_nf

    def test_container_uses_biocontainers(self, nfcore_module_dir):
        """Verify container uses BioContainers (not personal ghcr.io)."""
        main_nf = (nfcore_module_dir / "main.nf").read_text()

        assert "biocontainers/vcf-pg-loader" in main_nf
        assert "depot.galaxyproject.org/singularity/vcf-pg-loader" in main_nf
        assert "ghcr.io/zacharyr41" not in main_nf.lower()

    def test_outputs_emit_correct_channels(self, nfcore_module_dir):
        """Verify output channels match documented names."""
        main_nf = (nfcore_module_dir / "main.nf").read_text()

        assert "emit: report" in main_nf
        assert "emit: log" in main_nf
        assert "emit: row_count" in main_nf
