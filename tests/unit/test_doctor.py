"""Tests for the doctor module - dependency checking."""

from unittest.mock import MagicMock, patch

from vcf_pg_loader.doctor import (
    CheckResult,
    DependencyChecker,
    check_all,
)


class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_check_result_passed(self):
        """Should represent a passing check."""
        result = CheckResult(name="Python", passed=True, version="3.12.4")
        assert result.passed is True
        assert result.version == "3.12.4"
        assert result.message is None

    def test_check_result_failed(self):
        """Should represent a failing check with message."""
        result = CheckResult(
            name="Docker",
            passed=False,
            message="Docker not installed"
        )
        assert result.passed is False
        assert result.message == "Docker not installed"


class TestDependencyCheckerPython:
    """Tests for Python version checking."""

    def test_check_python_passes_for_current_version(self):
        """Should pass for Python 3.11+ (current runtime)."""
        checker = DependencyChecker()
        result = checker.check_python()
        assert result.passed is True
        assert "3.11" in result.version or "3.12" in result.version or "3.13" in result.version

    def test_check_python_includes_version(self):
        """Should include Python version in result."""
        checker = DependencyChecker()
        result = checker.check_python()
        assert result.version is not None
        assert "3." in result.version


class TestDependencyCheckerDocker:
    """Tests for Docker checking."""

    @patch("vcf_pg_loader.doctor.docker")
    def test_check_docker_passes_when_installed(self, mock_docker):
        """Should pass when Docker is available."""
        mock_client = MagicMock()
        mock_client.version.return_value = {"Version": "24.0.5"}
        mock_docker.from_env.return_value = mock_client

        checker = DependencyChecker()
        result = checker.check_docker()

        assert result.passed is True
        assert "24.0.5" in result.version

    @patch("vcf_pg_loader.doctor.docker")
    def test_check_docker_fails_when_not_installed(self, mock_docker):
        """Should fail when Docker is not available."""
        import docker.errors

        mock_docker.from_env.side_effect = docker.errors.DockerException("Not found")
        mock_docker.errors = docker.errors

        checker = DependencyChecker()
        result = checker.check_docker()

        assert result.passed is False
        assert result.message is not None

    @patch("vcf_pg_loader.doctor.docker")
    def test_check_docker_daemon_running(self, mock_docker):
        """Should check if Docker daemon is running."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_docker.from_env.return_value = mock_client

        checker = DependencyChecker()
        result = checker.check_docker_daemon()

        assert result.passed is True

    @patch("vcf_pg_loader.doctor.docker")
    def test_check_docker_daemon_not_running(self, mock_docker):
        """Should fail when Docker daemon not running."""
        import docker.errors

        mock_client = MagicMock()
        mock_client.ping.side_effect = docker.errors.APIError("Daemon not running")
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors = docker.errors

        checker = DependencyChecker()
        result = checker.check_docker_daemon()

        assert result.passed is False


class TestDependencyCheckerCyvcf2:
    """Tests for cyvcf2 checking."""

    def test_check_cyvcf2_passes(self):
        """Should pass when cyvcf2 is installed."""
        checker = DependencyChecker()
        result = checker.check_cyvcf2()

        assert result.passed is True
        assert result.version is not None

    @patch.dict("sys.modules", {"cyvcf2": None})
    def test_check_cyvcf2_fails_when_missing(self):
        """Should fail when cyvcf2 not installed."""
        checker = DependencyChecker()

        with patch("vcf_pg_loader.doctor.importlib.import_module") as mock_import:
            mock_import.side_effect = ImportError("No module named 'cyvcf2'")
            result = checker.check_cyvcf2()

        assert result.passed is False


class TestDependencyCheckerAllChecks:
    """Tests for running all checks."""

    def test_check_all_returns_list(self):
        """Should return list of CheckResults."""
        checker = DependencyChecker()
        results = checker.check_all()

        assert isinstance(results, list)
        assert len(results) >= 3
        assert all(isinstance(r, CheckResult) for r in results)

    def test_check_all_includes_python(self):
        """Should include Python check."""
        checker = DependencyChecker()
        results = checker.check_all()

        names = [r.name for r in results]
        assert "Python" in names

    def test_check_all_includes_docker(self):
        """Should include Docker check."""
        checker = DependencyChecker()
        results = checker.check_all()

        names = [r.name for r in results]
        assert "Docker" in names


class TestInstallInstructions:
    """Tests for installation instructions."""

    def test_get_docker_install_instructions_macos(self):
        """Should return macOS Docker install instructions."""
        checker = DependencyChecker()
        instructions = checker.get_install_instructions("docker", "darwin")

        assert "brew" in instructions.lower() or "docker" in instructions.lower()

    def test_get_docker_install_instructions_linux(self):
        """Should return Linux Docker install instructions."""
        checker = DependencyChecker()
        instructions = checker.get_install_instructions("docker", "linux")

        assert "get.docker.com" in instructions or "apt" in instructions.lower()


class TestConvenienceFunction:
    """Tests for module-level convenience functions."""

    def test_check_all_function(self):
        """Should provide module-level check_all function."""
        results = check_all()

        assert isinstance(results, list)
        assert len(results) >= 3
