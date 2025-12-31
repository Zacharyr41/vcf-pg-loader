"""Tests for the doctor module - dependency checking."""

from unittest.mock import MagicMock, patch

from vcf_pg_loader.doctor import (
    CheckResult,
    ContainerSecurityChecker,
    DependencyChecker,
    check_all,
    check_container_security,
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
        result = CheckResult(name="Docker", passed=False, message="Docker not installed")
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


class TestContainerSecurityChecker:
    """Tests for container security checking."""

    def test_not_in_container(self):
        """Should detect when not running in container."""
        checker = ContainerSecurityChecker()
        with patch("vcf_pg_loader.doctor.Path") as mock_path:
            mock_path.return_value.exists.return_value = False
            with patch("builtins.open", side_effect=FileNotFoundError):
                result = checker.is_in_container()
        assert result is False

    def test_in_container_via_dockerenv(self):
        """Should detect container via .dockerenv file."""
        checker = ContainerSecurityChecker()
        checker._in_container = None
        with patch("vcf_pg_loader.doctor.Path") as mock_path:
            mock_path.return_value.exists.return_value = True
            result = checker.is_in_container()
        assert result is True

    def test_check_non_root_passes_for_non_root(self):
        """Should pass when running as non-root."""
        checker = ContainerSecurityChecker()
        with patch("os.getuid", return_value=1000):
            result = checker.check_non_root()
        assert result.passed is True
        assert "1000" in result.version

    def test_check_non_root_fails_for_root(self):
        """Should fail when running as root."""
        checker = ContainerSecurityChecker()
        with patch("os.getuid", return_value=0):
            result = checker.check_non_root()
        assert result.passed is False
        assert "root" in result.message.lower()

    def test_check_read_only_filesystem_passes(self):
        """Should pass when root filesystem is read-only."""
        checker = ContainerSecurityChecker()
        with patch("vcf_pg_loader.doctor.Path") as mock_path:
            instance = mock_path.return_value
            instance.write_text.side_effect = PermissionError("Read-only")
            result = checker.check_read_only_filesystem()
        assert result.passed is True

    def test_check_read_only_filesystem_fails(self):
        """Should fail when root filesystem is writable."""
        checker = ContainerSecurityChecker()
        with patch("vcf_pg_loader.doctor.Path") as mock_path:
            instance = mock_path.return_value
            instance.write_text.return_value = None
            instance.unlink.return_value = None
            result = checker.check_read_only_filesystem()
        assert result.passed is False

    def test_check_capabilities_all_dropped(self):
        """Should pass when all capabilities are dropped."""
        checker = ContainerSecurityChecker()
        with patch("builtins.open") as mock_open:
            mock_open.return_value.__enter__.return_value = iter(["CapEff:\t0000000000000000\n"])
            result = checker.check_capabilities()
        assert result.passed is True
        assert "all dropped" in result.version

    def test_check_capabilities_with_dangerous_caps(self):
        """Should fail when dangerous capabilities are present."""
        checker = ContainerSecurityChecker()
        with patch("builtins.open") as mock_open:
            mock_open.return_value.__enter__.return_value = iter(["CapEff:\t00000000a80425fb\n"])
            result = checker.check_capabilities()
        assert result.passed is False
        assert "CAP" in result.message

    def test_check_all_returns_results(self):
        """Should return list of CheckResults."""
        checker = ContainerSecurityChecker()
        checker._in_container = True

        with (
            patch.object(checker, "check_non_root") as mock_non_root,
            patch.object(checker, "check_read_only_filesystem") as mock_ro,
            patch.object(checker, "check_capabilities") as mock_caps,
            patch.object(checker, "check_no_new_privileges") as mock_nnp,
            patch.object(checker, "check_network_isolation") as mock_net,
        ):
            mock_non_root.return_value = CheckResult("Non-root", True, "UID 1000")
            mock_ro.return_value = CheckResult("Read-only", True, "read-only")
            mock_caps.return_value = CheckResult("Caps", True, "all dropped")
            mock_nnp.return_value = CheckResult("NNP", True, "enabled")
            mock_net.return_value = CheckResult("Network", True, "isolated")

            results = checker.check_all()

        assert isinstance(results, list)
        assert len(results) == 5
        assert all(r.passed for r in results)

    def test_check_all_not_in_container(self):
        """Should return single result when not in container."""
        checker = ContainerSecurityChecker()
        checker._in_container = False

        results = checker.check_all()

        assert len(results) == 1
        assert results[0].name == "Container environment"
        assert "not in container" in results[0].version


class TestCheckContainerSecurityFunction:
    """Tests for convenience function."""

    def test_check_container_security_function(self):
        """Should provide module-level check_container_security function."""
        results = check_container_security()

        assert isinstance(results, list)
        assert len(results) >= 1
