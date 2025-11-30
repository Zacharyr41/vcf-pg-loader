"""System dependency checker for vcf-pg-loader."""

import importlib
import platform
import sys
from dataclasses import dataclass

import docker
import docker.errors


@dataclass
class CheckResult:
    """Result of a dependency check."""

    name: str
    passed: bool
    version: str | None = None
    message: str | None = None


INSTALL_INSTRUCTIONS = {
    "docker": {
        "darwin": "brew install --cask docker",
        "linux": "curl -fsSL https://get.docker.com | sh",
        "windows": "Download from https://docs.docker.com/desktop/install/windows-install/",
    },
    "python": {
        "darwin": "brew install python@3.11",
        "linux": "sudo apt install python3.11 or use pyenv",
        "windows": "Download from https://www.python.org/downloads/",
    },
}


class DependencyChecker:
    """Check system dependencies for vcf-pg-loader."""

    def check_python(self) -> CheckResult:
        """Check Python version is 3.11+."""
        version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        passed = sys.version_info >= (3, 11)

        return CheckResult(
            name="Python",
            passed=passed,
            version=version,
            message=None if passed else "Python 3.11+ required",
        )

    def check_docker(self) -> CheckResult:
        """Check if Docker is installed."""
        try:
            client = docker.from_env()
            version_info = client.version()
            version = version_info.get("Version", "unknown")
            return CheckResult(
                name="Docker",
                passed=True,
                version=version,
            )
        except docker.errors.DockerException as e:
            return CheckResult(
                name="Docker",
                passed=False,
                message=f"Docker not available: {e}",
            )

    def check_docker_daemon(self) -> CheckResult:
        """Check if Docker daemon is running."""
        try:
            client = docker.from_env()
            client.ping()
            return CheckResult(
                name="Docker daemon",
                passed=True,
                version="running",
            )
        except docker.errors.DockerException:
            return CheckResult(
                name="Docker daemon",
                passed=False,
                message="Docker daemon not running. Start Docker Desktop or run 'systemctl start docker'",
            )

    def check_cyvcf2(self) -> CheckResult:
        """Check if cyvcf2 is installed."""
        try:
            cyvcf2 = importlib.import_module("cyvcf2")
            version = getattr(cyvcf2, "__version__", "unknown")
            return CheckResult(
                name="cyvcf2",
                passed=True,
                version=version,
            )
        except ImportError:
            return CheckResult(
                name="cyvcf2",
                passed=False,
                message="cyvcf2 not installed. Install with: pip install cyvcf2",
            )

    def check_asyncpg(self) -> CheckResult:
        """Check if asyncpg is installed."""
        try:
            asyncpg = importlib.import_module("asyncpg")
            version = getattr(asyncpg, "__version__", "unknown")
            return CheckResult(
                name="asyncpg",
                passed=True,
                version=version,
            )
        except ImportError:
            return CheckResult(
                name="asyncpg",
                passed=False,
                message="asyncpg not installed",
            )

    def check_all(self) -> list[CheckResult]:
        """Run all dependency checks.

        Returns:
            List of CheckResult for each dependency.
        """
        return [
            self.check_python(),
            self.check_cyvcf2(),
            self.check_asyncpg(),
            self.check_docker(),
            self.check_docker_daemon(),
        ]

    def get_install_instructions(self, dependency: str, os_platform: str | None = None) -> str:
        """Get installation instructions for a dependency.

        Args:
            dependency: Name of the dependency (e.g., 'docker', 'python').
            os_platform: Platform name (darwin, linux, windows). Auto-detected if None.

        Returns:
            Installation instructions string.
        """
        if os_platform is None:
            os_platform = platform.system().lower()
            if os_platform not in ("darwin", "linux", "windows"):
                os_platform = "linux"

        instructions = INSTALL_INSTRUCTIONS.get(dependency, {})
        return instructions.get(os_platform, f"Please install {dependency}")

    def all_passed(self) -> bool:
        """Check if all dependencies are satisfied.

        Returns:
            True if all checks pass, False otherwise.
        """
        results = self.check_all()
        return all(r.passed for r in results)

    def core_passed(self) -> bool:
        """Check if core dependencies (Python, cyvcf2) are satisfied.

        These are required for basic functionality (parsing, benchmarks).
        Docker is only needed for managed database.

        Returns:
            True if core checks pass, False otherwise.
        """
        return self.check_python().passed and self.check_cyvcf2().passed


def check_all() -> list[CheckResult]:
    """Convenience function to run all dependency checks.

    Returns:
        List of CheckResult for each dependency.
    """
    checker = DependencyChecker()
    return checker.check_all()
