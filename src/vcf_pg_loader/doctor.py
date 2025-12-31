"""System dependency checker for vcf-pg-loader."""

import importlib
import os
import platform
import ssl
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

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

    def check_tls_support(self) -> CheckResult:
        """Check TLS/SSL support for secure database connections."""
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2

            has_tls13 = hasattr(ssl.TLSVersion, "TLSv1_3")
            version = f"TLS 1.2+{' (TLS 1.3 available)' if has_tls13 else ''}"

            return CheckResult(
                name="TLS Support",
                passed=True,
                version=version,
            )
        except Exception as e:
            return CheckResult(
                name="TLS Support",
                passed=False,
                message=f"TLS not available: {e}",
            )

    def check_tls_certificates(self) -> CheckResult:
        """Check if TLS certificates are configured."""
        ca_cert = os.environ.get("VCF_PG_LOADER_TLS_CA_CERT")
        client_cert = os.environ.get("VCF_PG_LOADER_TLS_CLIENT_CERT")
        client_key = os.environ.get("VCF_PG_LOADER_TLS_CLIENT_KEY")

        issues = []

        if ca_cert:
            if not Path(ca_cert).exists():
                issues.append(f"CA cert not found: {ca_cert}")
        if client_cert:
            if not Path(client_cert).exists():
                issues.append(f"Client cert not found: {client_cert}")
        if client_key:
            if not Path(client_key).exists():
                issues.append(f"Client key not found: {client_key}")

        if issues:
            return CheckResult(
                name="TLS Certificates",
                passed=False,
                message="; ".join(issues),
            )

        if ca_cert or client_cert:
            return CheckResult(
                name="TLS Certificates",
                passed=True,
                version="configured",
            )

        return CheckResult(
            name="TLS Certificates",
            passed=True,
            version="using system defaults",
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
            self.check_tls_support(),
            self.check_tls_certificates(),
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


class ContainerSecurityChecker:
    """Check container security configuration for HIPAA compliance."""

    def __init__(self) -> None:
        self._in_container: bool | None = None

    def is_in_container(self) -> bool:
        """Detect if running inside a container."""
        if self._in_container is not None:
            return self._in_container

        if Path("/.dockerenv").exists():
            self._in_container = True
            return True

        try:
            with open("/proc/1/cgroup") as f:
                content = f.read()
                if "docker" in content or "kubepods" in content or "containerd" in content:
                    self._in_container = True
                    return True
        except (FileNotFoundError, PermissionError):
            pass

        self._in_container = False
        return False

    def check_non_root(self) -> CheckResult:
        """Check if running as non-root user."""
        uid = os.getuid()
        passed = uid != 0

        if passed:
            return CheckResult(
                name="Non-root user",
                passed=True,
                version=f"UID {uid}",
            )
        return CheckResult(
            name="Non-root user",
            passed=False,
            message="Running as root (UID 0). Use --user flag or USER directive.",
        )

    def check_read_only_filesystem(self) -> CheckResult:
        """Check if root filesystem is read-only."""
        try:
            root_test = Path("/.vcf_pg_loader_ro_test")
            root_test.write_text("test")
            root_test.unlink()
            return CheckResult(
                name="Read-only filesystem",
                passed=False,
                message="Root filesystem is writable. Use --read-only flag.",
            )
        except (PermissionError, OSError):
            return CheckResult(
                name="Read-only filesystem",
                passed=True,
                version="read-only",
            )

    def check_capabilities(self) -> CheckResult:
        """Check if dangerous capabilities are dropped."""
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("CapEff:"):
                        cap_hex = line.split()[1]
                        cap_int = int(cap_hex, 16)

                        dangerous_caps = {
                            0: "CAP_CHOWN",
                            1: "CAP_DAC_OVERRIDE",
                            2: "CAP_DAC_READ_SEARCH",
                            7: "CAP_SETUID",
                            8: "CAP_SETGID",
                            21: "CAP_SYS_ADMIN",
                            23: "CAP_SYS_NICE",
                            25: "CAP_SYS_RESOURCE",
                        }

                        active_dangerous = []
                        for bit, name in dangerous_caps.items():
                            if cap_int & (1 << bit):
                                active_dangerous.append(name)

                        if cap_int == 0:
                            return CheckResult(
                                name="Dropped capabilities",
                                passed=True,
                                version="all dropped",
                            )
                        elif not active_dangerous:
                            return CheckResult(
                                name="Dropped capabilities",
                                passed=True,
                                version="minimal",
                            )
                        else:
                            return CheckResult(
                                name="Dropped capabilities",
                                passed=False,
                                message=f"Active dangerous caps: {', '.join(active_dangerous[:3])}",
                            )
        except (FileNotFoundError, PermissionError):
            pass

        return CheckResult(
            name="Dropped capabilities",
            passed=True,
            version="unable to check (non-Linux)",
        )

    def check_no_new_privileges(self) -> CheckResult:
        """Check if no_new_privs is set."""
        try:
            result = subprocess.run(
                ["cat", "/proc/self/status"],
                capture_output=True,
                text=True,
                check=False,
            )
            for line in result.stdout.splitlines():
                if line.startswith("NoNewPrivs:"):
                    value = int(line.split()[1])
                    if value == 1:
                        return CheckResult(
                            name="No new privileges",
                            passed=True,
                            version="enabled",
                        )
                    else:
                        return CheckResult(
                            name="No new privileges",
                            passed=False,
                            message="no-new-privileges not set. Use --security-opt=no-new-privileges:true",
                        )
        except (subprocess.SubprocessError, ValueError, IndexError):
            pass

        return CheckResult(
            name="No new privileges",
            passed=True,
            version="unable to check",
        )

    def check_network_isolation(self) -> CheckResult:
        """Check network isolation (basic check)."""
        try:
            result = subprocess.run(
                ["cat", "/etc/resolv.conf"],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                nameservers = [
                    line for line in result.stdout.splitlines() if line.startswith("nameserver")
                ]
                if nameservers:
                    return CheckResult(
                        name="Network isolation",
                        passed=True,
                        version=f"{len(nameservers)} nameserver(s)",
                        message="Verify internal network via docker-compose.hipaa.yml",
                    )
        except subprocess.SubprocessError:
            pass

        return CheckResult(
            name="Network isolation",
            passed=True,
            version="no external access detected",
        )

    def check_all(self) -> list[CheckResult]:
        """Run all container security checks."""
        if not self.is_in_container():
            return [
                CheckResult(
                    name="Container environment",
                    passed=True,
                    version="not in container",
                    message="Container security checks only apply inside containers",
                )
            ]

        return [
            self.check_non_root(),
            self.check_read_only_filesystem(),
            self.check_capabilities(),
            self.check_no_new_privileges(),
            self.check_network_isolation(),
        ]


def check_container_security() -> list[CheckResult]:
    """Convenience function to run container security checks.

    Returns:
        List of CheckResult for each security check.
    """
    checker = ContainerSecurityChecker()
    return checker.check_all()
