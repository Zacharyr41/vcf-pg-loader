"""Managed PostgreSQL database using Docker for zero-config usage."""

import time

import docker
import docker.errors

CONTAINER_NAME = "vcf-pg-loader-db"
DEFAULT_PORT = 5432
DEFAULT_USER = "vcfloader"
DEFAULT_PASSWORD = "vcfloader"
DEFAULT_DATABASE = "variants"
DEFAULT_IMAGE = "postgres:16-alpine"
VOLUME_NAME = "vcf-pg-loader-data"


class DockerNotAvailableError(Exception):
    """Raised when Docker is not available or not running."""

    pass


class ManagedDatabase:
    """Manages a local PostgreSQL container for zero-config database usage.

    This class handles starting, stopping, and managing a Docker container
    running PostgreSQL, allowing users to use vcf-pg-loader without manually
    setting up a database.

    Example:
        db = ManagedDatabase()
        url = db.start()
        # Use url for loading...
        db.stop()
    """

    def __init__(self):
        """Initialize connection to Docker daemon.

        Raises:
            DockerNotAvailableError: If Docker is not installed or not running.
        """
        try:
            self._client = docker.from_env()
            self._client.ping()
        except docker.errors.DockerException as e:
            raise DockerNotAvailableError(
                "Docker is not available. Please install Docker and ensure it's running.\n"
                "  macOS: brew install --cask docker\n"
                "  Linux: curl -fsSL https://get.docker.com | sh\n"
                "  Windows: https://docs.docker.com/desktop/install/windows-install/"
            ) from e

    def _get_container(self):
        """Get the managed container if it exists."""
        try:
            return self._client.containers.get(CONTAINER_NAME)
        except docker.errors.NotFound:
            return None

    def _get_host_port(self, container) -> str:
        """Extract the host port from container network settings."""
        ports = container.attrs.get("NetworkSettings", {}).get("Ports", {})
        tcp_port = ports.get("5432/tcp")
        if tcp_port and len(tcp_port) > 0:
            return tcp_port[0].get("HostPort", str(DEFAULT_PORT))
        return str(DEFAULT_PORT)

    def is_running(self) -> bool:
        """Check if the managed database container is running.

        Returns:
            True if container exists and is running, False otherwise.
        """
        container = self._get_container()
        if container is None:
            return False
        return container.status == "running"

    def get_url(self) -> str | None:
        """Get the PostgreSQL connection URL if the database is running.

        Returns:
            Connection URL string, or None if not running.
        """
        container = self._get_container()
        if container is None or container.status != "running":
            return None

        port = self._get_host_port(container)
        return f"postgresql://{DEFAULT_USER}:{DEFAULT_PASSWORD}@localhost:{port}/{DEFAULT_DATABASE}"

    def start(self, wait: bool = True, timeout: int = 30) -> str:
        """Start the managed PostgreSQL database.

        If the container doesn't exist, it will be created. If it exists but
        is stopped, it will be started. If it's already running, returns
        the connection URL immediately.

        Args:
            wait: If True, wait for PostgreSQL to be ready before returning.
            timeout: Maximum seconds to wait for database to be ready.

        Returns:
            PostgreSQL connection URL.
        """
        container = self._get_container()

        if container is None:
            container = self._client.containers.run(
                DEFAULT_IMAGE,
                name=CONTAINER_NAME,
                detach=True,
                ports={"5432/tcp": DEFAULT_PORT},
                environment={
                    "POSTGRES_USER": DEFAULT_USER,
                    "POSTGRES_PASSWORD": DEFAULT_PASSWORD,
                    "POSTGRES_DB": DEFAULT_DATABASE,
                },
                volumes={VOLUME_NAME: {"bind": "/var/lib/postgresql/data", "mode": "rw"}},
                restart_policy={"Name": "unless-stopped"},
            )
        elif container.status != "running":
            container.start()

        container.reload()

        if wait:
            self._wait_for_ready(timeout)

        return self.get_url()

    def _wait_for_ready(self, timeout: int = 30) -> None:
        """Wait for PostgreSQL to be ready to accept connections."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            container = self._get_container()
            if container and container.status == "running":
                exit_code, _ = container.exec_run(
                    f"pg_isready -U {DEFAULT_USER} -d {DEFAULT_DATABASE}",
                    demux=True,
                )
                if exit_code == 0:
                    return
            time.sleep(0.5)

        raise TimeoutError(f"PostgreSQL did not become ready within {timeout} seconds")

    def stop(self) -> None:
        """Stop the managed PostgreSQL container.

        Does nothing if the container is not running.
        """
        container = self._get_container()
        if container is not None and container.status == "running":
            container.stop()

    def status(self) -> dict:
        """Get the status of the managed database.

        Returns:
            Dict with running status, URL, and container info.
        """
        container = self._get_container()
        running = container is not None and container.status == "running"

        return {
            "running": running,
            "url": self.get_url() if running else None,
            "container_name": CONTAINER_NAME,
            "image": DEFAULT_IMAGE,
        }

    def reset(self) -> None:
        """Stop and remove the container and its data volume.

        This completely removes the managed database including all data.
        """
        container = self._get_container()
        if container is not None:
            if container.status == "running":
                container.stop()
            container.remove(v=True)

        try:
            volume = self._client.volumes.get(VOLUME_NAME)
            volume.remove()
        except docker.errors.NotFound:
            pass


def get_managed_db_url() -> str | None:
    """Convenience function to get URL of running managed database.

    Returns:
        Connection URL if managed database is running, None otherwise.
    """
    try:
        db = ManagedDatabase()
        return db.get_url()
    except DockerNotAvailableError:
        return None
