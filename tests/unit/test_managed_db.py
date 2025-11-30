"""Tests for managed PostgreSQL database functionality."""

from unittest.mock import MagicMock, patch

import pytest

from vcf_pg_loader.managed_db import (
    CONTAINER_NAME,
    DEFAULT_IMAGE,
    DEFAULT_PASSWORD,
    DEFAULT_PORT,
    DEFAULT_USER,
    DockerNotAvailableError,
    ManagedDatabase,
)


class TestManagedDatabaseConstants:
    """Tests for module constants."""

    def test_container_name_is_set(self):
        """Should have a consistent container name."""
        assert CONTAINER_NAME == "vcf-pg-loader-db"

    def test_default_port(self):
        """Should use standard PostgreSQL port."""
        assert DEFAULT_PORT == 5432

    def test_default_credentials(self):
        """Should have default credentials set."""
        assert DEFAULT_USER == "vcfloader"
        assert DEFAULT_PASSWORD == "vcfloader"

    def test_default_image(self):
        """Should use PostgreSQL 16 Alpine by default."""
        assert "postgres" in DEFAULT_IMAGE
        assert "16" in DEFAULT_IMAGE


class TestManagedDatabaseInit:
    """Tests for ManagedDatabase initialization."""

    @patch("vcf_pg_loader.managed_db.docker")
    def test_init_connects_to_docker(self, mock_docker):
        """Should connect to Docker daemon on init."""
        mock_client = MagicMock()
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()

        mock_docker.from_env.assert_called_once()
        assert db._client is mock_client

    @patch("vcf_pg_loader.managed_db.docker")
    def test_init_raises_when_docker_unavailable(self, mock_docker):
        """Should raise DockerNotAvailableError when Docker not available."""
        import docker.errors

        mock_docker.from_env.side_effect = docker.errors.DockerException("Not found")
        mock_docker.errors = docker.errors

        with pytest.raises(DockerNotAvailableError):
            ManagedDatabase()


class TestManagedDatabaseIsRunning:
    """Tests for is_running method."""

    @patch("vcf_pg_loader.managed_db.docker")
    def test_is_running_returns_true_when_container_running(self, mock_docker):
        """Should return True when container is running."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        assert db.is_running() is True

    @patch("vcf_pg_loader.managed_db.docker")
    def test_is_running_returns_false_when_container_stopped(self, mock_docker):
        """Should return False when container exists but stopped."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "exited"
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        assert db.is_running() is False

    @patch("vcf_pg_loader.managed_db.docker")
    def test_is_running_returns_false_when_container_not_found(self, mock_docker):
        """Should return False when container doesn't exist."""
        import docker.errors

        mock_client = MagicMock()
        mock_client.containers.get.side_effect = docker.errors.NotFound("Not found")
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors = docker.errors

        db = ManagedDatabase()
        assert db.is_running() is False


class TestManagedDatabaseGetUrl:
    """Tests for get_url method."""

    @patch("vcf_pg_loader.managed_db.docker")
    def test_get_url_returns_connection_string_when_running(self, mock_docker):
        """Should return PostgreSQL connection URL when running."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_container.attrs = {
            "NetworkSettings": {
                "Ports": {"5432/tcp": [{"HostPort": "5432"}]}
            }
        }
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        url = db.get_url()

        assert url is not None
        assert "postgresql://" in url
        assert "vcfloader" in url
        assert "localhost" in url

    @patch("vcf_pg_loader.managed_db.docker")
    def test_get_url_returns_none_when_not_running(self, mock_docker):
        """Should return None when container not running."""
        import docker.errors

        mock_client = MagicMock()
        mock_client.containers.get.side_effect = docker.errors.NotFound("Not found")
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors = docker.errors

        db = ManagedDatabase()
        assert db.get_url() is None


class TestManagedDatabaseStart:
    """Tests for start method."""

    @patch("vcf_pg_loader.managed_db.docker")
    def test_start_creates_container_when_not_exists(self, mock_docker):
        """Should create new container when none exists."""
        import docker.errors

        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_container.attrs = {
            "NetworkSettings": {
                "Ports": {"5432/tcp": [{"HostPort": "5432"}]}
            }
        }
        mock_container.exec_run.return_value = (0, b"ready")

        def get_side_effect(name):
            if mock_client.containers.run.called:
                return mock_container
            raise docker.errors.NotFound("Not found")

        mock_client.containers.get.side_effect = get_side_effect
        mock_client.containers.run.return_value = mock_container
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors = docker.errors

        db = ManagedDatabase()
        url = db.start()

        mock_client.containers.run.assert_called_once()
        assert "postgresql://" in url

    @patch("vcf_pg_loader.managed_db.docker")
    def test_start_starts_existing_stopped_container(self, mock_docker):
        """Should start existing container if stopped."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "exited"
        mock_container.attrs = {
            "NetworkSettings": {
                "Ports": {"5432/tcp": [{"HostPort": "5432"}]}
            }
        }
        mock_container.exec_run.return_value = (0, b"ready")

        def start_side_effect():
            mock_container.status = "running"

        mock_container.start.side_effect = start_side_effect
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        db.start()

        mock_container.start.assert_called_once()

    @patch("vcf_pg_loader.managed_db.docker")
    def test_start_returns_url_if_already_running(self, mock_docker):
        """Should return URL without restart if already running."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_container.attrs = {
            "NetworkSettings": {
                "Ports": {"5432/tcp": [{"HostPort": "5432"}]}
            }
        }
        mock_container.exec_run.return_value = (0, b"ready")
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        url = db.start()

        mock_container.start.assert_not_called()
        assert "postgresql://" in url


class TestManagedDatabaseStop:
    """Tests for stop method."""

    @patch("vcf_pg_loader.managed_db.docker")
    def test_stop_stops_running_container(self, mock_docker):
        """Should stop the container when running."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        db.stop()

        mock_container.stop.assert_called_once()

    @patch("vcf_pg_loader.managed_db.docker")
    def test_stop_does_nothing_when_not_running(self, mock_docker):
        """Should not error when container not running."""
        import docker.errors

        mock_client = MagicMock()
        mock_client.containers.get.side_effect = docker.errors.NotFound("Not found")
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors = docker.errors

        db = ManagedDatabase()
        db.stop()


class TestManagedDatabaseStatus:
    """Tests for status method."""

    @patch("vcf_pg_loader.managed_db.docker")
    def test_status_returns_info_when_running(self, mock_docker):
        """Should return status dict when container running."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_container.attrs = {
            "NetworkSettings": {
                "Ports": {"5432/tcp": [{"HostPort": "5432"}]}
            }
        }
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        status = db.status()

        assert status["running"] is True
        assert "url" in status
        assert status["container_name"] == CONTAINER_NAME

    @patch("vcf_pg_loader.managed_db.docker")
    def test_status_returns_not_running_when_stopped(self, mock_docker):
        """Should indicate not running when container stopped."""
        import docker.errors

        mock_client = MagicMock()
        mock_client.containers.get.side_effect = docker.errors.NotFound("Not found")
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors = docker.errors

        db = ManagedDatabase()
        status = db.status()

        assert status["running"] is False
        assert status["url"] is None


class TestManagedDatabaseReset:
    """Tests for reset method."""

    @patch("vcf_pg_loader.managed_db.docker")
    def test_reset_removes_container_and_volume(self, mock_docker):
        """Should remove container and its data volume."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_client.containers.get.return_value = mock_container
        mock_docker.from_env.return_value = mock_client

        db = ManagedDatabase()
        db.reset()

        mock_container.stop.assert_called_once()
        mock_container.remove.assert_called_once_with(v=True)
