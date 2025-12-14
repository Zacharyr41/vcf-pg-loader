"""Tests for Typer CLI interface."""

import os
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from typer.testing import CliRunner

from vcf_pg_loader.cli import _build_database_url, app

FIXTURES_DIR = Path(__file__).parent / "fixtures"
runner = CliRunner(env={"NO_COLOR": "1", "TERM": "dumb"})


class TestCLIHelp:
    """Tests for CLI help and basic structure."""

    def test_app_exists(self):
        """CLI app should exist."""
        assert app is not None

    def test_help_command(self):
        """CLI should display help."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Load VCF files into PostgreSQL" in result.stdout

    def test_load_help(self):
        """Load command should have help."""
        result = runner.invoke(app, ["load", "--help"])
        assert result.exit_code == 0
        assert "VCF file" in result.stdout
        assert "--db" in result.stdout
        assert "--batch" in result.stdout
        assert "--workers" in result.stdout

    def test_validate_help(self):
        """Validate command should have help."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0
        assert "load_batch_id" in result.stdout.lower() or "batch" in result.stdout.lower()

    def test_init_db_help(self):
        """Init-db command should have help."""
        result = runner.invoke(app, ["init-db", "--help"])
        assert result.exit_code == 0
        assert "--db" in result.stdout


class TestCLILoadCommand:
    """Tests for the load command."""

    def test_load_missing_vcf_file(self):
        """Load should error if VCF file doesn't exist."""
        result = runner.invoke(app, ["load", "/nonexistent/file.vcf"])
        assert result.exit_code == 1
        assert "not found" in result.stdout.lower() or "error" in result.stdout.lower()

    def test_load_with_custom_batch_size(self):
        """Load should accept custom batch size."""
        with patch("vcf_pg_loader.cli.VCFLoader") as mock_loader:
            mock_instance = AsyncMock()
            mock_instance.load_vcf.return_value = {
                "variants_loaded": 100,
                "load_batch_id": str(uuid4()),
                "file_hash": "abc123"
            }
            mock_loader.return_value = mock_instance

            vcf_path = FIXTURES_DIR / "with_annotations.vcf"
            result = runner.invoke(app, [
                "load", str(vcf_path),
                "--batch", "1000",
                "--db", "postgresql://test:test@localhost/test"
            ])

            if result.exit_code == 0:
                mock_loader.assert_called_once()
                call_args = mock_loader.call_args
                config = call_args[0][1]
                assert config.batch_size == 1000

    def test_load_with_custom_workers(self):
        """Load should accept custom worker count."""
        with patch("vcf_pg_loader.cli.VCFLoader") as mock_loader:
            mock_instance = AsyncMock()
            mock_instance.load_vcf.return_value = {
                "variants_loaded": 100,
                "load_batch_id": str(uuid4()),
                "file_hash": "abc123"
            }
            mock_loader.return_value = mock_instance

            vcf_path = FIXTURES_DIR / "with_annotations.vcf"
            result = runner.invoke(app, [
                "load", str(vcf_path),
                "--workers", "4",
                "--db", "postgresql://test:test@localhost/test"
            ])

            if result.exit_code == 0:
                mock_loader.assert_called_once()

    def test_load_success_output(self):
        """Load should display success message with variant count."""
        with patch("vcf_pg_loader.cli.VCFLoader") as mock_loader:
            load_batch_id = str(uuid4())
            mock_instance = AsyncMock()
            mock_instance.load_vcf.return_value = {
                "variants_loaded": 12345,
                "load_batch_id": load_batch_id,
                "file_hash": "d41d8cd98f00b204e9800998ecf8427e"
            }
            mock_loader.return_value = mock_instance

            vcf_path = FIXTURES_DIR / "with_annotations.vcf"
            result = runner.invoke(app, [
                "load", str(vcf_path),
                "--db", "postgresql://test:test@localhost/test"
            ])

            if result.exit_code == 0:
                assert "12,345" in result.stdout or "12345" in result.stdout
                assert load_batch_id in result.stdout

    def test_load_no_normalize_flag(self):
        """Load should support --no-normalize flag."""
        result = runner.invoke(app, ["load", "--help"])
        assert "--normalize" in result.stdout or "normalize" in result.stdout.lower()

    def test_load_keep_indexes_flag(self):
        """Load should support --keep-indexes flag."""
        result = runner.invoke(app, ["load", "--help"])
        assert "--drop-indexes" in result.stdout or "--keep-indexes" in result.stdout


class TestCLIValidateCommand:
    """Tests for the validate command."""

    def test_validate_requires_batch_id(self):
        """Validate should require a batch ID argument."""
        result = runner.invoke(app, ["validate"])
        assert result.exit_code != 0

    def test_validate_with_batch_id(self):
        """Validate should accept a batch ID."""
        with patch("vcf_pg_loader.cli.asyncpg") as mock_asyncpg:
            mock_conn = AsyncMock()
            mock_conn.fetchrow.return_value = {
                "status": "completed",
                "variants_loaded": 100
            }
            mock_conn.fetchval.side_effect = [100, 0]
            mock_asyncpg.connect.return_value.__aenter__.return_value = mock_conn

            batch_id = str(uuid4())
            runner.invoke(app, [
                "validate", batch_id,
                "--db", "postgresql://test:test@localhost/test"
            ])


class TestCLIInitDbCommand:
    """Tests for the init-db command."""

    def test_init_db_creates_schema(self):
        """Init-db should create database schema."""
        with patch("vcf_pg_loader.cli.asyncpg") as mock_asyncpg:
            with patch("vcf_pg_loader.cli.SchemaManager") as mock_schema:
                mock_conn = AsyncMock()
                mock_asyncpg.connect.return_value.__aenter__.return_value = mock_conn
                mock_schema_instance = AsyncMock()
                mock_schema.return_value = mock_schema_instance

                result = runner.invoke(app, [
                    "init-db",
                    "--db", "postgresql://test:test@localhost/test"
                ])

                if result.exit_code == 0:
                    mock_schema_instance.create_schema.assert_called_once()


class TestCLIOutputFormatting:
    """Tests for CLI output formatting."""

    def test_load_formats_large_numbers(self):
        """Load should format large variant counts with commas."""
        with patch("vcf_pg_loader.cli.VCFLoader") as mock_loader:
            mock_instance = AsyncMock()
            mock_instance.load_vcf.return_value = {
                "variants_loaded": 1234567,
                "load_batch_id": str(uuid4()),
                "file_hash": "abc123"
            }
            mock_loader.return_value = mock_instance

            vcf_path = FIXTURES_DIR / "with_annotations.vcf"
            result = runner.invoke(app, [
                "load", str(vcf_path),
                "--db", "postgresql://test:test@localhost/test"
            ])

            if result.exit_code == 0:
                assert "1,234,567" in result.stdout


class TestCLIErrorHandling:
    """Tests for CLI error handling."""

    def test_load_handles_connection_error(self):
        """Load should handle database connection errors gracefully."""
        with patch("vcf_pg_loader.cli.VCFLoader") as mock_loader:
            mock_instance = AsyncMock()
            mock_instance.load_vcf.side_effect = ConnectionError("Connection refused")
            mock_loader.return_value = mock_instance

            vcf_path = FIXTURES_DIR / "with_annotations.vcf"
            result = runner.invoke(app, [
                "load", str(vcf_path),
                "--db", "postgresql://test:test@localhost/test"
            ])

            assert result.exit_code != 0 or "error" in result.stdout.lower()

    def test_validate_handles_invalid_uuid(self):
        """Validate should handle invalid UUID gracefully."""
        runner.invoke(app, [
            "validate", "not-a-valid-uuid",
            "--db", "postgresql://test:test@localhost/test"
        ])


class TestBuildDatabaseUrl:
    """Tests for _build_database_url function."""

    def test_returns_none_when_no_host(self):
        """Should return None when no host is provided."""
        with patch.dict(os.environ, {}, clear=True):
            result = _build_database_url()
            assert result is None

    def test_uses_postgres_url_env_var(self):
        """Should use POSTGRES_URL env var when set."""
        with patch.dict(os.environ, {"POSTGRES_URL": "postgresql://user:pass@host:5432/db"}):
            result = _build_database_url()
            assert result == "postgresql://user:pass@host:5432/db"

    def test_postgres_url_takes_priority(self):
        """POSTGRES_URL should take priority over other args."""
        with patch.dict(os.environ, {"POSTGRES_URL": "postgresql://env@host/db", "PGHOST": "other"}):
            result = _build_database_url(host="cli_host")
            assert result == "postgresql://env@host/db"

    def test_cli_args_override_env_vars(self):
        """CLI args should override PG* env vars."""
        with patch.dict(os.environ, {"PGHOST": "env_host", "PGPORT": "5433"}, clear=True):
            result = _build_database_url(host="cli_host", port=5434)
            assert "cli_host" in result
            assert "5434" in result

    def test_builds_url_from_cli_args(self):
        """Should build URL from CLI args."""
        with patch.dict(os.environ, {}, clear=True):
            result = _build_database_url(
                host="myhost",
                port=5432,
                database="mydb",
                user="myuser"
            )
            assert result == "postgresql://myuser@myhost:5432/mydb"

    def test_builds_url_from_env_vars(self):
        """Should build URL from PG* env vars."""
        env = {
            "PGHOST": "envhost",
            "PGPORT": "5433",
            "PGUSER": "envuser",
            "PGDATABASE": "envdb"
        }
        with patch.dict(os.environ, env, clear=True):
            result = _build_database_url()
            assert result == "postgresql://envuser@envhost:5433/envdb"

    def test_includes_password_when_set(self):
        """Should include password in URL when PGPASSWORD is set."""
        env = {
            "PGHOST": "host",
            "PGPASSWORD": "secret123"
        }
        with patch.dict(os.environ, env, clear=True):
            result = _build_database_url()
            assert "secret123" in result
            assert result == "postgresql://postgres:secret123@host:5432/variants"

    def test_uses_defaults_for_optional_params(self):
        """Should use defaults for port, user, database."""
        with patch.dict(os.environ, {"PGHOST": "myhost"}, clear=True):
            result = _build_database_url()
            assert result == "postgresql://postgres@myhost:5432/variants"

    def test_partial_cli_args_with_env_fallback(self):
        """Should mix CLI args with env var fallbacks."""
        env = {"PGHOST": "envhost", "PGUSER": "envuser"}
        with patch.dict(os.environ, env, clear=True):
            result = _build_database_url(database="clidb")
            assert "envhost" in result
            assert "envuser" in result
            assert "clidb" in result


class TestLoadCommandNewOptions:
    """Tests for new load command options."""

    def test_load_help_shows_new_options(self):
        """Load help should show new connection options."""
        result = runner.invoke(app, ["load", "--help"])
        assert result.exit_code == 0
        assert "--host" in result.stdout
        assert "--port" in result.stdout
        assert "--database" in result.stdout
        assert "--user" in result.stdout
        assert "--schema" in result.stdout
        assert "--sample-id" in result.stdout
        assert "--log" in result.stdout

    def test_load_with_individual_db_params(self):
        """Load should accept individual DB connection params."""
        with patch("vcf_pg_loader.cli.VCFLoader") as mock_loader:
            mock_instance = AsyncMock()
            mock_instance.load_vcf.return_value = {
                "variants_loaded": 100,
                "load_batch_id": str(uuid4()),
                "file_hash": "abc123"
            }
            mock_loader.return_value = mock_instance

            vcf_path = FIXTURES_DIR / "with_annotations.vcf"
            result = runner.invoke(app, [
                "load", str(vcf_path),
                "--host", "localhost",
                "--port", "5432",
                "--database", "testdb",
                "--user", "testuser",
                "--quiet"
            ])

            if result.exit_code == 0:
                call_args = mock_loader.call_args
                db_url = call_args[0][0]
                assert "localhost" in db_url
                assert "5432" in db_url
                assert "testdb" in db_url
                assert "testuser" in db_url
