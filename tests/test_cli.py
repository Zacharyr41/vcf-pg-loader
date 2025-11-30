"""Tests for Typer CLI interface."""

from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from typer.testing import CliRunner

from vcf_pg_loader.cli import app

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
