"""Unit tests for compliance CLI commands."""

from typer.testing import CliRunner

from vcf_pg_loader.cli import app

runner = CliRunner(env={"NO_COLOR": "1", "TERM": "dumb"})
runner_no_db = CliRunner(env={"NO_COLOR": "1", "TERM": "dumb", "VCF_PG_LOADER_NO_MANAGED_DB": "1"})


class TestComplianceCommands:
    def test_compliance_help(self):
        result = runner.invoke(app, ["compliance", "--help"])
        assert result.exit_code == 0
        assert "HIPAA compliance" in result.output

    def test_compliance_check_help(self):
        result = runner.invoke(app, ["compliance", "check", "--help"])
        assert result.exit_code == 0
        assert "--id" in result.output
        assert "--db" in result.output
        assert "--json" in result.output

    def test_compliance_check_requires_db(self):
        result = runner_no_db.invoke(app, ["compliance", "check"])
        assert result.exit_code == 1
        assert "Database connection required" in result.output

    def test_compliance_report_help(self):
        result = runner.invoke(app, ["compliance", "report", "--help"])
        assert result.exit_code == 0
        assert "--format" in result.output
        assert "--output" in result.output

    def test_compliance_report_requires_db(self):
        result = runner_no_db.invoke(app, ["compliance", "report"])
        assert result.exit_code == 1
        assert "Database connection required" in result.output

    def test_compliance_status_help(self):
        result = runner.invoke(app, ["compliance", "status", "--help"])
        assert result.exit_code == 0
        assert "--json" in result.output

    def test_compliance_status_requires_db(self):
        result = runner_no_db.invoke(app, ["compliance", "status"])
        assert result.exit_code == 1
        assert "Database connection required" in result.output
