"""Unit tests for compliance report generation."""

import json
from datetime import UTC, datetime

import pytest

from vcf_pg_loader.compliance.checks import (
    CheckResult,
    ComplianceCheck,
    ComplianceReport,
    ComplianceStatus,
    Severity,
)
from vcf_pg_loader.compliance.reports import (
    ReportExporter,
    ReportFormat,
)


@pytest.fixture
def sample_report():
    checks = [
        ComplianceCheck(
            id="TLS_ENABLED",
            name="TLS Encryption",
            hipaa_reference="164.312(e)(1)",
            description="Verify TLS",
            severity=Severity.CRITICAL,
        ),
        ComplianceCheck(
            id="AUDIT_ENABLED",
            name="Audit Logging",
            hipaa_reference="164.312(b)",
            description="Verify audit",
            severity=Severity.CRITICAL,
        ),
        ComplianceCheck(
            id="SESSION_TIMEOUT",
            name="Session Timeout",
            hipaa_reference="164.312(a)(2)(iii)",
            description="Verify timeout",
            severity=Severity.MEDIUM,
        ),
    ]
    results = [
        CheckResult(check=checks[0], status=ComplianceStatus.PASS, message="TLS active"),
        CheckResult(
            check=checks[1],
            status=ComplianceStatus.FAIL,
            message="Audit not configured",
            remediation="Run 'vcf-pg-loader audit init'",
        ),
        CheckResult(check=checks[2], status=ComplianceStatus.WARN, message="Timeout is 120 min"),
    ]
    return ComplianceReport(
        results=results,
        timestamp=datetime(2024, 12, 24, 12, 0, 0, tzinfo=UTC),
    )


class TestReportFormat:
    def test_format_values(self):
        assert ReportFormat.JSON.value == "json"
        assert ReportFormat.HTML.value == "html"
        assert ReportFormat.TEXT.value == "text"


class TestReportExporterJSON:
    def test_export_json_format(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.JSON)

        data = json.loads(output)
        assert "timestamp" in data
        assert "results" in data
        assert "summary" in data

    def test_export_json_contains_all_results(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.JSON)

        data = json.loads(output)
        assert len(data["results"]) == 3

    def test_export_json_summary_correct(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.JSON)

        data = json.loads(output)
        assert data["summary"]["passed"] == 1
        assert data["summary"]["failed"] == 1
        assert data["summary"]["warned"] == 1
        assert data["summary"]["is_compliant"] is False

    def test_export_json_includes_remediation(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.JSON)

        data = json.loads(output)
        failed_result = [r for r in data["results"] if r["status"] == "fail"][0]
        assert failed_result["remediation"] is not None


class TestReportExporterHTML:
    def test_export_html_format(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.HTML)

        assert "<html" in output
        assert "</html>" in output

    def test_export_html_contains_title(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.HTML)

        assert "HIPAA Compliance Report" in output

    def test_export_html_contains_check_results(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.HTML)

        assert "TLS Encryption" in output
        assert "Audit Logging" in output
        assert "Session Timeout" in output

    def test_export_html_shows_status_colors(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.HTML)

        assert "pass" in output.lower() or "green" in output.lower()
        assert "fail" in output.lower() or "red" in output.lower()

    def test_export_html_contains_hipaa_references(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.HTML)

        assert "164.312(e)(1)" in output
        assert "164.312(b)" in output

    def test_export_html_contains_remediation(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.HTML)

        assert "vcf-pg-loader audit init" in output


class TestReportExporterText:
    def test_export_text_format(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.TEXT)

        assert isinstance(output, str)
        assert len(output) > 0

    def test_export_text_contains_summary(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.TEXT)

        assert "1 passed" in output.lower() or "passed: 1" in output.lower()
        assert "1 failed" in output.lower() or "failed: 1" in output.lower()

    def test_export_text_shows_status_indicators(self, sample_report):
        exporter = ReportExporter()
        output = exporter.export(sample_report, ReportFormat.TEXT)

        assert "PASS" in output or "✓" in output
        assert "FAIL" in output or "✗" in output


class TestExitCode:
    def test_exit_code_zero_when_compliant(self):
        checks = [
            ComplianceCheck(
                id="TEST",
                name="Test",
                hipaa_reference="164.312(x)",
                description="Test",
                severity=Severity.CRITICAL,
            ),
        ]
        results = [
            CheckResult(check=checks[0], status=ComplianceStatus.PASS, message="OK"),
        ]
        report = ComplianceReport(results=results)
        exporter = ReportExporter()

        assert exporter.get_exit_code(report) == 0

    def test_exit_code_one_when_critical_fail(self):
        checks = [
            ComplianceCheck(
                id="TEST",
                name="Test",
                hipaa_reference="164.312(x)",
                description="Test",
                severity=Severity.CRITICAL,
            ),
        ]
        results = [
            CheckResult(check=checks[0], status=ComplianceStatus.FAIL, message="Failed"),
        ]
        report = ComplianceReport(results=results)
        exporter = ReportExporter()

        assert exporter.get_exit_code(report) == 1

    def test_exit_code_one_when_high_fail(self):
        checks = [
            ComplianceCheck(
                id="TEST",
                name="Test",
                hipaa_reference="164.312(x)",
                description="Test",
                severity=Severity.HIGH,
            ),
        ]
        results = [
            CheckResult(check=checks[0], status=ComplianceStatus.FAIL, message="Failed"),
        ]
        report = ComplianceReport(results=results)
        exporter = ReportExporter()

        assert exporter.get_exit_code(report) == 1

    def test_exit_code_zero_when_only_medium_fail(self):
        checks = [
            ComplianceCheck(
                id="TEST",
                name="Test",
                hipaa_reference="164.312(x)",
                description="Test",
                severity=Severity.MEDIUM,
            ),
        ]
        results = [
            CheckResult(check=checks[0], status=ComplianceStatus.FAIL, message="Failed"),
        ]
        report = ComplianceReport(results=results)
        exporter = ReportExporter()

        assert exporter.get_exit_code(report) == 0

    def test_exit_code_zero_when_warnings_only(self):
        checks = [
            ComplianceCheck(
                id="TEST",
                name="Test",
                hipaa_reference="164.312(x)",
                description="Test",
                severity=Severity.CRITICAL,
            ),
        ]
        results = [
            CheckResult(check=checks[0], status=ComplianceStatus.WARN, message="Warning"),
        ]
        report = ComplianceReport(results=results)
        exporter = ReportExporter()

        assert exporter.get_exit_code(report) == 0
