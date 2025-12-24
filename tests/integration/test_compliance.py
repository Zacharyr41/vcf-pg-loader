"""Integration tests for HIPAA compliance checking."""

import pytest

from vcf_pg_loader.audit import AuditSchemaManager
from vcf_pg_loader.auth import AuthSchemaManager
from vcf_pg_loader.compliance import (
    ComplianceReport,
    ComplianceStatus,
    ComplianceValidator,
    ReportExporter,
    ReportFormat,
)


@pytest.mark.integration
class TestComplianceValidatorIntegration:
    @pytest.fixture
    async def compliance_db(self, test_db):
        yield test_db

    async def test_run_all_checks_with_empty_db(self, compliance_db):
        validator = ComplianceValidator(compliance_db)
        report = await validator.run_all_checks()

        assert isinstance(report, ComplianceReport)
        assert len(report.results) > 0
        failed = [r for r in report.results if r.status == ComplianceStatus.FAIL]
        assert len(failed) > 0

    async def test_audit_check_fails_without_schema(self, compliance_db):
        validator = ComplianceValidator(compliance_db)
        result = await validator.check_audit_logging()

        assert result.status == ComplianceStatus.FAIL
        assert "audit" in result.message.lower()

    async def test_audit_check_passes_with_schema(self, compliance_db):
        schema_manager = AuditSchemaManager()
        await schema_manager.create_audit_schema(compliance_db)
        await schema_manager.create_initial_partitions(compliance_db, months_ahead=1)

        validator = ComplianceValidator(compliance_db)
        result = await validator.check_audit_logging()

        assert result.status == ComplianceStatus.PASS

    async def test_auth_check_fails_without_schema(self, compliance_db):
        validator = ComplianceValidator(compliance_db)
        result = await validator.check_authentication()

        assert result.status == ComplianceStatus.FAIL

    async def test_auth_check_passes_with_schema(self, compliance_db):
        auth_schema = AuthSchemaManager()
        await auth_schema.create_schema(compliance_db)

        validator = ComplianceValidator(compliance_db)
        result = await validator.check_authentication()

        assert result.status in (ComplianceStatus.PASS, ComplianceStatus.WARN)

    async def test_immutability_check_fails_without_trigger(self, compliance_db):
        validator = ComplianceValidator(compliance_db)
        result = await validator.check_audit_immutability()

        assert result.status == ComplianceStatus.FAIL

    async def test_immutability_check_passes_with_trigger(self, compliance_db):
        schema_manager = AuditSchemaManager()
        await schema_manager.create_audit_schema(compliance_db)
        await schema_manager.create_initial_partitions(compliance_db, months_ahead=1)

        validator = ComplianceValidator(compliance_db)
        result = await validator.check_audit_immutability()

        assert result.status == ComplianceStatus.PASS


@pytest.mark.integration
class TestComplianceReportIntegration:
    @pytest.fixture
    async def full_compliance_db(self, test_db):
        audit_schema = AuditSchemaManager()
        await audit_schema.create_audit_schema(test_db)
        await audit_schema.create_initial_partitions(test_db, months_ahead=1)

        auth_schema = AuthSchemaManager()
        await auth_schema.create_schema(test_db)

        yield test_db

    async def test_generate_json_report(self, full_compliance_db):
        validator = ComplianceValidator(full_compliance_db)
        report = await validator.run_all_checks()

        exporter = ReportExporter()
        json_output = exporter.export(report, ReportFormat.JSON)

        assert "results" in json_output
        assert "summary" in json_output

    async def test_generate_html_report(self, full_compliance_db):
        validator = ComplianceValidator(full_compliance_db)
        report = await validator.run_all_checks()

        exporter = ReportExporter()
        html_output = exporter.export(report, ReportFormat.HTML)

        assert "<html" in html_output
        assert "HIPAA Compliance Report" in html_output

    async def test_exit_code_reflects_compliance(self, full_compliance_db):
        validator = ComplianceValidator(full_compliance_db)
        report = await validator.run_all_checks()

        exporter = ReportExporter()
        exit_code = exporter.get_exit_code(report)

        if report.is_compliant:
            assert exit_code == 0
        else:
            assert exit_code == 1
