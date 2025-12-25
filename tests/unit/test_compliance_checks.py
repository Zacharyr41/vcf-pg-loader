"""Unit tests for compliance check data models."""

import pytest

from vcf_pg_loader.compliance.checks import (
    CHECKS,
    CheckResult,
    ComplianceCheck,
    ComplianceReport,
    ComplianceStatus,
    Severity,
    get_check_by_id,
)


class TestComplianceStatus:
    def test_status_values(self):
        assert ComplianceStatus.PASS.value == "pass"
        assert ComplianceStatus.FAIL.value == "fail"
        assert ComplianceStatus.WARN.value == "warn"
        assert ComplianceStatus.SKIP.value == "skip"

    def test_status_is_enum(self):
        assert len(ComplianceStatus) == 4


class TestSeverity:
    def test_severity_values(self):
        assert Severity.CRITICAL.value == "critical"
        assert Severity.HIGH.value == "high"
        assert Severity.MEDIUM.value == "medium"
        assert Severity.LOW.value == "low"

    def test_severity_ordering(self):
        assert Severity.CRITICAL.weight > Severity.HIGH.weight
        assert Severity.HIGH.weight > Severity.MEDIUM.weight
        assert Severity.MEDIUM.weight > Severity.LOW.weight


class TestComplianceCheck:
    def test_create_check(self):
        check = ComplianceCheck(
            id="TEST_CHECK",
            name="Test Check",
            hipaa_reference="164.312(x)",
            description="A test compliance check",
            severity=Severity.HIGH,
        )
        assert check.id == "TEST_CHECK"
        assert check.name == "Test Check"
        assert check.hipaa_reference == "164.312(x)"
        assert check.description == "A test compliance check"
        assert check.severity == Severity.HIGH

    def test_check_equality(self):
        check1 = ComplianceCheck(
            id="TEST",
            name="Test",
            hipaa_reference="164.312(x)",
            description="Test",
            severity=Severity.LOW,
        )
        check2 = ComplianceCheck(
            id="TEST",
            name="Test",
            hipaa_reference="164.312(x)",
            description="Test",
            severity=Severity.LOW,
        )
        assert check1 == check2


class TestCheckResult:
    def test_create_passing_result(self):
        check = ComplianceCheck(
            id="TLS_ENABLED",
            name="TLS Check",
            hipaa_reference="164.312(e)(1)",
            description="Verify TLS",
            severity=Severity.CRITICAL,
        )
        result = CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message="TLS 1.3 in use",
        )
        assert result.status == ComplianceStatus.PASS
        assert result.remediation is None

    def test_create_failing_result_with_remediation(self):
        check = ComplianceCheck(
            id="AUDIT_ENABLED",
            name="Audit Check",
            hipaa_reference="164.312(b)",
            description="Verify audit logging",
            severity=Severity.CRITICAL,
        )
        result = CheckResult(
            check=check,
            status=ComplianceStatus.FAIL,
            message="Audit logging not configured",
            remediation="Run 'vcf-pg-loader audit init' to enable audit logging",
        )
        assert result.status == ComplianceStatus.FAIL
        assert result.remediation is not None

    def test_result_to_dict(self):
        check = ComplianceCheck(
            id="TEST",
            name="Test",
            hipaa_reference="164.312(x)",
            description="Test check",
            severity=Severity.MEDIUM,
        )
        result = CheckResult(
            check=check,
            status=ComplianceStatus.WARN,
            message="Warning message",
        )
        d = result.to_dict()
        assert d["check_id"] == "TEST"
        assert d["status"] == "warn"
        assert d["message"] == "Warning message"
        assert d["severity"] == "medium"


class TestComplianceReport:
    @pytest.fixture
    def sample_checks(self):
        return [
            ComplianceCheck(
                id="CHECK1",
                name="Check 1",
                hipaa_reference="164.312(a)",
                description="First check",
                severity=Severity.CRITICAL,
            ),
            ComplianceCheck(
                id="CHECK2",
                name="Check 2",
                hipaa_reference="164.312(b)",
                description="Second check",
                severity=Severity.HIGH,
            ),
            ComplianceCheck(
                id="CHECK3",
                name="Check 3",
                hipaa_reference="164.312(c)",
                description="Third check",
                severity=Severity.MEDIUM,
            ),
        ]

    def test_create_report(self, sample_checks):
        results = [
            CheckResult(check=sample_checks[0], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[1], status=ComplianceStatus.FAIL, message="Failed"),
            CheckResult(check=sample_checks[2], status=ComplianceStatus.WARN, message="Warning"),
        ]
        report = ComplianceReport(results=results)
        assert len(report.results) == 3
        assert report.timestamp is not None

    def test_report_counts(self, sample_checks):
        results = [
            CheckResult(check=sample_checks[0], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[1], status=ComplianceStatus.FAIL, message="Failed"),
            CheckResult(check=sample_checks[2], status=ComplianceStatus.PASS, message="OK"),
        ]
        report = ComplianceReport(results=results)
        assert report.passed_count == 2
        assert report.failed_count == 1
        assert report.warned_count == 0
        assert report.skipped_count == 0

    def test_report_is_compliant_all_pass(self, sample_checks):
        results = [
            CheckResult(check=sample_checks[0], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[1], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[2], status=ComplianceStatus.PASS, message="OK"),
        ]
        report = ComplianceReport(results=results)
        assert report.is_compliant is True

    def test_report_not_compliant_with_critical_fail(self, sample_checks):
        results = [
            CheckResult(check=sample_checks[0], status=ComplianceStatus.FAIL, message="Failed"),
            CheckResult(check=sample_checks[1], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[2], status=ComplianceStatus.PASS, message="OK"),
        ]
        report = ComplianceReport(results=results)
        assert report.is_compliant is False

    def test_report_not_compliant_with_high_fail(self, sample_checks):
        results = [
            CheckResult(check=sample_checks[0], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[1], status=ComplianceStatus.FAIL, message="Failed"),
            CheckResult(check=sample_checks[2], status=ComplianceStatus.PASS, message="OK"),
        ]
        report = ComplianceReport(results=results)
        assert report.is_compliant is False

    def test_report_compliant_with_medium_fail(self, sample_checks):
        results = [
            CheckResult(check=sample_checks[0], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[1], status=ComplianceStatus.PASS, message="OK"),
            CheckResult(check=sample_checks[2], status=ComplianceStatus.FAIL, message="Failed"),
        ]
        report = ComplianceReport(results=results)
        assert report.is_compliant is True

    def test_report_to_dict(self, sample_checks):
        results = [
            CheckResult(check=sample_checks[0], status=ComplianceStatus.PASS, message="OK"),
        ]
        report = ComplianceReport(results=results)
        d = report.to_dict()
        assert "timestamp" in d
        assert "results" in d
        assert "summary" in d
        assert d["summary"]["passed"] == 1
        assert d["summary"]["failed"] == 0


class TestCheckRegistry:
    """45 CFR 164.312: Compliance check registry tests."""

    def test_checks_list_not_empty(self):
        assert len(CHECKS) >= 13

    def test_required_checks_present(self):
        """Verify all HIPAA-required checks are registered."""
        check_ids = {c.id for c in CHECKS}
        assert "TLS_ENABLED" in check_ids
        assert "AUDIT_ENABLED" in check_ids
        assert "AUTH_REQUIRED" in check_ids
        assert "RBAC_CONFIGURED" in check_ids
        assert "ENCRYPTION_AT_REST" in check_ids
        assert "SESSION_TIMEOUT" in check_ids
        # New HIPAA-required checks
        assert "EMERGENCY_ACCESS" in check_ids  # 45 CFR 164.312(a)(2)(ii) - REQUIRED
        assert "MFA_ENABLED" in check_ids  # 45 CFR 164.312(d)
        assert "AUDIT_RETENTION" in check_ids  # 45 CFR 164.316(b)(2)(i) - REQUIRED

    def test_all_checks_have_hipaa_reference(self):
        for check in CHECKS:
            assert check.hipaa_reference.startswith("164.")

    def test_get_check_by_id(self):
        check = get_check_by_id("TLS_ENABLED")
        assert check is not None
        assert check.id == "TLS_ENABLED"
        assert check.severity == Severity.CRITICAL

    def test_get_check_by_id_not_found(self):
        check = get_check_by_id("NONEXISTENT")
        assert check is None

    def test_critical_checks(self):
        """45 CFR 164.312: Verify critical-severity checks."""
        critical_checks = [c for c in CHECKS if c.severity == Severity.CRITICAL]
        critical_ids = {c.id for c in critical_checks}
        assert "TLS_ENABLED" in critical_ids
        assert "AUDIT_ENABLED" in critical_ids
        assert "AUTH_REQUIRED" in critical_ids
        assert "AUDIT_IMMUTABILITY" in critical_ids
        # New critical checks
        assert "EMERGENCY_ACCESS" in critical_ids  # 45 CFR 164.312(a)(2)(ii) - REQUIRED
        assert "MFA_ENABLED" in critical_ids  # 45 CFR 164.312(d)
        assert "AUDIT_RETENTION" in critical_ids  # 45 CFR 164.316(b)(2)(i) - REQUIRED

    def test_hipaa_citations_format(self):
        """All checks must have proper HIPAA citations."""
        for check in CHECKS:
            assert check.hipaa_reference.startswith(
                "164."
            ), f"Check {check.id} has invalid HIPAA reference: {check.hipaa_reference}"

    def test_emergency_access_check(self):
        """45 CFR 164.312(a)(2)(ii): Emergency access procedure check."""
        check = get_check_by_id("EMERGENCY_ACCESS")
        assert check is not None
        assert check.severity == Severity.CRITICAL
        assert "164.312(a)(2)(ii)" in check.hipaa_reference

    def test_mfa_check(self):
        """45 CFR 164.312(d): MFA check."""
        check = get_check_by_id("MFA_ENABLED")
        assert check is not None
        assert check.severity == Severity.CRITICAL
        assert "164.312(d)" in check.hipaa_reference

    def test_audit_retention_check(self):
        """45 CFR 164.316(b)(2)(i): Audit retention check."""
        check = get_check_by_id("AUDIT_RETENTION")
        assert check is not None
        assert check.severity == Severity.CRITICAL
        assert "164.316(b)(2)(i)" in check.hipaa_reference
