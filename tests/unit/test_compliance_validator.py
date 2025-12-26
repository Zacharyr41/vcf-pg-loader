"""Unit tests for compliance validator."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from vcf_pg_loader.compliance.checks import (
    CHECKS,
    CheckResult,
    ComplianceReport,
    ComplianceStatus,
)
from vcf_pg_loader.compliance.validator import ComplianceValidator


class TestComplianceValidator:
    @pytest.fixture
    def mock_conn(self):
        conn = AsyncMock()
        conn.get_settings = MagicMock()
        return conn

    @pytest.fixture
    def validator(self, mock_conn):
        return ComplianceValidator(mock_conn)


class TestRunAllChecks(TestComplianceValidator):
    async def test_run_all_checks_returns_report(self, validator, mock_conn):
        mock_conn.get_settings.return_value = MagicMock(ssl=True)
        mock_conn.fetchval.return_value = True
        mock_conn.fetchrow.side_effect = [
            {
                "min_length": 12,
                "require_uppercase": True,
                "require_lowercase": True,
                "require_digit": True,
                "require_special": True,
            },
            {"total_users": 1, "mfa_users": 1},
            {"retention_years": 6, "enforce_minimum": True, "created_at": None},
        ]
        mock_conn.fetch.return_value = []

        report = await validator.run_all_checks()

        assert isinstance(report, ComplianceReport)
        assert len(report.results) > 0

    async def test_run_all_checks_covers_all_registered_checks(self, validator, mock_conn):
        mock_conn.get_settings.return_value = MagicMock(ssl=True)
        mock_conn.fetchval.return_value = True
        mock_conn.fetchrow.side_effect = [
            {
                "min_length": 12,
                "require_uppercase": True,
                "require_lowercase": True,
                "require_digit": True,
                "require_special": True,
            },
            {"total_users": 1, "mfa_users": 1},
            {"retention_years": 6, "enforce_minimum": True, "created_at": None},
        ]
        mock_conn.fetch.return_value = []

        report = await validator.run_all_checks()

        check_ids_in_report = {r.check.id for r in report.results}
        for check in CHECKS:
            assert check.id in check_ids_in_report


class TestRunCheck(TestComplianceValidator):
    async def test_run_check_by_id(self, validator, mock_conn):
        mock_conn.fetchval.return_value = True
        mock_conn.fetchrow.return_value = {"ssl": True, "version": "TLSv1.3"}

        result = await validator.run_check("TLS_ENABLED")

        assert isinstance(result, CheckResult)
        assert result.check.id == "TLS_ENABLED"

    async def test_run_check_unknown_id_raises(self, validator):
        with pytest.raises(ValueError, match="Unknown check"):
            await validator.run_check("NONEXISTENT_CHECK")


class TestCheckTLS(TestComplianceValidator):
    async def test_tls_pass_when_ssl_in_use(self, validator, mock_conn):
        mock_conn.get_settings.return_value = MagicMock(ssl=True)

        result = await validator.check_tls()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "TLS_ENABLED"

    async def test_tls_fail_when_ssl_not_in_use(self, validator, mock_conn):
        mock_conn.get_settings.return_value = MagicMock(ssl=None)

        result = await validator.check_tls()

        assert result.status == ComplianceStatus.FAIL
        assert result.remediation is not None


class TestCheckAuditLogging(TestComplianceValidator):
    async def test_audit_pass_when_tables_and_triggers_exist(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, True]

        result = await validator.check_audit_logging()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "AUDIT_ENABLED"

    async def test_audit_fail_when_table_missing(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [False]

        result = await validator.check_audit_logging()

        assert result.status == ComplianceStatus.FAIL
        assert "audit" in result.message.lower()

    async def test_audit_fail_when_trigger_missing(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, False]

        result = await validator.check_audit_logging()

        assert result.status == ComplianceStatus.FAIL


class TestCheckAuditImmutability(TestComplianceValidator):
    async def test_immutability_pass_when_trigger_exists(self, validator, mock_conn):
        mock_conn.fetchval.return_value = True

        result = await validator.check_audit_immutability()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "AUDIT_IMMUTABILITY"

    async def test_immutability_fail_when_trigger_missing(self, validator, mock_conn):
        mock_conn.fetchval.return_value = False

        result = await validator.check_audit_immutability()

        assert result.status == ComplianceStatus.FAIL


class TestCheckAuthentication(TestComplianceValidator):
    async def test_auth_pass_when_users_table_exists(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, True]

        result = await validator.check_authentication()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "AUTH_REQUIRED"

    async def test_auth_fail_when_users_table_missing(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [False]

        result = await validator.check_authentication()

        assert result.status == ComplianceStatus.FAIL


class TestCheckRBAC(TestComplianceValidator):
    async def test_rbac_pass_when_roles_configured(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, 3]

        result = await validator.check_rbac()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "RBAC_CONFIGURED"

    async def test_rbac_fail_when_roles_table_missing(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [False]

        result = await validator.check_rbac()

        assert result.status == ComplianceStatus.FAIL

    async def test_rbac_warn_when_no_roles_defined(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, 0]

        result = await validator.check_rbac()

        assert result.status == ComplianceStatus.WARN


class TestCheckEncryptionAtRest(TestComplianceValidator):
    async def test_encryption_pass_when_configured(self, validator, mock_conn):
        mock_conn.fetchval.return_value = True

        result = await validator.check_encryption_at_rest()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "ENCRYPTION_AT_REST"

    async def test_encryption_fail_when_not_configured(self, validator, mock_conn):
        mock_conn.fetchval.return_value = False

        result = await validator.check_encryption_at_rest()

        assert result.status == ComplianceStatus.FAIL


class TestCheckSessionTimeout(TestComplianceValidator):
    async def test_session_timeout_pass_when_configured(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, 30]

        result = await validator.check_session_timeout()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "SESSION_TIMEOUT"

    async def test_session_timeout_fail_when_table_missing(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [False]

        result = await validator.check_session_timeout()

        assert result.status == ComplianceStatus.FAIL

    async def test_session_timeout_warn_when_timeout_too_long(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, 1440]

        result = await validator.check_session_timeout()

        assert result.status == ComplianceStatus.WARN


class TestCheckPasswordPolicy(TestComplianceValidator):
    async def test_password_policy_pass_when_configured(self, validator, mock_conn):
        mock_conn.fetchrow.return_value = {
            "min_length": 12,
            "require_uppercase": True,
            "require_lowercase": True,
            "require_digit": True,
            "require_special": True,
        }

        result = await validator.check_password_policy()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "PASSWORD_POLICY"

    async def test_password_policy_fail_when_not_configured(self, validator, mock_conn):
        mock_conn.fetchrow.return_value = None

        result = await validator.check_password_policy()

        assert result.status == ComplianceStatus.FAIL

    async def test_password_policy_warn_when_weak(self, validator, mock_conn):
        mock_conn.fetchrow.return_value = {
            "min_length": 6,
            "require_uppercase": False,
            "require_lowercase": True,
            "require_digit": False,
            "require_special": False,
        }

        result = await validator.check_password_policy()

        assert result.status == ComplianceStatus.WARN


class TestCheckPHIDetection(TestComplianceValidator):
    async def test_phi_detection_pass_when_active(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [True, 5]

        result = await validator.check_phi_detection()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "PHI_DETECTION"

    async def test_phi_detection_fail_when_not_configured(self, validator, mock_conn):
        mock_conn.fetchval.side_effect = [False]

        result = await validator.check_phi_detection()

        assert result.status == ComplianceStatus.FAIL


class TestCheckSecureDisposal(TestComplianceValidator):
    async def test_secure_disposal_pass_when_configured(self, validator, mock_conn):
        mock_conn.fetchval.return_value = True

        result = await validator.check_secure_disposal()

        assert result.status == ComplianceStatus.PASS
        assert result.check.id == "SECURE_DISPOSAL"

    async def test_secure_disposal_warn_when_not_configured(self, validator, mock_conn):
        mock_conn.fetchval.return_value = False

        result = await validator.check_secure_disposal()

        assert result.status == ComplianceStatus.WARN
