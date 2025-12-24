"""HIPAA compliance validator for vcf-pg-loader."""

import asyncpg

from .checks import (
    CHECKS,
    CheckResult,
    ComplianceReport,
    ComplianceStatus,
    get_check_by_id,
)


class ComplianceValidator:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def run_all_checks(self) -> ComplianceReport:
        results = []
        for check in CHECKS:
            result = await self.run_check(check.id)
            results.append(result)
        return ComplianceReport(results=results)

    async def run_check(self, check_id: str) -> CheckResult:
        check = get_check_by_id(check_id)
        if check is None:
            raise ValueError(f"Unknown check: {check_id}")

        check_methods = {
            "TLS_ENABLED": self.check_tls,
            "AUDIT_ENABLED": self.check_audit_logging,
            "AUTH_REQUIRED": self.check_authentication,
            "RBAC_CONFIGURED": self.check_rbac,
            "ENCRYPTION_AT_REST": self.check_encryption_at_rest,
            "SESSION_TIMEOUT": self.check_session_timeout,
            "AUDIT_IMMUTABILITY": self.check_audit_immutability,
            "PASSWORD_POLICY": self.check_password_policy,
            "PHI_DETECTION": self.check_phi_detection,
            "SECURE_DISPOSAL": self.check_secure_disposal,
        }

        method = check_methods.get(check_id)
        if method:
            return await method()

        return CheckResult(
            check=check,
            status=ComplianceStatus.SKIP,
            message="Check not implemented",
        )

    async def check_tls(self) -> CheckResult:
        check = get_check_by_id("TLS_ENABLED")
        try:
            ssl_info = self.conn.get_settings().ssl
            if ssl_info:
                return CheckResult(
                    check=check,
                    status=ComplianceStatus.PASS,
                    message="TLS encryption is active",
                )
            else:
                return CheckResult(
                    check=check,
                    status=ComplianceStatus.FAIL,
                    message="Connection is not using TLS encryption",
                    remediation="Enable TLS by setting VCF_PG_LOADER_REQUIRE_TLS=true",
                )
        except Exception as e:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message=f"Could not verify TLS status: {e}",
                remediation="Ensure database connection supports TLS",
            )

    async def check_audit_logging(self) -> CheckResult:
        check = get_check_by_id("AUDIT_ENABLED")

        table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'hipaa_audit_log'
            )
            """
        )

        if not table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Audit log table does not exist",
                remediation="Run 'vcf-pg-loader audit init' to create audit logging schema",
            )

        trigger_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'audit_immutability'
            )
            """
        )

        if not trigger_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Audit immutability trigger is not configured",
                remediation="Run 'vcf-pg-loader audit init' to create audit triggers",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message="Audit logging is properly configured",
        )

    async def check_audit_immutability(self) -> CheckResult:
        check = get_check_by_id("AUDIT_IMMUTABILITY")

        trigger_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'audit_immutability'
            )
            """
        )

        if trigger_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.PASS,
                message="Audit log immutability trigger is active",
            )
        else:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Audit log immutability is not enforced",
                remediation="Run 'vcf-pg-loader audit init' to enable immutability triggers",
            )

    async def check_authentication(self) -> CheckResult:
        check = get_check_by_id("AUTH_REQUIRED")

        users_table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'hipaa_users'
            )
            """
        )

        if not users_table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="User authentication table does not exist",
                remediation="Run 'vcf-pg-loader auth init' to create authentication schema",
            )

        has_users = await self.conn.fetchval("SELECT EXISTS (SELECT 1 FROM hipaa_users)")

        if not has_users:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message="Authentication configured but no users exist",
                remediation="Create users with 'vcf-pg-loader auth create-user'",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message="User authentication is configured",
        )

    async def check_rbac(self) -> CheckResult:
        check = get_check_by_id("RBAC_CONFIGURED")

        roles_table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'hipaa_roles'
            )
            """
        )

        if not roles_table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="RBAC roles table does not exist",
                remediation="Run 'vcf-pg-loader roles init' to create RBAC schema",
            )

        role_count = await self.conn.fetchval("SELECT COUNT(*) FROM hipaa_roles")

        if role_count == 0:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message="RBAC configured but no roles defined",
                remediation="Create roles with 'vcf-pg-loader roles create'",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message=f"RBAC configured with {role_count} roles",
        )

    async def check_encryption_at_rest(self) -> CheckResult:
        check = get_check_by_id("ENCRYPTION_AT_REST")

        encryption_configured = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'encryption_keys'
            )
            """
        )

        if encryption_configured:
            return CheckResult(
                check=check,
                status=ComplianceStatus.PASS,
                message="Encryption at rest is configured",
            )
        else:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message="Encryption at rest not configured",
                remediation="Run 'vcf-pg-loader security init' to configure encryption",
            )

    async def check_session_timeout(self) -> CheckResult:
        check = get_check_by_id("SESSION_TIMEOUT")

        sessions_table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'hipaa_sessions'
            )
            """
        )

        if not sessions_table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Session management not configured",
                remediation="Run 'vcf-pg-loader session init' to configure sessions",
            )

        timeout_minutes = await self.conn.fetchval(
            """
            SELECT COALESCE(
                (SELECT value::int FROM app_settings WHERE key = 'session_timeout_minutes'),
                30
            )
            """
        )

        if timeout_minutes > 60:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message=f"Session timeout is {timeout_minutes} minutes (>60 minutes)",
                remediation="Reduce session timeout to 60 minutes or less for HIPAA compliance",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message=f"Session timeout configured at {timeout_minutes} minutes",
        )

    async def check_password_policy(self) -> CheckResult:
        check = get_check_by_id("PASSWORD_POLICY")

        policy = await self.conn.fetchrow(
            """
            SELECT min_length, require_uppercase, require_lowercase,
                   require_digit, require_special
            FROM password_policy
            LIMIT 1
            """
        )

        if policy is None:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="No password policy configured",
                remediation="Configure password policy in database settings",
            )

        is_strong = (
            policy["min_length"] >= 12
            and policy["require_uppercase"]
            and policy["require_lowercase"]
            and policy["require_digit"]
            and policy["require_special"]
        )

        if is_strong:
            return CheckResult(
                check=check,
                status=ComplianceStatus.PASS,
                message="Strong password policy is enforced",
            )
        else:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message="Password policy does not meet HIPAA recommendations",
                remediation="Require 12+ characters with uppercase, lowercase, digits, and special characters",
            )

    async def check_phi_detection(self) -> CheckResult:
        check = get_check_by_id("PHI_DETECTION")

        patterns_table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'phi_detection_patterns'
            )
            """
        )

        if not patterns_table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="PHI detection not configured",
                remediation="Run 'vcf-pg-loader phi init' to configure PHI detection",
            )

        pattern_count = await self.conn.fetchval(
            "SELECT COUNT(*) FROM phi_detection_patterns WHERE active = true"
        )

        if pattern_count == 0:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message="PHI detection configured but no active patterns",
                remediation="Add PHI detection patterns with 'vcf-pg-loader phi patterns add'",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message=f"PHI detection active with {pattern_count} patterns",
        )

    async def check_secure_disposal(self) -> CheckResult:
        check = get_check_by_id("SECURE_DISPOSAL")

        disposal_configured = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'disposal_audit'
            )
            """
        )

        if disposal_configured:
            return CheckResult(
                check=check,
                status=ComplianceStatus.PASS,
                message="Secure disposal procedures are in place",
            )
        else:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message="Secure disposal not configured",
                remediation="Run 'vcf-pg-loader data disposal-init' to configure secure disposal",
            )
