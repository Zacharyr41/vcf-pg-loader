"""HIPAA compliance validator for vcf-pg-loader.

This module implements compliance checks for HIPAA Security Rule
technical safeguards (45 CFR 164.312) and documentation requirements
(45 CFR 164.316).
"""

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
            "EMERGENCY_ACCESS": self.check_emergency_access,
            "MFA_ENABLED": self.check_mfa,
            "AUDIT_RETENTION": self.check_audit_retention,
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
                WHERE table_name = 'users'
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

        has_users = await self.conn.fetchval("SELECT EXISTS (SELECT 1 FROM users)")

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
                WHERE table_name = 'roles'
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

        role_count = await self.conn.fetchval("SELECT COUNT(*) FROM roles")

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

        if not encryption_configured:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Encryption at rest not configured (NIST SP 800-111)",
                remediation="Run 'vcf-pg-loader security init' to configure AES-256 encryption",
            )

        # 45 CFR 164.312(a)(2)(iv): Verify keys exist and are valid
        key_count = await self.conn.fetchval(
            "SELECT COUNT(*) FROM encryption_keys WHERE is_active = true"
        )

        if key_count == 0:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="No active encryption keys configured",
                remediation="Generate encryption keys with 'vcf-pg-loader security generate-key'",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message=f"Encryption at rest configured with {key_count} active key(s)",
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

        table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'password_policy'
            )
            """
        )

        if not table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="No password policy configured",
                remediation="Configure password policy in database settings",
            )

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

    async def check_emergency_access(self) -> CheckResult:
        """Check emergency access procedure implementation.

        HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED
        "Establish (and implement as needed) procedures for obtaining
        necessary electronic protected health information during an emergency."
        """
        check = get_check_by_id("EMERGENCY_ACCESS")

        table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'emergency_access_tokens'
            )
            """
        )

        if not table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Emergency access procedure not implemented",
                remediation="Run 'vcf-pg-loader emergency init' to configure break-glass access",
            )

        procedure_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_proc
                WHERE proname = 'grant_emergency_access'
            )
            """
        )

        if not procedure_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Emergency access grant procedure not found",
                remediation="Run 'vcf-pg-loader emergency init' to create emergency access procedures",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message="Emergency access (break-glass) procedure is configured",
        )

    async def check_mfa(self) -> CheckResult:
        """Check multi-factor authentication implementation.

        HIPAA Citation: 45 CFR 164.312(d) - REQUIRED standard
        "Implement procedures to verify that a person or entity seeking access
        to electronic protected health information is the one claimed."

        HHS Security Series Paper #4 defines MFA as using 2+ factors:
        - Something known (password, PIN)
        - Something possessed (token, smart card)
        - Something unique (biometric)
        """
        check = get_check_by_id("MFA_ENABLED")

        users_table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'mfa_enabled'
            )
            """
        )

        if not users_table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="MFA support not configured in user schema",
                remediation="Run 'vcf-pg-loader auth init' to update authentication schema",
            )

        mfa_stats = await self.conn.fetchrow(
            """
            SELECT
                COUNT(*) as total_users,
                COUNT(*) FILTER (WHERE mfa_enabled = true) as mfa_users
            FROM users
            WHERE is_active = true
            """
        )

        if mfa_stats is None or mfa_stats["total_users"] == 0:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message="No active users to evaluate MFA status",
            )

        total = mfa_stats["total_users"]
        mfa_count = mfa_stats["mfa_users"]
        mfa_pct = (mfa_count / total) * 100 if total > 0 else 0

        if mfa_count == 0:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="No users have MFA enabled",
                remediation="Enable MFA for all users with 'vcf-pg-loader auth enable-mfa'",
            )

        if mfa_pct < 100:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message=f"MFA enabled for {mfa_count}/{total} users ({mfa_pct:.0f}%)",
                remediation="Enable MFA for all users to meet HIPAA authentication requirements",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message=f"MFA enabled for all {total} active users",
        )

    async def check_audit_retention(self) -> CheckResult:
        """Check 6-year audit log retention policy.

        HIPAA Citation: 45 CFR 164.316(b)(2)(i) - REQUIRED
        "Retain the documentation required by paragraph (b)(1) of this section
        for 6 years from the date of its creation or the date when it last
        was in effect, whichever is later."

        This applies to: audit logs, security policies, risk assessments,
        incident records.
        """
        check = get_check_by_id("AUDIT_RETENTION")

        audit_table_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'hipaa_audit_log'
            )
            """
        )

        if not audit_table_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Audit log table does not exist",
                remediation="Run 'vcf-pg-loader audit init' to create audit logging schema",
            )

        retention_policy_exists = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'audit_retention_policy'
            )
            """
        )

        if not retention_policy_exists:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="Audit retention policy not configured",
                remediation="Run 'vcf-pg-loader audit retention-init' to configure 6-year retention",
            )

        policy = await self.conn.fetchrow(
            """
            SELECT retention_years, enforce_minimum, created_at
            FROM audit_retention_policy
            WHERE is_active = true
            ORDER BY created_at DESC
            LIMIT 1
            """
        )

        if policy is None:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message="No active audit retention policy",
                remediation="Configure active retention policy with minimum 6-year retention",
            )

        if policy["retention_years"] < 6:
            return CheckResult(
                check=check,
                status=ComplianceStatus.FAIL,
                message=f"Retention period {policy['retention_years']} years is below HIPAA minimum (6 years)",
                remediation="Update retention policy to minimum 6 years per 45 CFR 164.316(b)(2)(i)",
            )

        if not policy["enforce_minimum"]:
            return CheckResult(
                check=check,
                status=ComplianceStatus.WARN,
                message=f"Retention policy set to {policy['retention_years']} years but not enforced",
                remediation="Enable retention enforcement to prevent premature log deletion",
            )

        return CheckResult(
            check=check,
            status=ComplianceStatus.PASS,
            message=f"Audit retention policy enforced at {policy['retention_years']} years",
        )
