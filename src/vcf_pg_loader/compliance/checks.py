"""HIPAA compliance check definitions and data models.

HIPAA Security Rule Technical Safeguards: 45 CFR 164.312
Documentation Requirements: 45 CFR 164.316

This module defines compliance checks mapped to specific HIPAA citations.
Each check references the relevant CFR section for audit traceability.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class ComplianceStatus(Enum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    SKIP = "skip"


class Severity(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

    @property
    def weight(self) -> int:
        weights = {
            Severity.CRITICAL: 100,
            Severity.HIGH: 75,
            Severity.MEDIUM: 50,
            Severity.LOW: 25,
        }
        return weights[self]


@dataclass(frozen=True)
class ComplianceCheck:
    id: str
    name: str
    hipaa_reference: str
    description: str
    severity: Severity


@dataclass
class CheckResult:
    check: ComplianceCheck
    status: ComplianceStatus
    message: str
    remediation: str | None = None

    def to_dict(self) -> dict:
        return {
            "check_id": self.check.id,
            "check_name": self.check.name,
            "hipaa_reference": self.check.hipaa_reference,
            "severity": self.check.severity.value,
            "status": self.status.value,
            "message": self.message,
            "remediation": self.remediation,
        }


@dataclass
class ComplianceReport:
    results: list[CheckResult]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.status == ComplianceStatus.PASS)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == ComplianceStatus.FAIL)

    @property
    def warned_count(self) -> int:
        return sum(1 for r in self.results if r.status == ComplianceStatus.WARN)

    @property
    def skipped_count(self) -> int:
        return sum(1 for r in self.results if r.status == ComplianceStatus.SKIP)

    @property
    def is_compliant(self) -> bool:
        for result in self.results:
            if result.status == ComplianceStatus.FAIL:
                if result.check.severity in (Severity.CRITICAL, Severity.HIGH):
                    return False
        return True

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "results": [r.to_dict() for r in self.results],
            "summary": {
                "passed": self.passed_count,
                "failed": self.failed_count,
                "warned": self.warned_count,
                "skipped": self.skipped_count,
                "is_compliant": self.is_compliant,
            },
        }


CHECKS: list[ComplianceCheck] = [
    ComplianceCheck(
        id="TLS_ENABLED",
        name="TLS Encryption in Transit",
        hipaa_reference="164.312(e)(1)",
        description="Verify TLS 1.2+ is required for all database connections",
        severity=Severity.CRITICAL,
    ),
    ComplianceCheck(
        id="AUDIT_ENABLED",
        name="Audit Logging Active",
        hipaa_reference="164.312(b)",
        description="Verify comprehensive audit logging is enabled",
        severity=Severity.CRITICAL,
    ),
    ComplianceCheck(
        id="AUTH_REQUIRED",
        name="Authentication Required",
        hipaa_reference="164.312(d)",
        description="Verify user authentication is enforced",
        severity=Severity.CRITICAL,
    ),
    ComplianceCheck(
        id="RBAC_CONFIGURED",
        name="Role-Based Access Control",
        hipaa_reference="164.312(a)(1)",
        description="Verify RBAC is properly configured",
        severity=Severity.HIGH,
    ),
    ComplianceCheck(
        id="ENCRYPTION_AT_REST",
        name="Encryption at Rest",
        hipaa_reference="164.312(a)(2)(iv)",
        description="Verify data encryption at rest per NIST SP 800-111 (AES-256)",
        severity=Severity.HIGH,
    ),
    ComplianceCheck(
        id="SESSION_TIMEOUT",
        name="Automatic Session Timeout",
        hipaa_reference="164.312(a)(2)(iii)",
        description="Verify automatic logoff is configured",
        severity=Severity.MEDIUM,
    ),
    ComplianceCheck(
        id="AUDIT_IMMUTABILITY",
        name="Audit Log Immutability",
        hipaa_reference="164.312(b)",
        description="Verify audit logs cannot be modified or deleted",
        severity=Severity.CRITICAL,
    ),
    ComplianceCheck(
        id="PASSWORD_POLICY",
        name="Password Policy Enforcement",
        hipaa_reference="164.312(d)",
        description="Verify strong password requirements are enforced",
        severity=Severity.HIGH,
    ),
    ComplianceCheck(
        id="PHI_DETECTION",
        name="PHI Detection Active",
        hipaa_reference="164.514(b)",
        description="Verify PHI detection and alerting is configured",
        severity=Severity.HIGH,
    ),
    ComplianceCheck(
        id="SECURE_DISPOSAL",
        name="Secure Data Disposal",
        hipaa_reference="164.530(j)",
        description="Verify secure disposal procedures are in place",
        severity=Severity.MEDIUM,
    ),
    # 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
    # "Establish (and implement as needed) procedures for obtaining necessary
    # electronic protected health information during an emergency."
    ComplianceCheck(
        id="EMERGENCY_ACCESS",
        name="Emergency Access Procedure",
        hipaa_reference="164.312(a)(2)(ii)",
        description="Verify emergency access (break-glass) procedures are implemented",
        severity=Severity.CRITICAL,
    ),
    # 45 CFR 164.312(d) - REQUIRED standard
    # "Implement procedures to verify that a person or entity seeking access to
    # electronic protected health information is the one claimed."
    # HHS Security Series Paper #4: Multi-factor uses 2+ of: something known,
    # something possessed, something unique (biometric).
    ComplianceCheck(
        id="MFA_ENABLED",
        name="Multi-Factor Authentication",
        hipaa_reference="164.312(d)",
        description="Verify MFA is enabled for user authentication",
        severity=Severity.CRITICAL,
    ),
    # 45 CFR 164.316(b)(2)(i) - REQUIRED
    # "Retain the documentation required by paragraph (b)(1) of this section for
    # 6 years from the date of its creation or the date when it last was in
    # effect, whichever is later."
    ComplianceCheck(
        id="AUDIT_RETENTION",
        name="Audit Log Retention Policy",
        hipaa_reference="164.316(b)(2)(i)",
        description="Verify 6-year audit log retention policy is enforced",
        severity=Severity.CRITICAL,
    ),
]


def get_check_by_id(check_id: str) -> ComplianceCheck | None:
    for check in CHECKS:
        if check.id == check_id:
            return check
    return None
