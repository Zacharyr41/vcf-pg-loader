"""HIPAA-compliant audit event models."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

PHI_PATTERNS = [
    "patient",
    "name",
    "dob",
    "birth",
    "ssn",
    "mrn",
    "address",
    "phone",
    "email",
    "sample_id",
]


class AuditEventType(Enum):
    AUTH_LOGIN = "AUTH_LOGIN"
    AUTH_LOGOUT = "AUTH_LOGOUT"
    AUTH_FAILED = "AUTH_FAILED"
    SESSION_TIMEOUT = "SESSION_TIMEOUT"
    SESSION_TERMINATED = "SESSION_TERMINATED"
    DATA_READ = "DATA_READ"
    DATA_WRITE = "DATA_WRITE"
    DATA_DELETE = "DATA_DELETE"
    DATA_EXPORT = "DATA_EXPORT"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"
    CONFIG_CHANGE = "CONFIG_CHANGE"
    PERMISSION_CHANGE = "PERMISSION_CHANGE"
    PHI_ACCESS = "PHI_ACCESS"
    EMERGENCY_ACCESS = "EMERGENCY_ACCESS"


@dataclass
class AuditEvent:
    event_type: AuditEventType
    action: str
    success: bool
    user_id: int | None = None
    user_name: str = "system"
    session_id: UUID | None = None
    resource_type: str | None = None
    resource_id: str | None = None
    client_ip: str | None = None
    client_hostname: str | None = None
    application_name: str = "vcf-pg-loader"
    error_message: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    event_time: datetime | None = None

    def sanitize_details(self) -> dict[str, Any]:
        """Remove any potential PHI from details dict.

        Scans keys for PHI-related patterns and redacts values.
        """
        if not self.details:
            return {}

        sanitized = {}
        for key, value in self.details.items():
            key_lower = key.lower()
            if any(pattern in key_lower for pattern in PHI_PATTERNS):
                sanitized[key] = "[REDACTED]"
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_nested(value)
            elif isinstance(value, list):
                sanitized[key] = [
                    self._sanitize_nested(v) if isinstance(v, dict) else v for v in value
                ]
            else:
                sanitized[key] = value
        return sanitized

    def _sanitize_nested(self, d: dict[str, Any]) -> dict[str, Any]:
        """Recursively sanitize nested dicts."""
        result = {}
        for key, value in d.items():
            key_lower = key.lower()
            if any(pattern in key_lower for pattern in PHI_PATTERNS):
                result[key] = "[REDACTED]"
            elif isinstance(value, dict):
                result[key] = self._sanitize_nested(value)
            else:
                result[key] = value
        return result

    def to_db_row(self) -> dict[str, Any]:
        """Convert to dict suitable for database insertion."""
        return {
            "event_type": self.event_type.value,
            "action": self.action,
            "success": self.success,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "session_id": self.session_id,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "client_ip": self.client_ip,
            "client_hostname": self.client_hostname,
            "application_name": self.application_name,
            "error_message": self.error_message,
            "details": self.sanitize_details(),
        }
