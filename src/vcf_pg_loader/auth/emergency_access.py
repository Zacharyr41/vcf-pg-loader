"""HIPAA-compliant Emergency Access (Break-Glass) Procedures.

HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
"Establish (and implement as needed) procedures for obtaining necessary
electronic protected health information during an emergency."

This module implements time-limited emergency access with:
- Mandatory justification (minimum 20 characters)
- Automatic expiration (maximum 24 hours)
- Enhanced audit logging
- Post-incident review requirements
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from importlib.resources import files
from uuid import UUID

import asyncpg

from ..audit.logger import AuditLogger
from ..audit.models import AuditEvent, AuditEventType

logger = logging.getLogger(__name__)


class EmergencyType(Enum):
    PATIENT_EMERGENCY = "patient_emergency"
    SYSTEM_EMERGENCY = "system_emergency"
    DISASTER_RECOVERY = "disaster_recovery"
    LEGAL_REQUIREMENT = "legal_requirement"
    OTHER = "other"


@dataclass
class EmergencyToken:
    token_id: UUID
    user_id: int
    justification: str
    emergency_type: EmergencyType
    granted_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None
    granted_by: int | None = None
    access_scope: dict | None = None
    requires_review: bool = True
    reviewed_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict) -> "EmergencyToken":
        return cls(
            token_id=row["token_id"],
            user_id=row["user_id"],
            justification=row["justification"],
            emergency_type=EmergencyType(row["emergency_type"]),
            granted_at=row["granted_at"],
            expires_at=row["expires_at"],
            revoked_at=row.get("revoked_at"),
            granted_by=row.get("granted_by"),
            access_scope=row.get("access_scope"),
            requires_review=row.get("requires_review", True),
            reviewed_at=row.get("reviewed_at"),
        )

    def is_active(self) -> bool:
        if self.revoked_at is not None:
            return False
        return datetime.now(UTC) < self.expires_at

    def minutes_remaining(self) -> float:
        if not self.is_active():
            return 0
        remaining = self.expires_at - datetime.now(UTC)
        return max(0, remaining.total_seconds() / 60)


class EmergencyAccessManager:
    """Manages emergency access (break-glass) procedures.

    45 CFR 164.312(a)(2)(ii) requires procedures for obtaining necessary ePHI
    during emergencies. This class provides:
    - Time-limited emergency access tokens
    - Mandatory justification for audit trail
    - Automatic expiration (max 24 hours)
    - Post-incident review workflow
    """

    MIN_JUSTIFICATION_LENGTH = 20
    MAX_DURATION_HOURS = 24
    DEFAULT_DURATION_MINUTES = 60

    def __init__(self, audit_logger: AuditLogger | None = None):
        self._audit_logger = audit_logger

    async def create_schema(self, conn: asyncpg.Connection) -> None:
        """Create emergency access schema.

        45 CFR 164.312(a)(2)(ii): Establish emergency access procedures.
        """
        sql_path = files("vcf_pg_loader.db.schema").joinpath("emergency_access_tables.sql")
        sql = sql_path.read_text()
        await conn.execute(sql)
        logger.info("Emergency access schema created/updated")

    async def schema_exists(self, conn: asyncpg.Connection) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'emergency_access_tokens'
            )
            """
        )

    async def grant_access(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        justification: str,
        emergency_type: EmergencyType,
        duration_minutes: int | None = None,
        granted_by: int | None = None,
        access_scope: dict | None = None,
        client_ip: str | None = None,
        client_hostname: str | None = None,
    ) -> EmergencyToken:
        """Grant emergency access to a user.

        45 CFR 164.312(a)(2)(ii): Procedure for obtaining ePHI during emergency.

        Args:
            user_id: User receiving emergency access
            justification: REQUIRED explanation (min 20 chars) for audit trail
            emergency_type: Category of emergency
            duration_minutes: Access duration (default 60, max 1440)
            granted_by: User ID of person granting access (for approvals)
            access_scope: JSON scope limiting accessible resources
            client_ip: Client IP for audit
            client_hostname: Client hostname for audit

        Returns:
            EmergencyToken with access details

        Raises:
            ValueError: If justification too short or duration invalid
        """
        if len(justification) < self.MIN_JUSTIFICATION_LENGTH:
            raise ValueError(
                f"Justification must be at least {self.MIN_JUSTIFICATION_LENGTH} characters "
                f"(provided: {len(justification)})"
            )

        duration = (
            duration_minutes if duration_minutes is not None else self.DEFAULT_DURATION_MINUTES
        )
        max_minutes = self.MAX_DURATION_HOURS * 60

        if duration > max_minutes:
            raise ValueError(
                f"Emergency access duration cannot exceed {self.MAX_DURATION_HOURS} hours "
                f"(requested: {duration} minutes)"
            )

        if duration < 1:
            raise ValueError("Emergency access duration must be at least 1 minute")

        scope = access_scope or {"all_phi": False, "resources": []}

        token_id = await conn.fetchval(
            """
            SELECT grant_emergency_access($1, $2, $3, $4, $5, $6, $7::inet, $8)
            """,
            user_id,
            justification,
            emergency_type.value,
            duration,
            granted_by,
            scope,
            client_ip,
            client_hostname,
        )

        row = await conn.fetchrow(
            "SELECT * FROM emergency_access_tokens WHERE token_id = $1",
            token_id,
        )

        token = EmergencyToken.from_db_row(dict(row))

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.EMERGENCY_ACCESS,
                    action="emergency_access_granted",
                    user_id=granted_by,
                    success=True,
                    resource_type="emergency_token",
                    resource_id=str(token_id),
                    client_ip=client_ip,
                    client_hostname=client_hostname,
                    details={
                        "target_user_id": user_id,
                        "emergency_type": emergency_type.value,
                        "duration_minutes": duration,
                        "justification_length": len(justification),
                    },
                )
            )

        logger.warning(
            "Emergency access granted: user_id=%d, type=%s, duration=%d min, token=%s",
            user_id,
            emergency_type.value,
            duration,
            token_id,
        )

        return token

    async def validate_token(
        self,
        conn: asyncpg.Connection,
        token_id: UUID,
        resource_type: str | None = None,
        resource_id: str | None = None,
    ) -> tuple[bool, EmergencyToken | None, str]:
        """Validate an emergency access token.

        Args:
            conn: Database connection
            token_id: Token to validate
            resource_type: Optional resource being accessed
            resource_id: Optional resource ID being accessed

        Returns:
            Tuple of (is_valid, token_if_valid, message)
        """
        result = await conn.fetchrow(
            "SELECT * FROM validate_emergency_access($1, $2, $3)",
            token_id,
            resource_type,
            resource_id,
        )

        if not result["is_valid"]:
            return False, None, result["message"]

        row = await conn.fetchrow(
            "SELECT * FROM emergency_access_tokens WHERE token_id = $1",
            token_id,
        )

        token = EmergencyToken.from_db_row(dict(row))
        return True, token, result["message"]

    async def revoke_access(
        self,
        conn: asyncpg.Connection,
        token_id: UUID,
        revoked_by: int,
        reason: str,
    ) -> bool:
        """Revoke an emergency access token.

        Args:
            conn: Database connection
            token_id: Token to revoke
            revoked_by: User ID revoking access
            reason: Reason for revocation

        Returns:
            True if token was revoked, False if not found or already revoked
        """
        success = await conn.fetchval(
            "SELECT revoke_emergency_access($1, $2, $3)",
            token_id,
            revoked_by,
            reason,
        )

        if success and self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.EMERGENCY_ACCESS,
                    action="emergency_access_revoked",
                    user_id=revoked_by,
                    success=True,
                    resource_type="emergency_token",
                    resource_id=str(token_id),
                    details={"reason": reason},
                )
            )

        return success

    async def complete_review(
        self,
        conn: asyncpg.Connection,
        token_id: UUID,
        reviewed_by: int,
        review_notes: str,
    ) -> bool:
        """Complete post-incident review of emergency access.

        45 CFR 164.312(a)(2)(ii) implies review of emergency access usage
        to ensure it was appropriate and identify any policy improvements.

        Args:
            conn: Database connection
            token_id: Token to review
            reviewed_by: User ID completing review
            review_notes: Review findings and notes

        Returns:
            True if review was recorded, False if not applicable
        """
        success = await conn.fetchval(
            "SELECT complete_emergency_review($1, $2, $3)",
            token_id,
            reviewed_by,
            review_notes,
        )

        if success and self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.EMERGENCY_ACCESS,
                    action="emergency_access_reviewed",
                    user_id=reviewed_by,
                    success=True,
                    resource_type="emergency_token",
                    resource_id=str(token_id),
                    details={"notes_length": len(review_notes)},
                )
            )

        return success

    async def get_active_tokens(
        self,
        conn: asyncpg.Connection,
        user_id: int | None = None,
    ) -> list[EmergencyToken]:
        """Get active emergency access tokens.

        Args:
            conn: Database connection
            user_id: Optional filter by user

        Returns:
            List of active EmergencyToken objects
        """
        if user_id:
            rows = await conn.fetch(
                """
                SELECT t.* FROM emergency_access_tokens t
                WHERE t.user_id = $1
                  AND t.revoked_at IS NULL
                  AND t.expires_at > NOW()
                ORDER BY t.expires_at
                """,
                user_id,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT t.* FROM emergency_access_tokens t
                WHERE t.revoked_at IS NULL
                  AND t.expires_at > NOW()
                ORDER BY t.expires_at
                """
            )

        return [EmergencyToken.from_db_row(dict(row)) for row in rows]

    async def get_pending_reviews(
        self,
        conn: asyncpg.Connection,
    ) -> list[dict]:
        """Get emergency access tokens pending review.

        Returns:
            List of token summaries requiring review
        """
        rows = await conn.fetch(
            """
            SELECT * FROM v_pending_emergency_reviews
            ORDER BY ended_at DESC
            """
        )
        return [dict(row) for row in rows]

    async def get_token_audit(
        self,
        conn: asyncpg.Connection,
        token_id: UUID,
    ) -> list[dict]:
        """Get audit trail for a specific emergency access token.

        Args:
            conn: Database connection
            token_id: Token to get audit for

        Returns:
            List of audit events for the token
        """
        rows = await conn.fetch(
            """
            SELECT * FROM emergency_access_audit
            WHERE token_id = $1
            ORDER BY event_time
            """,
            token_id,
        )
        return [dict(row) for row in rows]
