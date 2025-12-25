"""HIPAA-compliant Audit Log Retention Policy Management.

HIPAA Citation: 45 CFR 164.316(b)(2)(i) - REQUIRED
"Retain the documentation required by paragraph (b)(1) of this section for
6 years from the date of its creation or the date when it last was in
effect, whichever is later."

This applies to:
- Audit logs
- Security policies
- Risk assessments
- Incident records
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import asyncpg

from .logger import AuditLogger
from .models import AuditEvent, AuditEventType

logger = logging.getLogger(__name__)

HIPAA_MINIMUM_RETENTION_YEARS = 6


@dataclass
class RetentionPolicy:
    policy_id: int
    retention_years: int
    enforce_minimum: bool
    is_active: bool
    created_at: datetime
    created_by: int | None = None
    notes: str | None = None

    @classmethod
    def from_db_row(cls, row: dict) -> "RetentionPolicy":
        return cls(
            policy_id=row["policy_id"],
            retention_years=row["retention_years"],
            enforce_minimum=row["enforce_minimum"],
            is_active=row["is_active"],
            created_at=row["created_at"],
            created_by=row.get("created_by"),
            notes=row.get("notes"),
        )

    def is_compliant(self) -> bool:
        """Check if policy meets HIPAA minimum requirements."""
        return self.retention_years >= HIPAA_MINIMUM_RETENTION_YEARS and self.enforce_minimum


@dataclass
class RetentionStatus:
    has_policy: bool
    is_compliant: bool
    retention_years: int
    enforcement_enabled: bool
    oldest_partition_date: date | None
    partition_count: int
    archived_partition_count: int

    def to_dict(self) -> dict:
        return {
            "has_policy": self.has_policy,
            "is_compliant": self.is_compliant,
            "retention_years": self.retention_years,
            "enforcement_enabled": self.enforcement_enabled,
            "oldest_partition_date": self.oldest_partition_date.isoformat()
            if self.oldest_partition_date
            else None,
            "partition_count": self.partition_count,
            "archived_partition_count": self.archived_partition_count,
        }


class AuditRetentionManager:
    """Manages HIPAA-compliant audit log retention.

    45 CFR 164.316(b)(2)(i) requires 6-year minimum retention for:
    - Security policies and procedures
    - Documentation of actions, activities, or assessments
    - Audit logs and incident records

    This class enforces retention policy by:
    - Preventing deletion of logs within retention window
    - Managing partition archival (detach but don't delete)
    - Tracking retention compliance status
    """

    def __init__(self, audit_logger: AuditLogger | None = None):
        self._audit_logger = audit_logger

    async def create_retention_schema(self, conn: asyncpg.Connection) -> None:
        """Create retention policy schema.

        45 CFR 164.316(b)(2)(i): Establish retention infrastructure.
        """
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_retention_policy (
                policy_id SERIAL PRIMARY KEY,
                retention_years INTEGER NOT NULL CHECK (retention_years >= 6),
                enforce_minimum BOOLEAN NOT NULL DEFAULT true,
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_by INTEGER,
                notes TEXT
            );

            INSERT INTO audit_retention_policy (retention_years, enforce_minimum, notes)
            SELECT 6, true, 'HIPAA minimum retention per 45 CFR 164.316(b)(2)(i)'
            WHERE NOT EXISTS (SELECT 1 FROM audit_retention_policy WHERE is_active = true);
            """
        )
        logger.info("Audit retention schema created/updated")

    async def schema_exists(self, conn: asyncpg.Connection) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'audit_retention_policy'
            )
            """
        )

    async def get_active_policy(self, conn: asyncpg.Connection) -> RetentionPolicy | None:
        """Get the current active retention policy."""
        row = await conn.fetchrow(
            """
            SELECT * FROM audit_retention_policy
            WHERE is_active = true
            ORDER BY created_at DESC
            LIMIT 1
            """
        )

        if not row:
            return None

        return RetentionPolicy.from_db_row(dict(row))

    async def set_retention_policy(
        self,
        conn: asyncpg.Connection,
        retention_years: int,
        enforce_minimum: bool = True,
        created_by: int | None = None,
        notes: str | None = None,
    ) -> RetentionPolicy:
        """Set or update the retention policy.

        45 CFR 164.316(b)(2)(i): Configure retention period (min 6 years).

        Args:
            conn: Database connection
            retention_years: Years to retain logs (minimum 6)
            enforce_minimum: Enforce minimum retention (prevent early deletion)
            created_by: User creating/updating policy
            notes: Optional notes about the policy

        Returns:
            New RetentionPolicy

        Raises:
            ValueError: If retention_years < 6
        """
        if retention_years < HIPAA_MINIMUM_RETENTION_YEARS:
            raise ValueError(
                f"Retention period must be at least {HIPAA_MINIMUM_RETENTION_YEARS} years "
                f"per 45 CFR 164.316(b)(2)(i). Requested: {retention_years} years."
            )

        async with conn.transaction():
            await conn.execute(
                "UPDATE audit_retention_policy SET is_active = false WHERE is_active = true"
            )

            policy_id = await conn.fetchval(
                """
                INSERT INTO audit_retention_policy (
                    retention_years, enforce_minimum, is_active, created_by, notes
                ) VALUES ($1, $2, true, $3, $4)
                RETURNING policy_id
                """,
                retention_years,
                enforce_minimum,
                created_by,
                notes,
            )

        row = await conn.fetchrow(
            "SELECT * FROM audit_retention_policy WHERE policy_id = $1",
            policy_id,
        )

        policy = RetentionPolicy.from_db_row(dict(row))

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.CONFIG_CHANGE,
                    action="retention_policy_updated",
                    user_id=created_by,
                    success=True,
                    details={
                        "retention_years": retention_years,
                        "enforce_minimum": enforce_minimum,
                        "hipaa_citation": "45 CFR 164.316(b)(2)(i)",
                    },
                )
            )

        logger.info(
            "Retention policy updated: years=%d, enforce=%s, policy_id=%d",
            retention_years,
            enforce_minimum,
            policy_id,
        )

        return policy

    async def get_retention_status(self, conn: asyncpg.Connection) -> RetentionStatus:
        """Get comprehensive retention status.

        Returns:
            RetentionStatus with compliance information
        """
        policy = await self.get_active_policy(conn)

        partition_info = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as partition_count,
                MIN(
                    CASE WHEN relname NOT LIKE '%_archived'
                    THEN substring(relname from 'hipaa_audit_log_([0-9]{4}_[0-9]{2})')
                    ELSE NULL END
                ) as oldest_partition
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname LIKE 'hipaa_audit_log_%'
              AND n.nspname = 'public'
              AND c.relkind = 'r'
            """
        )

        archived_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname LIKE 'hipaa_audit_log_%_archived'
              AND n.nspname = 'public'
            """
        )

        oldest_date = None
        if partition_info["oldest_partition"]:
            try:
                oldest_date = datetime.strptime(partition_info["oldest_partition"], "%Y_%m").date()
            except ValueError:
                pass

        return RetentionStatus(
            has_policy=policy is not None,
            is_compliant=policy.is_compliant() if policy else False,
            retention_years=policy.retention_years if policy else 0,
            enforcement_enabled=policy.enforce_minimum if policy else False,
            oldest_partition_date=oldest_date,
            partition_count=partition_info["partition_count"] or 0,
            archived_partition_count=archived_count or 0,
        )

    async def check_deletion_allowed(
        self,
        conn: asyncpg.Connection,
        partition_date: date,
    ) -> tuple[bool, str]:
        """Check if deleting a partition is allowed per retention policy.

        45 CFR 164.316(b)(2)(i): Enforce 6-year minimum retention.

        Args:
            conn: Database connection
            partition_date: Date of partition to check

        Returns:
            Tuple of (is_allowed, reason)
        """
        policy = await self.get_active_policy(conn)

        if not policy:
            return False, "No retention policy configured"

        if not policy.enforce_minimum:
            return True, "Retention enforcement is disabled"

        retention_cutoff = date.today() - timedelta(days=policy.retention_years * 365)

        if partition_date > retention_cutoff:
            days_remaining = (partition_date - retention_cutoff).days
            return False, (
                f"Partition {partition_date} is within {policy.retention_years}-year retention window. "
                f"{days_remaining} days until deletion allowed. "
                f"HIPAA Citation: 45 CFR 164.316(b)(2)(i)"
            )

        return True, f"Partition {partition_date} is outside retention window"

    async def archive_old_partitions(
        self,
        conn: asyncpg.Connection,
        older_than_years: int | None = None,
    ) -> list[str]:
        """Archive (detach but don't delete) old partitions.

        HIPAA requires retention, not immediate availability. Archiving
        detaches partitions from the main table for performance while
        preserving data for compliance.

        Args:
            conn: Database connection
            older_than_years: Archive partitions older than this. If None,
                             archives partitions older than retention period.

        Returns:
            List of archived partition names
        """
        policy = await self.get_active_policy(conn)
        years = older_than_years or (policy.retention_years if policy else 6)

        cutoff_date = date.today() - timedelta(days=years * 365)

        partitions = await conn.fetch(
            """
            SELECT c.relname as partition_name,
                   substring(c.relname from 'hipaa_audit_log_([0-9]{4}_[0-9]{2})') as partition_date
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_inherits i ON i.inhrelid = c.oid
            JOIN pg_class parent ON parent.oid = i.inhparent
            WHERE parent.relname = 'hipaa_audit_log'
              AND c.relname NOT LIKE '%_archived'
              AND n.nspname = 'public'
            ORDER BY partition_date
            """
        )

        archived = []
        for p in partitions:
            if not p["partition_date"]:
                continue

            try:
                partition_date = datetime.strptime(p["partition_date"], "%Y_%m").date()
            except ValueError:
                continue

            if partition_date < cutoff_date:
                archive_name = await conn.fetchval(
                    "SELECT archive_audit_partition($1)",
                    partition_date,
                )
                if archive_name:
                    archived.append(archive_name)
                    logger.info("Archived partition: %s -> %s", p["partition_name"], archive_name)

        if archived and self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.CONFIG_CHANGE,
                    action="audit_partitions_archived",
                    success=True,
                    details={
                        "partitions_archived": len(archived),
                        "cutoff_date": cutoff_date.isoformat(),
                        "partition_names": archived,
                    },
                )
            )

        return archived

    async def verify_retention_integrity(
        self,
        conn: asyncpg.Connection,
    ) -> tuple[bool, list[str]]:
        """Verify retention policy is being enforced correctly.

        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []

        policy = await self.get_active_policy(conn)
        if not policy:
            issues.append("No active retention policy configured")
            return False, issues

        if policy.retention_years < HIPAA_MINIMUM_RETENTION_YEARS:
            issues.append(
                f"Retention period ({policy.retention_years} years) is below "
                f"HIPAA minimum ({HIPAA_MINIMUM_RETENTION_YEARS} years)"
            )

        if not policy.enforce_minimum:
            issues.append("Retention enforcement is disabled - logs may be deleted prematurely")

        oldest = await conn.fetchval(
            """
            SELECT MIN(event_time)::date FROM hipaa_audit_log
            """
        )

        if oldest:
            expected_cutoff = date.today() - timedelta(days=policy.retention_years * 365)
            if oldest > expected_cutoff:
                issues.append(
                    f"Oldest log entry ({oldest}) is newer than expected. "
                    f"Logs may have been improperly deleted."
                )

        trigger_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'audit_immutability'
            )
            """
        )

        if not trigger_exists:
            issues.append("Audit immutability trigger not found - logs may be modifiable")

        return len(issues) == 0, issues
