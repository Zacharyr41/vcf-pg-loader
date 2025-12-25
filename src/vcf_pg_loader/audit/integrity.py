"""HIPAA-compliant audit log integrity verification with hash chain."""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum

import asyncpg

from .models import AuditEvent


class IntegrityStatus(Enum):
    VALID = "valid"
    CHAIN_BROKEN = "chain_broken"
    HASH_MISMATCH = "hash_mismatch"
    MISSING_HASH = "missing_hash"


@dataclass
class IntegrityViolation:
    audit_id: int
    event_time: datetime
    status: IntegrityStatus
    expected_hash: str | None = None
    actual_hash: str | None = None
    message: str = ""


@dataclass
class IntegrityReport:
    start_date: date
    end_date: date
    total_entries: int
    verified_entries: int
    violations: list[IntegrityViolation] = field(default_factory=list)
    first_entry_hash: str | None = None
    last_entry_hash: str | None = None
    verification_time: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_valid(self) -> bool:
        return len(self.violations) == 0

    @property
    def coverage_percent(self) -> float:
        if self.total_entries == 0:
            return 100.0
        return (self.verified_entries / self.total_entries) * 100

    def to_dict(self) -> dict:
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "total_entries": self.total_entries,
            "verified_entries": self.verified_entries,
            "is_valid": self.is_valid,
            "coverage_percent": round(self.coverage_percent, 2),
            "violation_count": len(self.violations),
            "violations": [
                {
                    "audit_id": v.audit_id,
                    "event_time": v.event_time.isoformat(),
                    "status": v.status.value,
                    "message": v.message,
                }
                for v in self.violations[:100]
            ],
            "first_entry_hash": self.first_entry_hash,
            "last_entry_hash": self.last_entry_hash,
            "verification_time": self.verification_time.isoformat(),
        }


@dataclass
class BackupMetadata:
    export_time: datetime
    start_date: date
    end_date: date
    entry_count: int
    first_hash: str | None
    last_hash: str | None
    checksum: str


class AuditIntegrity:
    """Hash chain integrity verification for HIPAA audit logs."""

    GENESIS_HASH = "0" * 64

    def compute_entry_hash(
        self,
        event_time: datetime,
        event_type: str,
        user_name: str,
        action: str,
        success: bool,
        details: dict | None,
        previous_hash: str,
    ) -> str:
        """Compute SHA-256 hash matching PostgreSQL json_build_object format."""
        hash_input = {
            "event_time": event_time.isoformat() if event_time else None,
            "event_type": event_type,
            "user_name": user_name,
            "action": action,
            "success": success,
            "details": details or {},
            "previous_hash": previous_hash,
        }
        canonical = json.dumps(hash_input, separators=(", ", ": "))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def compute_event_hash(self, event: AuditEvent, previous_hash: str) -> str:
        """Compute hash for an AuditEvent object."""
        return self.compute_entry_hash(
            event_time=event.event_time,
            event_type=event.event_type.value,
            user_name=event.user_name,
            action=event.action,
            success=event.success,
            details=event.sanitize_details(),
            previous_hash=previous_hash,
        )

    async def get_last_hash(self, conn: asyncpg.Connection) -> str:
        """Get hash of the most recent audit entry for chain continuation."""
        row = await conn.fetchrow(
            """
            SELECT entry_hash FROM hipaa_audit_log
            WHERE entry_hash IS NOT NULL
            ORDER BY created_date DESC, audit_id DESC
            LIMIT 1
            """
        )
        return row["entry_hash"] if row else self.GENESIS_HASH

    async def get_previous_hash_for_date(
        self,
        conn: asyncpg.Connection,
        target_date: date,
    ) -> str:
        """Get the last hash before a given date for chain verification."""
        row = await conn.fetchrow(
            """
            SELECT entry_hash FROM hipaa_audit_log
            WHERE entry_hash IS NOT NULL
              AND created_date < $1
            ORDER BY created_date DESC, audit_id DESC
            LIMIT 1
            """,
            target_date,
        )
        return row["entry_hash"] if row else self.GENESIS_HASH

    async def verify_chain_integrity(
        self,
        conn: asyncpg.Connection,
        start_date: date,
        end_date: date,
        batch_size: int = 10000,
    ) -> IntegrityReport:
        """Verify hash chain integrity for a date range.

        Checks that:
        1. All entries have hashes
        2. Each entry's previous_hash matches prior entry's entry_hash (chain continuity)

        Note: Hash computation is done by the database trigger, so we trust the
        stored hashes and only verify the chain is unbroken.
        """
        total = await conn.fetchval(
            """
            SELECT COUNT(*) FROM hipaa_audit_log
            WHERE created_date >= $1 AND created_date <= $2
            """,
            start_date,
            end_date,
        )

        violations: list[IntegrityViolation] = []
        verified = 0
        first_hash = None
        last_hash = None

        previous_hash = await self.get_previous_hash_for_date(conn, start_date)

        offset = 0
        while True:
            rows = await conn.fetch(
                """
                SELECT audit_id, created_date, event_time, event_type,
                       user_name, action, success, details,
                       previous_hash, entry_hash
                FROM hipaa_audit_log
                WHERE created_date >= $1 AND created_date <= $2
                ORDER BY created_date ASC, audit_id ASC
                LIMIT $3 OFFSET $4
                """,
                start_date,
                end_date,
                batch_size,
                offset,
            )

            if not rows:
                break

            for row in rows:
                if first_hash is None and row["entry_hash"]:
                    first_hash = row["entry_hash"]

                if row["entry_hash"] is None:
                    violations.append(
                        IntegrityViolation(
                            audit_id=row["audit_id"],
                            event_time=row["event_time"],
                            status=IntegrityStatus.MISSING_HASH,
                            message="Entry has no hash computed",
                        )
                    )
                    continue

                if row["previous_hash"] != previous_hash:
                    violations.append(
                        IntegrityViolation(
                            audit_id=row["audit_id"],
                            event_time=row["event_time"],
                            status=IntegrityStatus.CHAIN_BROKEN,
                            expected_hash=previous_hash,
                            actual_hash=row["previous_hash"],
                            message=f"Chain break: expected previous_hash={previous_hash[:16]}..., "
                            f"got {row['previous_hash'][:16] if row['previous_hash'] else 'NULL'}...",
                        )
                    )

                previous_hash = row["entry_hash"]
                last_hash = row["entry_hash"]
                verified += 1

            offset += batch_size

        return IntegrityReport(
            start_date=start_date,
            end_date=end_date,
            total_entries=total,
            verified_entries=verified,
            violations=violations,
            first_entry_hash=first_hash,
            last_entry_hash=last_hash,
        )

    async def export_with_integrity(
        self,
        conn: asyncpg.Connection,
        start_date: date,
        end_date: date,
    ) -> tuple[list[dict], BackupMetadata]:
        """Export audit logs with integrity metadata for backup verification.

        Returns:
            Tuple of (entries, metadata) where metadata can be used to verify import
        """
        rows = await conn.fetch(
            """
            SELECT audit_id, created_date, event_time, event_type,
                   user_id, user_name, session_id,
                   action, resource_type, resource_id,
                   client_ip, client_hostname, application_name,
                   success, error_message, details,
                   previous_hash, entry_hash
            FROM hipaa_audit_log
            WHERE created_date >= $1 AND created_date <= $2
            ORDER BY created_date ASC, audit_id ASC
            """,
            start_date,
            end_date,
        )

        entries = []
        for row in rows:
            entry = dict(row)
            if entry["event_time"]:
                entry["event_time"] = entry["event_time"].isoformat()
            if entry["created_date"]:
                entry["created_date"] = entry["created_date"].isoformat()
            if entry["session_id"]:
                entry["session_id"] = str(entry["session_id"])
            if entry["client_ip"]:
                entry["client_ip"] = str(entry["client_ip"])
            entries.append(entry)

        content_checksum = hashlib.sha256(
            json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

        metadata = BackupMetadata(
            export_time=datetime.now(UTC),
            start_date=start_date,
            end_date=end_date,
            entry_count=len(entries),
            first_hash=entries[0]["entry_hash"] if entries else None,
            last_hash=entries[-1]["entry_hash"] if entries else None,
            checksum=content_checksum,
        )

        return entries, metadata

    def verify_backup(
        self,
        entries: list[dict],
        metadata: BackupMetadata,
    ) -> tuple[bool, str]:
        """Verify that imported audit logs match their backup metadata.

        Returns:
            Tuple of (is_valid, message)
        """
        if len(entries) != metadata.entry_count:
            return (
                False,
                f"Entry count mismatch: expected {metadata.entry_count}, got {len(entries)}",
            )

        if entries:
            if entries[0].get("entry_hash") != metadata.first_hash:
                return False, "First entry hash does not match metadata"
            if entries[-1].get("entry_hash") != metadata.last_hash:
                return False, "Last entry hash does not match metadata"

        content_checksum = hashlib.sha256(
            json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

        if content_checksum != metadata.checksum:
            return False, "Content checksum mismatch (data may have been modified)"

        return True, "Backup verification passed"

    async def backfill_hashes(
        self,
        conn: asyncpg.Connection,
        batch_size: int = 1000,
    ) -> int:
        """Backfill hashes for entries that don't have them.

        This is needed for entries created before hash chain was implemented.
        Returns the number of entries updated.
        """
        previous_hash = await conn.fetchval(
            """
            SELECT entry_hash FROM hipaa_audit_log
            WHERE entry_hash IS NOT NULL
            ORDER BY created_date DESC, audit_id DESC
            LIMIT 1
            """
        )
        if not previous_hash:
            previous_hash = self.GENESIS_HASH

        rows = await conn.fetch(
            """
            SELECT audit_id, created_date, event_time, event_type,
                   user_name, action, success, details
            FROM hipaa_audit_log
            WHERE entry_hash IS NULL
            ORDER BY created_date ASC, audit_id ASC
            """
        )

        updated = 0
        for row in rows:
            entry_hash = self.compute_entry_hash(
                event_time=row["event_time"],
                event_type=row["event_type"],
                user_name=row["user_name"],
                action=row["action"],
                success=row["success"],
                details=row["details"],
                previous_hash=previous_hash,
            )

            await conn.execute(
                """
                UPDATE hipaa_audit_log
                SET previous_hash = $1, entry_hash = $2
                WHERE created_date = $3 AND audit_id = $4
                """,
                previous_hash,
                entry_hash,
                row["created_date"],
                row["audit_id"],
            )

            previous_hash = entry_hash
            updated += 1

        return updated
