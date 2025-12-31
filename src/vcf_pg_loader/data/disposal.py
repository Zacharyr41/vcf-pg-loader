"""Secure data disposal for HIPAA compliance.

HIPAA Reference: 164.530(j) - Retention and Disposal

Implements secure deletion with:
- Two-person authorization (configurable)
- Verification after disposal
- Certificate of destruction generation
- Full audit trail
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID

import asyncpg

from ..audit.logger import AuditLogger
from ..audit.models import AuditEvent, AuditEventType

logger = logging.getLogger(__name__)


class DisposalType(Enum):
    BATCH = "batch"
    SAMPLE = "sample"
    DATE_RANGE = "date_range"


class DisposalStatus(Enum):
    PENDING = "pending"
    AUTHORIZED = "authorized"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class VerificationStatus(Enum):
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class DisposalResult:
    disposal_id: UUID
    disposal_type: DisposalType
    target_identifier: str
    variants_disposed: int
    genotypes_disposed: int
    mappings_disposed: int
    status: DisposalStatus
    executed_at: datetime | None = None
    error_message: str | None = None


@dataclass
class VerificationResult:
    disposal_id: UUID
    passed: bool
    remaining_variants: int
    expected_deleted: int
    verified_at: datetime
    verified_by: int
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class DisposalCertificate:
    disposal_id: UUID
    certificate_hash: str
    generated_at: datetime
    disposal_type: str
    target_identifier: str
    variants_disposed: int
    mappings_disposed: int
    reason: str
    authorized_by: int
    authorized_at: datetime
    executed_by: int
    executed_at: datetime
    verified_by: int
    verified_at: datetime
    verification_result: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "certificate_of_destruction": {
                "disposal_id": str(self.disposal_id),
                "certificate_hash": self.certificate_hash,
                "generated_at": self.generated_at.isoformat(),
            },
            "disposal_details": {
                "type": self.disposal_type,
                "target": self.target_identifier,
                "reason": self.reason,
            },
            "data_destroyed": {
                "variants": self.variants_disposed,
                "mappings": self.mappings_disposed,
            },
            "authorization": {
                "authorized_by_user_id": self.authorized_by,
                "authorized_at": self.authorized_at.isoformat(),
            },
            "execution": {
                "executed_by_user_id": self.executed_by,
                "executed_at": self.executed_at.isoformat(),
            },
            "verification": {
                "verified_by_user_id": self.verified_by,
                "verified_at": self.verified_at.isoformat(),
                "result": self.verification_result,
            },
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


@dataclass
class ExpiredData:
    load_batch_id: UUID
    vcf_file_path: str
    loaded_at: datetime
    expires_at: datetime
    policy_name: str
    variant_count: int


@dataclass
class RetentionReport:
    generated_at: datetime
    expired_batches: list[ExpiredData]
    expiring_soon: list[ExpiredData]
    total_expired_variants: int
    total_expiring_variants: int


class DataDisposal:
    """Securely dispose of PHI data with verification and audit trail."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        audit_logger: AuditLogger | None = None,
        require_two_person_auth: bool = True,
    ):
        self._pool = pool
        self._audit_logger = audit_logger
        self._require_two_person = require_two_person_auth

    async def request_disposal(
        self,
        disposal_type: DisposalType,
        target_identifier: str,
        reason: str,
        authorized_by: int,
    ) -> UUID:
        """Request data disposal with authorization.

        Creates a disposal request that requires authorization before execution.
        If two-person authorization is required, a second authorizer must approve.
        """
        async with self._pool.acquire() as conn:
            disposal_id = await conn.fetchval(
                """
                INSERT INTO disposal_records (
                    disposal_type, target_identifier, reason, authorized_by,
                    authorization_required_count, execution_status
                ) VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING disposal_id
                """,
                disposal_type.value,
                target_identifier,
                reason,
                authorized_by,
                2 if self._require_two_person else 1,
                "pending" if self._require_two_person else "authorized",
            )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_DELETE,
                    action="disposal_requested",
                    success=True,
                    user_id=authorized_by,
                    resource_type=disposal_type.value,
                    resource_id=target_identifier,
                    details={
                        "disposal_id": str(disposal_id),
                        "reason": reason,
                        "requires_second_auth": self._require_two_person,
                    },
                )
            )

        logger.info(
            "Disposal request %s created for %s %s",
            disposal_id,
            disposal_type.value,
            target_identifier,
        )
        return disposal_id

    async def authorize_disposal(
        self,
        disposal_id: UUID,
        authorizer_id: int,
    ) -> bool:
        """Provide second authorization for disposal.

        Required when two-person authorization is enabled.
        """
        async with self._pool.acquire() as conn:
            record = await conn.fetchrow(
                """
                SELECT authorized_by, second_authorizer, execution_status
                FROM disposal_records WHERE disposal_id = $1
                """,
                disposal_id,
            )

            if not record:
                raise ValueError(f"Disposal {disposal_id} not found")

            if record["execution_status"] != "pending":
                raise ValueError(
                    f"Disposal {disposal_id} is not pending authorization "
                    f"(status: {record['execution_status']})"
                )

            if record["authorized_by"] == authorizer_id:
                raise ValueError("Second authorizer must be different from first authorizer")

            await conn.execute(
                """
                UPDATE disposal_records
                SET second_authorizer = $1,
                    second_authorized_at = NOW(),
                    execution_status = 'authorized'
                WHERE disposal_id = $2
                """,
                authorizer_id,
                disposal_id,
            )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_DELETE,
                    action="disposal_authorized",
                    success=True,
                    user_id=authorizer_id,
                    resource_type="disposal",
                    resource_id=str(disposal_id),
                )
            )

        logger.info("Disposal %s authorized by user %d", disposal_id, authorizer_id)
        return True

    async def dispose_batch(
        self,
        batch_id: UUID,
        reason: str,
        authorized_by: int,
        verification_required: bool = True,
    ) -> DisposalResult:
        """Securely dispose of all data from a load batch."""
        disposal_id = await self.request_disposal(
            DisposalType.BATCH,
            str(batch_id),
            reason,
            authorized_by,
        )

        if not self._require_two_person:
            return await self.execute_disposal(disposal_id, authorized_by)

        async with self._pool.acquire() as conn:
            record = await conn.fetchrow(
                "SELECT * FROM disposal_records WHERE disposal_id = $1", disposal_id
            )

        return DisposalResult(
            disposal_id=disposal_id,
            disposal_type=DisposalType.BATCH,
            target_identifier=str(batch_id),
            variants_disposed=0,
            genotypes_disposed=0,
            mappings_disposed=0,
            status=DisposalStatus(record["execution_status"]),
        )

    async def dispose_sample(
        self,
        sample_anonymous_id: UUID,
        reason: str,
        authorized_by: int,
    ) -> DisposalResult:
        """Remove all data for a specific sample (e.g., patient withdrawal)."""
        disposal_id = await self.request_disposal(
            DisposalType.SAMPLE,
            str(sample_anonymous_id),
            reason,
            authorized_by,
        )

        if not self._require_two_person:
            return await self.execute_disposal(disposal_id, authorized_by)

        async with self._pool.acquire() as conn:
            record = await conn.fetchrow(
                "SELECT * FROM disposal_records WHERE disposal_id = $1", disposal_id
            )

        return DisposalResult(
            disposal_id=disposal_id,
            disposal_type=DisposalType.SAMPLE,
            target_identifier=str(sample_anonymous_id),
            variants_disposed=0,
            genotypes_disposed=0,
            mappings_disposed=0,
            status=DisposalStatus(record["execution_status"]),
        )

    async def execute_disposal(
        self,
        disposal_id: UUID,
        executor_id: int,
    ) -> DisposalResult:
        """Execute an authorized disposal."""
        async with self._pool.acquire() as conn:
            record = await conn.fetchrow(
                "SELECT * FROM disposal_records WHERE disposal_id = $1", disposal_id
            )

            if not record:
                raise ValueError(f"Disposal {disposal_id} not found")

            if record["execution_status"] != "authorized":
                raise ValueError(
                    f"Disposal {disposal_id} is not authorized "
                    f"(status: {record['execution_status']})"
                )

            disposal_type = DisposalType(record["disposal_type"])
            target = record["target_identifier"]

            if disposal_type == DisposalType.BATCH:
                await conn.fetchval(
                    "SELECT dispose_batch_data($1, $2, $3)",
                    disposal_id,
                    UUID(target),
                    executor_id,
                )
            elif disposal_type == DisposalType.SAMPLE:
                await conn.fetchval(
                    "SELECT dispose_sample_data($1, $2, $3)",
                    disposal_id,
                    UUID(target),
                    executor_id,
                )
            else:
                raise ValueError(f"Unsupported disposal type: {disposal_type}")

            updated_record = await conn.fetchrow(
                "SELECT * FROM disposal_records WHERE disposal_id = $1", disposal_id
            )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_DELETE,
                    action="disposal_executed",
                    success=True,
                    user_id=executor_id,
                    resource_type=disposal_type.value,
                    resource_id=target,
                    details={
                        "disposal_id": str(disposal_id),
                        "variants_disposed": updated_record["variants_disposed"],
                        "mappings_disposed": updated_record["mappings_disposed"],
                    },
                )
            )

        logger.info(
            "Disposal %s executed: %d variants, %d mappings disposed",
            disposal_id,
            updated_record["variants_disposed"],
            updated_record["mappings_disposed"],
        )

        return DisposalResult(
            disposal_id=disposal_id,
            disposal_type=disposal_type,
            target_identifier=target,
            variants_disposed=updated_record["variants_disposed"],
            genotypes_disposed=updated_record["genotypes_disposed"] or 0,
            mappings_disposed=updated_record["mappings_disposed"],
            status=DisposalStatus(updated_record["execution_status"]),
            executed_at=updated_record["executed_at"],
        )

    async def verify_disposal(
        self,
        disposal_id: UUID,
        verifier_id: int,
    ) -> VerificationResult:
        """Verify data was properly disposed."""
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT verify_disposal($1, $2)", disposal_id, verifier_id)

            record = await conn.fetchrow(
                "SELECT * FROM disposal_records WHERE disposal_id = $1", disposal_id
            )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_DELETE,
                    action="disposal_verified",
                    success=result["verification_passed"],
                    user_id=verifier_id,
                    resource_type="disposal",
                    resource_id=str(disposal_id),
                    details={
                        "verification_passed": result["verification_passed"],
                        "remaining_variants": result["remaining_variants"],
                    },
                )
            )

        logger.info(
            "Disposal %s verification: %s",
            disposal_id,
            "PASSED" if result["verification_passed"] else "FAILED",
        )

        return VerificationResult(
            disposal_id=disposal_id,
            passed=result["verification_passed"],
            remaining_variants=result["remaining_variants"],
            expected_deleted=result["expected_deleted"],
            verified_at=record["verified_at"],
            verified_by=verifier_id,
            details=dict(result),
        )

    async def generate_disposal_certificate(
        self,
        disposal_id: UUID,
    ) -> DisposalCertificate:
        """Generate certificate of destruction for compliance."""
        async with self._pool.acquire() as conn:
            record = await conn.fetchrow(
                "SELECT * FROM disposal_records WHERE disposal_id = $1", disposal_id
            )

            if not record:
                raise ValueError(f"Disposal {disposal_id} not found")

            if record["verification_status"] != "passed":
                raise ValueError(
                    f"Cannot generate certificate: disposal {disposal_id} "
                    f"verification status is {record['verification_status']}"
                )

            cert_hash = await conn.fetchval("SELECT generate_certificate_hash($1)", disposal_id)

            updated_record = await conn.fetchrow(
                "SELECT * FROM disposal_records WHERE disposal_id = $1", disposal_id
            )

        certificate = DisposalCertificate(
            disposal_id=disposal_id,
            certificate_hash=cert_hash,
            generated_at=updated_record["certificate_generated_at"],
            disposal_type=updated_record["disposal_type"],
            target_identifier=updated_record["target_identifier"],
            variants_disposed=updated_record["variants_disposed"],
            mappings_disposed=updated_record["mappings_disposed"],
            reason=updated_record["reason"],
            authorized_by=updated_record["authorized_by"],
            authorized_at=updated_record["authorized_at"],
            executed_by=updated_record["executed_by"],
            executed_at=updated_record["executed_at"],
            verified_by=updated_record["verified_by"],
            verified_at=updated_record["verified_at"],
            verification_result=updated_record["verification_result"],
        )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_DELETE,
                    action="certificate_generated",
                    success=True,
                    resource_type="disposal",
                    resource_id=str(disposal_id),
                    details={"certificate_hash": cert_hash},
                )
            )

        logger.info("Certificate generated for disposal %s: %s", disposal_id, cert_hash)
        return certificate

    async def list_disposals(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        status: DisposalStatus | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List disposal records with optional filtering."""
        async with self._pool.acquire() as conn:
            query = """
                SELECT d.*, u1.username as authorized_by_name, u2.username as executed_by_name
                FROM disposal_records d
                LEFT JOIN users u1 ON d.authorized_by = u1.user_id
                LEFT JOIN users u2 ON d.executed_by = u2.user_id
                WHERE 1=1
            """
            params = []
            param_idx = 1

            if start_date:
                query += f" AND d.created_at >= ${param_idx}"
                params.append(start_date)
                param_idx += 1

            if end_date:
                query += f" AND d.created_at <= ${param_idx}"
                params.append(end_date)
                param_idx += 1

            if status:
                query += f" AND d.execution_status = ${param_idx}"
                params.append(status.value)
                param_idx += 1

            query += f" ORDER BY d.created_at DESC LIMIT ${param_idx}"
            params.append(limit)

            records = await conn.fetch(query, *params)

        return [dict(r) for r in records]

    async def cancel_disposal(
        self,
        disposal_id: UUID,
        cancelled_by: int,
        reason: str,
    ) -> bool:
        """Cancel a pending disposal request."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE disposal_records
                SET execution_status = 'cancelled',
                    error_message = $1
                WHERE disposal_id = $2
                AND execution_status IN ('pending', 'authorized')
                """,
                f"Cancelled by user {cancelled_by}: {reason}",
                disposal_id,
            )

        if "UPDATE 1" in result:
            if self._audit_logger:
                await self._audit_logger.log_event(
                    AuditEvent(
                        event_type=AuditEventType.DATA_DELETE,
                        action="disposal_cancelled",
                        success=True,
                        user_id=cancelled_by,
                        resource_type="disposal",
                        resource_id=str(disposal_id),
                        details={"cancellation_reason": reason},
                    )
                )
            logger.info("Disposal %s cancelled by user %d", disposal_id, cancelled_by)
            return True

        return False


class RetentionPolicy:
    """Manage data retention policies and find expired data."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def check_expired_data(self) -> list[ExpiredData]:
        """Find data past retention period."""
        async with self._pool.acquire() as conn:
            records = await conn.fetch(
                """
                SELECT load_batch_id, vcf_file_path, load_completed_at,
                       policy_name, expires_at, variant_count
                FROM v_expired_data
                WHERE is_expired = TRUE
                ORDER BY expires_at ASC
                """
            )

        return [
            ExpiredData(
                load_batch_id=r["load_batch_id"],
                vcf_file_path=r["vcf_file_path"],
                loaded_at=r["load_completed_at"],
                expires_at=r["expires_at"],
                policy_name=r["policy_name"],
                variant_count=r["variant_count"],
            )
            for r in records
        ]

    async def check_expiring_soon(
        self,
        days_ahead: int = 90,
    ) -> list[ExpiredData]:
        """Find data expiring within specified days."""
        async with self._pool.acquire() as conn:
            records = await conn.fetch(
                """
                SELECT load_batch_id, vcf_file_path, load_completed_at as loaded_at,
                       policy_name, expires_at, variant_count
                FROM v_upcoming_expirations
                WHERE expires_at <= NOW() + ($1 || ' days')::INTERVAL
                ORDER BY expires_at ASC
                """,
                str(days_ahead),
            )

        return [
            ExpiredData(
                load_batch_id=r["load_batch_id"],
                vcf_file_path=r["vcf_file_path"],
                loaded_at=r["loaded_at"],
                expires_at=r["expires_at"],
                policy_name=r["policy_name"],
                variant_count=r["variant_count"],
            )
            for r in records
        ]

    async def generate_expiration_report(
        self,
        expiring_days_ahead: int = 90,
    ) -> RetentionReport:
        """Report on data approaching expiration."""
        expired = await self.check_expired_data()
        expiring = await self.check_expiring_soon(expiring_days_ahead)

        return RetentionReport(
            generated_at=datetime.now(UTC),
            expired_batches=expired,
            expiring_soon=expiring,
            total_expired_variants=sum(e.variant_count for e in expired),
            total_expiring_variants=sum(e.variant_count for e in expiring),
        )

    async def get_policies(self) -> list[dict[str, Any]]:
        """Get all retention policies."""
        async with self._pool.acquire() as conn:
            records = await conn.fetch(
                """
                SELECT * FROM retention_policies
                WHERE is_active = TRUE
                ORDER BY policy_name
                """
            )
        return [dict(r) for r in records]

    async def create_policy(
        self,
        policy_name: str,
        retention_days: int,
        data_type: str,
        description: str | None = None,
        auto_dispose: bool = False,
        created_by: int | None = None,
    ) -> int:
        """Create a new retention policy."""
        async with self._pool.acquire() as conn:
            policy_id = await conn.fetchval(
                """
                INSERT INTO retention_policies (
                    policy_name, description, retention_days, data_type,
                    auto_dispose, created_by
                ) VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING policy_id
                """,
                policy_name,
                description,
                retention_days,
                data_type,
                auto_dispose,
                created_by,
            )
        return policy_id
