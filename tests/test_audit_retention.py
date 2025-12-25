"""Audit Log Retention Policy tests.

HIPAA Citation: 45 CFR 164.316(b)(2)(i) - REQUIRED
"Retain the documentation required by paragraph (b)(1) of this section for
6 years from the date of its creation or the date when it last was in
effect, whichever is later."

This applies to:
- Audit logs
- Security policies
- Risk assessments
- Incident records

This module tests:
- 6-year minimum retention enforcement
- Partition archival (detach but don't delete)
- Retention policy compliance verification
- Prevention of premature log deletion
"""

from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from vcf_pg_loader.audit.retention import (
    HIPAA_MINIMUM_RETENTION_YEARS,
    AuditRetentionManager,
    RetentionPolicy,
    RetentionStatus,
)


class TestRetentionPolicy:
    """45 CFR 164.316(b)(2)(i): Retention policy model."""

    def test_policy_from_db_row(self):
        now = datetime.now(UTC)
        row = {
            "policy_id": 1,
            "retention_years": 6,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": "HIPAA minimum retention",
        }

        policy = RetentionPolicy.from_db_row(row)

        assert policy.retention_years == 6
        assert policy.enforce_minimum is True
        assert policy.is_active is True

    def test_policy_is_compliant_true(self):
        """45 CFR 164.316(b)(2)(i): 6+ years with enforcement is compliant."""
        policy = RetentionPolicy(
            policy_id=1,
            retention_years=6,
            enforce_minimum=True,
            is_active=True,
            created_at=datetime.now(UTC),
        )

        assert policy.is_compliant() is True

    def test_policy_is_compliant_false_insufficient_years(self):
        """45 CFR 164.316(b)(2)(i): Less than 6 years is non-compliant."""
        policy = RetentionPolicy(
            policy_id=1,
            retention_years=5,
            enforce_minimum=True,
            is_active=True,
            created_at=datetime.now(UTC),
        )

        assert policy.is_compliant() is False

    def test_policy_is_compliant_false_no_enforcement(self):
        """45 CFR 164.316(b)(2)(i): Unenforced policy is non-compliant."""
        policy = RetentionPolicy(
            policy_id=1,
            retention_years=6,
            enforce_minimum=False,
            is_active=True,
            created_at=datetime.now(UTC),
        )

        assert policy.is_compliant() is False


class TestRetentionStatus:
    """45 CFR 164.316(b)(2)(i): Retention status tracking."""

    def test_status_to_dict(self):
        status = RetentionStatus(
            has_policy=True,
            is_compliant=True,
            retention_years=6,
            enforcement_enabled=True,
            oldest_partition_date=date(2019, 1, 1),
            partition_count=72,
            archived_partition_count=12,
        )

        d = status.to_dict()

        assert d["has_policy"] is True
        assert d["is_compliant"] is True
        assert d["retention_years"] == 6
        assert d["oldest_partition_date"] == "2019-01-01"

    def test_status_no_oldest_partition(self):
        status = RetentionStatus(
            has_policy=True,
            is_compliant=True,
            retention_years=6,
            enforcement_enabled=True,
            oldest_partition_date=None,
            partition_count=0,
            archived_partition_count=0,
        )

        d = status.to_dict()
        assert d["oldest_partition_date"] is None


class TestAuditRetentionManager:
    """45 CFR 164.316(b)(2)(i): Retention policy management."""

    @pytest.fixture
    def manager(self):
        return AuditRetentionManager()

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    def test_hipaa_minimum_constant(self):
        """45 CFR 164.316(b)(2)(i): Minimum retention is 6 years."""
        assert HIPAA_MINIMUM_RETENTION_YEARS == 6

    async def test_get_active_policy(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Retrieve active retention policy."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 7,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": "Extended retention",
        }

        policy = await manager.get_active_policy(mock_conn)

        assert policy is not None
        assert policy.retention_years == 7
        assert policy.is_compliant() is True

    async def test_get_active_policy_none(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Handle missing policy."""
        mock_conn.fetchrow.return_value = None

        policy = await manager.get_active_policy(mock_conn)

        assert policy is None

    async def test_set_retention_policy_success(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Set valid retention policy."""
        now = datetime.now(UTC)
        mock_conn.fetchval.return_value = 1
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 6,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }

        @asynccontextmanager
        async def mock_transaction():
            yield

        mock_conn.transaction = mock_transaction

        policy = await manager.set_retention_policy(
            mock_conn,
            retention_years=6,
            enforce_minimum=True,
            created_by=1,
        )

        assert policy.retention_years == 6
        assert policy.is_compliant() is True

    async def test_set_retention_policy_rejects_insufficient_years(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Reject < 6 year retention."""
        with pytest.raises(ValueError, match="at least 6 years"):
            await manager.set_retention_policy(
                mock_conn,
                retention_years=5,
                enforce_minimum=True,
            )

    async def test_get_retention_status(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Get comprehensive retention status."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.side_effect = [
            {
                "policy_id": 1,
                "retention_years": 6,
                "enforce_minimum": True,
                "is_active": True,
                "created_at": now,
                "created_by": 1,
                "notes": None,
            },
            {
                "partition_count": 72,
                "oldest_partition": "2019_01",
            },
        ]
        mock_conn.fetchval.return_value = 12

        status = await manager.get_retention_status(mock_conn)

        assert status.has_policy is True
        assert status.is_compliant is True
        assert status.retention_years == 6
        assert status.partition_count == 72
        assert status.archived_partition_count == 12

    async def test_check_deletion_allowed_within_window(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Block deletion within retention window."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 6,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }

        recent_date = date.today() - timedelta(days=365 * 3)
        is_allowed, reason = await manager.check_deletion_allowed(mock_conn, recent_date)

        assert is_allowed is False
        assert "retention window" in reason.lower()
        assert "164.316(b)(2)(i)" in reason

    async def test_check_deletion_allowed_outside_window(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Allow deletion outside retention window."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 6,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }

        old_date = date.today() - timedelta(days=365 * 7)
        is_allowed, reason = await manager.check_deletion_allowed(mock_conn, old_date)

        assert is_allowed is True
        assert "outside retention window" in reason.lower()

    async def test_check_deletion_allowed_no_policy(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Block deletion if no policy configured."""
        mock_conn.fetchrow.return_value = None

        is_allowed, reason = await manager.check_deletion_allowed(
            mock_conn, date.today() - timedelta(days=365 * 10)
        )

        assert is_allowed is False
        assert "no retention policy" in reason.lower()

    async def test_check_deletion_allowed_enforcement_disabled(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Allow if enforcement disabled (non-compliant)."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 6,
            "enforce_minimum": False,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }

        recent_date = date.today() - timedelta(days=365)
        is_allowed, reason = await manager.check_deletion_allowed(mock_conn, recent_date)

        assert is_allowed is True
        assert "disabled" in reason.lower()

    async def test_verify_retention_integrity_compliant(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Verify retention integrity."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 6,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }
        mock_conn.fetchval.side_effect = [
            date.today() - timedelta(days=365 * 7),
            True,
        ]

        is_valid, issues = await manager.verify_retention_integrity(mock_conn)

        assert is_valid is True
        assert len(issues) == 0

    async def test_verify_retention_integrity_no_policy(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Detect missing retention policy."""
        mock_conn.fetchrow.return_value = None

        is_valid, issues = await manager.verify_retention_integrity(mock_conn)

        assert is_valid is False
        assert any("no active retention policy" in issue.lower() for issue in issues)

    async def test_verify_retention_integrity_insufficient_years(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Detect insufficient retention period."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 5,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }
        mock_conn.fetchval.side_effect = [
            None,
            True,
        ]

        is_valid, issues = await manager.verify_retention_integrity(mock_conn)

        assert is_valid is False
        assert any("below hipaa minimum" in issue.lower() for issue in issues)

    async def test_verify_retention_integrity_enforcement_disabled(self, manager, mock_conn):
        """45 CFR 164.316(b)(2)(i): Detect disabled enforcement."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 6,
            "enforce_minimum": False,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }
        mock_conn.fetchval.side_effect = [
            date.today() - timedelta(days=365 * 7),
            True,
        ]

        is_valid, issues = await manager.verify_retention_integrity(mock_conn)

        assert is_valid is False
        assert any("enforcement is disabled" in issue.lower() for issue in issues)


class TestAuditRetentionWithAudit:
    """45 CFR 164.312(b): Audit controls for retention operations."""

    @pytest.fixture
    def audit_logger(self):
        return AsyncMock()

    @pytest.fixture
    def manager(self, audit_logger):
        return AuditRetentionManager(audit_logger=audit_logger)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    async def test_set_policy_logs_audit(self, manager, mock_conn, audit_logger):
        """45 CFR 164.312(b): Policy changes must be audited."""
        now = datetime.now(UTC)
        mock_conn.fetchval.return_value = 1
        mock_conn.fetchrow.return_value = {
            "policy_id": 1,
            "retention_years": 7,
            "enforce_minimum": True,
            "is_active": True,
            "created_at": now,
            "created_by": 1,
            "notes": None,
        }

        @asynccontextmanager
        async def mock_transaction():
            yield

        mock_conn.transaction = mock_transaction

        await manager.set_retention_policy(
            mock_conn,
            retention_years=7,
            enforce_minimum=True,
            created_by=1,
        )

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "retention_policy_updated"
        assert event.details["retention_years"] == 7
        assert "164.316(b)(2)(i)" in event.details["hipaa_citation"]
