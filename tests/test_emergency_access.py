"""Emergency Access Procedure tests.

HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
"Establish (and implement as needed) procedures for obtaining necessary
electronic protected health information during an emergency."

This module tests:
- Emergency token generation with mandatory justification
- Automatic expiration (time-limited access, max 24 hours)
- Enhanced audit logging for all emergency access
- Token revocation and post-incident review workflow
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from vcf_pg_loader.auth.emergency_access import (
    EmergencyAccessManager,
    EmergencyToken,
    EmergencyType,
)


class TestEmergencyType:
    """45 CFR 164.312(a)(2)(ii): Emergency types must be categorized."""

    def test_all_emergency_types_defined(self):
        expected = {
            "patient_emergency",
            "system_emergency",
            "disaster_recovery",
            "legal_requirement",
            "other",
        }
        actual = {e.value for e in EmergencyType}
        assert actual == expected


class TestEmergencyToken:
    """45 CFR 164.312(a)(2)(ii): Token model for emergency access."""

    def test_token_from_db_row(self):
        now = datetime.now(UTC)
        row = {
            "token_id": uuid4(),
            "user_id": 1,
            "justification": "Patient requires immediate treatment",
            "emergency_type": "patient_emergency",
            "granted_at": now,
            "expires_at": now + timedelta(hours=1),
            "revoked_at": None,
            "granted_by": 2,
            "access_scope": {"all_phi": True},
            "requires_review": True,
            "reviewed_at": None,
        }

        token = EmergencyToken.from_db_row(row)

        assert token.user_id == 1
        assert token.emergency_type == EmergencyType.PATIENT_EMERGENCY
        assert token.requires_review is True

    def test_token_is_active_true(self):
        now = datetime.now(UTC)
        token = EmergencyToken(
            token_id=uuid4(),
            user_id=1,
            justification="Test emergency",
            emergency_type=EmergencyType.PATIENT_EMERGENCY,
            granted_at=now,
            expires_at=now + timedelta(hours=1),
        )

        assert token.is_active() is True

    def test_token_is_active_false_when_expired(self):
        now = datetime.now(UTC)
        token = EmergencyToken(
            token_id=uuid4(),
            user_id=1,
            justification="Test emergency",
            emergency_type=EmergencyType.PATIENT_EMERGENCY,
            granted_at=now - timedelta(hours=2),
            expires_at=now - timedelta(hours=1),
        )

        assert token.is_active() is False

    def test_token_is_active_false_when_revoked(self):
        now = datetime.now(UTC)
        token = EmergencyToken(
            token_id=uuid4(),
            user_id=1,
            justification="Test emergency",
            emergency_type=EmergencyType.PATIENT_EMERGENCY,
            granted_at=now,
            expires_at=now + timedelta(hours=1),
            revoked_at=now,
        )

        assert token.is_active() is False

    def test_minutes_remaining(self):
        now = datetime.now(UTC)
        token = EmergencyToken(
            token_id=uuid4(),
            user_id=1,
            justification="Test emergency",
            emergency_type=EmergencyType.PATIENT_EMERGENCY,
            granted_at=now,
            expires_at=now + timedelta(minutes=30),
        )

        remaining = token.minutes_remaining()
        assert 29 <= remaining <= 30


class TestEmergencyAccessManager:
    """45 CFR 164.312(a)(2)(ii): Emergency access procedure implementation."""

    @pytest.fixture
    def manager(self):
        return EmergencyAccessManager()

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    async def test_grant_access_requires_justification(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Justification required for audit trail."""
        with pytest.raises(ValueError, match="at least 20 characters"):
            await manager.grant_access(
                mock_conn,
                user_id=1,
                justification="Too short",
                emergency_type=EmergencyType.PATIENT_EMERGENCY,
            )

    async def test_grant_access_max_duration_24_hours(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Emergency access must be time-limited."""
        with pytest.raises(ValueError, match="cannot exceed 24 hours"):
            await manager.grant_access(
                mock_conn,
                user_id=1,
                justification="Patient requires immediate access to records for treatment",
                emergency_type=EmergencyType.PATIENT_EMERGENCY,
                duration_minutes=1500,
            )

    async def test_grant_access_min_duration_1_minute(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Duration must be positive."""
        with pytest.raises(ValueError, match="at least 1 minute"):
            await manager.grant_access(
                mock_conn,
                user_id=1,
                justification="Patient requires immediate access to records",
                emergency_type=EmergencyType.PATIENT_EMERGENCY,
                duration_minutes=0,
            )

    async def test_grant_access_success(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Successful emergency access grant."""
        token_id = uuid4()
        now = datetime.now(UTC)

        mock_conn.fetchval.return_value = token_id
        mock_conn.fetchrow.return_value = {
            "token_id": token_id,
            "user_id": 1,
            "justification": "Patient requires immediate access to records for treatment",
            "emergency_type": "patient_emergency",
            "granted_at": now,
            "expires_at": now + timedelta(minutes=60),
            "revoked_at": None,
            "granted_by": 2,
            "access_scope": {"all_phi": False, "resources": []},
            "requires_review": True,
            "reviewed_at": None,
        }

        token = await manager.grant_access(
            mock_conn,
            user_id=1,
            justification="Patient requires immediate access to records for treatment",
            emergency_type=EmergencyType.PATIENT_EMERGENCY,
            duration_minutes=60,
            granted_by=2,
        )

        assert token.token_id == token_id
        assert token.user_id == 1
        assert token.emergency_type == EmergencyType.PATIENT_EMERGENCY
        assert token.requires_review is True

    async def test_validate_token_success(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Validate emergency access token."""
        token_id = uuid4()
        now = datetime.now(UTC)

        mock_conn.fetchrow.side_effect = [
            {
                "is_valid": True,
                "user_id": 1,
                "access_scope": {},
                "expires_at": now + timedelta(hours=1),
                "message": "Access granted",
            },
            {
                "token_id": token_id,
                "user_id": 1,
                "justification": "Test emergency access",
                "emergency_type": "patient_emergency",
                "granted_at": now,
                "expires_at": now + timedelta(hours=1),
                "revoked_at": None,
                "requires_review": True,
                "reviewed_at": None,
            },
        ]

        is_valid, token, message = await manager.validate_token(mock_conn, token_id)

        assert is_valid is True
        assert token is not None
        assert token.user_id == 1

    async def test_validate_token_expired(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Expired tokens must be rejected."""
        token_id = uuid4()

        mock_conn.fetchrow.return_value = {
            "is_valid": False,
            "user_id": 1,
            "access_scope": None,
            "expires_at": datetime.now(UTC) - timedelta(hours=1),
            "message": "Token has expired",
        }

        is_valid, token, message = await manager.validate_token(mock_conn, token_id)

        assert is_valid is False
        assert "expired" in message.lower()

    async def test_revoke_access(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Emergency access must be revocable."""
        token_id = uuid4()
        mock_conn.fetchval.return_value = True

        success = await manager.revoke_access(
            mock_conn,
            token_id=token_id,
            revoked_by=1,
            reason="Emergency resolved",
        )

        assert success is True

    async def test_complete_review(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Post-incident review of emergency access."""
        token_id = uuid4()
        mock_conn.fetchval.return_value = True

        success = await manager.complete_review(
            mock_conn,
            token_id=token_id,
            reviewed_by=3,
            review_notes="Access was appropriate. Patient received needed care.",
        )

        assert success is True

    async def test_get_pending_reviews(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(ii): Track pending reviews."""
        mock_conn.fetch.return_value = [
            {
                "token_id": uuid4(),
                "user_id": 1,
                "username": "doctor1",
                "justification": "Emergency treatment",
                "emergency_type": "patient_emergency",
                "granted_at": datetime.now(UTC) - timedelta(hours=2),
                "expires_at": datetime.now(UTC) - timedelta(hours=1),
                "ended_at": datetime.now(UTC) - timedelta(hours=1),
                "granted_by_username": "admin",
                "access_count": 5,
            }
        ]

        reviews = await manager.get_pending_reviews(mock_conn)

        assert len(reviews) == 1
        assert reviews[0]["username"] == "doctor1"
        assert reviews[0]["access_count"] == 5


class TestEmergencyAccessWithAudit:
    """45 CFR 164.312(b): Audit controls for emergency access."""

    @pytest.fixture
    def audit_logger(self):
        return AsyncMock()

    @pytest.fixture
    def manager(self, audit_logger):
        return EmergencyAccessManager(audit_logger=audit_logger)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    async def test_grant_access_logs_audit_event(self, manager, mock_conn, audit_logger):
        """45 CFR 164.312(b): Emergency access must be audited."""
        token_id = uuid4()
        now = datetime.now(UTC)

        mock_conn.fetchval.return_value = token_id
        mock_conn.fetchrow.return_value = {
            "token_id": token_id,
            "user_id": 1,
            "justification": "Patient requires immediate access to records",
            "emergency_type": "patient_emergency",
            "granted_at": now,
            "expires_at": now + timedelta(hours=1),
            "revoked_at": None,
            "granted_by": 2,
            "access_scope": {},
            "requires_review": True,
            "reviewed_at": None,
        }

        await manager.grant_access(
            mock_conn,
            user_id=1,
            justification="Patient requires immediate access to records",
            emergency_type=EmergencyType.PATIENT_EMERGENCY,
            granted_by=2,
        )

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "emergency_access_granted"
        assert event.details["target_user_id"] == 1

    async def test_revoke_access_logs_audit_event(self, manager, mock_conn, audit_logger):
        """45 CFR 164.312(b): Revocation must be audited."""
        token_id = uuid4()
        mock_conn.fetchval.return_value = True

        await manager.revoke_access(
            mock_conn,
            token_id=token_id,
            revoked_by=1,
            reason="Emergency resolved",
        )

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "emergency_access_revoked"
