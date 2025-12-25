"""Multi-Factor Authentication tests.

HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication - REQUIRED
"Implement procedures to verify that a person or entity seeking access to
electronic protected health information is the one claimed."

HHS Security Series Paper #4 defines authentication factors:
1. Something known (password, PIN, passphrase)
2. Something possessed (smart card, token, security key)
3. Something unique (biometric: fingerprint, facial recognition)

This module tests the "something possessed" factor via TOTP (RFC 6238)
using a mobile authenticator app as the token.
"""

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from vcf_pg_loader.auth.mfa import (
    MFAEnrollment,
    MFAManager,
    MFAStatus,
    TOTPGenerator,
)


class TestTOTPGenerator:
    """RFC 6238 TOTP implementation tests."""

    @pytest.fixture
    def totp(self):
        return TOTPGenerator()

    def test_generate_secret(self, totp):
        """Generate a base32-encoded secret."""
        secret = totp.generate_secret()

        assert len(secret) >= 32
        assert secret.isalnum()

    def test_generate_code(self, totp):
        """Generate a 6-digit TOTP code."""
        secret = totp.generate_secret()
        code = totp.generate(secret)

        assert len(code) == 6
        assert code.isdigit()

    def test_verify_code_valid(self, totp):
        """Verify a valid TOTP code."""
        secret = totp.generate_secret()
        code = totp.generate(secret)

        assert totp.verify(secret, code) is True

    def test_verify_code_invalid(self, totp):
        """Reject an invalid TOTP code."""
        secret = totp.generate_secret()

        assert totp.verify(secret, "000000") is False

    def test_verify_code_wrong_length(self, totp):
        """Reject codes with wrong length."""
        secret = totp.generate_secret()

        assert totp.verify(secret, "12345") is False
        assert totp.verify(secret, "1234567") is False

    def test_verify_with_time_window(self, totp):
        """45 CFR 164.312(d): Allow for clock skew in verification."""
        secret = totp.generate_secret()
        past_timestamp = time.time() - 30

        code = totp.generate(secret, timestamp=past_timestamp)

        assert totp.verify(secret, code, window=1) is True

    def test_generate_is_deterministic(self, totp):
        """Same timestamp produces same code."""
        secret = totp.generate_secret()
        timestamp = 1700000000.0

        code1 = totp.generate(secret, timestamp=timestamp)
        code2 = totp.generate(secret, timestamp=timestamp)

        assert code1 == code2

    def test_get_provisioning_uri(self, totp):
        """Generate otpauth:// URI for authenticator apps."""
        secret = "JBSWY3DPEHPK3PXP"
        uri = totp.get_provisioning_uri(
            secret=secret,
            username="testuser",
            issuer="vcf-pg-loader",
        )

        assert uri.startswith("otpauth://totp/")
        assert "secret=JBSWY3DPEHPK3PXP" in uri
        assert "issuer=" in uri


class TestMFAStatus:
    """45 CFR 164.312(d): MFA status tracking."""

    def test_status_from_db_row(self):
        now = datetime.now(UTC)
        row = {
            "user_id": 1,
            "mfa_enabled": True,
            "recovery_codes_remaining": 8,
            "mfa_enrolled_at": now,
        }

        status = MFAStatus.from_db_row(row)

        assert status.user_id == 1
        assert status.mfa_enabled is True
        assert status.recovery_codes_remaining == 8

    def test_status_not_enrolled(self):
        row = {
            "user_id": 1,
            "mfa_enabled": False,
            "recovery_codes_remaining": 0,
        }

        status = MFAStatus.from_db_row(row)

        assert status.mfa_enabled is False
        assert status.enrolled_at is None


class TestMFAManager:
    """45 CFR 164.312(d): MFA enrollment and verification."""

    @pytest.fixture
    def manager(self):
        return MFAManager()

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    async def test_enroll_user(self, manager, mock_conn):
        """45 CFR 164.312(d): Begin MFA enrollment."""
        mock_conn.fetchrow.return_value = {
            "user_id": 1,
            "username": "testuser",
            "mfa_enabled": False,
        }

        enrollment = await manager.enroll(mock_conn, user_id=1)

        assert isinstance(enrollment, MFAEnrollment)
        assert enrollment.user_id == 1
        assert len(enrollment.secret) >= 32
        assert enrollment.provisioning_uri.startswith("otpauth://totp/")
        assert len(enrollment.recovery_codes) == 10

    async def test_enroll_fails_if_already_enabled(self, manager, mock_conn):
        """45 CFR 164.312(d): Cannot re-enroll if MFA already enabled."""
        mock_conn.fetchrow.return_value = {
            "user_id": 1,
            "username": "testuser",
            "mfa_enabled": True,
        }

        with pytest.raises(ValueError, match="already enabled"):
            await manager.enroll(mock_conn, user_id=1)

    async def test_enroll_fails_if_user_not_found(self, manager, mock_conn):
        """45 CFR 164.312(d): User must exist to enroll."""
        mock_conn.fetchrow.return_value = None

        with pytest.raises(ValueError, match="not found"):
            await manager.enroll(mock_conn, user_id=999)

    async def test_confirm_enrollment_success(self, manager, mock_conn):
        """45 CFR 164.312(d): Confirm enrollment with valid TOTP code."""
        totp = TOTPGenerator()
        secret = totp.generate_secret()
        code = totp.generate(secret)

        mock_conn.fetchrow.return_value = {
            "mfa_secret": secret,
            "mfa_pending": True,
        }

        success = await manager.confirm_enrollment(mock_conn, user_id=1, code=code)

        assert success is True
        mock_conn.execute.assert_called()

    async def test_confirm_enrollment_invalid_code(self, manager, mock_conn):
        """45 CFR 164.312(d): Reject enrollment with invalid code."""
        secret = TOTPGenerator().generate_secret()

        mock_conn.fetchrow.return_value = {
            "mfa_secret": secret,
            "mfa_pending": True,
        }

        success = await manager.confirm_enrollment(mock_conn, user_id=1, code="000000")

        assert success is False

    async def test_verify_code_success(self, manager, mock_conn):
        """45 CFR 164.312(d): Verify user with valid TOTP code."""
        totp = TOTPGenerator()
        secret = totp.generate_secret()
        code = totp.generate(secret)

        mock_conn.fetchrow.return_value = {
            "mfa_secret": secret,
            "mfa_enabled": True,
        }

        is_valid = await manager.verify_code(mock_conn, user_id=1, code=code)

        assert is_valid is True

    async def test_verify_code_fails_if_mfa_not_enabled(self, manager, mock_conn):
        """45 CFR 164.312(d): Cannot verify if MFA not enabled."""
        mock_conn.fetchrow.return_value = {
            "mfa_secret": "some_secret",
            "mfa_enabled": False,
        }

        is_valid = await manager.verify_code(mock_conn, user_id=1, code="123456")

        assert is_valid is False

    async def test_verify_recovery_code_success(self, manager, mock_conn):
        """45 CFR 164.312(d): Verify with recovery code (backup factor)."""
        mock_conn.execute.return_value = "UPDATE 1"

        is_valid = await manager.verify_recovery_code(
            mock_conn, user_id=1, recovery_code="ABCD-EFGH"
        )

        assert is_valid is True

    async def test_verify_recovery_code_consumed(self, manager, mock_conn):
        """45 CFR 164.312(d): Recovery codes are single-use."""
        mock_conn.execute.return_value = "UPDATE 0"

        is_valid = await manager.verify_recovery_code(
            mock_conn, user_id=1, recovery_code="USED-CODE"
        )

        assert is_valid is False

    async def test_disable_mfa(self, manager, mock_conn):
        """45 CFR 164.312(d): Admin can disable MFA (audited)."""
        mock_conn.execute.return_value = "UPDATE 1"

        success = await manager.disable(
            mock_conn,
            user_id=1,
            disabled_by=999,
            reason="User lost device",
        )

        assert success is True

    async def test_get_status(self, manager, mock_conn):
        """45 CFR 164.312(d): Get MFA status for user."""
        now = datetime.now(UTC)
        mock_conn.fetchrow.return_value = {
            "user_id": 1,
            "mfa_enabled": True,
            "mfa_enrolled_at": now,
            "recovery_codes_remaining": 8,
        }

        status = await manager.get_status(mock_conn, user_id=1)

        assert status is not None
        assert status.mfa_enabled is True
        assert status.recovery_codes_remaining == 8

    async def test_regenerate_recovery_codes(self, manager, mock_conn):
        """45 CFR 164.312(d): Regenerate recovery codes with TOTP verification."""
        totp = TOTPGenerator()
        secret = totp.generate_secret()
        code = totp.generate(secret)

        mock_conn.fetchrow.return_value = {
            "mfa_secret": secret,
            "mfa_enabled": True,
        }

        codes = await manager.regenerate_recovery_codes(mock_conn, user_id=1, code=code)

        assert codes is not None
        assert len(codes) == 10


class TestMFAWithAudit:
    """45 CFR 164.312(b): Audit controls for MFA events."""

    @pytest.fixture
    def audit_logger(self):
        return AsyncMock()

    @pytest.fixture
    def manager(self, audit_logger):
        return MFAManager(audit_logger=audit_logger)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    async def test_enrollment_logs_audit(self, manager, mock_conn, audit_logger):
        """45 CFR 164.312(b): MFA enrollment must be audited."""
        mock_conn.fetchrow.return_value = {
            "user_id": 1,
            "username": "testuser",
            "mfa_enabled": False,
        }

        await manager.enroll(mock_conn, user_id=1)

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "mfa_enrollment_started"

    async def test_verification_logs_audit(self, manager, mock_conn, audit_logger):
        """45 CFR 164.312(b): MFA verification must be audited."""
        totp = TOTPGenerator()
        secret = totp.generate_secret()
        code = totp.generate(secret)

        mock_conn.fetchrow.return_value = {
            "mfa_secret": secret,
            "mfa_enabled": True,
        }

        await manager.verify_code(mock_conn, user_id=1, code=code)

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "mfa_verification"
        assert event.success is True
