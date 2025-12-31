"""MFA Integration Tests with real PostgreSQL database.

HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication - REQUIRED

These tests verify the full MFA lifecycle against a real database:
- User creation with MFA enrollment
- TOTP code generation and verification
- Recovery code usage
- MFA disable with audit trail
"""

import pytest

from vcf_pg_loader.auth.mfa import MFAManager, TOTPGenerator

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def mfa_db(postgres_container):
    """Create database with MFA tables."""
    import asyncpg

    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://")

    conn = await asyncpg.connect(url)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id SERIAL PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255),
            mfa_enabled BOOLEAN DEFAULT FALSE,
            mfa_secret VARCHAR(255),
            mfa_pending BOOLEAN DEFAULT FALSE,
            mfa_enrolled_at TIMESTAMPTZ
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS mfa_recovery_codes (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(user_id),
            code_hash VARCHAR(64) NOT NULL,
            is_used BOOLEAN DEFAULT FALSE,
            used_at TIMESTAMPTZ
        )
    """)

    yield conn

    await conn.execute("DROP TABLE IF EXISTS mfa_recovery_codes CASCADE")
    await conn.execute("DROP TABLE IF EXISTS users CASCADE")
    await conn.close()


@pytest.fixture
async def test_user(mfa_db):
    """Create a test user for MFA tests."""
    user_id = await mfa_db.fetchval(
        "INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING user_id",
        "testuser",
        "hashed_password",
    )
    return user_id


class TestMFAEnrollmentIntegration:
    """Test MFA enrollment flow against real database."""

    async def test_full_enrollment_flow(self, mfa_db, test_user):
        """45 CFR 164.312(d): Complete enrollment with TOTP verification."""
        manager = MFAManager()

        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        assert enrollment.user_id == test_user
        assert len(enrollment.secret) >= 32
        assert enrollment.provisioning_uri.startswith("otpauth://totp/")
        assert len(enrollment.recovery_codes) == 10

        user = await mfa_db.fetchrow(
            "SELECT mfa_pending, mfa_enabled FROM users WHERE user_id = $1", test_user
        )
        assert user["mfa_pending"] is True
        assert user["mfa_enabled"] is False

        code = TOTPGenerator().generate(enrollment.secret)
        confirmed = await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        assert confirmed is True

        user = await mfa_db.fetchrow(
            "SELECT mfa_pending, mfa_enabled, mfa_enrolled_at FROM users WHERE user_id = $1",
            test_user,
        )
        assert user["mfa_pending"] is False
        assert user["mfa_enabled"] is True
        assert user["mfa_enrolled_at"] is not None

    async def test_enrollment_creates_recovery_codes_in_db(self, mfa_db, test_user):
        """45 CFR 164.312(d): Recovery codes stored securely."""
        manager = MFAManager()

        await manager.enroll(mfa_db, user_id=test_user)

        count = await mfa_db.fetchval(
            "SELECT COUNT(*) FROM mfa_recovery_codes WHERE user_id = $1 AND is_used = false",
            test_user,
        )
        assert count == 10

        codes = await mfa_db.fetch(
            "SELECT code_hash FROM mfa_recovery_codes WHERE user_id = $1", test_user
        )
        for code_row in codes:
            assert len(code_row["code_hash"]) == 64

    async def test_enrollment_fails_if_already_enabled(self, mfa_db, test_user):
        """45 CFR 164.312(d): Cannot re-enroll if MFA already enabled."""
        await mfa_db.execute("UPDATE users SET mfa_enabled = true WHERE user_id = $1", test_user)

        manager = MFAManager()

        with pytest.raises(ValueError, match="already enabled"):
            await manager.enroll(mfa_db, user_id=test_user)


class TestMFAVerificationIntegration:
    """Test MFA verification against real database."""

    @pytest.fixture
    async def enrolled_user(self, mfa_db, test_user):
        """Create a user with MFA fully enrolled."""
        manager = MFAManager()
        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        code = TOTPGenerator().generate(enrollment.secret)
        await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        return {"user_id": test_user, "secret": enrollment.secret}

    async def test_verify_valid_totp_code(self, mfa_db, enrolled_user):
        """45 CFR 164.312(d): Valid TOTP code grants access."""
        manager = MFAManager()
        totp = TOTPGenerator()

        code = totp.generate(enrolled_user["secret"])
        is_valid = await manager.verify_code(mfa_db, user_id=enrolled_user["user_id"], code=code)

        assert is_valid is True

    async def test_verify_invalid_totp_code_rejected(self, mfa_db, enrolled_user):
        """45 CFR 164.312(d): Invalid code denies access."""
        manager = MFAManager()

        is_valid = await manager.verify_code(
            mfa_db, user_id=enrolled_user["user_id"], code="000000"
        )

        assert is_valid is False

    async def test_verify_recovery_code_success(self, mfa_db, test_user):
        """45 CFR 164.312(d): Recovery code grants access."""
        manager = MFAManager()
        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        code = TOTPGenerator().generate(enrollment.secret)
        await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        recovery_code = enrollment.recovery_codes[0]
        is_valid = await manager.verify_recovery_code(
            mfa_db, user_id=test_user, recovery_code=recovery_code
        )

        assert is_valid is True

        is_valid_again = await manager.verify_recovery_code(
            mfa_db, user_id=test_user, recovery_code=recovery_code
        )
        assert is_valid_again is False

    async def test_recovery_code_marked_used(self, mfa_db, test_user):
        """45 CFR 164.312(d): Used recovery codes are marked."""
        manager = MFAManager()
        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        code = TOTPGenerator().generate(enrollment.secret)
        await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        recovery_code = enrollment.recovery_codes[0]
        await manager.verify_recovery_code(mfa_db, user_id=test_user, recovery_code=recovery_code)

        row = await mfa_db.fetchrow(
            """
            SELECT is_used, used_at FROM mfa_recovery_codes
            WHERE user_id = $1 AND is_used = true
            """,
            test_user,
        )
        assert row is not None
        assert row["is_used"] is True
        assert row["used_at"] is not None


class TestMFADisableIntegration:
    """Test MFA disable flow against real database."""

    async def test_disable_mfa_clears_secret(self, mfa_db, test_user):
        """45 CFR 164.312(d): Disabling MFA removes credentials."""
        manager = MFAManager()
        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        code = TOTPGenerator().generate(enrollment.secret)
        await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        admin_user_id = 999
        success = await manager.disable(
            mfa_db, user_id=test_user, disabled_by=admin_user_id, reason="User request"
        )

        assert success is True

        user = await mfa_db.fetchrow(
            "SELECT mfa_enabled, mfa_secret FROM users WHERE user_id = $1", test_user
        )
        assert user["mfa_enabled"] is False
        assert user["mfa_secret"] is None

    async def test_disable_mfa_removes_recovery_codes(self, mfa_db, test_user):
        """45 CFR 164.312(d): Disabling MFA removes recovery codes."""
        manager = MFAManager()
        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        code = TOTPGenerator().generate(enrollment.secret)
        await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        await manager.disable(mfa_db, user_id=test_user, disabled_by=999, reason="Test")

        count = await mfa_db.fetchval(
            "SELECT COUNT(*) FROM mfa_recovery_codes WHERE user_id = $1", test_user
        )
        assert count == 0


class TestMFAStatusIntegration:
    """Test MFA status queries against real database."""

    async def test_get_status_not_enrolled(self, mfa_db, test_user):
        """Status shows MFA not enabled for new user."""
        manager = MFAManager()

        status = await manager.get_status(mfa_db, user_id=test_user)

        assert status is not None
        assert status.mfa_enabled is False
        assert status.recovery_codes_remaining == 0

    async def test_get_status_enrolled(self, mfa_db, test_user):
        """Status shows MFA enabled after enrollment."""
        manager = MFAManager()
        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        code = TOTPGenerator().generate(enrollment.secret)
        await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        status = await manager.get_status(mfa_db, user_id=test_user)

        assert status is not None
        assert status.mfa_enabled is True
        assert status.recovery_codes_remaining == 10
        assert status.enrolled_at is not None

    async def test_status_tracks_used_recovery_codes(self, mfa_db, test_user):
        """Status correctly counts remaining recovery codes."""
        manager = MFAManager()
        enrollment = await manager.enroll(mfa_db, user_id=test_user)

        code = TOTPGenerator().generate(enrollment.secret)
        await manager.confirm_enrollment(mfa_db, user_id=test_user, code=code)

        await manager.verify_recovery_code(
            mfa_db, user_id=test_user, recovery_code=enrollment.recovery_codes[0]
        )
        await manager.verify_recovery_code(
            mfa_db, user_id=test_user, recovery_code=enrollment.recovery_codes[1]
        )

        status = await manager.get_status(mfa_db, user_id=test_user)

        assert status.recovery_codes_remaining == 8
