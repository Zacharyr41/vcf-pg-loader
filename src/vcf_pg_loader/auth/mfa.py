"""HIPAA-compliant Multi-Factor Authentication (MFA) using TOTP.

HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication - REQUIRED
"Implement procedures to verify that a person or entity seeking access to
electronic protected health information is the one claimed."

HHS Security Series Paper #4 defines authentication factors:
1. Something known (password, PIN, passphrase)
2. Something possessed (smart card, token, security key)
3. Something unique (biometric: fingerprint, facial recognition)

This module implements TOTP (RFC 6238) for the "something possessed" factor,
using a mobile authenticator app as the token.
"""

import base64
import hashlib
import hmac
import logging
import secrets
import struct
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

import asyncpg

from ..audit.logger import AuditLogger
from ..audit.models import AuditEvent, AuditEventType

logger = logging.getLogger(__name__)

TOTP_DIGITS = 6
TOTP_PERIOD = 30
TOTP_ALGORITHM = "SHA1"
RECOVERY_CODE_COUNT = 10
RECOVERY_CODE_LENGTH = 8


@dataclass
class MFAEnrollment:
    user_id: int
    secret: str
    provisioning_uri: str
    recovery_codes: list[str]


@dataclass
class MFAStatus:
    user_id: int
    mfa_enabled: bool
    recovery_codes_remaining: int
    enrolled_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict) -> "MFAStatus":
        return cls(
            user_id=row["user_id"],
            mfa_enabled=row.get("mfa_enabled", False),
            recovery_codes_remaining=row.get("recovery_codes_remaining", 0),
            enrolled_at=row.get("mfa_enrolled_at"),
        )


class TOTPGenerator:
    """TOTP implementation per RFC 6238."""

    def __init__(
        self,
        digits: int = TOTP_DIGITS,
        period: int = TOTP_PERIOD,
        algorithm: str = TOTP_ALGORITHM,
    ):
        self.digits = digits
        self.period = period
        self.algorithm = algorithm

    def generate_secret(self, length: int = 32) -> str:
        """Generate a random base32-encoded secret."""
        random_bytes = secrets.token_bytes(length)
        return base64.b32encode(random_bytes).decode("ascii").rstrip("=")

    def _get_counter(self, timestamp: float | None = None) -> int:
        """Get TOTP counter for given timestamp."""
        if timestamp is None:
            timestamp = time.time()
        return int(timestamp // self.period)

    def _hotp(self, secret: str, counter: int) -> str:
        """Generate HOTP value per RFC 4226."""
        key = base64.b32decode(secret.upper() + "=" * ((8 - len(secret) % 8) % 8))

        counter_bytes = struct.pack(">Q", counter)

        if self.algorithm == "SHA1":
            hmac_hash = hmac.new(key, counter_bytes, hashlib.sha1).digest()
        elif self.algorithm == "SHA256":
            hmac_hash = hmac.new(key, counter_bytes, hashlib.sha256).digest()
        elif self.algorithm == "SHA512":
            hmac_hash = hmac.new(key, counter_bytes, hashlib.sha512).digest()
        else:
            raise ValueError(f"Unsupported algorithm: {self.algorithm}")

        offset = hmac_hash[-1] & 0x0F
        truncated = struct.unpack(">I", hmac_hash[offset : offset + 4])[0] & 0x7FFFFFFF

        code = truncated % (10**self.digits)
        return str(code).zfill(self.digits)

    def generate(self, secret: str, timestamp: float | None = None) -> str:
        """Generate TOTP code for current time window."""
        counter = self._get_counter(timestamp)
        return self._hotp(secret, counter)

    def verify(
        self,
        secret: str,
        code: str,
        timestamp: float | None = None,
        window: int = 1,
    ) -> bool:
        """Verify TOTP code with time window tolerance.

        Args:
            secret: Base32-encoded shared secret
            code: Code to verify
            timestamp: Optional timestamp (defaults to now)
            window: Number of periods to check before/after current

        Returns:
            True if code is valid within the window
        """
        if len(code) != self.digits:
            return False

        if timestamp is None:
            timestamp = time.time()

        counter = self._get_counter(timestamp)

        for offset in range(-window, window + 1):
            expected = self._hotp(secret, counter + offset)
            if hmac.compare_digest(code, expected):
                return True

        return False

    def get_provisioning_uri(
        self,
        secret: str,
        username: str,
        issuer: str = "vcf-pg-loader",
    ) -> str:
        """Generate otpauth:// URI for authenticator apps."""
        label = quote(f"{issuer}:{username}")
        params = {
            "secret": secret,
            "issuer": quote(issuer),
            "algorithm": self.algorithm,
            "digits": str(self.digits),
            "period": str(self.period),
        }
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"otpauth://totp/{label}?{query}"


class MFAManager:
    """Manages Multi-Factor Authentication for HIPAA compliance.

    45 CFR 164.312(d) requires verification of identity. MFA provides
    strong authentication by requiring multiple factors:
    - Something known (password) - handled by Authenticator
    - Something possessed (TOTP token) - handled by this class
    """

    def __init__(
        self,
        issuer: str = "vcf-pg-loader",
        audit_logger: AuditLogger | None = None,
    ):
        self._totp = TOTPGenerator()
        self._issuer = issuer
        self._audit_logger = audit_logger

    def _generate_recovery_codes(self) -> list[str]:
        """Generate recovery codes for account recovery."""
        codes = []
        for _ in range(RECOVERY_CODE_COUNT):
            code = secrets.token_hex(RECOVERY_CODE_LENGTH // 2).upper()
            formatted = f"{code[:4]}-{code[4:]}"
            codes.append(formatted)
        return codes

    def _hash_recovery_code(self, code: str) -> str:
        """Hash a recovery code for storage."""
        normalized = code.replace("-", "").upper()
        return hashlib.sha256(normalized.encode()).hexdigest()

    async def enroll(
        self,
        conn: asyncpg.Connection,
        user_id: int,
    ) -> MFAEnrollment:
        """Begin MFA enrollment for a user.

        45 CFR 164.312(d): Implement procedures to verify identity.

        Args:
            conn: Database connection
            user_id: User to enroll

        Returns:
            MFAEnrollment with secret, provisioning URI, and recovery codes

        Raises:
            ValueError: If user not found or MFA already enabled
        """
        user = await conn.fetchrow(
            "SELECT user_id, username, mfa_enabled FROM users WHERE user_id = $1",
            user_id,
        )

        if not user:
            raise ValueError(f"User {user_id} not found")

        if user["mfa_enabled"]:
            raise ValueError("MFA is already enabled for this user")

        secret = self._totp.generate_secret()
        provisioning_uri = self._totp.get_provisioning_uri(
            secret=secret,
            username=user["username"],
            issuer=self._issuer,
        )
        recovery_codes = self._generate_recovery_codes()

        await conn.execute(
            """
            UPDATE users
            SET mfa_secret = $2,
                mfa_pending = true
            WHERE user_id = $1
            """,
            user_id,
            secret,
        )

        hashed_codes = [self._hash_recovery_code(c) for c in recovery_codes]
        await conn.execute(
            "DELETE FROM mfa_recovery_codes WHERE user_id = $1",
            user_id,
        )
        for hashed in hashed_codes:
            await conn.execute(
                """
                INSERT INTO mfa_recovery_codes (user_id, code_hash, is_used)
                VALUES ($1, $2, false)
                """,
                user_id,
                hashed,
            )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.CONFIG_CHANGE,
                    action="mfa_enrollment_started",
                    user_id=user_id,
                    success=True,
                    details={"recovery_codes_generated": RECOVERY_CODE_COUNT},
                )
            )

        return MFAEnrollment(
            user_id=user_id,
            secret=secret,
            provisioning_uri=provisioning_uri,
            recovery_codes=recovery_codes,
        )

    async def confirm_enrollment(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        code: str,
    ) -> bool:
        """Confirm MFA enrollment by verifying a TOTP code.

        Args:
            conn: Database connection
            user_id: User confirming enrollment
            code: TOTP code from authenticator app

        Returns:
            True if enrollment confirmed, False if code invalid
        """
        row = await conn.fetchrow(
            "SELECT mfa_secret, mfa_pending FROM users WHERE user_id = $1",
            user_id,
        )

        if not row or not row["mfa_pending"]:
            return False

        if not self._totp.verify(row["mfa_secret"], code):
            if self._audit_logger:
                await self._audit_logger.log_event(
                    AuditEvent(
                        event_type=AuditEventType.AUTH_FAILED,
                        action="mfa_enrollment_verification_failed",
                        user_id=user_id,
                        success=False,
                    )
                )
            return False

        await conn.execute(
            """
            UPDATE users
            SET mfa_enabled = true,
                mfa_pending = false,
                mfa_enrolled_at = NOW()
            WHERE user_id = $1
            """,
            user_id,
        )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.CONFIG_CHANGE,
                    action="mfa_enrollment_confirmed",
                    user_id=user_id,
                    success=True,
                )
            )

        logger.info("MFA enrollment confirmed for user_id=%d", user_id)
        return True

    async def verify_code(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        code: str,
    ) -> bool:
        """Verify a TOTP code for authentication.

        45 CFR 164.312(d): Verify person seeking access is the one claimed.

        Args:
            conn: Database connection
            user_id: User authenticating
            code: TOTP code from authenticator app

        Returns:
            True if code valid, False otherwise
        """
        row = await conn.fetchrow(
            "SELECT mfa_secret, mfa_enabled FROM users WHERE user_id = $1",
            user_id,
        )

        if not row or not row["mfa_enabled"]:
            return False

        is_valid = self._totp.verify(row["mfa_secret"], code)

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.AUTH_LOGIN
                    if is_valid
                    else AuditEventType.AUTH_FAILED,
                    action="mfa_verification",
                    user_id=user_id,
                    success=is_valid,
                )
            )

        return is_valid

    async def verify_recovery_code(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        recovery_code: str,
    ) -> bool:
        """Verify and consume a recovery code.

        Args:
            conn: Database connection
            user_id: User authenticating
            recovery_code: Recovery code to verify

        Returns:
            True if code valid and consumed, False otherwise
        """
        code_hash = self._hash_recovery_code(recovery_code)

        result = await conn.execute(
            """
            UPDATE mfa_recovery_codes
            SET is_used = true, used_at = NOW()
            WHERE user_id = $1 AND code_hash = $2 AND is_used = false
            """,
            user_id,
            code_hash,
        )

        is_valid = result == "UPDATE 1"

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.AUTH_LOGIN
                    if is_valid
                    else AuditEventType.AUTH_FAILED,
                    action="mfa_recovery_code_used",
                    user_id=user_id,
                    success=is_valid,
                )
            )

        if is_valid:
            logger.warning("Recovery code used for user_id=%d", user_id)

        return is_valid

    async def disable(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        disabled_by: int,
        reason: str,
    ) -> bool:
        """Disable MFA for a user (admin action).

        Args:
            conn: Database connection
            user_id: User to disable MFA for
            disabled_by: Admin user performing action
            reason: Reason for disabling

        Returns:
            True if MFA was disabled
        """
        result = await conn.execute(
            """
            UPDATE users
            SET mfa_enabled = false,
                mfa_secret = NULL,
                mfa_pending = false
            WHERE user_id = $1 AND mfa_enabled = true
            """,
            user_id,
        )

        if result == "UPDATE 1":
            await conn.execute(
                "DELETE FROM mfa_recovery_codes WHERE user_id = $1",
                user_id,
            )

            if self._audit_logger:
                await self._audit_logger.log_event(
                    AuditEvent(
                        event_type=AuditEventType.CONFIG_CHANGE,
                        action="mfa_disabled",
                        user_id=disabled_by,
                        success=True,
                        details={"target_user_id": user_id, "reason": reason},
                    )
                )

            logger.warning(
                "MFA disabled for user_id=%d by user_id=%d, reason=%s",
                user_id,
                disabled_by,
                reason,
            )
            return True

        return False

    async def get_status(
        self,
        conn: asyncpg.Connection,
        user_id: int,
    ) -> MFAStatus | None:
        """Get MFA status for a user.

        Args:
            conn: Database connection
            user_id: User to check

        Returns:
            MFAStatus or None if user not found
        """
        row = await conn.fetchrow(
            """
            SELECT
                u.user_id,
                u.mfa_enabled,
                u.mfa_enrolled_at,
                COUNT(r.id) FILTER (WHERE r.is_used = false) as recovery_codes_remaining
            FROM users u
            LEFT JOIN mfa_recovery_codes r ON r.user_id = u.user_id
            WHERE u.user_id = $1
            GROUP BY u.user_id, u.mfa_enabled, u.mfa_enrolled_at
            """,
            user_id,
        )

        if not row:
            return None

        return MFAStatus.from_db_row(dict(row))

    async def regenerate_recovery_codes(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        code: str,
    ) -> list[str] | None:
        """Regenerate recovery codes (requires valid TOTP code).

        Args:
            conn: Database connection
            user_id: User to regenerate codes for
            code: Current TOTP code for verification

        Returns:
            New recovery codes, or None if verification failed
        """
        if not await self.verify_code(conn, user_id, code):
            return None

        recovery_codes = self._generate_recovery_codes()

        await conn.execute(
            "DELETE FROM mfa_recovery_codes WHERE user_id = $1",
            user_id,
        )

        hashed_codes = [self._hash_recovery_code(c) for c in recovery_codes]
        for hashed in hashed_codes:
            await conn.execute(
                """
                INSERT INTO mfa_recovery_codes (user_id, code_hash, is_used)
                VALUES ($1, $2, false)
                """,
                user_id,
                hashed,
            )

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.CONFIG_CHANGE,
                    action="mfa_recovery_codes_regenerated",
                    user_id=user_id,
                    success=True,
                )
            )

        return recovery_codes
