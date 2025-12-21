"""HIPAA-compliant authentication with Argon2id hashing.

HIPAA Reference: 164.312(d) - Person or Entity Authentication
NIST 800-63B aligned password requirements.
"""

import hashlib
import logging
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

from .models import AuthResult, AuthStatus, PasswordPolicy, Session, TokenPayload, User

logger = logging.getLogger(__name__)

DEFAULT_TOKEN_EXPIRY_HOURS = 8
JWT_ALGORITHM = "HS256"


class Authenticator:
    def __init__(
        self,
        jwt_secret: str | None = None,
        password_policy: PasswordPolicy | None = None,
        token_expiry_hours: int = DEFAULT_TOKEN_EXPIRY_HOURS,
    ):
        self._hasher = PasswordHasher(
            time_cost=3,
            memory_cost=65536,
            parallelism=4,
            hash_len=32,
            salt_len=16,
        )
        self._jwt_secret = jwt_secret or secrets.token_hex(32)
        self._policy = password_policy or PasswordPolicy()
        self._token_expiry_hours = token_expiry_hours

    def hash_password(self, password: str) -> str:
        return self._hasher.hash(password)

    def verify_password(self, password: str, password_hash: str) -> bool:
        try:
            self._hasher.verify(password_hash, password)
            return True
        except (VerifyMismatchError, InvalidHashError):
            return False

    def needs_rehash(self, password_hash: str) -> bool:
        return self._hasher.check_needs_rehash(password_hash)

    def _hash_token(self, token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    def _generate_token(self, session: Session) -> str:
        now = datetime.now(UTC)
        payload = {
            "session_id": str(session.session_id),
            "user_id": session.user_id,
            "username": session.username,
            "iat": int(now.timestamp()),
            "exp": int(session.expires_at.timestamp()),
        }
        return jwt.encode(payload, self._jwt_secret, algorithm=JWT_ALGORITHM)

    def decode_token(self, token: str) -> TokenPayload | None:
        try:
            payload = jwt.decode(token, self._jwt_secret, algorithms=[JWT_ALGORITHM])
            return TokenPayload(
                session_id=payload["session_id"],
                user_id=payload["user_id"],
                username=payload["username"],
                exp=payload["exp"],
                iat=payload["iat"],
            )
        except jwt.ExpiredSignatureError:
            logger.debug("Token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.debug("Invalid token: %s", e)
            return None

    async def authenticate(
        self,
        conn: asyncpg.Connection,
        username: str,
        password: str,
        client_ip: str | None = None,
        client_hostname: str | None = None,
    ) -> AuthResult:
        row = await conn.fetchrow(
            """
            SELECT user_id, username, email, password_hash,
                   is_active, is_locked, failed_login_attempts, locked_until,
                   password_changed_at, password_expires_at, must_change_password,
                   created_at, created_by, last_login_at, mfa_enabled
            FROM users WHERE username = $1
            """,
            username,
        )

        if not row:
            return AuthResult(
                status=AuthStatus.INVALID_CREDENTIALS,
                message="Invalid username or password",
            )

        user = User.from_db_row(dict(row))

        if not user.is_active:
            return AuthResult(
                status=AuthStatus.ACCOUNT_DISABLED,
                user=user,
                message="Account is disabled",
            )

        if user.is_locked:
            if user.locked_until and datetime.now(UTC) < user.locked_until:
                return AuthResult(
                    status=AuthStatus.ACCOUNT_LOCKED,
                    user=user,
                    message=f"Account locked until {user.locked_until.isoformat()}",
                )
            await conn.execute(
                """
                UPDATE users SET is_locked = false, failed_login_attempts = 0, locked_until = NULL
                WHERE user_id = $1
                """,
                user.user_id,
            )
            user.is_locked = False
            user.failed_login_attempts = 0

        if not self.verify_password(password, row["password_hash"]):
            new_attempts = user.failed_login_attempts + 1
            should_lock = new_attempts >= self._policy.lockout_threshold

            if should_lock:
                lockout_until = datetime.now(UTC) + timedelta(
                    minutes=self._policy.lockout_duration_minutes
                )
                await conn.execute(
                    """
                    UPDATE users SET failed_login_attempts = $2, is_locked = true, locked_until = $3
                    WHERE user_id = $1
                    """,
                    user.user_id,
                    new_attempts,
                    lockout_until,
                )
                return AuthResult(
                    status=AuthStatus.ACCOUNT_LOCKED,
                    user=user,
                    message=f"Account locked after {new_attempts} failed attempts",
                )
            else:
                await conn.execute(
                    "UPDATE users SET failed_login_attempts = $2 WHERE user_id = $1",
                    user.user_id,
                    new_attempts,
                )
            return AuthResult(
                status=AuthStatus.INVALID_CREDENTIALS,
                message="Invalid username or password",
            )

        if user.password_expires_at and datetime.now(UTC) > user.password_expires_at:
            return AuthResult(
                status=AuthStatus.PASSWORD_EXPIRED,
                user=user,
                message="Password has expired",
            )

        await conn.execute(
            """
            UPDATE users SET failed_login_attempts = 0, last_login_at = NOW()
            WHERE user_id = $1
            """,
            user.user_id,
        )

        if self.needs_rehash(row["password_hash"]):
            new_hash = self.hash_password(password)
            await conn.execute(
                "UPDATE users SET password_hash = $2 WHERE user_id = $1",
                user.user_id,
                new_hash,
            )

        session = await self._create_session(conn, user, client_ip, client_hostname)
        token = self._generate_token(session)

        return AuthResult(
            status=AuthStatus.SUCCESS,
            user=user,
            session=session,
            token=token,
        )

    async def _create_session(
        self,
        conn: asyncpg.Connection,
        user: User,
        client_ip: str | None,
        client_hostname: str | None,
    ) -> Session:
        session_id = uuid4()
        now = datetime.now(UTC)
        expires_at = now + timedelta(hours=self._token_expiry_hours)

        session = Session(
            session_id=session_id,
            user_id=user.user_id,
            username=user.username,
            created_at=now,
            expires_at=expires_at,
            last_activity_at=now,
            client_ip=client_ip,
            client_hostname=client_hostname,
        )

        temp_token = secrets.token_hex(32)
        token_hash = self._hash_token(temp_token)

        await conn.execute(
            """
            INSERT INTO user_sessions (session_id, user_id, token_hash, expires_at, client_ip, client_hostname)
            VALUES ($1, $2, $3, $4, $5::inet, $6)
            """,
            session_id,
            user.user_id,
            token_hash,
            expires_at,
            client_ip,
            client_hostname,
        )

        return session

    async def validate_session(self, conn: asyncpg.Connection, token: str) -> Session | None:
        payload = self.decode_token(token)
        if not payload:
            return None

        try:
            session_id = UUID(payload.session_id)
        except ValueError:
            return None

        row = await conn.fetchrow(
            """
            SELECT s.session_id, s.user_id, u.username, s.created_at, s.expires_at,
                   s.last_activity_at, s.client_ip, s.client_hostname
            FROM user_sessions s
            JOIN users u ON s.user_id = u.user_id
            WHERE s.session_id = $1 AND s.expires_at > NOW() AND u.is_active = true
            """,
            session_id,
        )

        if not row:
            return None

        await conn.execute(
            "UPDATE user_sessions SET last_activity_at = NOW() WHERE session_id = $1",
            session_id,
        )

        return Session(
            session_id=row["session_id"],
            user_id=row["user_id"],
            username=row["username"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            last_activity_at=row["last_activity_at"],
            client_ip=str(row["client_ip"]) if row["client_ip"] else None,
            client_hostname=row["client_hostname"],
        )

    async def logout(self, conn: asyncpg.Connection, token: str) -> bool:
        payload = self.decode_token(token)
        if not payload:
            return False

        try:
            session_id = UUID(payload.session_id)
        except ValueError:
            return False

        result = await conn.execute("DELETE FROM user_sessions WHERE session_id = $1", session_id)
        return result == "DELETE 1"

    async def logout_all_sessions(self, conn: asyncpg.Connection, user_id: int) -> int:
        result = await conn.execute("DELETE FROM user_sessions WHERE user_id = $1", user_id)
        count_str = result.split()[-1]
        return int(count_str) if count_str.isdigit() else 0

    async def change_password(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        current_password: str,
        new_password: str,
    ) -> tuple[bool, str]:
        row = await conn.fetchrow("SELECT password_hash FROM users WHERE user_id = $1", user_id)
        if not row:
            return False, "User not found"

        if not self.verify_password(current_password, row["password_hash"]):
            return False, "Current password is incorrect"

        errors = self._policy.validate(new_password)
        if errors:
            return False, "; ".join(errors)

        history = await conn.fetch(
            """
            SELECT password_hash FROM password_history
            WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2
            """,
            user_id,
            self._policy.history_count,
        )

        for h in history:
            if self.verify_password(new_password, h["password_hash"]):
                return False, f"Cannot reuse any of the last {self._policy.history_count} passwords"

        if self.verify_password(new_password, row["password_hash"]):
            return False, "New password cannot be the same as current password"

        new_hash = self.hash_password(new_password)

        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO password_history (user_id, password_hash)
                VALUES ($1, $2)
                """,
                user_id,
                row["password_hash"],
            )

            expires_at = None
            if self._policy.max_age_days:
                expires_at = datetime.now(UTC) + timedelta(days=self._policy.max_age_days)

            await conn.execute(
                """
                UPDATE users SET password_hash = $2, password_changed_at = NOW(),
                                 password_expires_at = $3, must_change_password = false
                WHERE user_id = $1
                """,
                user_id,
                new_hash,
                expires_at,
            )

            await conn.execute("DELETE FROM user_sessions WHERE user_id = $1", user_id)

        return True, "Password changed successfully"
