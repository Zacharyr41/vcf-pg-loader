"""User management operations.

HIPAA Reference: 164.312(d) - Person or Entity Authentication
"""

import logging
from datetime import UTC, datetime, timedelta

import asyncpg

from .authentication import Authenticator
from .models import PasswordPolicy, User

logger = logging.getLogger(__name__)


class UserManager:
    def __init__(
        self,
        authenticator: Authenticator | None = None,
        password_policy: PasswordPolicy | None = None,
    ):
        self._policy = password_policy or PasswordPolicy()
        self._auth = authenticator or Authenticator(password_policy=self._policy)

    async def create_user(
        self,
        conn: asyncpg.Connection,
        username: str,
        password: str,
        email: str | None = None,
        created_by: int | None = None,
        must_change_password: bool = False,
    ) -> tuple[User | None, str]:
        errors = self._policy.validate(password)
        if errors:
            return None, "; ".join(errors)

        existing = await conn.fetchrow("SELECT user_id FROM users WHERE username = $1", username)
        if existing:
            return None, f"Username '{username}' already exists"

        if email:
            existing_email = await conn.fetchrow(
                "SELECT user_id FROM users WHERE email = $1", email
            )
            if existing_email:
                return None, f"Email '{email}' already registered"

        password_hash = self._auth.hash_password(password)

        password_expires_at = None
        if self._policy.max_age_days:
            password_expires_at = datetime.now(UTC) + timedelta(days=self._policy.max_age_days)

        row = await conn.fetchrow(
            """
            INSERT INTO users (username, email, password_hash, created_by,
                               must_change_password, password_expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING user_id, username, email, is_active, is_locked, failed_login_attempts,
                      locked_until, password_changed_at, password_expires_at, must_change_password,
                      created_at, created_by, last_login_at, mfa_enabled
            """,
            username,
            email,
            password_hash,
            created_by,
            must_change_password,
            password_expires_at,
        )

        user = User.from_db_row(dict(row))
        logger.info("Created user: %s (id=%d)", username, user.user_id)
        return user, "User created successfully"

    async def get_user(self, conn: asyncpg.Connection, user_id: int) -> User | None:
        row = await conn.fetchrow(
            """
            SELECT user_id, username, email, is_active, is_locked, failed_login_attempts,
                   locked_until, password_changed_at, password_expires_at, must_change_password,
                   created_at, created_by, last_login_at, mfa_enabled
            FROM users WHERE user_id = $1
            """,
            user_id,
        )
        if not row:
            return None
        return User.from_db_row(dict(row))

    async def get_user_by_username(self, conn: asyncpg.Connection, username: str) -> User | None:
        row = await conn.fetchrow(
            """
            SELECT user_id, username, email, is_active, is_locked, failed_login_attempts,
                   locked_until, password_changed_at, password_expires_at, must_change_password,
                   created_at, created_by, last_login_at, mfa_enabled
            FROM users WHERE username = $1
            """,
            username,
        )
        if not row:
            return None
        return User.from_db_row(dict(row))

    async def list_users(
        self,
        conn: asyncpg.Connection,
        include_inactive: bool = False,
    ) -> list[User]:
        if include_inactive:
            rows = await conn.fetch(
                """
                SELECT user_id, username, email, is_active, is_locked, failed_login_attempts,
                       locked_until, password_changed_at, password_expires_at, must_change_password,
                       created_at, created_by, last_login_at, mfa_enabled
                FROM users ORDER BY username
                """
            )
        else:
            rows = await conn.fetch(
                """
                SELECT user_id, username, email, is_active, is_locked, failed_login_attempts,
                       locked_until, password_changed_at, password_expires_at, must_change_password,
                       created_at, created_by, last_login_at, mfa_enabled
                FROM users WHERE is_active = true ORDER BY username
                """
            )
        return [User.from_db_row(dict(row)) for row in rows]

    async def disable_user(self, conn: asyncpg.Connection, user_id: int) -> tuple[bool, str]:
        result = await conn.execute(
            "UPDATE users SET is_active = false WHERE user_id = $1 AND is_active = true",
            user_id,
        )
        if result == "UPDATE 1":
            await conn.execute("DELETE FROM user_sessions WHERE user_id = $1", user_id)
            logger.info("Disabled user: %d", user_id)
            return True, "User disabled"
        return False, "User not found or already disabled"

    async def enable_user(self, conn: asyncpg.Connection, user_id: int) -> tuple[bool, str]:
        result = await conn.execute(
            "UPDATE users SET is_active = true WHERE user_id = $1 AND is_active = false",
            user_id,
        )
        if result == "UPDATE 1":
            logger.info("Enabled user: %d", user_id)
            return True, "User enabled"
        return False, "User not found or already active"

    async def unlock_user(self, conn: asyncpg.Connection, user_id: int) -> tuple[bool, str]:
        result = await conn.execute(
            """
            UPDATE users SET is_locked = false, failed_login_attempts = 0, locked_until = NULL
            WHERE user_id = $1 AND is_locked = true
            """,
            user_id,
        )
        if result == "UPDATE 1":
            logger.info("Unlocked user: %d", user_id)
            return True, "User unlocked"
        return False, "User not found or not locked"

    async def reset_password(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        new_password: str,
        require_change: bool = True,
    ) -> tuple[bool, str]:
        errors = self._policy.validate(new_password)
        if errors:
            return False, "; ".join(errors)

        user = await self.get_user(conn, user_id)
        if not user:
            return False, "User not found"

        password_hash = self._auth.hash_password(new_password)

        password_expires_at = None
        if self._policy.max_age_days:
            password_expires_at = datetime.now(UTC) + timedelta(days=self._policy.max_age_days)

        await conn.execute(
            """
            UPDATE users SET password_hash = $2, password_changed_at = NOW(),
                             password_expires_at = $3, must_change_password = $4,
                             is_locked = false, failed_login_attempts = 0, locked_until = NULL
            WHERE user_id = $1
            """,
            user_id,
            password_hash,
            password_expires_at,
            require_change,
        )

        await conn.execute("DELETE FROM user_sessions WHERE user_id = $1", user_id)

        logger.info("Reset password for user: %d", user_id)
        return True, "Password reset successfully"

    async def update_email(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        email: str | None,
    ) -> tuple[bool, str]:
        if email:
            existing = await conn.fetchrow(
                "SELECT user_id FROM users WHERE email = $1 AND user_id != $2",
                email,
                user_id,
            )
            if existing:
                return False, f"Email '{email}' already registered"

        result = await conn.execute(
            "UPDATE users SET email = $2 WHERE user_id = $1",
            user_id,
            email,
        )
        if result == "UPDATE 1":
            return True, "Email updated"
        return False, "User not found"
