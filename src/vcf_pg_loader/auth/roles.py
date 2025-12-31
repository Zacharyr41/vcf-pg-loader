"""Role management for HIPAA-compliant RBAC.

HIPAA Reference: 164.312(a)(1) - Access Controls
Implements minimum necessary access principle.
"""

import json
import logging
from datetime import datetime

import asyncpg

from .models import Role, User, UserRole

logger = logging.getLogger(__name__)


class RoleManager:
    async def list_roles(self, conn: asyncpg.Connection) -> list[Role]:
        rows = await conn.fetch(
            """
            SELECT role_id, role_name, description, is_system_role, created_at
            FROM roles ORDER BY role_name
            """
        )
        return [Role.from_db_row(dict(row)) for row in rows]

    async def get_role(self, conn: asyncpg.Connection, role_name: str) -> Role | None:
        row = await conn.fetchrow(
            """
            SELECT role_id, role_name, description, is_system_role, created_at
            FROM roles WHERE role_name = $1
            """,
            role_name,
        )
        if not row:
            return None
        return Role.from_db_row(dict(row))

    async def assign_role(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        role_name: str,
        granted_by: int,
        expires_at: datetime | None = None,
    ) -> tuple[bool, str]:
        role = await self.get_role(conn, role_name)
        if not role:
            return False, f"Role '{role_name}' not found"

        existing = await conn.fetchrow(
            "SELECT 1 FROM user_roles WHERE user_id = $1 AND role_id = $2",
            user_id,
            role.role_id,
        )
        if existing:
            return False, f"User already has role '{role_name}'"

        await conn.execute(
            """
            INSERT INTO user_roles (user_id, role_id, granted_by, expires_at)
            VALUES ($1, $2, $3, $4)
            """,
            user_id,
            role.role_id,
            granted_by,
            expires_at,
        )

        await self._log_role_change(
            conn,
            event_type="assign",
            target_user_id=user_id,
            role_id=role.role_id,
            performed_by=granted_by,
            details={"expires_at": expires_at.isoformat() if expires_at else None},
        )

        logger.info("Assigned role '%s' to user %d by user %d", role_name, user_id, granted_by)
        return True, f"Role '{role_name}' assigned"

    async def revoke_role(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        role_name: str,
        revoked_by: int,
    ) -> tuple[bool, str]:
        role = await self.get_role(conn, role_name)
        if not role:
            return False, f"Role '{role_name}' not found"

        result = await conn.execute(
            "DELETE FROM user_roles WHERE user_id = $1 AND role_id = $2",
            user_id,
            role.role_id,
        )
        if result != "DELETE 1":
            return False, f"User does not have role '{role_name}'"

        await self._log_role_change(
            conn,
            event_type="revoke",
            target_user_id=user_id,
            role_id=role.role_id,
            performed_by=revoked_by,
            details={},
        )

        logger.info("Revoked role '%s' from user %d by user %d", role_name, user_id, revoked_by)
        return True, f"Role '{role_name}' revoked"

    async def get_user_roles(self, conn: asyncpg.Connection, user_id: int) -> list[UserRole]:
        rows = await conn.fetch(
            """
            SELECT ur.user_id, ur.role_id, r.role_name, ur.granted_by,
                   ur.granted_at, ur.expires_at
            FROM user_roles ur
            JOIN roles r ON ur.role_id = r.role_id
            WHERE ur.user_id = $1
              AND (ur.expires_at IS NULL OR ur.expires_at > NOW())
            ORDER BY r.role_name
            """,
            user_id,
        )
        return [UserRole.from_db_row(dict(row)) for row in rows]

    async def get_role_users(self, conn: asyncpg.Connection, role_name: str) -> list[User]:
        rows = await conn.fetch(
            """
            SELECT u.user_id, u.username, u.email, u.is_active, u.is_locked,
                   u.failed_login_attempts, u.locked_until, u.password_changed_at,
                   u.password_expires_at, u.must_change_password, u.created_at,
                   u.created_by, u.last_login_at, u.mfa_enabled
            FROM users u
            JOIN user_roles ur ON u.user_id = ur.user_id
            JOIN roles r ON ur.role_id = r.role_id
            WHERE r.role_name = $1
              AND (ur.expires_at IS NULL OR ur.expires_at > NOW())
            ORDER BY u.username
            """,
            role_name,
        )
        return [User.from_db_row(dict(row)) for row in rows]

    async def cleanup_expired_roles(self, conn: asyncpg.Connection) -> int:
        return await conn.fetchval("SELECT cleanup_expired_roles()") or 0

    async def _log_role_change(
        self,
        conn: asyncpg.Connection,
        event_type: str,
        target_user_id: int,
        role_id: int,
        performed_by: int,
        details: dict,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO role_audit (event_type, target_user_id, role_id, performed_by, details)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            """,
            event_type,
            target_user_id,
            role_id,
            performed_by,
            json.dumps(details),
        )
