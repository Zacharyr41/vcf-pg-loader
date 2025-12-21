"""HIPAA-compliant session management.

HIPAA Reference: 164.312(a)(2)(iii) - Automatic logoff after inactivity.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg

from ..audit.logger import AuditLogger
from ..audit.models import AuditEvent, AuditEventType
from .models import Session

logger = logging.getLogger(__name__)


@dataclass
class SessionConfig:
    inactivity_timeout_minutes: int = 30
    absolute_timeout_hours: int = 8
    max_concurrent_sessions: int = 3
    extend_on_activity: bool = True


class SessionManager:
    def __init__(
        self,
        config: SessionConfig | None = None,
        audit_logger: AuditLogger | None = None,
    ):
        self._config = config or SessionConfig()
        self._audit_logger = audit_logger

    @property
    def config(self) -> SessionConfig:
        return self._config

    async def get_session(
        self,
        conn: asyncpg.Connection,
        session_id: UUID,
    ) -> Session | None:
        row = await conn.fetchrow(
            """
            SELECT s.session_id, s.user_id, u.username, s.created_at, s.expires_at,
                   s.last_activity_at, s.client_ip, s.client_hostname, s.is_active,
                   s.terminated_reason, s.terminated_at
            FROM user_sessions s
            JOIN users u ON s.user_id = u.user_id
            WHERE s.session_id = $1
            """,
            session_id,
        )
        if not row:
            return None

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

    async def validate_session(
        self,
        conn: asyncpg.Connection,
        session_id: UUID,
        update_activity: bool = True,
    ) -> Session | None:
        row = await conn.fetchrow(
            """
            SELECT s.session_id, s.user_id, u.username, s.created_at, s.expires_at,
                   s.last_activity_at, s.client_ip, s.client_hostname, s.is_active
            FROM user_sessions s
            JOIN users u ON s.user_id = u.user_id
            WHERE s.session_id = $1 AND s.is_active = true AND u.is_active = true
            """,
            session_id,
        )

        if not row:
            return None

        now = datetime.now(UTC)

        if row["expires_at"] < now:
            await self.terminate_session(conn, session_id, "timeout")
            return None

        if row["last_activity_at"]:
            inactivity_threshold = now - timedelta(minutes=self._config.inactivity_timeout_minutes)
            if row["last_activity_at"] < inactivity_threshold:
                await self.terminate_session(conn, session_id, "inactivity")
                return None

        if update_activity and self._config.extend_on_activity:
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

    async def terminate_session(
        self,
        conn: asyncpg.Connection,
        session_id: UUID,
        reason: str,
    ) -> bool:
        row = await conn.fetchrow(
            """
            UPDATE user_sessions
            SET is_active = false, terminated_reason = $2, terminated_at = NOW()
            WHERE session_id = $1 AND is_active = true
            RETURNING user_id, (SELECT username FROM users WHERE user_id = user_sessions.user_id) as username
            """,
            session_id,
            reason,
        )

        if row and self._audit_logger:
            event_type = (
                AuditEventType.SESSION_TIMEOUT
                if reason in ("timeout", "inactivity")
                else AuditEventType.SESSION_TERMINATED
            )
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=event_type,
                    action=f"session_{reason}",
                    success=True,
                    user_id=row["user_id"],
                    user_name=row["username"],
                    session_id=session_id,
                    resource_type="session",
                    resource_id=str(session_id),
                    details={"reason": reason},
                )
            )

        return row is not None

    async def terminate_user_sessions(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        reason: str,
        exclude_session_id: UUID | None = None,
    ) -> int:
        if exclude_session_id:
            result = await conn.execute(
                """
                UPDATE user_sessions
                SET is_active = false, terminated_reason = $2, terminated_at = NOW()
                WHERE user_id = $1 AND is_active = true AND session_id != $3
                """,
                user_id,
                reason,
                exclude_session_id,
            )
        else:
            result = await conn.execute(
                """
                UPDATE user_sessions
                SET is_active = false, terminated_reason = $2, terminated_at = NOW()
                WHERE user_id = $1 AND is_active = true
                """,
                user_id,
                reason,
            )

        count_str = result.split()[-1]
        count = int(count_str) if count_str.isdigit() else 0

        if count > 0 and self._audit_logger:
            username = await conn.fetchval("SELECT username FROM users WHERE user_id = $1", user_id)
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.SESSION_TERMINATED,
                    action="terminate_user_sessions",
                    success=True,
                    user_id=user_id,
                    user_name=username or "unknown",
                    resource_type="session",
                    details={"reason": reason, "sessions_terminated": count},
                )
            )

        return count

    async def get_active_session_count(
        self,
        conn: asyncpg.Connection,
        user_id: int,
    ) -> int:
        return (
            await conn.fetchval(
                """
                SELECT COUNT(*) FROM user_sessions
                WHERE user_id = $1 AND is_active = true AND expires_at > NOW()
                """,
                user_id,
            )
            or 0
        )

    async def enforce_concurrent_limit(
        self,
        conn: asyncpg.Connection,
        user_id: int,
    ) -> int:
        active_count = await self.get_active_session_count(conn, user_id)

        if active_count < self._config.max_concurrent_sessions:
            return 0

        sessions_to_terminate = active_count - self._config.max_concurrent_sessions + 1

        oldest_sessions = await conn.fetch(
            """
            SELECT session_id FROM user_sessions
            WHERE user_id = $1 AND is_active = true
            ORDER BY last_activity_at ASC NULLS FIRST
            LIMIT $2
            """,
            user_id,
            sessions_to_terminate,
        )

        terminated = 0
        for row in oldest_sessions:
            if await self.terminate_session(conn, row["session_id"], "concurrent_limit"):
                terminated += 1

        return terminated

    async def cleanup_expired_sessions(
        self,
        conn: asyncpg.Connection,
    ) -> int:
        now = datetime.now(UTC)
        inactivity_threshold = now - timedelta(minutes=self._config.inactivity_timeout_minutes)

        rows = await conn.fetch(
            """
            SELECT s.session_id, s.user_id, u.username
            FROM user_sessions s
            JOIN users u ON s.user_id = u.user_id
            WHERE s.is_active = true
              AND (s.expires_at < $1 OR s.last_activity_at < $2)
            """,
            now,
            inactivity_threshold,
        )

        result = await conn.execute(
            """
            UPDATE user_sessions
            SET is_active = false, terminated_reason = 'timeout', terminated_at = NOW()
            WHERE is_active = true
              AND (expires_at < $1 OR last_activity_at < $2)
            """,
            now,
            inactivity_threshold,
        )

        count_str = result.split()[-1]
        count = int(count_str) if count_str.isdigit() else 0

        if count > 0 and self._audit_logger:
            for row in rows:
                await self._audit_logger.log_event(
                    AuditEvent(
                        event_type=AuditEventType.SESSION_TIMEOUT,
                        action="session_cleanup",
                        success=True,
                        user_id=row["user_id"],
                        user_name=row["username"],
                        session_id=row["session_id"],
                        resource_type="session",
                        resource_id=str(row["session_id"]),
                        details={"reason": "cleanup"},
                    )
                )

        logger.info("Cleaned up %d expired sessions", count)
        return count

    async def list_active_sessions(
        self,
        conn: asyncpg.Connection,
        user_id: int | None = None,
    ) -> list[dict]:
        if user_id is not None:
            rows = await conn.fetch(
                """
                SELECT s.session_id, s.user_id, u.username, s.created_at, s.expires_at,
                       s.last_activity_at, s.client_ip, s.client_hostname, s.application_name
                FROM user_sessions s
                JOIN users u ON s.user_id = u.user_id
                WHERE s.is_active = true AND s.user_id = $1
                ORDER BY s.last_activity_at DESC
                """,
                user_id,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT s.session_id, s.user_id, u.username, s.created_at, s.expires_at,
                       s.last_activity_at, s.client_ip, s.client_hostname, s.application_name
                FROM user_sessions s
                JOIN users u ON s.user_id = u.user_id
                WHERE s.is_active = true
                ORDER BY s.last_activity_at DESC
                """
            )

        return [
            {
                "session_id": row["session_id"],
                "user_id": row["user_id"],
                "username": row["username"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "last_activity_at": row["last_activity_at"],
                "client_ip": str(row["client_ip"]) if row["client_ip"] else None,
                "client_hostname": row["client_hostname"],
                "application_name": row["application_name"],
            }
            for row in rows
        ]

    async def get_session_history(
        self,
        conn: asyncpg.Connection,
        user_id: int,
        limit: int = 50,
    ) -> list[dict]:
        rows = await conn.fetch(
            """
            SELECT s.session_id, s.created_at, s.expires_at, s.last_activity_at,
                   s.client_ip, s.client_hostname, s.is_active,
                   s.terminated_reason, s.terminated_at
            FROM user_sessions s
            WHERE s.user_id = $1
            ORDER BY s.created_at DESC
            LIMIT $2
            """,
            user_id,
            limit,
        )

        return [
            {
                "session_id": row["session_id"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "last_activity_at": row["last_activity_at"],
                "client_ip": str(row["client_ip"]) if row["client_ip"] else None,
                "client_hostname": row["client_hostname"],
                "is_active": row["is_active"],
                "terminated_reason": row["terminated_reason"],
                "terminated_at": row["terminated_at"],
            }
            for row in rows
        ]
