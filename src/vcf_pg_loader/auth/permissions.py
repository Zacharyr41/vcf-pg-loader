"""Permission checking for HIPAA-compliant RBAC.

HIPAA Reference: 164.312(a)(1) - Access Controls
Default deny principle - no permission means no access.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import asyncpg

from .models import Permission

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

DEFAULT_CACHE_TTL_SECONDS = 300


class PermissionChecker:
    def __init__(self, cache_ttl: float = DEFAULT_CACHE_TTL_SECONDS):
        self._cache_ttl = cache_ttl
        self._cache: dict[int, tuple[set[str], float]] = {}
        self._lock = asyncio.Lock()

    async def list_permissions(self, conn: asyncpg.Connection) -> list[Permission]:
        rows = await conn.fetch(
            """
            SELECT permission_id, permission_name, resource_type, action, description
            FROM permissions ORDER BY resource_type, action
            """
        )
        return [Permission.from_db_row(dict(row)) for row in rows]

    async def has_permission(self, conn: asyncpg.Connection, user_id: int, permission: str) -> bool:
        permissions = await self.get_user_permissions(conn, user_id)
        return permission in permissions

    async def get_user_permissions(self, conn: asyncpg.Connection, user_id: int) -> set[str]:
        async with self._lock:
            if user_id in self._cache:
                permissions, cached_at = self._cache[user_id]
                if time.time() - cached_at < self._cache_ttl:
                    return permissions

        rows = await conn.fetch(
            """
            SELECT DISTINCT p.permission_name
            FROM permissions p
            JOIN role_permissions rp ON p.permission_id = rp.permission_id
            JOIN user_roles ur ON rp.role_id = ur.role_id
            WHERE ur.user_id = $1
              AND (ur.expires_at IS NULL OR ur.expires_at > NOW())
            """,
            user_id,
        )

        permissions = {row["permission_name"] for row in rows}

        async with self._lock:
            self._cache[user_id] = (permissions, time.time())

        return permissions

    async def invalidate_cache(self, user_id: int | None = None) -> None:
        async with self._lock:
            if user_id is None:
                self._cache.clear()
            elif user_id in self._cache:
                del self._cache[user_id]

    async def check_permission(
        self, conn: asyncpg.Connection, user_id: int, permission: str
    ) -> tuple[bool, str]:
        has_perm = await self.has_permission(conn, user_id, permission)
        if has_perm:
            return True, "Permission granted"
        return False, f"Permission denied: {permission}"

    def require_permission(self, permission: str) -> Callable:
        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            @wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                self_obj = args[0] if args else None

                conn = kwargs.get("conn")
                if conn is None and self_obj:
                    conn = getattr(self_obj, "_conn", None)
                if conn is None:
                    for arg in args:
                        if isinstance(arg, asyncpg.Connection):
                            conn = arg
                            break

                user_id = kwargs.get("user_id")
                if user_id is None and self_obj:
                    user_id = getattr(self_obj, "_user_id", None)
                if user_id is None:
                    session = kwargs.get("session")
                    if session is None and self_obj:
                        session = getattr(self_obj, "_session", None)
                    if session:
                        user_id = getattr(session, "user_id", None)

                if conn is None or user_id is None:
                    raise PermissionError(
                        f"Cannot check permission '{permission}': missing connection or user context"
                    )

                has_perm = await self.has_permission(conn, user_id, permission)
                if not has_perm:
                    raise PermissionError(f"Permission denied: {permission}")

                return await func(*args, **kwargs)

            return wrapper

        return decorator


class PermissionError(Exception):
    pass
