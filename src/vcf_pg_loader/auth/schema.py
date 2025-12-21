"""Authentication schema management.

HIPAA Reference: 164.312(d) - Person or Entity Authentication
"""

import logging
from importlib.resources import files

import asyncpg

logger = logging.getLogger(__name__)


class AuthSchemaManager:
    async def create_auth_schema(self, conn: asyncpg.Connection) -> None:
        sql_path = files("vcf_pg_loader.db.schema").joinpath("users_tables.sql")
        sql = sql_path.read_text()

        await conn.execute(sql)
        logger.info("Auth schema created/updated")

    async def schema_exists(self, conn: asyncpg.Connection) -> bool:
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'users'
            )
            """
        )
        return bool(result)

    async def get_user_count(self, conn: asyncpg.Connection) -> int:
        return await conn.fetchval("SELECT COUNT(*) FROM users") or 0

    async def get_active_session_count(self, conn: asyncpg.Connection) -> int:
        return (
            await conn.fetchval("SELECT COUNT(*) FROM user_sessions WHERE expires_at > NOW()") or 0
        )

    async def cleanup_expired_sessions(self, conn: asyncpg.Connection) -> int:
        return await conn.fetchval("SELECT cleanup_expired_sessions()") or 0
