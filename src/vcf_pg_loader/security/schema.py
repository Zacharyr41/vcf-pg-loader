"""Security schema management.

HIPAA Reference: 45 CFR 164.312(a)(2)(iv) - Encryption and Decryption
"""

import logging
from importlib.resources import files

import asyncpg

logger = logging.getLogger(__name__)


class SecuritySchemaManager:
    """Manages encryption and security schema.

    45 CFR 164.312(a)(2)(iv): Implement encryption/decryption mechanism.
    """

    async def create_encryption_schema(self, conn: asyncpg.Connection) -> None:
        """Create encryption schema from SQL file.

        Includes:
        - encryption_keys table
        - encryption_key_rotations table
        - encrypted_data_registry table
        - audit_retention_policy table
        """
        sql_path = files("vcf_pg_loader.db.schema").joinpath("encryption_tables.sql")
        sql = sql_path.read_text()
        await conn.execute(sql)
        logger.info("Encryption schema created/updated")

    async def schema_exists(self, conn: asyncpg.Connection) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'encryption_keys'
            )
            """
        )

    async def retention_policy_exists(self, conn: asyncpg.Connection) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'audit_retention_policy'
            )
            """
        )

    async def get_active_key_count(self, conn: asyncpg.Connection) -> int:
        if not await self.schema_exists(conn):
            return 0
        return (
            await conn.fetchval("SELECT COUNT(*) FROM encryption_keys WHERE is_active = true") or 0
        )
