"""PHI Vault schema management.

HIPAA Reference: 164.514(b) - De-identification Standard
"""

from pathlib import Path

import asyncpg


class PHISchemaManager:
    """Manages PHI vault schema for sample ID mapping.

    Creates and maintains:
    - phi_vault schema with restricted access
    - sample_id_mapping table with immutability triggers
    - reverse_lookup_audit table for compliance tracking
    """

    SQL_FILE = Path(__file__).parent.parent / "db" / "schema" / "phi_mapping_tables.sql"

    async def create_phi_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete PHI vault schema from SQL file."""
        sql = self.SQL_FILE.read_text()
        await conn.execute(sql)

    async def verify_schema_exists(self, conn: asyncpg.Connection) -> bool:
        """Check if PHI vault schema and tables exist."""
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata
                WHERE schema_name = 'phi_vault'
            )
            """
        )
        if not result:
            return False

        tables_exist = await conn.fetchval(
            """
            SELECT COUNT(*) = 2 FROM information_schema.tables
            WHERE table_schema = 'phi_vault'
              AND table_name IN ('sample_id_mapping', 'reverse_lookup_audit')
            """
        )
        return bool(tables_exist)

    async def verify_immutability(self, conn: asyncpg.Connection) -> bool:
        """Verify that immutability trigger is active on mapping table."""
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_trigger t
                JOIN pg_class c ON t.tgrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE t.tgname = 'mapping_immutability'
                  AND c.relname = 'sample_id_mapping'
                  AND n.nspname = 'phi_vault'
            )
            """
        )
        return bool(result)

    async def get_mapping_stats(self, conn: asyncpg.Connection) -> dict:
        """Get mapping statistics from phi_vault."""
        row = await conn.fetchrow("SELECT * FROM phi_vault.v_mapping_stats")
        return dict(row) if row else {}

    async def get_lookup_stats(self, conn: asyncpg.Connection) -> dict:
        """Get reverse lookup statistics."""
        row = await conn.fetchrow("SELECT * FROM phi_vault.v_lookup_stats")
        return dict(row) if row else {}

    async def get_mappings_by_batch(
        self,
        conn: asyncpg.Connection,
        load_batch_id: str,
    ) -> list[dict]:
        """Get all mappings for a specific load batch."""
        rows = await conn.fetch(
            """
            SELECT anonymous_id, source_file, created_at
            FROM phi_vault.sample_id_mapping
            WHERE load_batch_id = $1
            ORDER BY created_at
            """,
            load_batch_id,
        )
        return [dict(row) for row in rows]

    async def drop_phi_schema(self, conn: asyncpg.Connection) -> None:
        """Drop PHI vault schema (use with caution - destroys all mappings)."""
        await conn.execute("DROP SCHEMA IF EXISTS phi_vault CASCADE")
