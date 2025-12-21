"""Disposal schema management for HIPAA-compliant data disposal."""

from pathlib import Path

import asyncpg


class DisposalSchemaManager:
    """Manages disposal tracking schema.

    Creates and maintains tables for:
    - Disposal records with two-person authorization
    - Retention policies
    - Verification tracking
    - Certificate of destruction
    """

    SQL_FILE = Path(__file__).parent.parent / "db" / "schema" / "disposal_tables.sql"

    async def create_disposal_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete disposal schema from SQL file."""
        sql = self.SQL_FILE.read_text()
        await conn.execute(sql)

    async def verify_schema_exists(self, conn: asyncpg.Connection) -> bool:
        """Verify disposal schema tables exist."""
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'disposal_records'
            )
            """
        )
        return bool(result)

    async def get_disposal_stats(self, conn: asyncpg.Connection) -> dict:
        """Get disposal statistics for monitoring."""
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total_disposals,
                COUNT(*) FILTER (WHERE execution_status = 'completed') as completed,
                COUNT(*) FILTER (WHERE execution_status = 'pending') as pending,
                COUNT(*) FILTER (WHERE execution_status = 'failed') as failed,
                COUNT(*) FILTER (WHERE verification_status = 'passed') as verified,
                SUM(variants_disposed) as total_variants_disposed,
                SUM(mappings_disposed) as total_mappings_disposed
            FROM disposal_records
            """
        )
        return dict(stats) if stats else {}

    async def get_pending_authorizations(self, conn: asyncpg.Connection) -> list[dict]:
        """Get disposals pending second authorization."""
        rows = await conn.fetch(
            """
            SELECT d.*, u.username as authorized_by_name
            FROM disposal_records d
            LEFT JOIN users u ON d.authorized_by = u.user_id
            WHERE d.execution_status = 'pending'
            AND d.authorization_required_count > 1
            ORDER BY d.created_at DESC
            """
        )
        return [dict(row) for row in rows]
