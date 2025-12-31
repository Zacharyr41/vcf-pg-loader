"""HIPAA-compliant audit schema management."""

from datetime import date
from pathlib import Path

import asyncpg


class AuditSchemaManager:
    """Manages HIPAA-compliant audit logging schema.

    Creates and maintains partitioned audit tables with:
    - Immutability triggers (no UPDATE/DELETE)
    - Monthly partitions for 6-year retention management
    - Compliance query views
    """

    SQL_FILE = Path(__file__).parent.parent / "db" / "schema" / "audit_tables.sql"

    async def create_audit_schema(self, conn: asyncpg.Connection) -> None:
        """Create complete audit logging schema from SQL file."""
        sql = self.SQL_FILE.read_text()
        await conn.execute(sql)

    async def create_initial_partitions(
        self,
        conn: asyncpg.Connection,
        months_ahead: int = 12,
    ) -> list[str]:
        """Create partitions for current month plus future months.

        Args:
            conn: Database connection
            months_ahead: Number of months of partitions to create ahead

        Returns:
            List of created partition names
        """
        today = date.today()
        result = await conn.fetch(
            "SELECT partition_name, created FROM create_audit_partitions_range($1, $2)",
            today,
            months_ahead,
        )
        return [row["partition_name"] for row in result if row["created"]]

    async def ensure_partition_exists(
        self,
        conn: asyncpg.Connection,
        target_date: date,
    ) -> str | None:
        """Ensure partition exists for a specific date.

        Args:
            conn: Database connection
            target_date: Date that needs a partition

        Returns:
            Partition name if created, None if already existed
        """
        result = await conn.fetchval(
            "SELECT create_audit_partition($1)",
            target_date,
        )
        return result

    async def get_partition_info(
        self,
        conn: asyncpg.Connection,
    ) -> list[dict]:
        """Get information about all audit log partitions.

        Returns list of dicts with partition_name, row_count, size_bytes.
        """
        rows = await conn.fetch(
            """
            SELECT
                c.relname as partition_name,
                pg_relation_size(c.oid) as size_bytes,
                (SELECT reltuples::bigint FROM pg_class WHERE oid = c.oid) as row_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_inherits i ON i.inhrelid = c.oid
            JOIN pg_class parent ON parent.oid = i.inhparent
            WHERE parent.relname = 'hipaa_audit_log'
              AND n.nspname = 'public'
            ORDER BY c.relname
            """
        )
        return [dict(row) for row in rows]

    async def archive_old_partitions(
        self,
        conn: asyncpg.Connection,
        retention_years: int = 6,
    ) -> list[str]:
        """Archive partitions older than retention period.

        HIPAA requires 6-year retention minimum. This detaches old partitions
        but does NOT delete them. Archived partitions should be backed up
        and stored according to organization policy.

        Args:
            conn: Database connection
            retention_years: Years to retain (default 6 for HIPAA)

        Returns:
            List of archived partition names
        """
        cutoff = date.today().replace(year=date.today().year - retention_years)
        cutoff = cutoff.replace(day=1)

        partitions = await self.get_partition_info(conn)
        archived = []

        for partition in partitions:
            name = partition["partition_name"]
            try:
                year = int(name[-7:-3])
                month = int(name[-2:])
                partition_date = date(year, month, 1)
                if partition_date < cutoff:
                    result = await conn.fetchval(
                        "SELECT archive_audit_partition($1)",
                        partition_date,
                    )
                    if result:
                        archived.append(result)
            except (ValueError, IndexError):
                continue

        return archived

    async def verify_immutability(self, conn: asyncpg.Connection) -> bool:
        """Verify that audit immutability triggers are in place."""
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'audit_immutability'
                  AND tgrelid = 'hipaa_audit_log'::regclass
            )
            """
        )
        return bool(result)

    async def get_audit_stats(self, conn: asyncpg.Connection) -> dict:
        """Get audit log statistics for monitoring."""
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(event_time) as oldest_event,
                MAX(event_time) as newest_event,
                COUNT(*) FILTER (WHERE event_type = 'AUTH_FAILED') as failed_auth_count,
                COUNT(*) FILTER (WHERE event_type = 'PHI_ACCESS') as phi_access_count
            FROM hipaa_audit_log
            """
        )
        return dict(stats) if stats else {}
