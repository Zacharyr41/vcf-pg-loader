"""Partition management for chromosome-based partitioning.

Provides utilities for:
- Querying partition statistics
- Enabling parallel query execution
- Verifying partition pruning
"""

import asyncpg


async def get_partition_stats(conn: asyncpg.Connection) -> dict[str, int]:
    """Get row counts for each partition of the variants table.

    Args:
        conn: Database connection

    Returns:
        Dictionary mapping partition name to row count
    """
    rows = await conn.fetch("""
        SELECT
            c.relname as partition_name,
            COALESCE(s.n_live_tup, 0)::bigint as row_count
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        LEFT JOIN pg_stat_user_tables s ON s.relname = c.relname
        WHERE i.inhparent = 'variants'::regclass
        ORDER BY c.relname
    """)

    return {row["partition_name"]: row["row_count"] for row in rows}


async def enable_parallel_query(conn: asyncpg.Connection, workers: int = 4) -> None:
    """Enable parallel query execution with specified worker count.

    Sets max_parallel_workers_per_gather for the current session.
    This allows PostgreSQL to use multiple workers for parallel scans
    across partitions.

    Args:
        conn: Database connection
        workers: Number of parallel workers (default: 4)
    """
    await conn.execute(f"SET max_parallel_workers_per_gather = {workers}")


async def verify_partition_pruning(conn: asyncpg.Connection, chrom: str) -> dict:
    """Verify that partition pruning is active for a chromosome query.

    Args:
        conn: Database connection
        chrom: Chromosome to query (e.g., 'chr1')

    Returns:
        Dictionary with pruning verification results:
        - uses_pruning: bool - whether partition pruning is used
        - partitions_scanned: int - number of partitions in plan
        - partition_names: list[str] - names of partitions scanned
    """
    explain_result = await conn.fetch(
        """
        EXPLAIN (FORMAT JSON) SELECT * FROM variants WHERE chrom = $1
        """,
        chrom,
    )

    import json

    plan_json = json.loads(explain_result[0][0])

    partition_names = []
    partitions_scanned = 0

    def extract_partitions(node: dict) -> None:
        nonlocal partitions_scanned, partition_names
        if "Relation Name" in node:
            rel_name = node["Relation Name"]
            if rel_name.startswith("variants_") and rel_name != "variants":
                partitions_scanned += 1
                partition_names.append(rel_name)
        if "Plans" in node:
            for child in node["Plans"]:
                extract_partitions(child)

    if plan_json and len(plan_json) > 0:
        extract_partitions(plan_json[0].get("Plan", {}))

    uses_pruning = partitions_scanned == 1

    return {
        "uses_pruning": uses_pruning,
        "partitions_scanned": partitions_scanned,
        "partition_names": partition_names,
    }
