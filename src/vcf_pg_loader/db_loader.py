"""Database loading functions for variants."""

from uuid import uuid4

import asyncpg

from .columns import VARIANT_COLUMNS, get_record_values_full
from .models import VariantRecord


async def load_variants(
    conn: asyncpg.Connection,
    batch: list[VariantRecord],
    load_batch_id: str | None = None
) -> int:
    """Load a batch of variants into the database.

    Args:
        conn: Database connection
        batch: List of VariantRecord objects to load
        load_batch_id: Optional batch ID for audit tracking

    Returns:
        Number of variants loaded
    """
    if not batch:
        return 0

    batch_id = load_batch_id or str(uuid4())

    records = [
        get_record_values_full(r, batch_id, None)
        for r in batch
    ]

    await conn.copy_records_to_table(
        "variants",
        records=records,
        columns=VARIANT_COLUMNS
    )

    return len(batch)


async def load_variants_with_sample(
    conn: asyncpg.Connection,
    batch: list[VariantRecord],
    sample_id: str,
    load_batch_id: str | None = None
) -> int:
    """Load a batch of variants with sample ID into the database.

    Args:
        conn: Database connection
        batch: List of VariantRecord objects to load
        sample_id: Sample identifier to associate with variants
        load_batch_id: Optional batch ID for audit tracking

    Returns:
        Number of variants loaded
    """
    if not batch:
        return 0

    batch_id = load_batch_id or str(uuid4())

    records = [
        get_record_values_full(r, batch_id, sample_id)
        for r in batch
    ]

    await conn.copy_records_to_table(
        "variants",
        records=records,
        columns=VARIANT_COLUMNS
    )

    return len(batch)
