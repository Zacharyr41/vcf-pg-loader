"""Database loading functions for variants."""

import json
from uuid import uuid4

import asyncpg
from asyncpg import Range

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
        _variant_to_record(r, batch_id, None)
        for r in batch
    ]

    await conn.copy_records_to_table(
        "variants",
        records=records,
        columns=_get_columns()
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
        _variant_to_record(r, batch_id, sample_id)
        for r in batch
    ]

    await conn.copy_records_to_table(
        "variants",
        records=records,
        columns=_get_columns()
    )

    return len(batch)


def _variant_to_record(
    r: VariantRecord,
    batch_id: str,
    sample_id: str | None
) -> tuple:
    """Convert a VariantRecord to a database record tuple."""
    end_pos = r.end_pos or r.pos + len(r.ref)
    info_json = json.dumps(r.info) if r.info else "{}"

    return (
        r.chrom,
        Range(r.pos, end_pos),
        r.pos,
        r.end_pos,
        r.ref,
        r.alt,
        r.qual,
        r.filter if r.filter else None,
        r.rs_id,
        r.gene,
        r.transcript,
        r.hgvs_c,
        r.hgvs_p,
        r.consequence,
        r.impact,
        r.is_coding,
        r.is_lof,
        r.af_gnomad,
        r.af_gnomad_popmax,
        r.af_1kg,
        r.cadd_phred,
        r.clinvar_sig,
        r.clinvar_review,
        info_json,
        batch_id,
        sample_id,
    )


def _get_columns() -> list[str]:
    """Get column names for COPY operation."""
    from .columns import VARIANT_COLUMNS

    return VARIANT_COLUMNS
