"""Single source of truth for variant column definitions."""

import json
from uuid import UUID

from asyncpg import Range

from .models import VariantRecord

VARIANT_COLUMNS: list[str] = [
    "chrom",
    "pos_range",
    "pos",
    "end_pos",
    "ref",
    "alt",
    "qual",
    "filter",
    "rs_id",
    "gene",
    "transcript",
    "hgvs_c",
    "hgvs_p",
    "consequence",
    "impact",
    "is_coding",
    "is_lof",
    "af_gnomad",
    "af_gnomad_popmax",
    "af_1kg",
    "cadd_phred",
    "clinvar_sig",
    "clinvar_review",
    "info",
    "load_batch_id",
    "sample_id",
]

VARIANT_COLUMNS_BASIC: list[str] = [
    "chrom",
    "pos_range",
    "pos",
    "end_pos",
    "ref",
    "alt",
    "qual",
    "filter",
    "rs_id",
    "gene",
    "consequence",
    "impact",
    "hgvs_c",
    "hgvs_p",
    "af_gnomad",
    "cadd_phred",
    "clinvar_sig",
    "load_batch_id",
]


def get_record_values(record: VariantRecord, load_batch_id: UUID) -> tuple:
    """Extract values from VariantRecord in VARIANT_COLUMNS_BASIC order.

    This ensures column order and value order are always in sync.
    """
    return (
        record.chrom,
        Range(record.pos, record.end_pos or record.pos + len(record.ref)),
        record.pos,
        record.end_pos,
        record.ref,
        record.alt,
        record.qual,
        record.filter if record.filter else None,
        record.rs_id,
        record.gene,
        record.consequence,
        record.impact,
        record.hgvs_c,
        record.hgvs_p,
        record.af_gnomad,
        record.cadd_phred,
        record.clinvar_sig,
        load_batch_id,
    )


def get_record_values_full(
    record: VariantRecord, load_batch_id: str, sample_id: str | None
) -> tuple:
    """Extract values from VariantRecord in VARIANT_COLUMNS order.

    This ensures column order and value order are always in sync.
    Includes all fields for full database schema.
    """
    end_pos = record.end_pos or record.pos + len(record.ref)
    info_json = json.dumps(record.info) if record.info else "{}"

    return (
        record.chrom,
        Range(record.pos, end_pos),
        record.pos,
        record.end_pos,
        record.ref,
        record.alt,
        record.qual,
        record.filter if record.filter else None,
        record.rs_id,
        record.gene,
        record.transcript,
        record.hgvs_c,
        record.hgvs_p,
        record.consequence,
        record.impact,
        record.is_coding,
        record.is_lof,
        record.af_gnomad,
        record.af_gnomad_popmax,
        record.af_1kg,
        record.cadd_phred,
        record.clinvar_sig,
        record.clinvar_review,
        info_json,
        load_batch_id,
        sample_id,
    )
