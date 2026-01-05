"""Variant matching utilities for GWAS and PRS data integration."""

import asyncpg


def normalize_chromosome(chrom: str, add_chr: bool = False) -> str:
    """Normalize chromosome string for consistent matching.

    Args:
        chrom: Chromosome string (may or may not have 'chr' prefix)
        add_chr: If True, ensures 'chr' prefix is present; if False, removes it

    Returns:
        Normalized chromosome string
    """
    if add_chr:
        if chrom.startswith("chr"):
            return chrom
        return f"chr{chrom}"
    else:
        if chrom.startswith("chr"):
            return chrom[3:]
        return chrom


def match_variant(
    chromosome: str,
    position: int,
    effect_allele: str,
    other_allele: str | None,
    rsid: str | None,
    variant_lookup: dict[tuple[str, int, str, str], int],
    rsid_lookup: dict[str, int],
) -> int | None:
    """Match a variant to the database using position+alleles or rsID.

    Tries to match by:
    1. Chromosome + position + alleles (both orientations, both chr formats)
    2. rsID fallback

    Args:
        chromosome: Chromosome (with or without 'chr' prefix)
        position: Genomic position
        effect_allele: Effect allele from summary stats
        other_allele: Other/reference allele
        rsid: Optional dbSNP rsID
        variant_lookup: Dict mapping (chrom, pos, ref, alt) to variant_id
        rsid_lookup: Dict mapping rsid to variant_id

    Returns:
        variant_id if matched, None otherwise
    """
    chrom_bare = normalize_chromosome(chromosome, add_chr=False)
    ea = effect_allele.upper()
    oa = other_allele.upper() if other_allele else ""

    if oa:
        key1 = (chrom_bare, position, oa, ea)
        if key1 in variant_lookup:
            return variant_lookup[key1]

        key2 = (chrom_bare, position, ea, oa)
        if key2 in variant_lookup:
            return variant_lookup[key2]

    if rsid and rsid in rsid_lookup:
        return rsid_lookup[rsid]

    return None


async def build_variant_lookups(
    conn: asyncpg.Connection,
) -> tuple[dict[tuple[str, int, str, str], int], dict[str, int]]:
    """Build lookup dictionaries for efficient variant matching.

    Stores variants with normalized (bare) chromosome names for consistent matching.
    Loads all variants into memory for fast lookups. For large databases,
    consider using database-side matching instead.

    Args:
        conn: Database connection

    Returns:
        Tuple of (position_lookup, rsid_lookup):
        - position_lookup: Maps (chrom, pos, ref, alt) to variant_id (bare chromosome)
        - rsid_lookup: Maps rsid string to variant_id
    """
    variant_lookup: dict[tuple[str, int, str, str], int] = {}
    rsid_lookup: dict[str, int] = {}

    rows = await conn.fetch("""
        SELECT variant_id, chrom, pos, ref, alt, rs_id
        FROM variants
    """)

    for row in rows:
        chrom_bare = normalize_chromosome(str(row["chrom"]), add_chr=False)

        key = (chrom_bare, row["pos"], row["ref"].upper(), row["alt"].upper())
        variant_lookup[key] = row["variant_id"]

        if row["rs_id"]:
            rsid_lookup[row["rs_id"]] = row["variant_id"]

    return variant_lookup, rsid_lookup
