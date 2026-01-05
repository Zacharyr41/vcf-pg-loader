"""Export GWAS summary statistics to common PRS tool input formats.

Supports:
- PLINK 2.0 --score format (SNP, A1, BETA)
- PRS-CS format (SNP, A1, A2, BETA, P/SE)
- LDpred2 bigsnpr format (chr, pos, a0, a1, beta, beta_se, n_eff)
- PRSice-2 format (SNP, A1, A2, BETA, SE, P)
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class VariantFilter:
    """Filter options for PRS export variants."""

    hapmap3_only: bool = False
    min_info: float | None = None
    min_maf: float | None = None


def _build_filter_clause(variant_filter: VariantFilter | None) -> tuple[str, list]:
    """Build SQL WHERE clause for variant filtering.

    Returns:
        Tuple of (WHERE clause string, parameter list)
    """
    conditions = []
    params: list = []
    param_idx = 2

    if variant_filter is None:
        return "", params

    if variant_filter.hapmap3_only:
        conditions.append("v.in_hapmap3 = TRUE")

    if variant_filter.min_info is not None:
        conditions.append(f"v.info_score >= ${param_idx}")
        params.append(variant_filter.min_info)
        param_idx += 1

    if variant_filter.min_maf is not None:
        conditions.append(f"v.maf >= ${param_idx}")
        params.append(variant_filter.min_maf)
        param_idx += 1

    if conditions:
        return " AND " + " AND ".join(conditions), params
    return "", params


def _normalize_chromosome(chrom: str) -> str:
    """Normalize chromosome to numeric/letter format (no 'chr' prefix)."""
    if chrom.startswith("chr"):
        return chrom[3:]
    return chrom


async def _get_study_neff(conn: asyncpg.Connection, study_id: int) -> float:
    """Calculate effective sample size for a study.

    For case-control studies: n_eff = 4 / (1/n_cases + 1/n_controls)
    For quantitative traits: n_eff = sample_size
    """
    row = await conn.fetchrow(
        """
        SELECT sample_size, n_cases, n_controls
        FROM studies
        WHERE study_id = $1
        """,
        study_id,
    )

    if row is None:
        return 0.0

    if row["n_cases"] and row["n_controls"]:
        return 4 / (1 / row["n_cases"] + 1 / row["n_controls"])
    elif row["sample_size"]:
        return float(row["sample_size"])
    return 0.0


async def export_plink_score(
    conn: asyncpg.Connection,
    study_id: int,
    output_path: Path,
    variant_filter: VariantFilter | None = None,
) -> int:
    """Export GWAS summary statistics in PLINK 2.0 --score format.

    Format:
        SNP     A1      BETA
        rs123   A       0.05

    Args:
        conn: Database connection
        study_id: Study ID to export
        output_path: Path to output file
        variant_filter: Optional variant filter

    Returns:
        Number of variants exported
    """
    filter_clause, filter_params = _build_filter_clause(variant_filter)

    query = f"""
        SELECT
            v.rs_id AS snp,
            g.effect_allele AS a1,
            g.beta
        FROM gwas_summary_stats g
        JOIN variants v ON v.variant_id = g.variant_id
        WHERE g.study_id = $1
            AND v.rs_id IS NOT NULL
            AND g.beta IS NOT NULL
            {filter_clause}
        ORDER BY v.chrom, v.pos
    """

    rows = await conn.fetch(query, study_id, *filter_params)

    count = 0
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("SNP\tA1\tBETA\n")
        for row in rows:
            f.write(f"{row['snp']}\t{row['a1']}\t{row['beta']}\n")
            count += 1

    logger.info("Exported %d variants to PLINK score format: %s", count, output_path)
    return count


async def export_prs_cs(
    conn: asyncpg.Connection,
    study_id: int,
    output_path: Path,
    use_se: bool = True,
    variant_filter: VariantFilter | None = None,
) -> int:
    """Export GWAS summary statistics in PRS-CS format.

    Format with SE:
        SNP     A1      A2      BETA    SE
        rs123   A       G       0.05    0.01

    Format with P:
        SNP     A1      A2      BETA    P
        rs123   A       G       0.05    1e-8

    Args:
        conn: Database connection
        study_id: Study ID to export
        output_path: Path to output file
        use_se: If True, include standard error; if False, include p-value
        variant_filter: Optional variant filter

    Returns:
        Number of variants exported
    """
    filter_clause, filter_params = _build_filter_clause(variant_filter)

    last_col = "g.standard_error AS last_val" if use_se else "g.p_value AS last_val"

    query = f"""
        SELECT
            v.rs_id AS snp,
            g.effect_allele AS a1,
            g.other_allele AS a2,
            g.beta,
            {last_col}
        FROM gwas_summary_stats g
        JOIN variants v ON v.variant_id = g.variant_id
        WHERE g.study_id = $1
            AND v.rs_id IS NOT NULL
            AND g.beta IS NOT NULL
            AND g.other_allele IS NOT NULL
            {filter_clause}
        ORDER BY v.chrom, v.pos
    """

    rows = await conn.fetch(query, study_id, *filter_params)

    header = "SNP\tA1\tA2\tBETA\tSE" if use_se else "SNP\tA1\tA2\tBETA\tP"

    count = 0
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(header + "\n")
        for row in rows:
            f.write(f"{row['snp']}\t{row['a1']}\t{row['a2']}\t{row['beta']}\t{row['last_val']}\n")
            count += 1

    logger.info("Exported %d variants to PRS-CS format: %s", count, output_path)
    return count


async def export_ldpred2(
    conn: asyncpg.Connection,
    study_id: int,
    output_path: Path,
    variant_filter: VariantFilter | None = None,
) -> int:
    """Export GWAS summary statistics in LDpred2 bigsnpr format.

    Format:
        chr     pos     a0      a1      beta    beta_se n_eff
        1       12345   G       A       0.05    0.01    50000

    Note: a1 is the effect allele, a0 is the other allele.
    Chromosome is numeric (no 'chr' prefix).

    Args:
        conn: Database connection
        study_id: Study ID to export
        output_path: Path to output file
        variant_filter: Optional variant filter

    Returns:
        Number of variants exported
    """
    n_eff = await _get_study_neff(conn, study_id)
    filter_clause, filter_params = _build_filter_clause(variant_filter)

    query = f"""
        SELECT
            v.chrom,
            v.pos,
            g.other_allele AS a0,
            g.effect_allele AS a1,
            g.beta,
            g.standard_error AS beta_se
        FROM gwas_summary_stats g
        JOIN variants v ON v.variant_id = g.variant_id
        WHERE g.study_id = $1
            AND g.beta IS NOT NULL
            AND g.standard_error IS NOT NULL
            AND g.other_allele IS NOT NULL
            {filter_clause}
        ORDER BY v.chrom, v.pos
    """

    rows = await conn.fetch(query, study_id, *filter_params)

    count = 0
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("chr\tpos\ta0\ta1\tbeta\tbeta_se\tn_eff\n")
        for row in rows:
            chrom = _normalize_chromosome(str(row["chrom"]))
            f.write(
                f"{chrom}\t{row['pos']}\t{row['a0']}\t{row['a1']}\t"
                f"{row['beta']}\t{row['beta_se']}\t{n_eff:.0f}\n"
            )
            count += 1

    logger.info("Exported %d variants to LDpred2 format: %s", count, output_path)
    return count


async def export_prsice2(
    conn: asyncpg.Connection,
    study_id: int,
    output_path: Path,
    variant_filter: VariantFilter | None = None,
) -> int:
    """Export GWAS summary statistics in PRSice-2 format.

    Format:
        SNP     A1      A2      BETA    SE      P
        rs123   A       G       0.05    0.01    1e-8

    Args:
        conn: Database connection
        study_id: Study ID to export
        output_path: Path to output file
        variant_filter: Optional variant filter

    Returns:
        Number of variants exported
    """
    filter_clause, filter_params = _build_filter_clause(variant_filter)

    query = f"""
        SELECT
            v.rs_id AS snp,
            g.effect_allele AS a1,
            g.other_allele AS a2,
            g.beta,
            g.standard_error AS se,
            g.p_value AS p
        FROM gwas_summary_stats g
        JOIN variants v ON v.variant_id = g.variant_id
        WHERE g.study_id = $1
            AND v.rs_id IS NOT NULL
            AND g.beta IS NOT NULL
            AND g.standard_error IS NOT NULL
            AND g.other_allele IS NOT NULL
            {filter_clause}
        ORDER BY v.chrom, v.pos
    """

    rows = await conn.fetch(query, study_id, *filter_params)

    count = 0
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("SNP\tA1\tA2\tBETA\tSE\tP\n")
        for row in rows:
            f.write(
                f"{row['snp']}\t{row['a1']}\t{row['a2']}\t"
                f"{row['beta']}\t{row['se']}\t{row['p']}\n"
            )
            count += 1

    logger.info("Exported %d variants to PRSice-2 format: %s", count, output_path)
    return count
