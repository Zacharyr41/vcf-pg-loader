"""Sample-level QC metric computation.

Computes per-sample quality metrics:
- Call rate
- Het/hom ratio
- Ti/Tv ratio
- Sex inference from X chromosome heterozygosity
- F_inbreeding coefficient

Reference: Pe'er pipeline filters samples on call rate >99%,
contamination <2.5%, and sex verification.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import asyncpg

from .schema import SampleQCSchemaManager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SampleQCConfig:
    """Configuration for sample QC thresholds.

    All thresholds have sensible defaults based on Pe'er pipeline standards.
    """

    male_x_het_threshold: float = 0.05
    female_x_het_threshold: float = 0.15
    min_call_rate: float = 0.99
    max_contamination: float = 0.025
    x_par_start: int = 2781479
    x_par_end: int = 155701383


DEFAULT_QC_CONFIG = SampleQCConfig()

TRANSITIONS = {
    ("A", "G"),
    ("G", "A"),
    ("C", "T"),
    ("T", "C"),
}


def compute_sample_call_rate(n_called: int, n_total: int) -> float:
    """Compute sample call rate.

    Args:
        n_called: Number of variants with genotype call
        n_total: Total number of variants

    Returns:
        Call rate as float between 0 and 1
    """
    if n_total == 0:
        return 0.0
    return n_called / n_total


def compute_het_hom_ratio(n_het: int, n_hom_var: int) -> float | None:
    """Compute heterozygous to homozygous variant ratio.

    Args:
        n_het: Number of heterozygous genotypes
        n_hom_var: Number of homozygous variant genotypes

    Returns:
        Het/hom ratio, or None if n_hom_var is 0
    """
    if n_hom_var == 0:
        return None
    return n_het / n_hom_var


def classify_transition_transversion(ref: str, alt: str) -> str | None:
    """Classify a SNP as transition or transversion.

    Args:
        ref: Reference allele
        alt: Alternate allele

    Returns:
        "transition", "transversion", or None for non-SNPs
    """
    ref = ref.upper()
    alt = alt.upper()

    if len(ref) != 1 or len(alt) != 1:
        return None

    if ref == alt:
        return None

    if (ref, alt) in TRANSITIONS:
        return "transition"

    return "transversion"


def compute_ti_tv_ratio(transitions: int, transversions: int) -> float | None:
    """Compute transition to transversion ratio.

    Args:
        transitions: Number of transitions
        transversions: Number of transversions

    Returns:
        Ti/Tv ratio, or None if transversions is 0
    """
    if transversions == 0:
        return None
    return transitions / transversions


def infer_sex_from_x_het(
    x_het_rate: float,
    male_threshold: float | None = None,
    female_threshold: float | None = None,
) -> str:
    """Infer genetic sex from X chromosome heterozygosity rate.

    Males (XY) should have very low X chromosome heterozygosity
    since they only have one X chromosome.
    Females (XX) should have typical heterozygosity rates.

    Args:
        x_het_rate: Heterozygosity rate on X chromosome
        male_threshold: Max het rate to infer male (default: 0.05)
        female_threshold: Min het rate to infer female (default: 0.15)

    Returns:
        "M" for male, "F" for female, "unknown" for ambiguous
    """
    if male_threshold is None:
        male_threshold = DEFAULT_QC_CONFIG.male_x_het_threshold
    if female_threshold is None:
        female_threshold = DEFAULT_QC_CONFIG.female_x_het_threshold

    if x_het_rate <= male_threshold:
        return "M"
    elif x_het_rate >= female_threshold:
        return "F"
    else:
        return "unknown"


def compute_f_inbreeding(observed_het: float, expected_het: float) -> float:
    """Compute inbreeding coefficient F.

    F = 1 - (observed_het / expected_het)

    Positive F indicates excess homozygosity (inbreeding).
    Negative F indicates excess heterozygosity (possible contamination).

    Args:
        observed_het: Observed heterozygosity count or rate
        expected_het: Expected heterozygosity under HWE

    Returns:
        F coefficient, or NaN if expected_het is 0
    """
    if expected_het == 0:
        return float("nan")
    return 1.0 - (observed_het / expected_het)


def evaluate_qc_pass(
    call_rate: float,
    contamination_estimate: float | None = None,
    sex_concordant: bool | None = None,
    min_call_rate: float | None = None,
    max_contamination: float | None = None,
) -> bool:
    """Evaluate if a sample passes QC criteria.

    Criteria (Pe'er pipeline thresholds by default):
    - call_rate >= min_call_rate (default: 99%)
    - contamination_estimate < max_contamination (default: 2.5%) or NULL
    - sex_concordant = TRUE or NULL

    Args:
        call_rate: Sample call rate
        contamination_estimate: Estimated contamination fraction
        sex_concordant: Whether inferred sex matches reported sex
        min_call_rate: Minimum acceptable call rate (default: 0.99)
        max_contamination: Maximum acceptable contamination (default: 0.025)

    Returns:
        True if sample passes all criteria
    """
    if min_call_rate is None:
        min_call_rate = DEFAULT_QC_CONFIG.min_call_rate
    if max_contamination is None:
        max_contamination = DEFAULT_QC_CONFIG.max_contamination

    if call_rate < min_call_rate:
        return False

    if contamination_estimate is not None and contamination_estimate >= max_contamination:
        return False

    if sex_concordant is not None and not sex_concordant:
        return False

    return True


@dataclass
class SampleQCMetrics:
    """Container for sample-level QC metrics."""

    sample_id: str
    call_rate: float
    n_called: int
    n_snp: int
    n_het: int
    n_hom_var: int
    het_hom_ratio: float | None = None
    ti_tv_ratio: float | None = None
    n_singleton: int | None = None
    f_inbreeding: float | None = None
    mean_dp: float | None = None
    mean_gq: float | None = None
    sex_inferred: str | None = None
    sex_reported: str | None = None
    sex_concordant: bool | None = None
    contamination_estimate: float | None = None
    batch_id: int | None = None
    _extra: dict[str, Any] = field(default_factory=dict)

    def to_db_row(self) -> dict[str, Any]:
        """Convert to dict suitable for database insertion."""
        return {
            "sample_id": self.sample_id,
            "call_rate": self.call_rate,
            "n_called": self.n_called,
            "n_snp": self.n_snp,
            "n_het": self.n_het,
            "n_hom_var": self.n_hom_var,
            "het_hom_ratio": self.het_hom_ratio,
            "ti_tv_ratio": self.ti_tv_ratio,
            "n_singleton": self.n_singleton,
            "f_inbreeding": self.f_inbreeding,
            "mean_dp": self.mean_dp,
            "mean_gq": self.mean_gq,
            "sex_inferred": self.sex_inferred,
            "sex_reported": self.sex_reported,
            "sex_concordant": self.sex_concordant,
            "contamination_estimate": self.contamination_estimate,
            "batch_id": self.batch_id,
        }


class SampleQCComputer:
    """Computes sample QC metrics from loaded variant data."""

    def __init__(
        self,
        schema_manager: SampleQCSchemaManager | None = None,
        config: SampleQCConfig | None = None,
    ):
        self._schema_manager = schema_manager or SampleQCSchemaManager()
        self._config = config or DEFAULT_QC_CONFIG

    async def compute_for_batch(
        self,
        conn: asyncpg.Connection,
        batch_id: int,
        sex_reported: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Compute sample QC metrics for all samples in a batch.

        Args:
            conn: Database connection
            batch_id: Load batch ID to compute QC for
            sex_reported: Optional dict mapping sample_id to reported sex

        Returns:
            Summary statistics for the batch
        """
        sex_reported = sex_reported or {}

        samples = await self._get_samples_in_batch(conn, batch_id)
        if not samples:
            logger.warning("No samples found for batch %d", batch_id)
            return {"samples_processed": 0}

        results = []
        for sample_id in samples:
            metrics = await self._compute_sample_metrics(
                conn, sample_id, batch_id, sex_reported.get(sample_id)
            )
            results.append(metrics)

            await self._schema_manager.upsert_sample_qc(
                conn,
                **metrics.to_db_row(),
            )

        await self._schema_manager.refresh_summary_view(conn)

        n_pass = sum(
            1
            for m in results
            if evaluate_qc_pass(
                m.call_rate,
                m.contamination_estimate,
                m.sex_concordant,
                min_call_rate=self._config.min_call_rate,
                max_contamination=self._config.max_contamination,
            )
        )

        return {
            "samples_processed": len(results),
            "samples_pass": n_pass,
            "samples_fail": len(results) - n_pass,
            "mean_call_rate": sum(m.call_rate for m in results) / len(results) if results else 0,
            "batch_id": batch_id,
        }

    async def compute_for_sample(
        self,
        conn: asyncpg.Connection,
        sample_id: str,
        batch_id: int | None = None,
        sex_reported: str | None = None,
    ) -> SampleQCMetrics:
        """Compute QC metrics for a single sample.

        Args:
            conn: Database connection
            sample_id: Sample identifier
            batch_id: Optional batch ID
            sex_reported: Optional reported sex

        Returns:
            SampleQCMetrics for the sample
        """
        metrics = await self._compute_sample_metrics(conn, sample_id, batch_id, sex_reported)

        await self._schema_manager.upsert_sample_qc(
            conn,
            **metrics.to_db_row(),
        )

        return metrics

    async def _get_samples_in_batch(self, conn: asyncpg.Connection, batch_id: int) -> list[str]:
        """Get list of unique sample IDs in a batch."""
        rows = await conn.fetch(
            """
            SELECT DISTINCT sample_id
            FROM variants
            WHERE load_batch_id = (
                SELECT load_batch_id FROM variant_load_audit
                WHERE audit_id = $1
            )
            AND sample_id IS NOT NULL
            """,
            batch_id,
        )
        return [r["sample_id"] for r in rows]

    async def _compute_sample_metrics(
        self,
        conn: asyncpg.Connection,
        sample_id: str,
        batch_id: int | None,
        sex_reported: str | None,
    ) -> SampleQCMetrics:
        """Compute all QC metrics for a single sample."""
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as n_total,
                COUNT(*) FILTER (WHERE n_het IS NOT NULL OR n_hom_alt IS NOT NULL) as n_called,
                COUNT(*) FILTER (WHERE variant_type = 'snp') as n_snp,
                COALESCE(SUM(n_het), 0) as n_het,
                COALESCE(SUM(n_hom_alt), 0) as n_hom_alt,
                AVG(CASE WHEN info->>'DP' IS NOT NULL THEN (info->>'DP')::float END) as mean_dp,
                AVG(CASE WHEN info->>'GQ' IS NOT NULL THEN (info->>'GQ')::float END) as mean_gq
            FROM variants
            WHERE sample_id = $1
            """,
            sample_id,
        )

        n_total = stats["n_total"]
        n_called = stats["n_called"]
        n_snp = stats["n_snp"]
        n_het = int(stats["n_het"])
        n_hom_var = int(stats["n_hom_alt"])
        mean_dp = stats["mean_dp"]
        mean_gq = stats["mean_gq"]

        call_rate = compute_sample_call_rate(n_called, n_total)
        het_hom_ratio = compute_het_hom_ratio(n_het, n_hom_var)

        ti_tv = await self._compute_ti_tv_for_sample(conn, sample_id)
        ti_tv_ratio = compute_ti_tv_ratio(ti_tv["transitions"], ti_tv["transversions"])

        x_stats = await self._compute_x_chromosome_stats(conn, sample_id)
        sex_inferred = infer_sex_from_x_het(
            x_stats["x_het_rate"],
            male_threshold=self._config.male_x_het_threshold,
            female_threshold=self._config.female_x_het_threshold,
        )

        sex_concordant = None
        if sex_reported and sex_inferred != "unknown":
            sex_concordant = sex_inferred == sex_reported.upper()[0]

        expected_het = await self._compute_expected_het(conn, sample_id)
        f_inbreeding = compute_f_inbreeding(n_het, expected_het) if expected_het else None

        n_singleton = await self._count_singletons(conn, sample_id)

        return SampleQCMetrics(
            sample_id=sample_id,
            call_rate=call_rate,
            n_called=n_called,
            n_snp=n_snp,
            n_het=n_het,
            n_hom_var=n_hom_var,
            het_hom_ratio=het_hom_ratio,
            ti_tv_ratio=ti_tv_ratio,
            n_singleton=n_singleton,
            f_inbreeding=f_inbreeding,
            mean_dp=mean_dp,
            mean_gq=mean_gq,
            sex_inferred=sex_inferred,
            sex_reported=sex_reported,
            sex_concordant=sex_concordant,
            batch_id=batch_id,
        )

    async def _compute_ti_tv_for_sample(
        self, conn: asyncpg.Connection, sample_id: str
    ) -> dict[str, int]:
        """Count transitions and transversions for a sample."""
        rows = await conn.fetch(
            """
            SELECT ref, alt
            FROM variants
            WHERE sample_id = $1
            AND LENGTH(ref) = 1 AND LENGTH(alt) = 1
            AND n_het > 0 OR n_hom_alt > 0
            """,
            sample_id,
        )

        transitions = 0
        transversions = 0
        for row in rows:
            classification = classify_transition_transversion(row["ref"], row["alt"])
            if classification == "transition":
                transitions += 1
            elif classification == "transversion":
                transversions += 1

        return {"transitions": transitions, "transversions": transversions}

    async def _compute_x_chromosome_stats(
        self, conn: asyncpg.Connection, sample_id: str
    ) -> dict[str, float]:
        """Compute X chromosome heterozygosity statistics."""
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as n_total,
                COUNT(*) FILTER (WHERE n_het > 0) as n_het
            FROM variants
            WHERE sample_id = $1
            AND chrom IN ('chrX', 'X')
            AND pos > $2 AND pos < $3
            """,
            sample_id,
            self._config.x_par_start,
            self._config.x_par_end,
        )

        n_total = stats["n_total"] or 0
        n_het = stats["n_het"] or 0

        x_het_rate = n_het / n_total if n_total > 0 else 0.0
        return {"x_het_rate": x_het_rate, "x_n_total": n_total, "x_n_het": n_het}

    async def _compute_expected_het(self, conn: asyncpg.Connection, sample_id: str) -> float:
        """Compute expected heterozygosity under HWE."""
        result = await conn.fetchval(
            """
            SELECT SUM(2 * maf * (1 - maf))
            FROM variants
            WHERE sample_id = $1
            AND maf IS NOT NULL
            """,
            sample_id,
        )
        return float(result) if result else 0.0

    async def _count_singletons(self, conn: asyncpg.Connection, sample_id: str) -> int:
        """Count singleton variants (MAC = 1) for a sample."""
        result = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM variants
            WHERE sample_id = $1
            AND mac = 1
            AND (n_het > 0 OR n_hom_alt > 0)
            """,
            sample_id,
        )
        return result or 0
