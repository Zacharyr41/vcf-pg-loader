"""Genotype data loading with support for hard calls and dosages.

Supports:
- FORMAT field parsing (GT, GQ, DP, AD, DS, GP)
- Allele balance computation
- Dosage computation from GP when DS missing
- ADJ filter criteria (GQ>=20, DP>=10, AB>=0.2 for hets)

Reference: gnomAD ADJ filter for high-quality genotype calls.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
from cyvcf2 import VCF

logger = logging.getLogger(__name__)

HET_GENOTYPES = {"0/1", "0|1", "1|0", "1/0"}


@dataclass
class GenotypeRecord:
    """Container for a single genotype record."""

    variant_id: int
    sample_id: int
    gt: str
    phased: bool = False
    gq: int | None = None
    dp: int | None = None
    ad: list[int] | None = None
    dosage: float | None = None
    gp: list[float] | None = None
    allele_balance: float | None = None

    def to_db_row(self) -> dict[str, Any]:
        """Convert to dict suitable for database insertion."""
        return {
            "variant_id": self.variant_id,
            "sample_id": self.sample_id,
            "gt": self.gt,
            "phased": self.phased,
            "gq": self.gq,
            "dp": self.dp,
            "ad": self.ad,
            "dosage": self.dosage,
            "gp": self.gp,
            "allele_balance": self.allele_balance,
        }


def parse_genotype_fields(
    gt: str,
    gq: int | None,
    dp: int | None,
    ad: list[int] | None,
    ds: float | None,
    gp: list[float] | None,
) -> GenotypeRecord:
    """Parse genotype FORMAT fields into a GenotypeRecord.

    Args:
        gt: Genotype string (e.g., "0/1", "0|1")
        gq: Genotype quality
        dp: Read depth
        ad: Allelic depths
        ds: Dosage (if provided)
        gp: Genotype probabilities [P(RR), P(RA), P(AA)]

    Returns:
        GenotypeRecord with parsed fields
    """
    phased = "|" in gt

    allele_balance = compute_allele_balance(ad)

    dosage = ds
    if dosage is None and gp is not None:
        dosage = dosage_from_gp(gp)

    return GenotypeRecord(
        variant_id=0,
        sample_id=0,
        gt=gt,
        phased=phased,
        gq=gq,
        dp=dp,
        ad=ad,
        dosage=dosage,
        gp=gp,
        allele_balance=allele_balance,
    )


def compute_allele_balance(ad: list[int] | None) -> float | None:
    """Compute allele balance from allelic depths.

    Allele balance = ALT depth / total depth

    Args:
        ad: Allelic depths [REF, ALT, ...] for each allele

    Returns:
        Allele balance as float, or None if AD is missing or zero total
    """
    if ad is None or len(ad) < 2:
        return None

    total = sum(ad)
    if total == 0:
        return None

    alt_depth = sum(ad[1:])
    return alt_depth / total


def dosage_from_gp(gp: list[float] | None) -> float | None:
    """Compute dosage from genotype probabilities.

    Dosage = P(RA) + 2 * P(AA)

    Where GP is [P(RR), P(RA), P(AA)]

    Args:
        gp: Genotype probabilities [P(RR), P(RA), P(AA)]

    Returns:
        Dosage as float, or None if GP is missing or invalid
    """
    if gp is None or len(gp) != 3:
        return None

    return gp[1] + 2 * gp[2]


def evaluate_adj_filter(
    gt: str,
    gq: int | None,
    dp: int | None,
    allele_balance: float | None,
) -> bool:
    """Evaluate gnomAD ADJ filter criteria.

    ADJ criteria:
    - GQ >= 20 (or missing)
    - DP >= 10 (or missing)
    - AB >= 0.2 for heterozygotes (or missing)

    Args:
        gt: Genotype string
        gq: Genotype quality
        dp: Read depth
        allele_balance: Computed allele balance

    Returns:
        True if genotype passes ADJ filter
    """
    if gq is not None and gq < 20:
        return False

    if dp is not None and dp < 10:
        return False

    if gt in HET_GENOTYPES:
        if allele_balance is not None and allele_balance < 0.2:
            return False

    return True


def validate_dosage(dosage: float | None) -> bool:
    """Validate dosage is within valid range [0, 2].

    Args:
        dosage: Dosage value

    Returns:
        True if dosage is valid (None or 0-2)
    """
    if dosage is None:
        return True
    return 0 <= dosage <= 2


def get_partition_number(sample_id: int, num_partitions: int = 16) -> int:
    """Get the hash partition number for a sample_id.

    Uses PostgreSQL-compatible hash function.

    Args:
        sample_id: Sample ID (integer)
        num_partitions: Number of partitions

    Returns:
        Partition number (0 to num_partitions-1)
    """
    return sample_id % num_partitions


class GenotypeLoader:
    """Loads genotype data from VCF files into PostgreSQL."""

    def __init__(
        self,
        adj_filter: bool = False,
        dosage_only: bool = False,
        batch_size: int = 10000,
    ):
        """Initialize genotype loader.

        Args:
            adj_filter: Only store genotypes passing ADJ criteria
            dosage_only: Store only dosage, not hard calls
            batch_size: Number of records per batch insert
        """
        self.adj_filter = adj_filter
        self.dosage_only = dosage_only
        self.batch_size = batch_size

    async def load_from_vcf(
        self,
        conn: asyncpg.Connection,
        vcf_path: Path,
        variant_id_start: int = 1,
        sample_id_map: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """Load genotypes from a VCF file.

        Args:
            conn: Database connection
            vcf_path: Path to VCF file
            variant_id_start: Starting variant_id
            sample_id_map: Optional mapping from sample names to sample_ids

        Returns:
            Statistics about loaded genotypes
        """
        vcf = VCF(str(vcf_path))
        samples = vcf.samples

        if sample_id_map is None:
            sample_id_map = await self._get_sample_id_map(conn, samples)

        records: list[GenotypeRecord] = []
        variant_id = variant_id_start
        total_loaded = 0
        total_skipped = 0

        for variant in vcf:
            gt_array = variant.genotypes
            gq_array = self._safe_format(variant, "GQ")
            dp_array = self._safe_format(variant, "DP")
            ad_array = self._safe_format_2d(variant, "AD")
            ds_array = self._safe_format(variant, "DS")
            gp_array = self._safe_format_2d(variant, "GP")

            for sample_idx, sample_name in enumerate(samples):
                sample_id = sample_id_map.get(sample_name)
                if sample_id is None:
                    continue

                gt_data = gt_array[sample_idx] if gt_array else None
                if gt_data is None:
                    continue

                gt = self._format_gt(gt_data)
                gq = self._safe_get(gq_array, sample_idx)
                dp = self._safe_get(dp_array, sample_idx)
                ad = self._safe_get_list(ad_array, sample_idx)
                ds = self._safe_get(ds_array, sample_idx)
                gp = self._safe_get_list(gp_array, sample_idx)

                allele_balance = compute_allele_balance(ad)

                if self.adj_filter:
                    if not evaluate_adj_filter(gt, gq, dp, allele_balance):
                        total_skipped += 1
                        continue

                dosage = ds
                if dosage is None and gp is not None:
                    dosage = dosage_from_gp(gp)

                if self.dosage_only:
                    gt = "."
                    gq = None
                    dp = None
                    ad = None
                    allele_balance = None

                record = GenotypeRecord(
                    variant_id=variant_id,
                    sample_id=sample_id,
                    gt=gt,
                    phased="|" in (gt if gt != "." else ""),
                    gq=gq,
                    dp=dp,
                    ad=ad,
                    dosage=dosage,
                    gp=gp,
                    allele_balance=allele_balance,
                )
                records.append(record)

                if len(records) >= self.batch_size:
                    await self._insert_batch(conn, records)
                    total_loaded += len(records)
                    records = []

            variant_id += 1

        if records:
            await self._insert_batch(conn, records)
            total_loaded += len(records)

        vcf.close()

        return {
            "genotypes_loaded": total_loaded,
            "genotypes_skipped": total_skipped,
            "variants_processed": variant_id - variant_id_start,
            "samples_processed": len(samples),
        }

    async def _get_sample_id_map(
        self, conn: asyncpg.Connection, sample_names: list[str]
    ) -> dict[str, int]:
        """Get mapping from sample names to sample_ids."""
        rows = await conn.fetch(
            """
            SELECT external_id, sample_id FROM samples
            WHERE external_id = ANY($1)
            """,
            sample_names,
        )
        return {r["external_id"]: r["sample_id"] for r in rows}

    async def _insert_batch(self, conn: asyncpg.Connection, records: list[GenotypeRecord]) -> None:
        """Insert a batch of genotype records."""
        await conn.executemany(
            """
            INSERT INTO genotypes (
                variant_id, sample_id, gt, phased, gq, dp, ad, dosage, gp, allele_balance
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (variant_id, sample_id) DO UPDATE SET
                gt = EXCLUDED.gt,
                phased = EXCLUDED.phased,
                gq = EXCLUDED.gq,
                dp = EXCLUDED.dp,
                ad = EXCLUDED.ad,
                dosage = EXCLUDED.dosage,
                gp = EXCLUDED.gp,
                allele_balance = EXCLUDED.allele_balance
            """,
            [
                (
                    r.variant_id,
                    r.sample_id,
                    r.gt,
                    r.phased,
                    r.gq,
                    r.dp,
                    r.ad,
                    r.dosage,
                    r.gp,
                    r.allele_balance,
                )
                for r in records
            ],
        )

    def _format_gt(self, gt_data: list) -> str:
        """Format genotype data from cyvcf2 to string."""
        if len(gt_data) < 3:
            return "./."

        a1, a2, phased = gt_data[0], gt_data[1], gt_data[2]

        if a1 < 0:
            a1_str = "."
        else:
            a1_str = str(a1)

        if a2 < 0:
            a2_str = "."
        else:
            a2_str = str(a2)

        sep = "|" if phased else "/"
        return f"{a1_str}{sep}{a2_str}"

    def _safe_format(self, variant, field: str) -> list | None:
        """Safely get FORMAT field array."""
        try:
            return variant.format(field)
        except KeyError:
            return None

    def _safe_format_2d(self, variant, field: str) -> list | None:
        """Safely get 2D FORMAT field array (like AD, GP)."""
        try:
            return variant.format(field)
        except KeyError:
            return None

    def _safe_get(self, array, idx: int) -> int | float | None:
        """Safely get value from array."""
        if array is None:
            return None
        try:
            val = array[idx]
            if hasattr(val, "__len__"):
                val = val[0]
            if val < 0 or val == -2147483648:
                return None
            return int(val) if isinstance(val, int | float) and val == int(val) else float(val)
        except (IndexError, TypeError):
            return None

    def _safe_get_list(self, array, idx: int) -> list | None:
        """Safely get list value from 2D array."""
        if array is None:
            return None
        try:
            val = array[idx]
            if val is None:
                return None
            result = [int(v) if v >= 0 else None for v in val]
            if all(v is None for v in result):
                return None
            return [v if v is not None else 0 for v in result]
        except (IndexError, TypeError):
            return None
