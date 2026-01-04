"""HapMap3 reference panel loader for PRS analysis.

HapMap3 SNPs (~1.1-1.3 million) are used by PRS-CS, LDpred2, and most
Bayesian PRS methods because they have reliable LD estimates across populations.
"""

import csv
import gzip
import logging
from pathlib import Path
from typing import TypedDict

import asyncpg

logger = logging.getLogger(__name__)


class HapMap3LoadResult(TypedDict):
    """Result of HapMap3 reference panel loading."""

    panel_name: str
    variants_loaded: int
    build: str


class HapMap3MatchResult(TypedDict):
    """Result of matching a variant to HapMap3."""

    rsid: str | None
    a1: str
    a2: str


def normalize_chrom(chrom: str) -> str:
    """Normalize chromosome to bare format (no 'chr' prefix)."""
    if chrom.startswith("chr"):
        return chrom[3:]
    return chrom


def complement_allele(allele: str) -> str:
    """Return the complement of a nucleotide allele."""
    complements = {"A": "T", "T": "A", "C": "G", "G": "C"}
    return complements.get(allele.upper(), allele)


def is_strand_ambiguous(allele1: str, allele2: str) -> bool:
    """Check if a SNP is strand-ambiguous (A/T or C/G)."""
    pair = frozenset([allele1.upper(), allele2.upper()])
    return pair in (frozenset(["A", "T"]), frozenset(["C", "G"]))


def match_hapmap3_variant(
    lookup: dict[tuple[str, int], list[dict]],
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
) -> HapMap3MatchResult | None:
    """Match a variant against the HapMap3 lookup table.

    Handles:
    - Exact allele match
    - Allele flip (ref/alt swapped)
    - Strand complement for non-ambiguous SNPs

    Args:
        lookup: Dict mapping (chrom, pos) to list of HapMap3 entries
        chrom: Chromosome (with or without 'chr' prefix)
        pos: Genomic position
        ref: Reference allele
        alt: Alternate allele

    Returns:
        HapMap3MatchResult if matched, None otherwise
    """
    norm_chrom = normalize_chrom(chrom)
    key = (norm_chrom, pos)

    if key not in lookup:
        return None

    ref_upper = ref.upper()
    alt_upper = alt.upper()

    for entry in lookup[key]:
        a1 = entry["a1"].upper()
        a2 = entry["a2"].upper()

        if {ref_upper, alt_upper} == {a1, a2}:
            return HapMap3MatchResult(
                rsid=entry.get("rsid"),
                a1=entry["a1"],
                a2=entry["a2"],
            )

        if not is_strand_ambiguous(ref_upper, alt_upper):
            ref_comp = complement_allele(ref_upper)
            alt_comp = complement_allele(alt_upper)
            if {ref_comp, alt_comp} == {a1, a2}:
                return HapMap3MatchResult(
                    rsid=entry.get("rsid"),
                    a1=entry["a1"],
                    a2=entry["a2"],
                )

    return None


class HapMap3Loader:
    """Load HapMap3 reference panel data into PostgreSQL."""

    def __init__(self, batch_size: int = 10000):
        self.batch_size = batch_size

    async def load_reference_panel(
        self,
        conn: asyncpg.Connection,
        tsv_path: Path,
        build: str = "grch38",
    ) -> HapMap3LoadResult:
        """Load HapMap3 reference panel from TSV file.

        Expected TSV format (from LDpred2):
        rsid    chrom    position    a1    a2

        Args:
            conn: Database connection
            tsv_path: Path to TSV file (can be gzipped)
            build: Genome build (grch37 or grch38)

        Returns:
            HapMap3LoadResult with loading statistics
        """
        panel_name = f"hapmap3_{build.lower()}"

        await conn.execute(
            "DELETE FROM reference_panels WHERE panel_name = $1",
            panel_name,
        )

        variants_loaded = 0
        batch = []

        open_func = gzip.open if str(tsv_path).endswith(".gz") else open
        mode = "rt" if str(tsv_path).endswith(".gz") else "r"

        with open_func(tsv_path, mode) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                batch.append(
                    (
                        panel_name,
                        row.get("rsid"),
                        normalize_chrom(row["chrom"]),
                        int(row["position"]),
                        row["a1"],
                        row["a2"],
                    )
                )

                if len(batch) >= self.batch_size:
                    await self._insert_batch(conn, batch)
                    variants_loaded += len(batch)
                    batch = []

            if batch:
                await self._insert_batch(conn, batch)
                variants_loaded += len(batch)

        logger.info(
            "Loaded %d HapMap3 variants for %s from %s",
            variants_loaded,
            panel_name,
            tsv_path.name,
        )

        return HapMap3LoadResult(
            panel_name=panel_name,
            variants_loaded=variants_loaded,
            build=build,
        )

    async def _insert_batch(
        self,
        conn: asyncpg.Connection,
        batch: list[tuple],
    ) -> None:
        """Insert a batch of reference panel entries."""
        await conn.executemany(
            """
            INSERT INTO reference_panels (panel_name, rsid, chrom, position, a1, a2)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (panel_name, chrom, position, a1, a2) DO NOTHING
            """,
            batch,
        )

    async def build_lookup(
        self,
        conn: asyncpg.Connection,
        panel_name: str = "hapmap3_grch38",
    ) -> dict[tuple[str, int], list[dict]]:
        """Build in-memory lookup dictionary for fast variant matching.

        Args:
            conn: Database connection
            panel_name: Reference panel name to load

        Returns:
            Dict mapping (chrom, position) to list of reference entries
        """
        rows = await conn.fetch(
            """
            SELECT rsid, chrom, position, a1, a2
            FROM reference_panels
            WHERE panel_name = $1
            """,
            panel_name,
        )

        lookup: dict[tuple[str, int], list[dict]] = {}
        for row in rows:
            key = (row["chrom"], row["position"])
            entry = {
                "rsid": row["rsid"],
                "a1": row["a1"],
                "a2": row["a2"],
            }
            if key not in lookup:
                lookup[key] = []
            lookup[key].append(entry)

        logger.debug(
            "Built HapMap3 lookup with %d positions from %s",
            len(lookup),
            panel_name,
        )

        return lookup
