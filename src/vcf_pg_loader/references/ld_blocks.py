"""LD block loader for PRS analysis (Berisa & Pickrell 2016).

LD blocks partition the genome into ~1,703 largely independent regions.
These are used by:
- PRS-CS: Gibbs sampler updates effects within blocks
- SBayesR: Similar block-wise updates
- LDpred2: Optional block-based LD matrix computation

Block definitions are population-specific (EUR, AFR, EAS, SAS) due to
different LD patterns across ancestries.
"""

import csv
import gzip
import logging
from pathlib import Path
from typing import TypedDict

import asyncpg

logger = logging.getLogger(__name__)


class LDBlockLoadResult(TypedDict):
    """Result of LD block loading."""

    blocks_loaded: int
    population: str
    build: str
    source: str


def normalize_chrom_for_ld(chrom: str) -> str:
    """Normalize chromosome to bare format (no 'chr' prefix)."""
    if chrom.startswith("chr"):
        return chrom[3:]
    return chrom


class LDBlockLoader:
    """Load LD block definitions into PostgreSQL."""

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size

    async def load_berisa_pickrell_blocks(
        self,
        conn: asyncpg.Connection,
        bed_path: Path,
        population: str,
        build: str = "grch37",
        source: str = "Berisa_Pickrell_2016",
    ) -> LDBlockLoadResult:
        """Load LD blocks from BED file.

        Expected BED format:
        chrom    start    end    block_id    n_snps_1kg

        Args:
            conn: Database connection
            bed_path: Path to BED file (can be gzipped)
            population: Population code (EUR, AFR, EAS, SAS)
            build: Genome build (grch37 or grch38)
            source: Block definition source

        Returns:
            LDBlockLoadResult with loading statistics
        """
        population = population.upper()
        build_normalized = build.upper()
        if build_normalized.startswith("GRCH"):
            build_normalized = f"GRCh{build_normalized[4:]}"

        await conn.execute(
            """
            DELETE FROM ld_blocks
            WHERE population = $1 AND genome_build = $2 AND source = $3
            """,
            population,
            build_normalized,
            source,
        )

        blocks_loaded = 0
        batch = []

        open_func = gzip.open if str(bed_path).endswith(".gz") else open
        mode = "rt" if str(bed_path).endswith(".gz") else "r"

        with open_func(bed_path, mode) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                chrom = normalize_chrom_for_ld(row["chrom"])
                start_pos = int(row["start"])
                end_pos = int(row["end"])
                n_snps = int(row.get("n_snps_1kg", 0)) if row.get("n_snps_1kg") else None

                batch.append(
                    (
                        chrom,
                        start_pos,
                        end_pos,
                        population,
                        source,
                        build_normalized,
                        n_snps,
                    )
                )

                if len(batch) >= self.batch_size:
                    await self._insert_batch(conn, batch)
                    blocks_loaded += len(batch)
                    batch = []

            if batch:
                await self._insert_batch(conn, batch)
                blocks_loaded += len(batch)

        logger.info(
            "Loaded %d LD blocks for %s (%s) from %s",
            blocks_loaded,
            population,
            build_normalized,
            bed_path.name,
        )

        return LDBlockLoadResult(
            blocks_loaded=blocks_loaded,
            population=population,
            build=build,
            source=source,
        )

    async def _insert_batch(
        self,
        conn: asyncpg.Connection,
        batch: list[tuple],
    ) -> None:
        """Insert a batch of LD block entries."""
        await conn.executemany(
            """
            INSERT INTO ld_blocks (chrom, start_pos, end_pos, population, source, genome_build, n_snps_1kg)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (source, population, chrom, start_pos, genome_build) DO UPDATE
            SET end_pos = EXCLUDED.end_pos, n_snps_1kg = EXCLUDED.n_snps_1kg
            """,
            batch,
        )

    async def assign_variants_to_blocks(
        self,
        conn: asyncpg.Connection,
        population: str,
        build: str | None = None,
    ) -> int:
        """Assign variants to LD blocks using range queries.

        Uses efficient range query with GIST index to find blocks containing
        each variant's position.

        Args:
            conn: Database connection
            population: Population code (EUR, AFR, EAS, SAS)
            build: Optional genome build filter

        Returns:
            Number of variants updated
        """
        population = population.upper()

        if build:
            build_normalized = build.upper()
            if build_normalized.startswith("GRCH"):
                build_normalized = f"GRCh{build_normalized[4:]}"

            result = await conn.execute(
                """
                UPDATE variants v
                SET ld_block_id = lb.block_id
                FROM ld_blocks lb
                WHERE lb.population = $1
                  AND lb.genome_build = $2
                  AND lb.chrom = CASE
                      WHEN v.chrom::text LIKE 'chr%' THEN SUBSTRING(v.chrom::text FROM 4)
                      ELSE v.chrom::text
                  END
                  AND int8range(lb.start_pos, lb.end_pos, '[]') @> v.pos
                  AND v.ld_block_id IS NULL
                """,
                population,
                build_normalized,
            )
        else:
            result = await conn.execute(
                """
                UPDATE variants v
                SET ld_block_id = lb.block_id
                FROM ld_blocks lb
                WHERE lb.population = $1
                  AND lb.chrom = CASE
                      WHEN v.chrom::text LIKE 'chr%' THEN SUBSTRING(v.chrom::text FROM 4)
                      ELSE v.chrom::text
                  END
                  AND int8range(lb.start_pos, lb.end_pos, '[]') @> v.pos
                  AND v.ld_block_id IS NULL
                """,
                population,
            )

        updated = int(result.split()[-1])

        logger.info(
            "Assigned %d variants to %s LD blocks",
            updated,
            population,
        )

        return updated

    async def get_block_stats(
        self,
        conn: asyncpg.Connection,
        population: str | None = None,
    ) -> list[dict]:
        """Get statistics for loaded LD blocks.

        Args:
            conn: Database connection
            population: Optional population filter

        Returns:
            List of dicts with block statistics per population
        """
        if population:
            rows = await conn.fetch(
                """
                SELECT
                    population,
                    genome_build,
                    source,
                    COUNT(*) as block_count,
                    MIN(start_pos) as min_start,
                    MAX(end_pos) as max_end,
                    AVG(n_snps_1kg) as avg_snps
                FROM ld_blocks
                WHERE population = $1
                GROUP BY population, genome_build, source
                ORDER BY population, genome_build
                """,
                population.upper(),
            )
        else:
            rows = await conn.fetch("""
                SELECT
                    population,
                    genome_build,
                    source,
                    COUNT(*) as block_count,
                    MIN(start_pos) as min_start,
                    MAX(end_pos) as max_end,
                    AVG(n_snps_1kg) as avg_snps
                FROM ld_blocks
                GROUP BY population, genome_build, source
                ORDER BY population, genome_build
            """)

        return [dict(row) for row in rows]
