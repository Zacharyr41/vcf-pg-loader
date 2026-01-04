"""PGS Catalog score loader for PostgreSQL."""

import logging
from pathlib import Path
from typing import TypedDict

import asyncpg

from .pgs_catalog import (
    PGSCatalogParser,
    validate_genome_build,
)
from .schema import PRSSchemaManager

logger = logging.getLogger(__name__)


class PGSImportResult(TypedDict):
    """Result of PGS import operation."""

    pgs_id: str
    weights_imported: int
    weights_matched: int
    weights_unmatched: int


class PGSLoader:
    """Load PGS Catalog scores into PostgreSQL."""

    def __init__(self, batch_size: int = 10000):
        self.batch_size = batch_size
        self.schema_manager = PRSSchemaManager()

    async def import_pgs(
        self,
        conn: asyncpg.Connection,
        pgs_path: Path,
        pgs_id_override: str | None = None,
        validate_build: bool = False,
    ) -> PGSImportResult:
        """Import PGS Catalog scoring file.

        Args:
            conn: Database connection
            pgs_path: Path to PGS Catalog format scoring file
            pgs_id_override: Override PGS ID from file header
            validate_build: Whether to validate genome build against database

        Returns:
            PGSImportResult with import statistics

        Raises:
            GenomeBuildMismatchError: If validate_build=True and builds don't match
        """
        parser = PGSCatalogParser(pgs_path)
        metadata = parser.metadata

        pgs_id = pgs_id_override or metadata.pgs_id

        if validate_build:
            db_build = await self._get_database_build(conn)
            if db_build:
                validate_genome_build(metadata.genome_build, db_build)

        await self.schema_manager.create_score(
            conn,
            pgs_id=pgs_id,
            trait_name=metadata.trait_name,
            trait_ontology_id=metadata.trait_ontology_id,
            publication_pmid=metadata.publication_pmid,
            n_variants=metadata.n_variants,
            genome_build=metadata.genome_build,
            weight_type=metadata.weight_type,
            reporting_ancestry=metadata.reporting_ancestry,
        )
        logger.info(f"Created/updated PGS score: {pgs_id}")

        await conn.execute(
            "DELETE FROM prs_weights WHERE pgs_id = $1",
            pgs_id,
        )

        variant_lookup, rsid_lookup = await self._build_variant_lookups(conn)

        weights_imported = 0
        weights_matched = 0
        weights_unmatched = 0
        batch = []

        for weight in parser.iter_weights():
            variant_id = self._match_variant(
                weight.chromosome,
                weight.position,
                weight.effect_allele,
                weight.other_allele,
                weight.rsid,
                variant_lookup,
                rsid_lookup,
            )

            if variant_id is not None:
                weights_matched += 1
            else:
                weights_unmatched += 1

            batch.append(
                (
                    variant_id,
                    pgs_id,
                    weight.effect_allele,
                    weight.effect_weight,
                    weight.is_interaction,
                    weight.is_haplotype,
                    weight.is_dominant,
                    weight.is_recessive,
                    weight.allele_frequency,
                    weight.locus_name,
                    weight.chromosome,
                    weight.position,
                    weight.rsid,
                    weight.other_allele,
                )
            )

            if len(batch) >= self.batch_size:
                await self._insert_batch(conn, batch)
                weights_imported += len(batch)
                batch = []

        if batch:
            await self._insert_batch(conn, batch)
            weights_imported += len(batch)

        logger.info(
            f"Imported {weights_imported} weights for PGS {pgs_id} "
            f"(matched: {weights_matched}, unmatched: {weights_unmatched})"
        )

        return PGSImportResult(
            pgs_id=pgs_id,
            weights_imported=weights_imported,
            weights_matched=weights_matched,
            weights_unmatched=weights_unmatched,
        )

    async def _get_database_build(self, conn: asyncpg.Connection) -> str | None:
        """Get genome build from most recent load audit."""
        row = await conn.fetchrow("""
            SELECT reference_genome
            FROM variant_load_audit
            WHERE status = 'completed'
            ORDER BY load_completed_at DESC NULLS LAST
            LIMIT 1
        """)
        return row["reference_genome"] if row else None

    async def _build_variant_lookups(
        self, conn: asyncpg.Connection
    ) -> tuple[dict[tuple[str, int, str, str], int], dict[str, int]]:
        """Build lookup dictionaries for variant matching."""
        variant_lookup: dict[tuple[str, int, str, str], int] = {}
        rsid_lookup: dict[str, int] = {}

        rows = await conn.fetch("""
            SELECT variant_id, chrom, pos, ref, alt, rs_id
            FROM variants
        """)

        for row in rows:
            chrom = str(row["chrom"])
            chrom_normalized = chrom.replace("chr", "")

            key = (chrom_normalized, row["pos"], row["ref"].upper(), row["alt"].upper())
            variant_lookup[key] = row["variant_id"]

            key_with_chr = (chrom, row["pos"], row["ref"].upper(), row["alt"].upper())
            variant_lookup[key_with_chr] = row["variant_id"]

            if row["rs_id"]:
                rsid_lookup[row["rs_id"]] = row["variant_id"]

        return variant_lookup, rsid_lookup

    def _match_variant(
        self,
        chromosome: str | None,
        position: int | None,
        effect_allele: str,
        other_allele: str | None,
        rsid: str | None,
        variant_lookup: dict[tuple[str, int, str, str], int],
        rsid_lookup: dict[str, int],
    ) -> int | None:
        """Match a PRS weight to the variants table."""
        if chromosome and position:
            chrom = chromosome.replace("chr", "")

            if other_allele:
                key1 = (chrom, position, other_allele.upper(), effect_allele.upper())
                if key1 in variant_lookup:
                    return variant_lookup[key1]

                key2 = (chrom, position, effect_allele.upper(), other_allele.upper())
                if key2 in variant_lookup:
                    return variant_lookup[key2]

        if rsid and rsid in rsid_lookup:
            return rsid_lookup[rsid]

        return None

    async def _insert_batch(self, conn: asyncpg.Connection, batch: list[tuple]) -> None:
        """Insert a batch of PRS weights."""
        await conn.executemany(
            """
            INSERT INTO prs_weights (
                variant_id, pgs_id, effect_allele, effect_weight,
                is_interaction, is_haplotype, is_dominant, is_recessive,
                allele_frequency, locus_name, chr_name, chr_position,
                rsid, other_allele
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            """,
            batch,
        )
