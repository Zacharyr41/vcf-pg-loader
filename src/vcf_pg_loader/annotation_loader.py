"""Loader for annotation reference databases."""
import logging
from pathlib import Path
from typing import TypedDict

import asyncpg
from cyvcf2 import VCF

from .annotation_config import AnnotationFieldConfig
from .annotation_schema import AnnotationSchemaManager

logger = logging.getLogger(__name__)


class AnnotationLoadResult(TypedDict):
    """Result of loading an annotation source."""
    source_name: str
    variants_loaded: int
    table_name: str


class AnnotationLoader:
    """Load population databases as annotation reference tables."""

    def __init__(
        self,
        human_genome: bool = True,
        batch_size: int = 10000,
        unlogged: bool = False,
    ):
        self.human_genome = human_genome
        self.batch_size = batch_size
        self.unlogged = unlogged
        self.schema_manager = AnnotationSchemaManager(
            human_genome=human_genome,
            unlogged=unlogged,
        )

    async def load_annotation_source(
        self,
        vcf_path: Path,
        source_name: str,
        field_config: list[AnnotationFieldConfig],
        conn: asyncpg.Connection,
        version: str | None = None,
        source_type: str | None = None,
    ) -> AnnotationLoadResult:
        """Load a VCF file as an annotation reference source.

        Args:
            vcf_path: Path to the VCF file
            source_name: Name for this annotation source
            field_config: Field configuration for extraction
            conn: Database connection
            version: Version string for the source
            source_type: Type of annotation (population, pathogenicity, etc.)

        Returns:
            AnnotationLoadResult with loading statistics
        """
        await self.schema_manager.create_annotation_registry(conn)

        table_name = await self.schema_manager.create_annotation_source_table(
            conn, source_name, field_config
        )

        await self.schema_manager.register_source(
            conn,
            source_name,
            field_config,
            vcf_path=str(vcf_path),
            source_type=source_type,
            version=version,
        )

        variant_count = await self._load_variants(
            vcf_path, table_name, field_config, conn
        )

        await self.schema_manager.create_variant_lookup_index(conn, source_name)

        await self.schema_manager.update_variant_count(conn, source_name, variant_count)

        logger.info(f"Loaded {variant_count} variants into {table_name}")

        return AnnotationLoadResult(
            source_name=source_name,
            variants_loaded=variant_count,
            table_name=table_name,
        )

    async def _load_variants(
        self,
        vcf_path: Path,
        table_name: str,
        field_config: list[AnnotationFieldConfig],
        conn: asyncpg.Connection,
    ) -> int:
        """Load variants from VCF into the annotation table.

        Args:
            vcf_path: Path to the VCF file
            table_name: Name of the target table
            field_config: Field configuration
            conn: Database connection

        Returns:
            Number of variants loaded
        """
        vcf = VCF(str(vcf_path))
        batch = []
        total_count = 0

        columns = ["chrom", "pos", "ref", "alt"] + [f.alias for f in field_config]

        for variant in vcf:
            for alt_idx, alt in enumerate(variant.ALT):
                if alt is None:
                    continue

                if self.human_genome:
                    chrom = f"chr{variant.CHROM.replace('chr', '')}"
                else:
                    chrom = variant.CHROM

                row = [chrom, variant.POS, variant.REF, alt]

                for field_cfg in field_config:
                    value = self._extract_field_value(variant, field_cfg, alt_idx)
                    row.append(value)

                batch.append(tuple(row))

                if len(batch) >= self.batch_size:
                    await self._copy_batch(conn, table_name, columns, batch)
                    total_count += len(batch)
                    batch = []

        if batch:
            await self._copy_batch(conn, table_name, columns, batch)
            total_count += len(batch)

        vcf.close()
        return total_count

    async def _copy_batch(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        columns: list[str],
        batch: list[tuple],
    ) -> None:
        """Copy a batch of records using PostgreSQL COPY protocol."""
        staging_table = f"staging_{table_name}"
        column_list = ", ".join(columns)

        await conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
        await conn.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS)")

        await conn.copy_records_to_table(
            staging_table,
            records=batch,
            columns=columns,
        )

        await conn.execute(f"""
            INSERT INTO {table_name} ({column_list})
            SELECT {column_list} FROM {staging_table}
            ON CONFLICT (chrom, pos, ref, alt) DO NOTHING
        """)

        await conn.execute(f"DROP TABLE {staging_table}")

    def _extract_field_value(
        self,
        variant,
        field_cfg: AnnotationFieldConfig,
        alt_idx: int,
    ):
        """Extract a field value from a variant.

        Args:
            variant: cyvcf2 Variant object
            field_cfg: Field configuration
            alt_idx: Index of the alternate allele

        Returns:
            Extracted value (may be None)
        """
        if field_cfg.is_special_field():
            if field_cfg.field.upper() == "FILTER":
                if variant.FILTER is None:
                    return field_cfg.missing_string or "PASS"
                return variant.FILTER
            return None

        try:
            value = variant.INFO.get(field_cfg.field)
        except KeyError:
            return field_cfg.missing_value if field_cfg.field_type != "String" else field_cfg.missing_string

        if value is None:
            return field_cfg.missing_value if field_cfg.field_type != "String" else field_cfg.missing_string

        if isinstance(value, (list, tuple)):
            if alt_idx < len(value):
                value = value[alt_idx]
            else:
                value = value[0] if value else None

        if field_cfg.field_type == "Float" and value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                return field_cfg.missing_value

        if field_cfg.field_type == "Integer" and value is not None:
            try:
                return int(value)
            except (ValueError, TypeError):
                return field_cfg.missing_value

        return value
