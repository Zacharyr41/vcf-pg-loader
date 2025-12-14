"""Variant annotator using SQL JOINs against reference databases."""
import logging
import re
from typing import Any

import asyncpg

from .annotation_schema import AnnotationSchemaManager, validate_identifier
from .expression import FilterExpressionParser

logger = logging.getLogger(__name__)


class VariantAnnotator:
    """Annotate variants using loaded reference databases via SQL JOINs."""

    def __init__(
        self,
        conn: asyncpg.Connection,
        normalize_chr_prefix: bool = True,
    ):
        self.conn = conn
        self.normalize_chr_prefix = normalize_chr_prefix
        self.schema_manager = AnnotationSchemaManager()
        self.expression_parser = FilterExpressionParser()
        self._field_cache: dict[str, list[str]] = {}

    async def annotate_variants(
        self,
        sources: list[str],
        load_batch_id: str | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Annotate variants from the variants table with reference data.

        Args:
            sources: List of annotation source names to join
            load_batch_id: Optional load batch ID to filter variants
            filter_expr: Optional filter expression (echtvar-style)
            limit: Optional limit on number of results

        Returns:
            List of annotated variant dictionaries
        """
        available_fields = await self._get_available_fields(sources)

        query, params = await self._build_annotation_query(
            sources=sources,
            load_batch_id=load_batch_id,
            filter_expr=filter_expr,
            available_fields=available_fields,
            limit=limit,
        )

        logger.debug(f"Executing annotation query: {query}")

        rows = await self.conn.fetch(query, *params)

        return [dict(row) for row in rows]

    async def annotate_batch(
        self,
        variants: list[tuple[str, int, str, str]],
        sources: list[str],
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """Annotate a batch of variants.

        Args:
            variants: List of (chrom, pos, ref, alt) tuples
            sources: List of annotation source names
            filter_expr: Optional filter expression

        Returns:
            List of annotated variant dictionaries
        """
        if not variants:
            return []

        available_fields = await self._get_available_fields(sources)

        temp_table = await self._create_temp_variant_table(variants)

        try:
            query = await self._build_batch_annotation_query(
                temp_table=temp_table,
                sources=sources,
                filter_expr=filter_expr,
                available_fields=available_fields,
            )

            rows = await self.conn.fetch(query)
            return [dict(row) for row in rows]

        finally:
            await self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

    async def _get_source_fields_cached(self, source: str) -> list[str]:
        """Get fields for a source, using cache to avoid repeated DB queries."""
        if source not in self._field_cache:
            self._field_cache[source] = await self.schema_manager.get_source_fields(
                self.conn, source
            )
        return self._field_cache[source]

    async def _get_available_fields(self, sources: list[str]) -> set[str]:
        """Get all available annotation fields from the specified sources."""
        all_fields = set()

        for source in sources:
            fields = await self._get_source_fields_cached(source)
            all_fields.update(fields)

        return all_fields

    async def _build_annotation_query(
        self,
        sources: list[str],
        load_batch_id: str | None,
        filter_expr: str | None,
        available_fields: set[str],
        limit: int | None,
    ) -> tuple[str, list]:
        """Build the SQL query for annotation lookup."""
        select_parts = [
            "v.chrom",
            "v.pos",
            "v.ref",
            "v.alt",
            "v.qual",
            "v.gene",
            "v.consequence",
            "v.impact",
        ]

        join_parts = []
        for i, source in enumerate(sources):
            validate_identifier(source, "source name")
            table_name = f"anno_{source}"
            alias = f"a{i}"

            fields = await self._get_source_fields_cached(source)
            for field in fields:
                validate_identifier(field, "field name")
                select_parts.append(f"{alias}.{field}")

            join = f"""
                LEFT JOIN {table_name} {alias}
                ON v.chrom = {alias}.chrom
                AND v.pos = {alias}.pos
                AND v.ref = {alias}.ref
                AND v.alt = {alias}.alt
            """
            join_parts.append(join)

        where_parts = []
        params: list = []
        if load_batch_id:
            params.append(load_batch_id)
            where_parts.append(f"v.load_batch_id = ${len(params)}")

        if filter_expr:
            sql_filter = self.expression_parser.parse(filter_expr, available_fields)
            sql_filter = await self._qualify_filter_fields(sql_filter, sources)
            where_parts.append(f"({sql_filter})")

        query = f"""
            SELECT {', '.join(select_parts)}
            FROM variants v
            {' '.join(join_parts)}
        """

        if where_parts:
            query += f" WHERE {' AND '.join(where_parts)}"

        if limit:
            params.append(limit)
            query += f" LIMIT ${len(params)}"

        return query, params

    async def _build_batch_annotation_query(
        self,
        temp_table: str,
        sources: list[str],
        filter_expr: str | None,
        available_fields: set[str],
    ) -> str:
        """Build the SQL query for batch annotation."""
        select_parts = ["t.chrom", "t.pos", "t.ref", "t.alt"]

        join_parts = []
        for i, source in enumerate(sources):
            validate_identifier(source, "source name")
            table_name = f"anno_{source}"
            alias = f"a{i}"

            fields = await self._get_source_fields_cached(source)
            for field in fields:
                validate_identifier(field, "field name")
                select_parts.append(f"{alias}.{field}")

            join = f"""
                LEFT JOIN {table_name} {alias}
                ON t.chrom = {alias}.chrom
                AND t.pos = {alias}.pos
                AND t.ref = {alias}.ref
                AND t.alt = {alias}.alt
            """
            join_parts.append(join)

        query = f"""
            SELECT {', '.join(select_parts)}
            FROM {temp_table} t
            {' '.join(join_parts)}
        """

        if filter_expr:
            sql_filter = self.expression_parser.parse(filter_expr, available_fields)
            sql_filter = await self._qualify_filter_fields(sql_filter, sources)
            query += f" WHERE {sql_filter}"

        return query

    async def _create_temp_variant_table(
        self,
        variants: list[tuple[str, int, str, str]],
    ) -> str:
        """Create a temporary table with variants to annotate."""
        temp_table = "temp_variants_to_annotate"

        await self.conn.execute(f"""
            CREATE TEMP TABLE {temp_table} (
                chrom TEXT NOT NULL,
                pos BIGINT NOT NULL,
                ref TEXT NOT NULL,
                alt TEXT NOT NULL
            )
        """)

        await self.conn.executemany(
            f"INSERT INTO {temp_table} (chrom, pos, ref, alt) VALUES ($1, $2, $3, $4)",
            variants,
        )

        return temp_table

    async def _qualify_filter_fields(
        self,
        sql_filter: str,
        sources: list[str],
    ) -> str:
        """Add table aliases to field names in the filter expression."""
        field_to_alias: dict[str, str] = {}
        for i, source in enumerate(sources):
            alias = f"a{i}"
            fields = await self._get_source_fields_cached(source)
            for field in fields:
                if field not in field_to_alias:
                    field_to_alias[field] = alias

        for field, alias in sorted(field_to_alias.items(), key=lambda x: -len(x[0])):
            sql_filter = re.sub(
                rf'\b{re.escape(field)}\b',
                f"{alias}.{field}",
                sql_filter
            )

        return sql_filter


async def annotate_query_vcf(
    conn: asyncpg.Connection,
    query_batch_id: str,
    sources: list[str],
    filter_expr: str | None = None,
) -> list[dict[str, Any]]:
    """Convenience function to annotate a loaded VCF.

    Args:
        conn: Database connection
        query_batch_id: Load batch ID of the query VCF
        sources: List of annotation source names
        filter_expr: Optional filter expression

    Returns:
        List of annotated variant dictionaries
    """
    annotator = VariantAnnotator(conn)
    return await annotator.annotate_variants(
        sources=sources,
        load_batch_id=query_batch_id,
        filter_expr=filter_expr,
    )
