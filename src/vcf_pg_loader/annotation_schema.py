"""PostgreSQL schema management for annotation reference tables."""
import json
import re

import asyncpg

from .annotation_config import AnnotationFieldConfig, config_to_dict


def validate_identifier(name: str, identifier_type: str = "identifier") -> None:
    if not name:
        raise ValueError(f"Invalid {identifier_type}: cannot be empty")
    if len(name) > 63:
        raise ValueError(f"Invalid {identifier_type}: '{name}' exceeds 63 character limit")
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}' must start with a letter or underscore "
            "and contain only alphanumeric characters and underscores"
        )


class AnnotationSchemaManager:
    """Manages PostgreSQL schema for annotation reference tables."""

    def __init__(self, human_genome: bool = True, unlogged: bool = False):
        self.human_genome = human_genome
        self.unlogged = unlogged

    async def create_annotation_registry(self, conn: asyncpg.Connection) -> None:
        """Create the annotation_sources registry table.

        This table tracks all loaded annotation sources (gnomAD, ClinVar, etc.)
        with their metadata and field configurations.
        """
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS annotation_sources (
                source_id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                source_type VARCHAR(50),
                version VARCHAR(50),
                vcf_path TEXT,
                field_config JSONB NOT NULL,
                loaded_at TIMESTAMPTZ DEFAULT NOW(),
                variant_count BIGINT DEFAULT 0
            )
        """)

    async def create_annotation_source_table(
        self,
        conn: asyncpg.Connection,
        source_name: str,
        fields: list[AnnotationFieldConfig],
    ) -> str:
        """Create a table for a specific annotation source.

        Args:
            conn: Database connection
            source_name: Name of the annotation source (e.g., "gnomad_v3_1_2")
            fields: List of field configurations

        Returns:
            The name of the created table
        """
        validate_identifier(source_name, "source name")
        table_name = f"anno_{source_name}"

        if self.human_genome:
            chrom_type = "chromosome_type"
        else:
            chrom_type = "TEXT"

        field_defs = []
        for field_cfg in fields:
            sql_type = field_cfg.to_sql_type()
            field_defs.append(f"{field_cfg.alias} {sql_type}")

        field_columns = ",\n                ".join(field_defs)

        unlogged_clause = "UNLOGGED" if self.unlogged else ""

        await conn.execute(f"""
            CREATE {unlogged_clause} TABLE IF NOT EXISTS {table_name} (
                chrom {chrom_type} NOT NULL,
                pos BIGINT NOT NULL,
                ref TEXT NOT NULL,
                alt TEXT NOT NULL,
                {field_columns},
                PRIMARY KEY (chrom, pos, ref, alt)
            )
        """)

        return table_name

    async def create_variant_lookup_index(
        self,
        conn: asyncpg.Connection,
        source_name: str,
    ) -> None:
        """Create optimized index for variant lookup.

        Args:
            conn: Database connection
            source_name: Name of the annotation source
        """
        validate_identifier(source_name, "source name")
        table_name = f"anno_{source_name}"
        index_name = f"idx_{table_name}_lookup"

        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_name} (chrom, pos, ref, alt)
        """)

    async def drop_annotation_source(
        self,
        conn: asyncpg.Connection,
        source_name: str,
    ) -> bool:
        """Drop an annotation source table and its registry entry.

        Args:
            conn: Database connection
            source_name: Name of the annotation source

        Returns:
            True if the source was dropped, False if it didn't exist
        """
        validate_identifier(source_name, "source name")
        table_name = f"anno_{source_name}"

        result = await conn.fetchval(
            "DELETE FROM annotation_sources WHERE name = $1 RETURNING source_id",
            source_name,
        )

        await conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        return result is not None

    async def register_source(
        self,
        conn: asyncpg.Connection,
        source_name: str,
        fields: list[AnnotationFieldConfig],
        vcf_path: str | None = None,
        source_type: str | None = None,
        version: str | None = None,
    ) -> int:
        """Register an annotation source in the registry.

        Args:
            conn: Database connection
            source_name: Name of the annotation source
            fields: Field configurations
            vcf_path: Path to the source VCF file
            source_type: Type of annotation (population, pathogenicity, etc.)
            version: Version string

        Returns:
            The source_id of the registered source
        """
        field_config_json = json.dumps(config_to_dict(fields))

        source_id = await conn.fetchval(
            """
            INSERT INTO annotation_sources (name, source_type, version, vcf_path, field_config)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                version = EXCLUDED.version,
                vcf_path = EXCLUDED.vcf_path,
                field_config = EXCLUDED.field_config,
                loaded_at = NOW()
            RETURNING source_id
            """,
            source_name,
            source_type,
            version,
            vcf_path,
            field_config_json,
        )

        return source_id

    async def update_variant_count(
        self,
        conn: asyncpg.Connection,
        source_name: str,
        count: int,
    ) -> None:
        """Update the variant count for an annotation source.

        Args:
            conn: Database connection
            source_name: Name of the annotation source
            count: Number of variants loaded
        """
        await conn.execute(
            "UPDATE annotation_sources SET variant_count = $1 WHERE name = $2",
            count,
            source_name,
        )

    async def get_source_info(
        self,
        conn: asyncpg.Connection,
        source_name: str,
    ) -> dict | None:
        """Get information about an annotation source.

        Args:
            conn: Database connection
            source_name: Name of the annotation source

        Returns:
            Dictionary with source information, or None if not found
        """
        row = await conn.fetchrow(
            """
            SELECT source_id, name, source_type, version, vcf_path,
                   field_config, loaded_at, variant_count
            FROM annotation_sources
            WHERE name = $1
            """,
            source_name,
        )

        if row is None:
            return None

        return dict(row)

    async def list_sources(
        self,
        conn: asyncpg.Connection,
    ) -> list[dict]:
        """List all registered annotation sources.

        Args:
            conn: Database connection

        Returns:
            List of source information dictionaries
        """
        rows = await conn.fetch(
            """
            SELECT source_id, name, source_type, version, loaded_at, variant_count
            FROM annotation_sources
            ORDER BY name
            """
        )

        return [dict(row) for row in rows]

    async def get_source_fields(
        self,
        conn: asyncpg.Connection,
        source_name: str,
    ) -> list[str]:
        """Get the field aliases for an annotation source.

        Args:
            conn: Database connection
            source_name: Name of the annotation source

        Returns:
            List of field alias names
        """
        row = await conn.fetchrow(
            "SELECT field_config FROM annotation_sources WHERE name = $1",
            source_name,
        )

        if row is None:
            return []

        config = json.loads(row["field_config"])
        return [item["alias"] for item in config]
