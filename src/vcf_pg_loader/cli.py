"""vcf-pg-loader: High-performance VCF to PostgreSQL loader CLI."""

import asyncio
from pathlib import Path
from uuid import UUID

import asyncpg
import typer
from rich.console import Console

from .loader import LoadConfig, VCFLoader
from .schema import SchemaManager

app = typer.Typer(
    name="vcf-pg-loader",
    help="Load VCF files into PostgreSQL with clinical-grade compliance"
)
console = Console()


@app.command()
def load(
    vcf_path: Path = typer.Argument(..., help="Path to VCF file (.vcf, .vcf.gz)"),
    db_url: str = typer.Option(
        "postgresql://localhost/variants",
        "--db", "-d",
        help="PostgreSQL connection URL"
    ),
    batch_size: int = typer.Option(50000, "--batch", "-b", help="Records per batch"),
    workers: int = typer.Option(8, "--workers", "-w", help="Parallel workers"),
    normalize: bool = typer.Option(True, "--normalize/--no-normalize", help="Normalize variants"),
    drop_indexes: bool = typer.Option(True, "--drop-indexes/--keep-indexes", help="Drop indexes during load"),
    human_genome: bool = typer.Option(True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"),
    force: bool = typer.Option(False, "--force", "-f", help="Force reload even if file was already loaded"),
) -> None:
    """Load a VCF file into PostgreSQL."""
    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    config = LoadConfig(
        batch_size=batch_size,
        workers=workers,
        normalize=normalize,
        drop_indexes=drop_indexes,
        human_genome=human_genome
    )

    loader = VCFLoader(db_url, config)

    try:
        console.print(f"Loading {vcf_path.name}...")
        result = asyncio.run(loader.load_vcf(vcf_path, force_reload=force))

        if result.get("skipped"):
            console.print("[yellow]⊘[/yellow] Skipped: file already loaded")
            console.print(f"  Previous Batch ID: {result['previous_load_id']}")
            console.print(f"  File SHA256: {result['file_hash']}")
            console.print("  Use --force to reload")
        else:
            console.print(f"[green]✓[/green] Loaded {result['variants_loaded']:,} variants")
            console.print(f"  Batch ID: {result['load_batch_id']}")
            console.print(f"  File SHA256: {result['file_hash']}")

    except ConnectionError as e:
        console.print(f"[red]Error: Database connection failed: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command()
def validate(
    load_batch_id: str = typer.Argument(..., help="Load batch UUID to validate"),
    db_url: str = typer.Option(
        "postgresql://localhost/variants",
        "--db", "-d",
        help="PostgreSQL connection URL"
    ),
) -> None:
    """Validate a completed load."""
    try:
        batch_uuid = UUID(load_batch_id)
    except ValueError:
        console.print(f"[red]Error: Invalid UUID format: {load_batch_id}[/red]")
        raise typer.Exit(1) from None

    async def run_validation() -> None:
        conn = await asyncpg.connect(db_url)

        try:
            audit = await conn.fetchrow(
                "SELECT * FROM variant_load_audit WHERE load_batch_id = $1",
                batch_uuid
            )

            if not audit:
                console.print(f"[red]Load batch not found: {load_batch_id}[/red]")
                raise typer.Exit(1)

            actual_count = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1",
                batch_uuid
            )

            duplicates = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT chrom, pos, ref, alt, COUNT(*)
                    FROM variants WHERE load_batch_id = $1
                    GROUP BY chrom, pos, ref, alt HAVING COUNT(*) > 1
                ) dupes
            """, batch_uuid)

            console.print(f"Load Batch: {load_batch_id}")
            console.print(f"Status: {audit['status']}")
            console.print(f"Expected variants: {audit['variants_loaded']:,}")
            console.print(f"Actual variants: {actual_count:,}")
            console.print(f"Duplicates: {duplicates}")

            if actual_count == audit["variants_loaded"] and duplicates == 0:
                console.print("[green]✓ Validation passed[/green]")
            else:
                console.print("[red]✗ Validation failed[/red]")
                raise typer.Exit(1)

        finally:
            await conn.close()

    try:
        asyncio.run(run_validation())
    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@app.command("init-db")
def init_db(
    db_url: str = typer.Option(
        "postgresql://localhost/variants",
        "--db", "-d",
        help="PostgreSQL connection URL"
    ),
    human_genome: bool = typer.Option(True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"),
) -> None:
    """Initialize database schema."""
    async def run_init() -> None:
        conn = await asyncpg.connect(db_url)

        try:
            schema_manager = SchemaManager(human_genome=human_genome)
            await schema_manager.create_schema(conn)
            await schema_manager.create_indexes(conn)
            console.print("[green]✓[/green] Database schema initialized")

        finally:
            await conn.close()

    try:
        asyncio.run(run_init())
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
