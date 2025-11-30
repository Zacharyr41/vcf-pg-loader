"""vcf-pg-loader: High-performance VCF to PostgreSQL loader CLI."""

import asyncio
import logging
from pathlib import Path
from typing import Annotated
from uuid import UUID

import asyncpg
import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

from .config import load_config
from .loader import LoadConfig, VCFLoader
from .schema import SchemaManager

app = typer.Typer(
    name="vcf-pg-loader",
    help="Load VCF files into PostgreSQL with clinical-grade compliance"
)
console = Console()


def setup_logging(verbose: bool, quiet: bool) -> None:
    """Configure logging based on verbosity flags."""
    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )
    logging.getLogger("vcf_pg_loader").setLevel(level)


def _resolve_database_url(db_url: str | None, quiet: bool) -> str | None:
    """Resolve database URL, using managed database if needed.

    Args:
        db_url: User-provided URL, 'auto', or None.
        quiet: Whether to suppress output.

    Returns:
        Resolved database URL, or None if failed.
    """
    from .managed_db import DockerNotAvailableError, ManagedDatabase
    from .schema import SchemaManager

    if db_url is not None and db_url.lower() != "auto":
        return db_url

    try:
        db = ManagedDatabase()

        if db.is_running():
            url = db.get_url()
            if not quiet:
                console.print("[dim]Using managed database[/dim]")
            return url

        if not quiet:
            console.print("Starting managed database...")

        url = db.start()

        if not quiet:
            console.print("[green]✓[/green] Database started")

        async def init_schema():
            import asyncpg
            conn = await asyncpg.connect(url)
            try:
                schema_manager = SchemaManager(human_genome=True)
                await schema_manager.create_schema(conn)
            finally:
                await conn.close()

        asyncio.run(init_schema())

        return url

    except DockerNotAvailableError as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("\n[yellow]Tip:[/yellow] Provide a database URL with --db postgresql://...")
        return None


@app.command()
def load(
    vcf_path: Path = typer.Argument(..., help="Path to VCF file (.vcf, .vcf.gz)"),
    db_url: Annotated[str | None, typer.Option(
        "--db", "-d",
        help="PostgreSQL URL ('auto' for managed DB, omit to auto-detect)"
    )] = None,
    batch_size: int = typer.Option(50000, "--batch", "-b", help="Records per batch"),
    workers: int = typer.Option(8, "--workers", "-w", help="Parallel workers"),
    normalize: bool = typer.Option(True, "--normalize/--no-normalize", help="Normalize variants"),
    drop_indexes: bool = typer.Option(True, "--drop-indexes/--keep-indexes", help="Drop indexes during load"),
    human_genome: bool = typer.Option(True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"),
    force: bool = typer.Option(False, "--force", "-f", help="Force reload even if file was already loaded"),
    config_file: Annotated[Path | None, typer.Option("--config", "-c", help="TOML configuration file")] = None,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress bar"),
) -> None:
    """Load a VCF file into PostgreSQL.

    If --db is not specified, uses the managed database (auto-starts if needed).
    Use --db auto to explicitly use managed database, or provide a PostgreSQL URL.
    """
    setup_logging(verbose, quiet)

    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    resolved_db_url = _resolve_database_url(db_url, quiet)
    if resolved_db_url is None:
        raise typer.Exit(1)

    if config_file:
        base_config = load_config(config_file)
        config = LoadConfig(
            batch_size=batch_size if batch_size != 50000 else base_config.batch_size,
            workers=workers if workers != 8 else base_config.workers,
            normalize=normalize,
            drop_indexes=drop_indexes,
            human_genome=human_genome,
            log_level="DEBUG" if verbose else ("WARNING" if quiet else base_config.log_level),
        )
    else:
        config = LoadConfig(
            batch_size=batch_size,
            workers=workers,
            normalize=normalize,
            drop_indexes=drop_indexes,
            human_genome=human_genome,
            log_level="DEBUG" if verbose else ("WARNING" if quiet else "INFO"),
        )

    loader = VCFLoader(resolved_db_url, config)

    try:
        if not quiet:
            console.print(f"Loading {vcf_path.name}...")

        if progress and not quiet:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress_bar:
                task = progress_bar.add_task("Loading variants...", total=None)

                def update_progress(batch_num: int, batch_size: int, total: int):
                    progress_bar.update(task, completed=total, description=f"Loaded {total:,} variants")

                config.progress_callback = update_progress
                result = asyncio.run(loader.load_vcf(vcf_path, force_reload=force))
        else:
            result = asyncio.run(loader.load_vcf(vcf_path, force_reload=force))

        if result.get("skipped"):
            if not quiet:
                console.print("[yellow]⊘[/yellow] Skipped: file already loaded")
                console.print(f"  Previous Batch ID: {result['previous_load_id']}")
                console.print(f"  File SHA256: {result['file_hash']}")
                console.print("  Use --force to reload")
        else:
            if not quiet:
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


@app.command()
def benchmark(
    vcf_path: Annotated[Path | None, typer.Option("--vcf", "-f", help="Path to VCF file")] = None,
    synthetic: Annotated[int | None, typer.Option("--synthetic", "-s", help="Generate synthetic VCF with N variants")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL (omit for parsing-only benchmark)")] = None,
    batch_size: int = typer.Option(50000, "--batch", "-b", help="Records per batch"),
    normalize: bool = typer.Option(True, "--normalize/--no-normalize", help="Normalize variants"),
    human_genome: bool = typer.Option(True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"),
    json_output: bool = typer.Option(False, "--json", help="Output results as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimal output"),
) -> None:
    """Run performance benchmarks on VCF parsing and loading.

    Examples:

        # Quick benchmark with built-in fixture
        vcf-pg-loader benchmark

        # Generate and benchmark 100K synthetic variants
        vcf-pg-loader benchmark --synthetic 100000

        # Benchmark a specific VCF file
        vcf-pg-loader benchmark --vcf sample.vcf.gz

        # Full benchmark including database loading
        vcf-pg-loader benchmark --synthetic 50000 --db postgresql://localhost/variants
    """
    import json

    from .benchmark import run_benchmark

    if vcf_path and not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    try:
        result = run_benchmark(
            vcf_path=vcf_path,
            synthetic_count=synthetic,
            db_url=db_url,
            batch_size=batch_size,
            normalize=normalize,
            human_genome=human_genome,
        )

        if json_output:
            console.print(json.dumps(result.to_dict(), indent=2))
        else:
            if not quiet:
                source = "synthetic" if result.synthetic else Path(result.vcf_path).name
                console.print(f"\n[bold]Benchmark Results[/bold] ({source})")
                console.print(f"  Variants: {result.variant_count:,}")
                console.print(f"  Batch size: {result.batch_size:,}")
                console.print(f"  Normalized: {result.normalized}")
                console.print()

            console.print(
                f"[cyan]Parsing:[/cyan] {result.variant_count:,} variants in "
                f"{result.parsing_time:.2f}s ([green]{result.parsing_rate:,.0f}/sec[/green])"
            )

            if result.loading_time is not None:
                console.print(
                    f"[cyan]Loading:[/cyan] {result.variant_count:,} variants in "
                    f"{result.loading_time:.2f}s ([green]{result.loading_rate:,.0f}/sec[/green])"
                )

    except FileNotFoundError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


db_app = typer.Typer(help="Manage the local PostgreSQL database")
app.add_typer(db_app, name="db")


@db_app.command("start")
def db_start(
    port: int = typer.Option(5432, "--port", "-p", help="Port to expose PostgreSQL on"),
) -> None:
    """Start the managed PostgreSQL database.

    Starts a Docker container running PostgreSQL. Data is persisted
    between runs in a Docker volume.
    """
    from .managed_db import DockerNotAvailableError, ManagedDatabase

    try:
        db = ManagedDatabase()

        if db.is_running():
            console.print("[yellow]Database already running[/yellow]")
            console.print(f"  URL: {db.get_url()}")
            return

        console.print("Starting managed database...")
        url = db.start()
        console.print("[green]✓[/green] Database started")
        console.print(f"  URL: {url}")

    except DockerNotAvailableError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@db_app.command("stop")
def db_stop() -> None:
    """Stop the managed PostgreSQL database.

    Data is preserved and will be available when you start again.
    """
    from .managed_db import DockerNotAvailableError, ManagedDatabase

    try:
        db = ManagedDatabase()

        if not db.is_running():
            console.print("[yellow]Database is not running[/yellow]")
            return

        db.stop()
        console.print("[green]✓[/green] Database stopped")

    except DockerNotAvailableError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@db_app.command("status")
def db_status() -> None:
    """Show status of the managed database."""
    from .managed_db import DockerNotAvailableError, ManagedDatabase

    try:
        db = ManagedDatabase()
        status = db.status()

        if status["running"]:
            console.print("[green]●[/green] Database running")
            console.print(f"  Container: {status['container_name']}")
            console.print(f"  Image: {status['image']}")
            console.print(f"  URL: {status['url']}")
        else:
            console.print("[dim]○[/dim] Database not running")
            console.print("  Run 'vcf-pg-loader db start' to start")

    except DockerNotAvailableError:
        console.print("[red]○[/red] Docker not available")
        console.print("  Install Docker to use managed database")


@db_app.command("url")
def db_url() -> None:
    """Print the database connection URL.

    Useful for scripting or connecting with other tools.
    """
    from .managed_db import DockerNotAvailableError, ManagedDatabase

    try:
        db = ManagedDatabase()
        url = db.get_url()

        if url:
            console.print(url)
        else:
            console.print("[red]Database not running[/red]", err=True)
            raise typer.Exit(1)

    except DockerNotAvailableError as e:
        console.print(f"[red]Error: {e}[/red]", err=True)
        raise typer.Exit(1) from None


@db_app.command("shell")
def db_shell() -> None:
    """Open a psql shell to the managed database."""
    import subprocess

    from .managed_db import (
        CONTAINER_NAME,
        DEFAULT_DATABASE,
        DEFAULT_USER,
        DockerNotAvailableError,
        ManagedDatabase,
    )

    try:
        db = ManagedDatabase()

        if not db.is_running():
            console.print("[red]Database not running. Run 'vcf-pg-loader db start' first.[/red]")
            raise typer.Exit(1)

        console.print(f"Connecting to {DEFAULT_DATABASE}...")
        subprocess.run(
            ["docker", "exec", "-it", CONTAINER_NAME, "psql", "-U", DEFAULT_USER, "-d", DEFAULT_DATABASE],
            check=True,
        )

    except DockerNotAvailableError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None
    except subprocess.CalledProcessError:
        raise typer.Exit(1) from None


@db_app.command("reset")
def db_reset(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
) -> None:
    """Stop and remove the database including all data.

    This is destructive and cannot be undone.
    """
    from .managed_db import DockerNotAvailableError, ManagedDatabase

    if not force:
        confirm = typer.confirm("This will delete all data. Are you sure?")
        if not confirm:
            console.print("Cancelled")
            return

    try:
        db = ManagedDatabase()
        db.reset()
        console.print("[green]✓[/green] Database reset complete")

    except DockerNotAvailableError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command()
def doctor() -> None:
    """Check system dependencies and configuration.

    Verifies that all required dependencies are installed and
    provides installation instructions for any that are missing.
    """
    from .doctor import DependencyChecker

    console.print("\n[bold]vcf-pg-loader System Check[/bold]")
    console.print("─" * 30)

    checker = DependencyChecker()
    results = checker.check_all()

    all_passed = True
    for result in results:
        if result.passed:
            version_str = f" ({result.version})" if result.version else ""
            console.print(f"[green]✓[/green] {result.name}{version_str}")
        else:
            all_passed = False
            console.print(f"[red]✗[/red] {result.name}")
            if result.message:
                console.print(f"    {result.message}")

    console.print()

    if all_passed:
        console.print("[green]All systems ready![/green]")
    else:
        console.print("[yellow]Some dependencies are missing.[/yellow]")
        console.print("\nNote: Parsing and benchmarks work without Docker.")
        console.print("      Database features require Docker or external PostgreSQL.")


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
