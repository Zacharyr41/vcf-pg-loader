"""vcf-pg-loader: High-performance VCF to PostgreSQL loader CLI."""

import asyncio
import logging
import os
from pathlib import Path
from typing import Annotated
from uuid import UUID

import asyncpg
import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

from . import __version__
from .annotation_config import load_field_config
from .annotation_loader import AnnotationLoader
from .annotation_schema import AnnotationSchemaManager
from .annotator import VariantAnnotator
from .config import load_config
from .expression import FilterExpressionParser
from .loader import LoadConfig, VCFLoader
from .schema import SchemaManager


def version_callback(value: bool) -> None:
    if value:
        print(__version__)
        raise typer.Exit()


app = typer.Typer(
    name="vcf-pg-loader", help="Load VCF files into PostgreSQL with clinical-grade compliance"
)
console = Console()


@app.callback()
def main_callback(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version", callback=version_callback, is_eager=True, help="Show version and exit"
        ),
    ] = None,
) -> None:
    pass


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
        datefmt="%H:%M:%S",
    )
    logging.getLogger("vcf_pg_loader").setLevel(level)


def _build_database_url(
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
) -> str | None:
    """Build database URL from individual connection parameters.

    Priority (highest to lowest):
        1. POSTGRES_URL environment variable
        2. Provided CLI arguments
        3. PG* environment variables
    """
    if url := os.environ.get("POSTGRES_URL"):
        return url

    resolved_host = host or os.environ.get("PGHOST")
    if not resolved_host:
        return None

    resolved_port = port or int(os.environ.get("PGPORT", "5432"))
    resolved_user = user or os.environ.get("PGUSER", "postgres")
    resolved_database = database or os.environ.get("PGDATABASE", "variants")
    password = os.environ.get("PGPASSWORD", "")

    if password:
        return f"postgresql://{resolved_user}:{password}@{resolved_host}:{resolved_port}/{resolved_database}"
    return f"postgresql://{resolved_user}@{resolved_host}:{resolved_port}/{resolved_database}"


def _get_database_url_from_env() -> str | None:
    """Build database URL from environment variables (legacy helper)."""
    return _build_database_url()


def _resolve_database_url(
    db_url: str | None,
    quiet: bool,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
) -> str | None:
    """Resolve database URL, using managed database if needed.

    Args:
        db_url: User-provided URL, 'auto', or None.
        quiet: Whether to suppress output.
        host: PostgreSQL host (CLI arg).
        port: PostgreSQL port (CLI arg).
        database: Database name (CLI arg).
        user: Database user (CLI arg).

    Returns:
        Resolved database URL, or None if failed.
    """
    from .managed_db import DockerNotAvailableError, ManagedDatabase
    from .schema import SchemaManager

    if db_url is not None and db_url.lower() != "auto":
        return db_url

    if built_url := _build_database_url(host, port, database, user):
        if not quiet:
            console.print("[dim]Using database from CLI args/environment variables[/dim]")
        return built_url

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
    db_url: Annotated[
        str | None,
        typer.Option(
            "--db", "-d", help="PostgreSQL URL ('auto' for managed DB, omit to auto-detect)"
        ),
    ] = None,
    host: Annotated[str | None, typer.Option("--host", help="PostgreSQL host")] = None,
    port: Annotated[int | None, typer.Option("--port", help="PostgreSQL port")] = None,
    database: Annotated[str | None, typer.Option("--database", help="Database name")] = None,
    user: Annotated[str | None, typer.Option("--user", help="Database user")] = None,
    schema: Annotated[str, typer.Option("--schema", help="Target schema")] = "public",
    sample_id: Annotated[str | None, typer.Option("--sample-id", help="Sample ID override")] = None,
    batch_size: int = typer.Option(50000, "--batch", "-b", help="Records per batch"),
    workers: int = typer.Option(8, "--workers", "-w", help="Parallel workers"),
    normalize: bool = typer.Option(True, "--normalize/--no-normalize", help="Normalize variants"),
    drop_indexes: bool = typer.Option(
        True, "--drop-indexes/--keep-indexes", help="Drop indexes during load"
    ),
    human_genome: bool = typer.Option(
        True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force reload even if file was already loaded"
    ),
    config_file: Annotated[
        Path | None, typer.Option("--config", "-c", help="TOML configuration file")
    ] = None,
    report: Annotated[
        Path | None, typer.Option("--report", "-r", help="Write JSON report to file")
    ] = None,
    log_file: Annotated[Path | None, typer.Option("--log", help="Write log to file")] = None,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress bar"),
) -> None:
    """Load a VCF file into PostgreSQL.

    If --db is not specified, uses the managed database (auto-starts if needed).
    Use --db auto to explicitly use managed database, or provide a PostgreSQL URL.
    Can also specify connection via --host, --port, --database, --user options.
    """
    setup_logging(verbose, quiet)

    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    resolved_db_url = _resolve_database_url(db_url, quiet, host, port, database, user)
    if resolved_db_url is None:
        raise typer.Exit(1)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
        )
        logging.getLogger("vcf_pg_loader").addHandler(file_handler)

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
                    progress_bar.update(
                        task, completed=total, description=f"Loaded {total:,} variants"
                    )

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
            report_data = {
                "status": "skipped",
                "variants_loaded": 0,
                "load_batch_id": str(result.get("previous_load_id", "")),
                "file_hash": result.get("file_hash", ""),
            }
        else:
            if not quiet:
                console.print(f"[green]✓[/green] Loaded {result['variants_loaded']:,} variants")
                console.print(f"  Batch ID: {result['load_batch_id']}")
                console.print(f"  File SHA256: {result['file_hash']}")
            report_data = {
                "status": "success",
                "variants_loaded": result.get("variants_loaded", 0),
                "load_batch_id": str(result.get("load_batch_id", "")),
                "file_hash": result.get("file_hash", ""),
            }

        if report:
            import json
            import time

            report_data["elapsed_seconds"] = result.get("elapsed_seconds", 0)
            report_data["vcf_file"] = str(vcf_path)
            report_data["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            report_data["sample_id"] = sample_id or vcf_path.stem
            report_data["schema"] = schema
            with open(report, "w") as f:
                json.dump(report_data, f, indent=2)
                f.write("\n")
            if not quiet:
                console.print(f"  Report: {report}")

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
        "postgresql://localhost/variants", "--db", "-d", help="PostgreSQL connection URL"
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
                "SELECT * FROM variant_load_audit WHERE load_batch_id = $1", batch_uuid
            )

            if not audit:
                console.print(f"[red]Load batch not found: {load_batch_id}[/red]")
                raise typer.Exit(1)

            actual_count = await conn.fetchval(
                "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1", batch_uuid
            )

            duplicates = await conn.fetchval(
                """
                SELECT COUNT(*) FROM (
                    SELECT chrom, pos, ref, alt, COUNT(*)
                    FROM variants WHERE load_batch_id = $1
                    GROUP BY chrom, pos, ref, alt HAVING COUNT(*) > 1
                ) dupes
            """,
                batch_uuid,
            )

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
        "postgresql://localhost/variants", "--db", "-d", help="PostgreSQL connection URL"
    ),
    human_genome: bool = typer.Option(
        True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"
    ),
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
    synthetic: Annotated[
        int | None, typer.Option("--synthetic", "-s", help="Generate synthetic VCF with N variants")
    ] = None,
    db_url: Annotated[
        str | None,
        typer.Option("--db", "-d", help="PostgreSQL URL (omit for parsing-only benchmark)"),
    ] = None,
    batch_size: int = typer.Option(50000, "--batch", "-b", help="Records per batch"),
    normalize: bool = typer.Option(True, "--normalize/--no-normalize", help="Normalize variants"),
    human_genome: bool = typer.Option(
        True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"
    ),
    realistic: bool = typer.Option(
        False,
        "--realistic",
        "-r",
        help="Generate realistic VCF with annotations and complex variants",
    ),
    giab: bool = typer.Option(
        False, "--giab", "-g", help="Generate GIAB-style VCF with platform/callset metadata"
    ),
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

        # GIAB-style benchmark with platform/callset metadata
        vcf-pg-loader benchmark --synthetic 100000 --giab --db postgresql://localhost/variants
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
            realistic=realistic,
            giab=giab,
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


@app.command("load-annotation")
def load_annotation(
    vcf_path: Path = typer.Argument(..., help="Path to annotation VCF file (.vcf, .vcf.gz)"),
    name: Annotated[
        str | None, typer.Option("--name", "-n", help="Name for this annotation source")
    ] = None,
    config_file: Annotated[
        Path | None, typer.Option("--config", "-c", help="JSON field configuration file")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    version: Annotated[
        str | None, typer.Option("--version", "-v", help="Version string for this source")
    ] = None,
    source_type: Annotated[
        str | None,
        typer.Option("--type", "-t", help="Source type (population, pathogenicity, etc.)"),
    ] = None,
    human_genome: bool = typer.Option(
        True, "--human-genome/--no-human-genome", help="Use human chromosome enum type"
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Load an annotation VCF file as a reference database.

    The annotation source can then be used to annotate query VCFs via SQL JOINs.

    Example:
        vcf-pg-loader load-annotation gnomad.vcf.gz --name gnomad_v3 --config gnomad.json
    """
    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    if name is None:
        console.print("[red]Error: --name is required[/red]")
        raise typer.Exit(1)

    if config_file is None:
        console.print("[red]Error: --config is required[/red]")
        raise typer.Exit(1)

    if not config_file.exists():
        console.print(f"[red]Error: Config file not found: {config_file}[/red]")
        raise typer.Exit(1)

    resolved_db_url = _resolve_database_url(db_url, quiet)
    if resolved_db_url is None:
        raise typer.Exit(1)

    try:
        field_config = load_field_config(config_file)
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        raise typer.Exit(1) from None

    async def run_load() -> dict:
        conn = await asyncpg.connect(resolved_db_url)
        try:
            schema_manager = SchemaManager(human_genome=human_genome)
            await schema_manager.create_schema(conn)

            loader = AnnotationLoader(human_genome=human_genome)
            result = await loader.load_annotation_source(
                vcf_path=vcf_path,
                source_name=name,
                field_config=field_config,
                conn=conn,
                version=version,
                source_type=source_type,
            )
            return result
        finally:
            await conn.close()

    try:
        result = asyncio.run(run_load())
        if not quiet:
            console.print(f"[green]✓[/green] Loaded {result['variants_loaded']:,} variants")
            console.print(f"  Source: {result['source_name']}")
            console.print(f"  Table: {result['table_name']}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("list-annotations")
def list_annotations(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List all loaded annotation sources."""
    import json

    resolved_db_url = _resolve_database_url(db_url, quiet)
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_list() -> list:
        conn = await asyncpg.connect(resolved_db_url)
        try:
            schema_manager = AnnotationSchemaManager()

            await schema_manager.create_annotation_registry(conn)

            sources = await schema_manager.list_sources(conn)
            return sources
        finally:
            await conn.close()

    try:
        sources = asyncio.run(run_list())

        if json_output:
            console.print(json.dumps([dict(s) for s in sources], indent=2, default=str))
        elif sources:
            for source in sources:
                console.print(f"[cyan]{source['name']}[/cyan]")
                if source.get("version"):
                    console.print(f"  Version: {source['version']}")
                if source.get("source_type"):
                    console.print(f"  Type: {source['source_type']}")
                console.print(f"  Variants: {source.get('variant_count', 0):,}")
                console.print()
        else:
            if not quiet:
                console.print("[dim]No annotation sources loaded[/dim]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("annotate")
def annotate(
    batch_id: str = typer.Argument(..., help="Load batch ID of variants to annotate"),
    source: Annotated[
        list[str] | None, typer.Option("--source", "-s", help="Annotation source(s) to use")
    ] = None,
    filter_expr: Annotated[
        str | None, typer.Option("--filter", "-f", help="Filter expression (echtvar-style)")
    ] = None,
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Output file path")] = None,
    format: Annotated[str, typer.Option("--format", help="Output format (tsv, json)")] = "tsv",
    limit: Annotated[
        int | None, typer.Option("--limit", "-l", help="Limit number of results")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Annotate loaded variants using reference databases.

    Example:
        vcf-pg-loader annotate <batch-id> --source gnomad_v3 --filter "gnomad_af < 0.01"
    """
    import csv
    import json
    import sys

    if source is None or len(source) == 0:
        console.print("[red]Error: --source is required[/red]")
        raise typer.Exit(1)

    if filter_expr:
        parser = FilterExpressionParser()
        errors = parser.validate(filter_expr, set())
        syntax_errors = [e for e in errors if "Unknown field" not in e]
        if syntax_errors:
            console.print(f"[red]Error in filter expression: {'; '.join(syntax_errors)}[/red]")
            raise typer.Exit(1)

    resolved_db_url = _resolve_database_url(db_url, quiet)
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_annotate() -> list:
        conn = await asyncpg.connect(resolved_db_url)
        try:
            annotator = VariantAnnotator(conn)
            results = await annotator.annotate_variants(
                sources=source,
                load_batch_id=batch_id,
                filter_expr=filter_expr,
                limit=limit,
            )
            return results
        finally:
            await conn.close()

    try:
        results = asyncio.run(run_annotate())

        if output:
            out_file = open(output, "w")
        else:
            out_file = sys.stdout

        try:
            if format == "json":
                json.dump(results, out_file, indent=2, default=str)
                out_file.write("\n")
            else:
                if results:
                    writer = csv.DictWriter(out_file, fieldnames=results[0].keys(), delimiter="\t")
                    writer.writeheader()
                    writer.writerows(results)

            if not quiet and output:
                console.print(
                    f"[green]✓[/green] Wrote {len(results)} annotated variants to {output}"
                )
        finally:
            if output:
                out_file.close()

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("annotation-query")
def annotation_query(
    sql: str = typer.Option(..., "--sql", help="SQL query to execute"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    format: Annotated[str, typer.Option("--format", help="Output format (tsv, json)")] = "tsv",
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Execute an ad-hoc SQL query against annotation tables.

    Example:
        vcf-pg-loader annotation-query --sql "SELECT * FROM anno_gnomad LIMIT 10"
    """
    import csv
    import json
    import sys

    resolved_db_url = _resolve_database_url(db_url, quiet)
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_query() -> list:
        conn = await asyncpg.connect(resolved_db_url)
        try:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    try:
        results = asyncio.run(run_query())

        if format == "json":
            print(json.dumps(results, indent=2, default=str))
        else:
            if results:
                writer = csv.DictWriter(sys.stdout, fieldnames=results[0].keys(), delimiter="\t")
                writer.writeheader()
                writer.writerows(results)

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
            [
                "docker",
                "exec",
                "-it",
                CONTAINER_NAME,
                "psql",
                "-U",
                DEFAULT_USER,
                "-d",
                DEFAULT_DATABASE,
            ],
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
