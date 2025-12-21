"""vcf-pg-loader: High-performance VCF to PostgreSQL loader CLI."""

import asyncio
import logging
import os
from datetime import date as date_type
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
from .secrets import (
    CredentialValidationError,
    get_database_password,
    validate_no_password_in_url,
)
from .tls import TLSConfig, TLSError, get_ssl_param_for_asyncpg

_default_tls_config: TLSConfig | None = None


def _get_ssl_param() -> bool | str:
    """Get SSL parameter for asyncpg connections using default TLS config."""
    return get_ssl_param_for_asyncpg(_default_tls_config)


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
    password_env_var: str = "VCF_PG_LOADER_DB_PASSWORD",
) -> str | None:
    """Build database URL from individual connection parameters.

    Priority (highest to lowest):
        1. POSTGRES_URL environment variable (validated for no embedded password)
        2. Provided CLI arguments with password from secrets provider
        3. PG* environment variables

    Args:
        host: PostgreSQL host.
        port: PostgreSQL port.
        database: Database name.
        user: Database user.
        password_env_var: Environment variable name for password.

    Returns:
        Database connection URL, or None if insufficient info.

    Raises:
        CredentialValidationError: If password detected in POSTGRES_URL.
    """
    logger = logging.getLogger(__name__)

    if url := os.environ.get("POSTGRES_URL"):
        validate_no_password_in_url(url)
        password = get_database_password(password_env_var=password_env_var)
        if password:
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(url)
            user_part = parsed.username or "postgres"
            netloc = f"{user_part}:{password}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            url_with_password = urlunparse(
                (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
            )
            logger.info("Using POSTGRES_URL with password from %s", password_env_var)
            return url_with_password
        logger.info("Using POSTGRES_URL (no password provided)")
        return url

    resolved_host = host or os.environ.get("PGHOST")
    if not resolved_host:
        return None

    resolved_port = port or int(os.environ.get("PGPORT", "5432"))
    resolved_user = user or os.environ.get("PGUSER", "postgres")
    resolved_database = database or os.environ.get("PGDATABASE", "variants")

    password = get_database_password(password_env_var=password_env_var)

    if password:
        logger.info("Database password loaded via secrets provider")
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
    password_env_var: str = "VCF_PG_LOADER_DB_PASSWORD",
) -> str | None:
    """Resolve database URL, using managed database if needed.

    Args:
        db_url: User-provided URL, 'auto', or None.
        quiet: Whether to suppress output.
        host: PostgreSQL host (CLI arg).
        port: PostgreSQL port (CLI arg).
        database: Database name (CLI arg).
        user: Database user (CLI arg).
        password_env_var: Environment variable name for password.

    Returns:
        Resolved database URL, or None if failed.

    Raises:
        CredentialValidationError: If password detected in provided URL.
    """
    from urllib.parse import urlparse, urlunparse

    from .managed_db import DockerNotAvailableError, ManagedDatabase
    from .schema import SchemaManager

    logger = logging.getLogger(__name__)

    if db_url is not None and db_url.lower() != "auto":
        validate_no_password_in_url(db_url)

        password = get_database_password(password_env_var=password_env_var)
        if password:
            parsed = urlparse(db_url)
            user_part = parsed.username or "postgres"
            netloc = f"{user_part}:{password}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            url_with_password = urlunparse(
                (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
            )
            logger.info("Using --db URL with password from %s", password_env_var)
            return url_with_password
        return db_url

    if built_url := _build_database_url(host, port, database, user, password_env_var):
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

            conn = await asyncpg.connect(url, ssl=_get_ssl_param())
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
    db_password_env: Annotated[
        str,
        typer.Option(
            "--db-password-env",
            help="Environment variable for database password",
        ),
    ] = "VCF_PG_LOADER_DB_PASSWORD",
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
    require_tls: bool = typer.Option(
        True, "--require-tls/--no-require-tls", help="Require TLS for database connections"
    ),
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

    try:
        resolved_db_url = _resolve_database_url(
            db_url, quiet, host, port, database, user, db_password_env
        )
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
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
        tls_config = TLSConfig(require_tls=require_tls)
        config = LoadConfig(
            batch_size=batch_size if batch_size != 50000 else base_config.batch_size,
            workers=workers if workers != 8 else base_config.workers,
            normalize=normalize,
            drop_indexes=drop_indexes,
            human_genome=human_genome,
            log_level="DEBUG" if verbose else ("WARNING" if quiet else base_config.log_level),
            tls_config=tls_config,
        )
    else:
        tls_config = TLSConfig(require_tls=require_tls)
        config = LoadConfig(
            batch_size=batch_size,
            workers=workers,
            normalize=normalize,
            drop_indexes=drop_indexes,
            human_genome=human_genome,
            log_level="DEBUG" if verbose else ("WARNING" if quiet else "INFO"),
            tls_config=tls_config,
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

    except TLSError as e:
        console.print(f"[red]TLS Error: {e}[/red]")
        console.print("[yellow]Tip:[/yellow] Use --no-require-tls for non-TLS connections")
        raise typer.Exit(1) from None
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
        conn = await asyncpg.connect(db_url, ssl=_get_ssl_param())

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
    skip_audit: bool = typer.Option(False, "--skip-audit", help="Skip HIPAA audit schema creation"),
) -> None:
    """Initialize database schema.

    Creates the complete database schema including:
    - Variants table (partitioned by chromosome)
    - Load audit table
    - Samples table
    - HIPAA-compliant audit logging (unless --skip-audit)
    """

    async def run_init() -> None:
        conn = await asyncpg.connect(db_url, ssl=_get_ssl_param())

        try:
            schema_manager = SchemaManager(human_genome=human_genome)
            await schema_manager.create_schema(conn)
            await schema_manager.create_indexes(conn)
            console.print("[green]✓[/green] Database schema initialized")

            if not skip_audit:
                partitions = await schema_manager.get_audit_partition_info(conn)
                console.print(
                    f"[green]✓[/green] HIPAA audit schema created ({len(partitions)} partitions)"
                )

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

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    try:
        field_config = load_field_config(config_file)
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        raise typer.Exit(1) from None

    async def run_load() -> dict:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
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

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_list() -> list:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
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

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_annotate() -> list:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
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

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_query() -> list:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
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


audit_app = typer.Typer(help="HIPAA audit log management and verification")
app.add_typer(audit_app, name="audit")


@audit_app.command("verify")
def audit_verify(
    start_date: Annotated[str, typer.Option("--start-date", "-s", help="Start date (YYYY-MM-DD)")],
    end_date: Annotated[str, typer.Option("--end-date", "-e", help="End date (YYYY-MM-DD)")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Verify audit log integrity for a date range.

    Checks hash chain integrity to detect any tampering with audit records.
    Returns non-zero exit code if tampering is detected.

    Example:
        vcf-pg-loader audit verify --start-date 2024-01-01 --end-date 2024-12-31
    """
    import json

    from .audit import AuditIntegrity

    try:
        start = date_type.fromisoformat(start_date)
        end = date_type.fromisoformat(end_date)
    except ValueError as e:
        console.print(f"[red]Error: Invalid date format: {e}[/red]")
        raise typer.Exit(1) from None

    if start > end:
        console.print("[red]Error: start-date must be before end-date[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_verify():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            integrity = AuditIntegrity()
            report = await integrity.verify_chain_integrity(conn, start, end)
            return report
        finally:
            await conn.close()

    try:
        report = asyncio.run(run_verify())

        if json_output:
            console.print(json.dumps(report.to_dict(), indent=2))
        else:
            if not quiet:
                console.print("\n[bold]Audit Integrity Report[/bold]")
                console.print(f"  Date Range: {report.start_date} to {report.end_date}")
                console.print(f"  Total Entries: {report.total_entries:,}")
                console.print(f"  Verified: {report.verified_entries:,}")
                console.print(f"  Coverage: {report.coverage_percent:.1f}%")
                console.print()

            if report.is_valid:
                console.print("[green]✓ Audit log integrity verified[/green]")
            else:
                console.print(
                    f"[red]✗ {len(report.violations)} integrity violations detected[/red]"
                )
                for v in report.violations[:10]:
                    console.print(f"  - Audit ID {v.audit_id}: {v.status.value} - {v.message}")
                if len(report.violations) > 10:
                    console.print(f"  ... and {len(report.violations) - 10} more")
                raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@audit_app.command("export")
def audit_export(
    output: Annotated[Path, typer.Option("--output", "-o", help="Output file path")],
    start_date: Annotated[str, typer.Option("--start-date", "-s", help="Start date (YYYY-MM-DD)")],
    end_date: Annotated[str, typer.Option("--end-date", "-e", help="End date (YYYY-MM-DD)")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Export audit logs with integrity metadata for backup.

    Exports audit entries with checksums for later verification.

    Example:
        vcf-pg-loader audit export -o backup.json --start-date 2024-01-01 --end-date 2024-12-31
    """
    import json

    from .audit import AuditIntegrity

    try:
        start = date_type.fromisoformat(start_date)
        end = date_type.fromisoformat(end_date)
    except ValueError as e:
        console.print(f"[red]Error: Invalid date format: {e}[/red]")
        raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_export():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            integrity = AuditIntegrity()
            entries, metadata = await integrity.export_with_integrity(conn, start, end)
            return entries, metadata
        finally:
            await conn.close()

    try:
        entries, metadata = asyncio.run(run_export())

        export_data = {
            "metadata": {
                "export_time": metadata.export_time.isoformat(),
                "start_date": metadata.start_date.isoformat(),
                "end_date": metadata.end_date.isoformat(),
                "entry_count": metadata.entry_count,
                "first_hash": metadata.first_hash,
                "last_hash": metadata.last_hash,
                "checksum": metadata.checksum,
            },
            "entries": entries,
        }

        with open(output, "w") as f:
            json.dump(export_data, f, indent=2)

        if not quiet:
            console.print(
                f"[green]✓[/green] Exported {metadata.entry_count:,} audit entries to {output}"
            )
            console.print(f"  Checksum: {metadata.checksum[:16]}...")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@audit_app.command("verify-backup")
def audit_verify_backup(
    backup_file: Annotated[Path, typer.Argument(help="Backup file to verify")],
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Verify integrity of an exported audit backup.

    Checks that the backup file matches its embedded checksums.

    Example:
        vcf-pg-loader audit verify-backup backup.json
    """
    import json

    from .audit import AuditIntegrity, BackupMetadata

    if not backup_file.exists():
        console.print(f"[red]Error: File not found: {backup_file}[/red]")
        raise typer.Exit(1)

    try:
        with open(backup_file) as f:
            data = json.load(f)

        meta = data["metadata"]
        metadata = BackupMetadata(
            export_time=date_type.fromisoformat(meta["export_time"][:10]),
            start_date=date_type.fromisoformat(meta["start_date"]),
            end_date=date_type.fromisoformat(meta["end_date"]),
            entry_count=meta["entry_count"],
            first_hash=meta["first_hash"],
            last_hash=meta["last_hash"],
            checksum=meta["checksum"],
        )

        integrity = AuditIntegrity()
        is_valid, message = integrity.verify_backup(data["entries"], metadata)

        if is_valid:
            console.print(f"[green]✓[/green] {message}")
            if not quiet:
                console.print(f"  Entries: {metadata.entry_count:,}")
                console.print(f"  Date Range: {metadata.start_date} to {metadata.end_date}")
        else:
            console.print(f"[red]✗ Backup verification failed: {message}[/red]")
            raise typer.Exit(1)

    except json.JSONDecodeError as e:
        console.print(f"[red]Error: Invalid JSON in backup file: {e}[/red]")
        raise typer.Exit(1) from None
    except KeyError as e:
        console.print(f"[red]Error: Missing required field in backup: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@audit_app.command("stats")
def audit_stats(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Show audit log statistics."""
    import json

    from .audit import AuditSchemaManager

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_stats():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuditSchemaManager()
            stats = await schema_manager.get_audit_stats(conn)
            partitions = await schema_manager.get_partition_info(conn)
            immutable = await schema_manager.verify_immutability(conn)
            return stats, partitions, immutable
        finally:
            await conn.close()

    try:
        stats, partitions, immutable = asyncio.run(run_stats())

        if json_output:
            output = {
                "stats": stats,
                "partitions": partitions,
                "immutability_trigger_active": immutable,
            }
            for key in output["stats"]:
                if hasattr(output["stats"][key], "isoformat"):
                    output["stats"][key] = output["stats"][key].isoformat()
            console.print(json.dumps(output, indent=2, default=str))
        else:
            console.print("[bold]Audit Log Statistics[/bold]")
            console.print(f"  Total Events: {stats.get('total_events', 0):,}")
            console.print(f"  Unique Users: {stats.get('unique_users', 0):,}")
            console.print(f"  Failed Auth: {stats.get('failed_auth_count', 0):,}")
            console.print(f"  PHI Access: {stats.get('phi_access_count', 0):,}")
            if stats.get("oldest_event"):
                console.print(f"  Oldest Event: {stats['oldest_event']}")
            if stats.get("newest_event"):
                console.print(f"  Newest Event: {stats['newest_event']}")
            console.print(f"  Partitions: {len(partitions)}")
            if immutable:
                console.print("[green]  ✓ Immutability trigger active[/green]")
            else:
                console.print("[red]  ✗ Immutability trigger NOT active[/red]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


auth_app = typer.Typer(help="User authentication and management (HIPAA 164.312(d))")
app.add_typer(auth_app, name="auth")


def _get_jwt_secret() -> str:
    import os
    import secrets as secrets_module

    secret = os.environ.get("VCF_PG_LOADER_JWT_SECRET")
    if not secret:
        secret = secrets_module.token_hex(32)
    return secret


@auth_app.command("login")
def auth_login(
    username: Annotated[str, typer.Option("--username", "-u", prompt=True, help="Username")],
    password: Annotated[
        str, typer.Option("--password", "-p", prompt=True, hide_input=True, help="Password")
    ],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Authenticate and create a session."""
    from .auth import Authenticator, AuthSchemaManager, AuthStatus, SessionStorage

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_login():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            if not await schema_manager.schema_exists(conn):
                await schema_manager.create_auth_schema(conn)

            auth = Authenticator(jwt_secret=_get_jwt_secret())
            result = await auth.authenticate(conn, username, password)
            return result, auth.session_config
        finally:
            await conn.close()

    try:
        result, session_config = asyncio.run(run_login())

        if result.status == AuthStatus.SUCCESS and result.token and result.session:
            storage = SessionStorage()
            storage.save_token(
                result.token,
                result.user.username,
                result.session.expires_at,
                session_id=result.session.session_id,
                inactivity_timeout_minutes=session_config.inactivity_timeout_minutes,
            )
            if not quiet:
                console.print(f"[green]✓[/green] Logged in as {result.user.username}")
                console.print(f"  Session expires: {result.session.expires_at.isoformat()}")
                console.print(
                    f"  Inactivity timeout: {session_config.inactivity_timeout_minutes} minutes"
                )
        else:
            console.print(f"[red]Login failed: {result.message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@auth_app.command("logout")
def auth_logout(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """End the current session."""
    from .auth import Authenticator, SessionStorage

    storage = SessionStorage()
    token, username = storage.load_token()

    if not token:
        if not quiet:
            console.print("[yellow]No active session[/yellow]")
        return

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if resolved_db_url:

        async def run_logout():
            conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
            try:
                auth = Authenticator(jwt_secret=_get_jwt_secret())
                await auth.logout(conn, token)
            finally:
                await conn.close()

        try:
            asyncio.run(run_logout())
        except Exception:
            pass

    storage.clear_token()
    if not quiet:
        console.print(f"[green]✓[/green] Logged out ({username})")


@auth_app.command("whoami")
def auth_whoami(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Show current authenticated user."""
    from .auth import Authenticator, SessionStorage

    storage = SessionStorage()
    token, username = storage.load_token()

    if not token:
        console.print("[yellow]Not logged in[/yellow]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if resolved_db_url:

        async def run_whoami():
            conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
            try:
                auth = Authenticator(jwt_secret=_get_jwt_secret())
                session = await auth.validate_session(conn, token)
                return session
            finally:
                await conn.close()

        try:
            session = asyncio.run(run_whoami())
            if session:
                console.print(f"Username: {session.username}")
                console.print(f"User ID: {session.user_id}")
                console.print(f"Session ID: {session.session_id}")
                console.print(f"Expires: {session.expires_at.isoformat()}")
            else:
                console.print("[yellow]Session expired or invalid[/yellow]")
                storage.clear_token()
                raise typer.Exit(1)
        except Exception as e:
            if not isinstance(e, SystemExit):
                console.print(f"[red]Error: {e}[/red]")
                raise typer.Exit(1) from None
            raise
    else:
        console.print(f"Username: {username}")
        console.print("[dim]Connect to database for full session info[/dim]")


@auth_app.command("create-user")
def auth_create_user(
    username: Annotated[str, typer.Option("--username", "-u", prompt=True, help="Username")],
    email: Annotated[str | None, typer.Option("--email", "-e", help="Email address")] = None,
    password: Annotated[
        str | None,
        typer.Option("--password", "-p", help="Password (will prompt if not provided)"),
    ] = None,
    require_change: bool = typer.Option(
        True, "--require-change/--no-require-change", help="Require password change on first login"
    ),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Create a new user account."""
    from .auth import Authenticator, AuthSchemaManager, SessionStorage, UserManager

    if password is None:
        password = typer.prompt("Password", hide_input=True, confirmation_prompt=True)

    storage = SessionStorage()
    token, _ = storage.load_token()

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_create():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            if not await schema_manager.schema_exists(conn):
                await schema_manager.create_auth_schema(conn)

            created_by = None
            if token:
                auth = Authenticator(jwt_secret=_get_jwt_secret())
                session = await auth.validate_session(conn, token)
                if session:
                    created_by = session.user_id

            manager = UserManager()
            user, message = await manager.create_user(
                conn, username, password, email, created_by, require_change
            )
            return user, message
        finally:
            await conn.close()

    try:
        user, message = asyncio.run(run_create())
        if user:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
                console.print(f"  User ID: {user.user_id}")
                console.print(f"  Username: {user.username}")
                if user.email:
                    console.print(f"  Email: {user.email}")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@auth_app.command("change-password")
def auth_change_password(
    current_password: Annotated[
        str,
        typer.Option("--current", "-c", prompt=True, hide_input=True, help="Current password"),
    ],
    new_password: Annotated[
        str,
        typer.Option(
            "--new",
            "-n",
            prompt=True,
            hide_input=True,
            confirmation_prompt=True,
            help="New password",
        ),
    ],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Change your password."""
    from .auth import Authenticator, SessionStorage

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_change():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            success, message = await auth.change_password(
                conn, session.user_id, current_password, new_password
            )
            return success, message
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_change())
        if success:
            storage.clear_token()
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
                console.print("[dim]Please log in again with your new password[/dim]")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@auth_app.command("reset-password")
def auth_reset_password(
    username: Annotated[str, typer.Option("--username", "-u", prompt=True, help="Username")],
    new_password: Annotated[
        str | None,
        typer.Option("--password", "-p", help="New password (will prompt if not provided)"),
    ] = None,
    no_require_change: bool = typer.Option(
        False, "--no-require-change", help="Don't require password change on next login"
    ),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Reset a user's password (admin only)."""
    from .auth import Authenticator, SessionStorage, UserManager

    if new_password is None:
        new_password = typer.prompt("New password", hide_input=True, confirmation_prompt=True)

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_reset():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = UserManager()
            target_user = await manager.get_user_by_username(conn, username)
            if not target_user:
                return False, f"User '{username}' not found"

            success, message = await manager.reset_password(
                conn, target_user.user_id, new_password, require_change=not no_require_change
            )
            return success, message
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_reset())
        if success:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@auth_app.command("list-users")
def auth_list_users(
    include_inactive: bool = typer.Option(
        False, "--include-inactive", "-a", help="Include disabled users"
    ),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List all users."""
    import json

    from .auth import AuthSchemaManager, UserManager

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_list():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            if not await schema_manager.schema_exists(conn):
                return []

            manager = UserManager()
            users = await manager.list_users(conn, include_inactive)
            return users
        finally:
            await conn.close()

    try:
        users = asyncio.run(run_list())

        if json_output:
            output = []
            for u in users:
                output.append(
                    {
                        "user_id": u.user_id,
                        "username": u.username,
                        "email": u.email,
                        "is_active": u.is_active,
                        "is_locked": u.is_locked,
                        "last_login_at": u.last_login_at.isoformat() if u.last_login_at else None,
                    }
                )
            console.print(json.dumps(output, indent=2))
        elif users:
            for u in users:
                status = ""
                if not u.is_active:
                    status = " [red](disabled)[/red]"
                elif u.is_locked:
                    status = " [yellow](locked)[/yellow]"
                console.print(f"[cyan]{u.username}[/cyan]{status}")
                console.print(f"  ID: {u.user_id}")
                if u.email:
                    console.print(f"  Email: {u.email}")
                if u.last_login_at:
                    console.print(f"  Last login: {u.last_login_at.isoformat()}")
                console.print()
        else:
            if not quiet:
                console.print("[dim]No users found[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@auth_app.command("disable-user")
def auth_disable_user(
    username: Annotated[str, typer.Option("--username", "-u", prompt=True, help="Username")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Disable a user account."""
    from .auth import Authenticator, SessionStorage, UserManager

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_disable():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = UserManager()
            target_user = await manager.get_user_by_username(conn, username)
            if not target_user:
                return False, f"User '{username}' not found"

            if target_user.user_id == session.user_id:
                return False, "Cannot disable your own account"

            success, message = await manager.disable_user(conn, target_user.user_id)
            return success, message
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_disable())
        if success:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@auth_app.command("enable-user")
def auth_enable_user(
    username: Annotated[str, typer.Option("--username", "-u", prompt=True, help="Username")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Enable a disabled user account."""
    from .auth import Authenticator, SessionStorage, UserManager

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_enable():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = UserManager()
            target_user = await manager.get_user_by_username(conn, username)
            if not target_user:
                return False, f"User '{username}' not found"

            success, message = await manager.enable_user(conn, target_user.user_id)
            return success, message
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_enable())
        if success:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@auth_app.command("unlock-user")
def auth_unlock_user(
    username: Annotated[str, typer.Option("--username", "-u", prompt=True, help="Username")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Unlock a locked user account."""
    from .auth import Authenticator, SessionStorage, UserManager

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_unlock():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = UserManager()
            target_user = await manager.get_user_by_username(conn, username)
            if not target_user:
                return False, f"User '{username}' not found"

            success, message = await manager.unlock_user(conn, target_user.user_id)
            return success, message
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_unlock())
        if success:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


session_app = typer.Typer(help="Session management (HIPAA 164.312(a)(2)(iii))")
app.add_typer(session_app, name="session")


@session_app.command("status")
def session_status(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Show current session status and remaining time."""
    import json as json_module
    from datetime import UTC, datetime

    from .auth import Authenticator, SessionStorage

    storage = SessionStorage()
    info = storage.get_session_info()

    if not info:
        console.print("[yellow]No active session[/yellow]")
        raise typer.Exit(1)

    token = info.get("token")
    if not token:
        console.print("[yellow]No active session[/yellow]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_status():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token, update_activity=False)
            return session, auth.session_config
        finally:
            await conn.close()

    try:
        session, config = asyncio.run(run_status())
        now = datetime.now(UTC)

        if session:
            time_remaining = session.expires_at - now
            minutes_remaining = int(time_remaining.total_seconds() / 60)

            if json_output:
                output = {
                    "session_id": str(session.session_id),
                    "username": session.username,
                    "user_id": session.user_id,
                    "created_at": session.created_at.isoformat(),
                    "expires_at": session.expires_at.isoformat(),
                    "last_activity_at": session.last_activity_at.isoformat()
                    if session.last_activity_at
                    else None,
                    "minutes_remaining": minutes_remaining,
                    "client_ip": session.client_ip,
                    "inactivity_timeout_minutes": config.inactivity_timeout_minutes,
                }
                print(json_module.dumps(output, indent=2))
            else:
                console.print("[green]Session Active[/green]")
                console.print(f"  Username: {session.username}")
                console.print(f"  Session ID: {session.session_id}")
                console.print(f"  Created: {session.created_at.isoformat()}")
                console.print(f"  Expires: {session.expires_at.isoformat()}")
                console.print(f"  Time remaining: {minutes_remaining} minutes")
                console.print(f"  Inactivity timeout: {config.inactivity_timeout_minutes} minutes")
                if session.last_activity_at:
                    console.print(f"  Last activity: {session.last_activity_at.isoformat()}")
        else:
            storage.clear_token()
            console.print("[yellow]Session expired or invalid[/yellow]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@session_app.command("list")
def session_list(
    user: Annotated[str | None, typer.Option("--user", "-u", help="Filter by username")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List active sessions (admin only)."""
    import json as json_module

    from .auth import Authenticator, SessionManager, SessionStorage
    from .auth.users import UserManager

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_list():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return None, "Session expired or invalid"

            user_id = None
            if user:
                user_manager = UserManager()
                target_user = await user_manager.get_user_by_username(conn, user)
                if not target_user:
                    return None, f"User '{user}' not found"
                user_id = target_user.user_id

            manager = SessionManager()
            sessions = await manager.list_active_sessions(conn, user_id)
            return sessions, None
        finally:
            await conn.close()

    try:
        sessions, error = asyncio.run(run_list())

        if error:
            console.print(f"[red]{error}[/red]")
            if "expired" in error.lower():
                storage.clear_token()
            raise typer.Exit(1)

        if json_output:
            output = []
            for s in sessions:
                output.append(
                    {
                        "session_id": str(s["session_id"]),
                        "user_id": s["user_id"],
                        "username": s["username"],
                        "created_at": s["created_at"].isoformat(),
                        "expires_at": s["expires_at"].isoformat(),
                        "last_activity_at": s["last_activity_at"].isoformat()
                        if s["last_activity_at"]
                        else None,
                        "client_ip": s["client_ip"],
                    }
                )
            print(json_module.dumps(output, indent=2))
        else:
            if not sessions:
                console.print("[dim]No active sessions[/dim]")
            else:
                console.print(f"[bold]Active Sessions ({len(sessions)})[/bold]")
                for s in sessions:
                    console.print(f"\n  Session: {s['session_id']}")
                    console.print(f"    User: {s['username']} (ID: {s['user_id']})")
                    console.print(f"    Created: {s['created_at'].isoformat()}")
                    console.print(f"    Expires: {s['expires_at'].isoformat()}")
                    if s["last_activity_at"]:
                        console.print(f"    Last activity: {s['last_activity_at'].isoformat()}")
                    if s["client_ip"]:
                        console.print(f"    Client IP: {s['client_ip']}")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@session_app.command("terminate")
def session_terminate(
    session_id: Annotated[str, typer.Argument(help="Session ID to terminate")],
    reason: Annotated[str, typer.Option("--reason", "-r", help="Termination reason")] = "admin",
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Terminate a specific session (admin only)."""
    from uuid import UUID

    from .auth import Authenticator, SessionManager, SessionStorage

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        target_session_id = UUID(session_id)
    except ValueError:
        console.print(f"[red]Invalid session ID: {session_id}[/red]")
        raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_terminate():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired or invalid"

            manager = SessionManager()
            success = await manager.terminate_session(conn, target_session_id, reason)
            if success:
                return True, f"Session {session_id} terminated"
            else:
                return False, f"Session {session_id} not found or already terminated"
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_terminate())

        if success:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
        else:
            console.print(f"[red]{message}[/red]")
            if "expired" in message.lower():
                storage.clear_token()
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@session_app.command("terminate-all")
def session_terminate_all(
    user: Annotated[str, typer.Option("--user", "-u", help="Username to terminate sessions for")],
    reason: Annotated[str, typer.Option("--reason", "-r", help="Termination reason")] = "admin",
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Terminate all sessions for a user (admin only)."""
    from .auth import Authenticator, SessionManager, SessionStorage
    from .auth.users import UserManager

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_terminate():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return 0, "Session expired or invalid"

            user_manager = UserManager()
            target_user = await user_manager.get_user_by_username(conn, user)
            if not target_user:
                return 0, f"User '{user}' not found"

            manager = SessionManager()
            count = await manager.terminate_user_sessions(conn, target_user.user_id, reason)
            return count, None
        finally:
            await conn.close()

    try:
        count, error = asyncio.run(run_terminate())

        if error:
            console.print(f"[red]{error}[/red]")
            if "expired" in error.lower():
                storage.clear_token()
            raise typer.Exit(1)

        if not quiet:
            console.print(f"[green]✓[/green] Terminated {count} session(s) for user '{user}'")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@session_app.command("cleanup")
def session_cleanup(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Clean up expired sessions."""
    from .auth import Authenticator, SessionManager, SessionStorage

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_cleanup():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return 0, "Session expired or invalid"

            manager = SessionManager()
            count = await manager.cleanup_expired_sessions(conn)
            return count, None
        finally:
            await conn.close()

    try:
        count, error = asyncio.run(run_cleanup())

        if error:
            console.print(f"[red]{error}[/red]")
            if "expired" in error.lower():
                storage.clear_token()
            raise typer.Exit(1)

        if not quiet:
            console.print(f"[green]✓[/green] Cleaned up {count} expired session(s)")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


roles_app = typer.Typer(help="Role-Based Access Control (HIPAA 164.312(a)(1))")
app.add_typer(roles_app, name="roles")


@roles_app.command("list")
def roles_list(
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List all available roles."""
    import json

    from .auth.roles import RoleManager

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_list():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            manager = RoleManager()
            roles = await manager.list_roles(conn)
            return roles
        finally:
            await conn.close()

    try:
        roles = asyncio.run(run_list())

        if json_output:
            output = []
            for r in roles:
                output.append(
                    {
                        "role_id": r.role_id,
                        "role_name": r.role_name,
                        "description": r.description,
                        "is_system_role": r.is_system_role,
                    }
                )
            console.print(json.dumps(output, indent=2))
        elif roles:
            for r in roles:
                system_tag = " [dim](system)[/dim]" if r.is_system_role else ""
                console.print(f"[cyan]{r.role_name}[/cyan]{system_tag}")
                if r.description:
                    console.print(f"  {r.description}")
        else:
            if not quiet:
                console.print("[dim]No roles defined[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@roles_app.command("assign")
def roles_assign(
    username: Annotated[str, typer.Option("--user", "-u", help="Username")],
    role: Annotated[str, typer.Option("--role", "-r", help="Role name")],
    expires: Annotated[
        str | None, typer.Option("--expires", help="Expiry date (YYYY-MM-DD)")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Assign a role to a user."""
    from datetime import datetime

    from .auth import Authenticator, AuthSchemaManager, SessionStorage, UserManager
    from .auth.roles import RoleManager

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    expires_at = None
    if expires:
        try:
            expires_at = datetime.fromisoformat(expires)
        except ValueError:
            console.print("[red]Invalid date format. Use YYYY-MM-DD[/red]")
            raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_assign():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            if not await schema_manager.schema_exists(conn):
                await schema_manager.create_auth_schema(conn)

            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            user_manager = UserManager()
            target_user = await user_manager.get_user_by_username(conn, username)
            if not target_user:
                return False, f"User '{username}' not found"

            role_manager = RoleManager()
            success, message = await role_manager.assign_role(
                conn, target_user.user_id, role, session.user_id, expires_at
            )
            return success, message
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_assign())
        if success:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@roles_app.command("revoke")
def roles_revoke(
    username: Annotated[str, typer.Option("--user", "-u", help="Username")],
    role: Annotated[str, typer.Option("--role", "-r", help="Role name")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Revoke a role from a user."""
    from .auth import Authenticator, AuthSchemaManager, SessionStorage, UserManager
    from .auth.roles import RoleManager

    storage = SessionStorage()
    token, _ = storage.load_token()

    if not token:
        console.print("[red]Not logged in. Use 'vcf-pg-loader auth login' first.[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_revoke():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            if not await schema_manager.schema_exists(conn):
                return False, "Auth schema not initialized"

            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            user_manager = UserManager()
            target_user = await user_manager.get_user_by_username(conn, username)
            if not target_user:
                return False, f"User '{username}' not found"

            role_manager = RoleManager()
            success, message = await role_manager.revoke_role(
                conn, target_user.user_id, role, session.user_id
            )
            return success, message
        finally:
            await conn.close()

    try:
        success, message = asyncio.run(run_revoke())
        if success:
            if not quiet:
                console.print(f"[green]✓[/green] {message}")
        else:
            console.print(f"[red]Failed: {message}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@roles_app.command("show")
def roles_show(
    username: Annotated[str, typer.Option("--user", "-u", help="Username")],
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Show roles assigned to a user."""
    import json

    from .auth import AuthSchemaManager, UserManager
    from .auth.roles import RoleManager

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_show():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            if not await schema_manager.schema_exists(conn):
                return None, []

            user_manager = UserManager()
            target_user = await user_manager.get_user_by_username(conn, username)
            if not target_user:
                return None, []

            role_manager = RoleManager()
            roles = await role_manager.get_user_roles(conn, target_user.user_id)
            return target_user, roles
        finally:
            await conn.close()

    try:
        user, roles = asyncio.run(run_show())

        if user is None:
            console.print(f"[red]User '{username}' not found[/red]")
            raise typer.Exit(1)

        if json_output:
            output = []
            for r in roles:
                output.append(
                    {
                        "role_name": r.role_name,
                        "granted_at": r.granted_at.isoformat() if r.granted_at else None,
                        "expires_at": r.expires_at.isoformat() if r.expires_at else None,
                    }
                )
            console.print(json.dumps(output, indent=2))
        elif roles:
            console.print(f"[bold]Roles for {username}:[/bold]")
            for r in roles:
                expires_str = ""
                if r.expires_at:
                    expires_str = f" [dim](expires {r.expires_at.isoformat()})[/dim]"
                console.print(f"  [cyan]{r.role_name}[/cyan]{expires_str}")
        else:
            if not quiet:
                console.print(f"[dim]No roles assigned to {username}[/dim]")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


permissions_app = typer.Typer(help="Permission management (HIPAA 164.312(a)(1))")
app.add_typer(permissions_app, name="permissions")


@permissions_app.command("list")
def permissions_list(
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List all available permissions."""
    import json

    from .auth.permissions import PermissionChecker

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_list():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            checker = PermissionChecker()
            permissions = await checker.list_permissions(conn)
            return permissions
        finally:
            await conn.close()

    try:
        permissions = asyncio.run(run_list())

        if json_output:
            output = []
            for p in permissions:
                output.append(
                    {
                        "permission_name": p.permission_name,
                        "resource_type": p.resource_type,
                        "action": p.action,
                        "description": p.description,
                    }
                )
            console.print(json.dumps(output, indent=2))
        elif permissions:
            current_resource = None
            for p in permissions:
                if p.resource_type != current_resource:
                    if current_resource is not None:
                        console.print()
                    console.print(f"[bold]{p.resource_type}:[/bold]")
                    current_resource = p.resource_type
                desc = f" - {p.description}" if p.description else ""
                console.print(f"  [cyan]{p.permission_name}[/cyan]{desc}")
        else:
            if not quiet:
                console.print("[dim]No permissions defined[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@permissions_app.command("check")
def permissions_check(
    username: Annotated[str, typer.Option("--user", "-u", help="Username")],
    permission: Annotated[str, typer.Option("--permission", "-p", help="Permission name")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Check if a user has a specific permission."""
    from .auth import AuthSchemaManager, UserManager
    from .auth.permissions import PermissionChecker

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_check():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            if not await schema_manager.schema_exists(conn):
                return None, False

            user_manager = UserManager()
            target_user = await user_manager.get_user_by_username(conn, username)
            if not target_user:
                return None, False

            checker = PermissionChecker()
            has_perm = await checker.has_permission(conn, target_user.user_id, permission)
            return target_user, has_perm
        finally:
            await conn.close()

    try:
        user, has_perm = asyncio.run(run_check())

        if user is None:
            console.print(f"[red]User '{username}' not found[/red]")
            raise typer.Exit(1)

        if has_perm:
            console.print(f"[green]✓[/green] {username} has permission '{permission}'")
        else:
            console.print(f"[red]✗[/red] {username} does NOT have permission '{permission}'")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
