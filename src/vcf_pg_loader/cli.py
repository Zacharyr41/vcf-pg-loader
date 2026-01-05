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

    if os.environ.get("VCF_PG_LOADER_NO_MANAGED_DB"):
        return None

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
    anonymize: bool = typer.Option(
        True, "--anonymize/--no-anonymize", help="Anonymize sample IDs for HIPAA compliance"
    ),
    sanitize_headers: bool = typer.Option(
        True, "--sanitize-headers/--no-sanitize-headers", help="Sanitize VCF headers to remove PHI"
    ),
    phi_scan: bool = typer.Option(False, "--phi-scan", help="Scan for PHI before loading"),
    fail_on_phi: bool = typer.Option(
        False, "--fail-on-phi", help="Fail if PHI is detected during scan"
    ),
    hipaa_mode: Annotated[
        bool | None,
        typer.Option(
            "--hipaa-mode/--no-hipaa-mode",
            help="Enable/disable all HIPAA compliance features (TLS, anonymization, header sanitization). "
            "Default: enabled. Use --no-hipaa-mode for local development.",
        ),
    ] = None,
    min_info_score: Annotated[
        float | None,
        typer.Option(
            "--min-info-score",
            help="Filter imputed variants below this R²/INFO threshold (e.g., 0.8)",
        ),
    ] = None,
    imputation_source: Annotated[
        str,
        typer.Option(
            "--imputation-source",
            help="Imputation source: minimac4, beagle, impute2, or auto (default: auto-detect)",
        ),
    ] = "auto",
    store_genotypes: bool = typer.Option(
        False, "--store-genotypes", help="Enable per-sample genotype storage"
    ),
    adj_filter: bool = typer.Option(
        False,
        "--adj-filter",
        help="Only store genotypes passing ADJ criteria (GQ>=20, DP>=10, AB>=0.2)",
    ),
    dosage_only: bool = typer.Option(
        False, "--dosage-only", help="Store only dosage values, not hard calls (space saving)"
    ),
    parallel_query_workers: Annotated[
        int | None,
        typer.Option(
            "--parallel-query-workers",
            help="PostgreSQL parallel query workers (max_parallel_workers_per_gather). "
            "Higher values speed up PRS queries across chromosome partitions.",
        ),
    ] = None,
) -> None:
    """Load a VCF file into PostgreSQL.

    If --db is not specified, uses the managed database (auto-starts if needed).
    Use --db auto to explicitly use managed database, or provide a PostgreSQL URL.
    Can also specify connection via --host, --port, --database, --user options.
    """
    setup_logging(verbose, quiet)

    if hipaa_mode is False:
        require_tls = False
        anonymize = False
        sanitize_headers = False
        if not quiet:
            console.print(
                "[yellow]⚠ HIPAA mode disabled - not for production use with PHI[/yellow]"
            )

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
            anonymize=anonymize,
            sanitize_headers=sanitize_headers,
            phi_scan=phi_scan,
            fail_on_phi=fail_on_phi,
            min_info_score=min_info_score,
            imputation_source=imputation_source,
            store_genotypes=store_genotypes,
            adj_filter=adj_filter,
            dosage_only=dosage_only,
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
            anonymize=anonymize,
            sanitize_headers=sanitize_headers,
            phi_scan=phi_scan,
            fail_on_phi=fail_on_phi,
            min_info_score=min_info_score,
            imputation_source=imputation_source,
            store_genotypes=store_genotypes,
            adj_filter=adj_filter,
            dosage_only=dosage_only,
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
                variants_skipped = result.get("variants_skipped", 0)
                if variants_skipped > 0:
                    console.print(
                        f"[green]✓[/green] Loaded {result['variants_loaded']:,} variants "
                        f"(skipped {variants_skipped:,} with INFO < {min_info_score})"
                    )
                else:
                    console.print(f"[green]✓[/green] Loaded {result['variants_loaded']:,} variants")
                console.print(f"  Batch ID: {result['load_batch_id']}")
                console.print(f"  File SHA256: {result['file_hash']}")
            report_data = {
                "status": "success",
                "variants_loaded": result.get("variants_loaded", 0),
                "variants_skipped": result.get("variants_skipped", 0),
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
    require_tls: bool = typer.Option(
        True, "--require-tls/--no-require-tls", help="Require TLS for database connections"
    ),
) -> None:
    """Validate a completed load."""
    try:
        batch_uuid = UUID(load_batch_id)
    except ValueError:
        console.print(f"[red]Error: Invalid UUID format: {load_batch_id}[/red]")
        raise typer.Exit(1) from None

    resolved_db_url = _resolve_database_url(db_url, quiet=False)
    if not resolved_db_url:
        console.print("[red]Error: Could not resolve database URL[/red]")
        raise typer.Exit(1)

    global _default_tls_config
    _default_tls_config = TLSConfig(require_tls=require_tls)

    async def run_validation() -> None:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())

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
    require_tls: bool = typer.Option(
        True, "--require-tls/--no-require-tls", help="Require TLS for database connections"
    ),
    create_validation_functions: Annotated[
        bool,
        typer.Option(
            "--create-validation-functions/--skip-validation-functions",
            help="Create SQL validation functions (HWE, allele frequency, etc.)",
        ),
    ] = True,
    parallel_query_workers: Annotated[
        int | None,
        typer.Option(
            "--parallel-query-workers",
            help="Set PostgreSQL max_parallel_workers_per_gather for session",
        ),
    ] = None,
) -> None:
    """Initialize database schema.

    Creates the complete database schema including:
    - Variants table (partitioned by chromosome)
    - Load audit table
    - Samples table
    - HIPAA-compliant audit logging (unless --skip-audit)
    - SQL validation functions (HWE, allele frequency, n_eff, alleles_match)
    """
    resolved_db_url = _resolve_database_url(db_url, quiet=False)
    if not resolved_db_url:
        console.print("[red]Error: Could not resolve database URL[/red]")
        raise typer.Exit(1)

    global _default_tls_config
    _default_tls_config = TLSConfig(require_tls=require_tls)

    async def run_init() -> None:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())

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

            if create_validation_functions:
                await schema_manager.create_validation_functions(conn)
                console.print("[green]✓[/green] SQL validation functions created")

            if parallel_query_workers is not None:
                await schema_manager.enable_parallel_query(conn, workers=parallel_query_workers)
                console.print(
                    f"[green]✓[/green] Parallel query workers set to {parallel_query_workers}"
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


@app.command("import-gwas")
def import_gwas(
    tsv_path: Annotated[Path, typer.Argument(help="Path to GWAS-SSF format TSV file")],
    study_accession: Annotated[
        str,
        typer.Option("--study-accession", "-a", help="GWAS Catalog accession (e.g., GCST90002357)"),
    ] = ...,
    trait_name: Annotated[
        str | None, typer.Option("--trait", "-t", help="Human-readable trait name")
    ] = None,
    trait_ontology_id: Annotated[str | None, typer.Option("--efo", help="EFO ontology ID")] = None,
    publication_pmid: Annotated[str | None, typer.Option("--pmid", help="PubMed ID")] = None,
    sample_size: Annotated[
        int | None, typer.Option("--sample-size", "-n", help="Total sample size")
    ] = None,
    n_cases: Annotated[
        int | None, typer.Option("--n-cases", help="Number of cases (binary traits)")
    ] = None,
    n_controls: Annotated[
        int | None, typer.Option("--n-controls", help="Number of controls (binary traits)")
    ] = None,
    genome_build: Annotated[
        str, typer.Option("--genome-build", "-g", help="Reference genome build")
    ] = "GRCh38",
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Import GWAS summary statistics from a GWAS-SSF format TSV file.

    Creates study metadata record and imports per-variant summary statistics.
    Matches variants to existing database variants by chr:pos:ref:alt or rsID.
    Computes is_effect_allele_alt to track effect allele orientation vs VCF.

    Example:
        vcf-pg-loader import-gwas gwas.tsv -a GCST90002357 -t "Height" -n 253288
    """
    setup_logging(verbose, quiet)

    if not tsv_path.exists():
        console.print(f"[red]Error: TSV file not found: {tsv_path}[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .gwas import GWASLoader, GWASSchemaManager

    async def run_import() -> dict:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = SchemaManager()
            await schema_manager.create_schema(conn)

            gwas_schema = GWASSchemaManager()
            await gwas_schema.create_gwas_schema(conn)

            loader = GWASLoader()
            result = await loader.import_gwas(
                conn=conn,
                tsv_path=tsv_path,
                study_accession=study_accession,
                trait_name=trait_name,
                trait_ontology_id=trait_ontology_id,
                publication_pmid=publication_pmid,
                sample_size=sample_size,
                n_cases=n_cases,
                n_controls=n_controls,
                genome_build=genome_build,
            )

            await gwas_schema.create_gwas_indexes(conn)

            return result
        finally:
            await conn.close()

    try:
        result = asyncio.run(run_import())
        if not quiet:
            console.print(f"[green]✓[/green] Imported {result['stats_imported']:,} statistics")
            console.print(f"  Study ID: {result['study_id']}")
            console.print(f"  Study Accession: {study_accession}")
            console.print(f"  Matched variants: {result['stats_matched']:,}")
            console.print(f"  Unmatched variants: {result['stats_unmatched']:,}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("list-studies")
def list_studies(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List all imported GWAS studies."""
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
            from .gwas import GWASSchemaManager

            gwas_schema = GWASSchemaManager()
            if not await gwas_schema.verify_gwas_schema(conn):
                return []

            rows = await conn.fetch("""
                SELECT study_id, study_accession, trait_name, sample_size,
                       n_cases, n_controls, genome_build, created_at
                FROM studies
                ORDER BY created_at DESC
            """)
            return [dict(r) for r in rows]
        finally:
            await conn.close()

    try:
        studies = asyncio.run(run_list())

        if json_output:
            console.print(json.dumps(studies, indent=2, default=str))
        elif studies:
            for study in studies:
                console.print(f"[cyan]{study['study_accession']}[/cyan]")
                if study.get("trait_name"):
                    console.print(f"  Trait: {study['trait_name']}")
                if study.get("sample_size"):
                    console.print(f"  Sample size: {study['sample_size']:,}")
                if study.get("n_cases"):
                    console.print(
                        f"  Cases/Controls: {study['n_cases']:,}/{study.get('n_controls', 0):,}"
                    )
                console.print(f"  Genome build: {study.get('genome_build', 'N/A')}")
                console.print()
        else:
            if not quiet:
                console.print("[dim]No GWAS studies imported[/dim]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("import-pgs")
def import_pgs(
    pgs_path: Annotated[Path, typer.Argument(help="Path to PGS Catalog scoring file")],
    pgs_id: Annotated[
        str | None,
        typer.Option("--pgs-id", "-i", help="Override PGS ID from file header"),
    ] = None,
    validate_build: Annotated[
        bool,
        typer.Option("--validate-build", help="Validate genome build matches database"),
    ] = False,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Import PGS Catalog scoring file for PRS weights.

    Parses PGS Catalog format files with header metadata (###PGS CATALOG SCORING FILE)
    and imports per-variant weights. Matches variants to existing database variants
    by chr:pos:ref:alt or rsID.

    Supports advanced PRS features including interaction terms, haplotype effects,
    and dominance/recessive models.

    Example:
        vcf-pg-loader import-pgs PGS000001.txt
        vcf-pg-loader import-pgs PGS000001.txt --validate-build
    """
    setup_logging(verbose, quiet)

    if not pgs_path.exists():
        console.print(f"[red]Error: PGS file not found: {pgs_path}[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .prs import PGSLoader, PRSSchemaManager

    async def run_import() -> dict:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            prs_schema = PRSSchemaManager()
            await prs_schema.create_prs_schema(conn)

            loader = PGSLoader()
            result = await loader.import_pgs(
                conn=conn,
                pgs_path=pgs_path,
                pgs_id_override=pgs_id,
                validate_build=validate_build,
            )

            return result
        finally:
            await conn.close()

    try:
        result = asyncio.run(run_import())
        if not quiet:
            console.print(f"[green]✓[/green] Imported {result['weights_imported']:,} weights")
            console.print(f"  PGS ID: {result['pgs_id']}")
            console.print(f"  Matched variants: {result['weights_matched']:,}")
            console.print(f"  Unmatched variants: {result['weights_unmatched']:,}")
            match_rate = (
                result["weights_matched"] / result["weights_imported"] * 100
                if result["weights_imported"] > 0
                else 0
            )
            console.print(f"  Match rate: {match_rate:.1f}%")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("list-pgs")
def list_pgs(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List all imported PGS Catalog scores."""
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
            from .prs import PRSSchemaManager

            prs_schema = PRSSchemaManager()
            if not await prs_schema.verify_prs_schema(conn):
                return []

            return await prs_schema.list_scores(conn)
        finally:
            await conn.close()

    try:
        scores = asyncio.run(run_list())

        if json_output:
            console.print(json.dumps(scores, indent=2, default=str))
        elif scores:
            for score in scores:
                console.print(f"[cyan]{score['pgs_id']}[/cyan]")
                if score.get("trait_name"):
                    console.print(f"  Trait: {score['trait_name']}")
                if score.get("weight_type"):
                    console.print(f"  Weight type: {score['weight_type']}")
                console.print(f"  Genome build: {score.get('genome_build', 'N/A')}")
                console.print(f"  Weights: {score.get('weight_count', 0):,}")
                console.print(f"  Matched: {score.get('matched_count', 0):,}")
                console.print()
        else:
            if not quiet:
                console.print("[dim]No PGS scores imported[/dim]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("import-frequencies")
def import_frequencies(
    vcf_path: Annotated[Path, typer.Argument(help="Path to VCF file with population frequencies")],
    source: Annotated[
        str,
        typer.Option(
            "--source",
            "-s",
            help="Frequency source (gnomAD_v2.1, gnomAD_v3, gnomAD_v4, TOPMed)",
        ),
    ] = ...,
    subset: Annotated[
        str, typer.Option("--subset", help="Data subset (all, controls, non_neuro, non_cancer)")
    ] = "all",
    prefix: Annotated[
        str, typer.Option("--prefix", help="INFO field prefix (e.g., 'gnomad_' for vcfanno)")
    ] = "",
    update_popmax: Annotated[
        bool, typer.Option("--update-popmax/--no-update-popmax", help="Update popmax in variants")
    ] = True,
    batch_size: Annotated[
        int, typer.Option("--batch-size", "-b", help="Batch size for imports")
    ] = 10000,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Import population frequencies from gnomAD-annotated VCF.

    Parses gnomAD INFO fields for all populations (AFR, AMR, ASJ, EAS, FIN, NFE, SAS)
    and stores them in the normalized population_frequencies table. Automatically
    computes popmax (excluding bottlenecked populations ASJ/FIN) and updates variants.

    Supports:
    - Direct gnomAD VCF files
    - Pre-annotated VCF files (via vcfanno)
    - gnomAD v2, v3, v4 formats
    - TOPMed format

    Example:
        vcf-pg-loader import-frequencies annotated.vcf.gz -s gnomAD_v3 --db postgresql://...
    """
    setup_logging(verbose, quiet)

    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .annotations import PopulationFreqLoader, PopulationFreqSchemaManager

    async def run_import() -> dict:
        import cyvcf2

        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            popfreq_schema = PopulationFreqSchemaManager()
            await popfreq_schema.create_population_frequencies_table(conn)
            await popfreq_schema.create_popfreq_indexes(conn)

            variant_lookup = await _build_variant_lookup(conn)

            loader = PopulationFreqLoader(batch_size=batch_size)
            vcf = cyvcf2.VCF(str(vcf_path))

            total_variants = 0
            matched_variants = 0
            frequencies_inserted = 0
            batch = []

            for variant in vcf:
                total_variants += 1
                chrom = variant.CHROM
                if not chrom.startswith("chr"):
                    chrom = f"chr{chrom}"

                key = (chrom, variant.POS, variant.REF, variant.ALT[0] if variant.ALT else "")
                variant_id = variant_lookup.get(key)

                if variant_id is None:
                    continue

                matched_variants += 1
                info_dict = _extract_info_dict(variant)
                batch.append((variant_id, info_dict))

                if len(batch) >= batch_size:
                    result = await loader.import_batch_frequencies(
                        conn=conn,
                        batch=batch,
                        source=source,
                        subset=subset,
                        prefix=prefix,
                        update_popmax=update_popmax,
                    )
                    frequencies_inserted += result["frequencies_inserted"]
                    batch = []

            if batch:
                result = await loader.import_batch_frequencies(
                    conn=conn,
                    batch=batch,
                    source=source,
                    subset=subset,
                    prefix=prefix,
                    update_popmax=update_popmax,
                )
                frequencies_inserted += result["frequencies_inserted"]

            vcf.close()

            return {
                "total_variants": total_variants,
                "matched_variants": matched_variants,
                "frequencies_inserted": frequencies_inserted,
            }
        finally:
            await conn.close()

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            disable=quiet,
        ) as progress:
            progress.add_task("Importing frequencies...", total=None)
            result = asyncio.run(run_import())

        if not quiet:
            console.print(f"[green]✓[/green] Imported frequencies from {vcf_path.name}")
            console.print(f"  Source: {source}")
            console.print(f"  Variants processed: {result['total_variants']:,}")
            console.print(f"  Matched variants: {result['matched_variants']:,}")
            console.print(f"  Frequencies inserted: {result['frequencies_inserted']:,}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


async def _build_variant_lookup(conn: asyncpg.Connection) -> dict[tuple, int]:
    """Build lookup dict mapping (chrom, pos, ref, alt) to variant_id."""
    rows = await conn.fetch("""
        SELECT variant_id, chrom, pos, ref, alt
        FROM variants
    """)
    return {(row["chrom"], row["pos"], row["ref"], row["alt"]): row["variant_id"] for row in rows}


def _extract_info_dict(variant) -> dict:
    """Extract INFO fields from cyvcf2 variant as dictionary."""
    info_dict = {}
    for key in variant.INFO:
        try:
            value = variant.INFO.get(key)
            info_dict[key] = value
        except Exception:
            pass
    return info_dict


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


def _load_ld_blocks(
    bed_path: Path | None,
    build: str,
    population: str | None,
    db_url: str | None,
    quiet: bool,
) -> None:
    """Helper to load LD blocks."""
    if population is None:
        console.print("[red]Error: --population is required for ld-blocks[/red]")
        raise typer.Exit(1)

    population = population.upper()
    if population not in ("EUR", "AFR", "EAS", "SAS"):
        console.print(
            f"[red]Error: Invalid population '{population}'. Use EUR, AFR, EAS, or SAS[/red]"
        )
        raise typer.Exit(1)

    build = build.lower()
    if build not in ("grch37", "grch38"):
        console.print(f"[red]Error: Invalid build '{build}'. Use grch37 or grch38[/red]")
        raise typer.Exit(1)

    if bed_path is None:
        import importlib.resources

        try:
            with importlib.resources.files("vcf_pg_loader").joinpath(
                f"data/references/ld_blocks_{population.lower()}_{build}.bed.gz"
            ) as bundled_path:
                if bundled_path.exists():
                    bed_path = Path(bundled_path)
                else:
                    data_dir = Path(__file__).parent / "data" / "references"
                    bed_path = data_dir / f"ld_blocks_{population.lower()}_{build}.bed.gz"
                    if not bed_path.exists():
                        bed_path = data_dir / f"ld_blocks_{population.lower()}_{build}.bed"
        except Exception:
            data_dir = Path(__file__).parent / "data" / "references"
            bed_path = data_dir / f"ld_blocks_{population.lower()}_{build}.bed.gz"
            if not bed_path.exists():
                bed_path = data_dir / f"ld_blocks_{population.lower()}_{build}.bed"

    if not bed_path.exists():
        console.print(
            f"[red]Error: LD blocks file not found: {bed_path}[/red]\n"
            f"Please provide a path to the LD blocks BED file."
        )
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .references import LDBlockLoader, ReferenceSchemaManager

    async def run_load() -> dict:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            ref_schema = ReferenceSchemaManager()
            await ref_schema.create_ld_blocks_table(conn)

            loader = LDBlockLoader()
            result = await loader.load_berisa_pickrell_blocks(
                conn=conn,
                bed_path=bed_path,
                population=population,
                build=build,
            )

            return result
        finally:
            await conn.close()

    try:
        result = asyncio.run(run_load())
        if not quiet:
            console.print(f"[green]✓[/green] Loaded {result['blocks_loaded']:,} LD blocks")
            console.print(f"  Population: {result['population']}")
            console.print(f"  Build: {result['build']}")
            console.print(f"  Source: {result['source']}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("download-reference")
def download_reference(
    panel_type: Annotated[
        str,
        typer.Argument(help="Reference panel type (hapmap3)"),
    ],
    build: Annotated[
        str,
        typer.Option("--build", "-b", help="Genome build (grch37 or grch38)"),
    ] = "grch38",
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output directory for downloaded files"),
    ] = None,
    force: bool = typer.Option(False, "--force", "-f", help="Force re-download even if cached"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Download reference panel data for PRS analysis.

    Downloads HapMap3 (~1.1M SNPs) from authoritative sources (LDpred2 figshare).
    Files are cached locally for reuse.

    Example:
        vcf-pg-loader download-reference hapmap3 --build grch38
        vcf-pg-loader download-reference hapmap3 --build grch37 --force
        vcf-pg-loader download-reference hapmap3 --output /custom/path
    """
    setup_logging(verbose, quiet)

    panel_type_lower = panel_type.lower().replace("_", "-")
    if panel_type_lower != "hapmap3":
        console.print(f"[red]Error: Unknown panel type '{panel_type}'. Supported: hapmap3[/red]")
        raise typer.Exit(1)

    from .references.hapmap3_download import HapMap3DownloadConfig, HapMap3Downloader

    try:
        config = HapMap3DownloadConfig(
            build=build,
            cache_dir=output if output else HapMap3DownloadConfig().cache_dir,
        )
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None

    downloader = HapMap3Downloader(config)

    if not quiet:
        if downloader.is_cached() and not force:
            console.print(f"[yellow]HapMap3 {build} already cached at:[/yellow]")
            console.print(f"  {config.get_cache_path()}")
            console.print("[dim]Use --force to re-download[/dim]")
            return

    async def run_download() -> Path:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            disable=quiet,
        ) as progress:
            task = progress.add_task(f"Downloading HapMap3 {build}...", total=None)

            def update_progress(downloaded: int, total: int) -> None:
                progress.update(task, completed=downloaded, total=total)

            result = await downloader.download(
                force=force,
                progress_callback=update_progress if not quiet else None,
            )
            return result

    try:
        result_path = asyncio.run(run_download())
        if not quiet:
            console.print("[green]✓[/green] Downloaded HapMap3 reference")
            console.print(f"  Path: {result_path}")
            console.print(f"  Build: {build}")
    except Exception as e:
        console.print(f"[red]Error downloading reference: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("load-reference")
def load_reference(
    panel_type: Annotated[
        str,
        typer.Argument(help="Reference panel type (hapmap3, ld-blocks)"),
    ],
    file_path: Annotated[
        Path | None,
        typer.Argument(help="Path to reference panel file (TSV or BED)"),
    ] = None,
    build: Annotated[
        str,
        typer.Option("--build", "-b", help="Genome build (grch37 or grch38)"),
    ] = "grch38",
    population: Annotated[
        str | None,
        typer.Option("--population", "-p", help="Population for LD blocks (EUR, AFR, EAS, SAS)"),
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Load a reference panel into the database for PRS analysis.

    Reference panels like HapMap3 are used by PRS-CS, LDpred2, and other
    Bayesian PRS methods to restrict analysis to well-characterized SNPs.

    LD blocks from Berisa & Pickrell (2016) are used by PRS-CS and SBayesR
    to partition the genome into largely independent regions.

    If no file path is provided, looks for bundled reference data in the
    package data directory.

    Example:
        vcf-pg-loader load-reference hapmap3 --build grch38
        vcf-pg-loader load-reference hapmap3 /path/to/hapmap3.tsv --build grch37
        vcf-pg-loader load-reference ld-blocks --population EUR --build grch37
        vcf-pg-loader load-reference ld-blocks /path/to/blocks.bed --population EUR
    """
    setup_logging(verbose, quiet)

    panel_type_lower = panel_type.lower().replace("_", "-")
    if panel_type_lower not in ("hapmap3", "ld-blocks"):
        console.print(
            f"[red]Error: Unknown panel type '{panel_type}'. Supported: hapmap3, ld-blocks[/red]"
        )
        raise typer.Exit(1)

    if panel_type_lower == "ld-blocks":
        _load_ld_blocks(file_path, build, population, db_url, quiet)
        return

    tsv_path = file_path
    if panel_type_lower != "hapmap3":
        console.print(f"[red]Error: Unknown panel type '{panel_type}'. Supported: hapmap3[/red]")
        raise typer.Exit(1)

    build = build.lower()
    if build not in ("grch37", "grch38"):
        console.print(f"[red]Error: Invalid build '{build}'. Use grch37 or grch38[/red]")
        raise typer.Exit(1)

    if tsv_path is None:
        from .references.hapmap3_download import HapMap3DownloadConfig

        download_config = HapMap3DownloadConfig(build=build)
        cached_path = download_config.get_cache_path()

        if cached_path.exists():
            tsv_path = cached_path
            if not quiet:
                console.print(f"[dim]Using cached HapMap3: {cached_path}[/dim]")
        else:
            import importlib.resources

            try:
                with importlib.resources.files("vcf_pg_loader").joinpath(
                    f"data/references/hapmap3_{build}.tsv.gz"
                ) as bundled_path:
                    if bundled_path.exists():
                        tsv_path = Path(bundled_path)
                    else:
                        data_dir = Path(__file__).parent / "data" / "references"
                        tsv_path = data_dir / f"hapmap3_{build}.tsv.gz"
                        if not tsv_path.exists():
                            tsv_path = data_dir / f"hapmap3_{build}.tsv"
            except Exception:
                data_dir = Path(__file__).parent / "data" / "references"
                tsv_path = data_dir / f"hapmap3_{build}.tsv.gz"
                if not tsv_path.exists():
                    tsv_path = data_dir / f"hapmap3_{build}.tsv"

    if not tsv_path.exists():
        console.print(
            f"[red]Error: Reference file not found: {tsv_path}[/red]\n"
            f"[yellow]Tip: Download the full HapMap3 reference with:[/yellow]\n"
            f"  vcf-pg-loader download-reference hapmap3 --build {build}\n"
            f"Or provide a path to your own HapMap3 file."
        )
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .references import HapMap3Loader, ReferenceSchemaManager

    async def run_load() -> dict:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            ref_schema = ReferenceSchemaManager()
            await ref_schema.create_reference_panels_table(conn)

            loader = HapMap3Loader()
            result = await loader.load_reference_panel(
                conn=conn,
                tsv_path=tsv_path,
                build=build,
            )

            return result
        finally:
            await conn.close()

    try:
        result = asyncio.run(run_load())
        if not quiet:
            console.print(f"[green]✓[/green] Loaded {result['variants_loaded']:,} variants")
            console.print(f"  Panel: {result['panel_name']}")
            console.print(f"  Build: {result['build']}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("annotate-ld-blocks")
def annotate_ld_blocks(
    population: Annotated[
        str,
        typer.Option("--population", "-p", help="Population (EUR, AFR, EAS, SAS)"),
    ] = "EUR",
    build: Annotated[
        str | None,
        typer.Option("--build", "-b", help="Optional genome build filter"),
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Annotate loaded variants with LD block assignments.

    Assigns each variant to the LD block containing its position using
    efficient range queries. LD blocks must be loaded first using:
        vcf-pg-loader load-reference ld-blocks --population EUR

    Example:
        vcf-pg-loader annotate-ld-blocks --population EUR
        vcf-pg-loader annotate-ld-blocks --population AFR --build grch37
    """
    setup_logging(verbose, quiet)

    population = population.upper()
    if population not in ("EUR", "AFR", "EAS", "SAS"):
        console.print(
            f"[red]Error: Invalid population '{population}'. Use EUR, AFR, EAS, or SAS[/red]"
        )
        raise typer.Exit(1)

    if build:
        build = build.lower()
        if build not in ("grch37", "grch38"):
            console.print(f"[red]Error: Invalid build '{build}'. Use grch37 or grch38[/red]")
            raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .references import LDBlockLoader, ReferenceSchemaManager

    async def run_annotate() -> int:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            ref_schema = ReferenceSchemaManager()
            await ref_schema.add_ld_block_id_column(conn)

            loader = LDBlockLoader()
            updated = await loader.assign_variants_to_blocks(
                conn=conn,
                population=population,
                build=build,
            )

            return updated
        finally:
            await conn.close()

    try:
        updated = asyncio.run(run_annotate())
        if not quiet:
            console.print(
                f"[green]✓[/green] Assigned {updated:,} variants to {population} LD blocks"
            )
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command()
def doctor(
    check_container_security: bool = typer.Option(
        False,
        "--check-container-security",
        help="Run container security checks for HIPAA compliance",
    ),
) -> None:
    """Check system dependencies and configuration.

    Verifies that all required dependencies are installed and
    provides installation instructions for any that are missing.

    Use --check-container-security to validate container security
    settings when running inside a Docker container.
    """
    from .doctor import ContainerSecurityChecker, DependencyChecker

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

    if check_container_security:
        console.print("\n[bold]Container Security Checks[/bold]")
        console.print("─" * 30)

        security_checker = ContainerSecurityChecker()
        security_results = security_checker.check_all()

        for result in security_results:
            if result.passed:
                version_str = f" ({result.version})" if result.version else ""
                console.print(f"[green]✓[/green] {result.name}{version_str}")
                if result.message:
                    console.print(f"    [dim]{result.message}[/dim]")
            else:
                all_passed = False
                console.print(f"[red]✗[/red] {result.name}")
                if result.message:
                    console.print(f"    {result.message}")

    console.print()

    if all_passed:
        console.print("[green]All systems ready![/green]")
    else:
        console.print("[yellow]Some checks failed.[/yellow]")
        if not check_container_security:
            console.print("\nNote: Parsing and benchmarks work without Docker.")
            console.print("      Database features require Docker or external PostgreSQL.")
        else:
            console.print("\nSee docs/deployment/container-security.md for remediation.")


@app.command("compute-sample-qc")
def compute_sample_qc(
    batch_id: Annotated[
        int | None, typer.Option("--batch-id", "-b", help="Batch audit_id to compute QC for")
    ] = None,
    sample_id: Annotated[
        str | None, typer.Option("--sample-id", "-s", help="Single sample ID to compute QC for")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Compute sample-level QC metrics from loaded genotype data.

    Computes per-sample metrics including call rate, het/hom ratio, Ti/Tv ratio,
    sex inference from X chromosome heterozygosity, and F_inbreeding coefficient.

    Samples pass QC if:
    - call_rate >= 99%
    - contamination_estimate < 2.5% (or not available)
    - sex_concordant = TRUE (or not available)

    Example:
        vcf-pg-loader compute-sample-qc --batch-id 1
        vcf-pg-loader compute-sample-qc --sample-id SAMPLE001
    """
    setup_logging(verbose, quiet)

    if batch_id is None and sample_id is None:
        console.print("[red]Error: Either --batch-id or --sample-id required[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .qc.sample_qc import SampleQCComputer
    from .qc.schema import SampleQCSchemaManager

    async def run_compute() -> dict:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = SampleQCSchemaManager()
            await schema_manager.create_sample_qc_schema(conn)

            computer = SampleQCComputer(schema_manager)

            if batch_id is not None:
                result = await computer.compute_for_batch(conn, batch_id)
            else:
                metrics = await computer.compute_for_sample(conn, sample_id)
                result = metrics.to_db_row()
                result["qc_pass"] = (
                    metrics.call_rate >= 0.99
                    and (
                        metrics.contamination_estimate is None
                        or metrics.contamination_estimate < 0.025
                    )
                    and (metrics.sex_concordant is None or metrics.sex_concordant)
                )

            return result
        finally:
            await conn.close()

    try:
        result = asyncio.run(run_compute())

        if json_output:
            import json as json_module

            console.print(json_module.dumps(result, indent=2, default=str))
        elif not quiet:
            if batch_id is not None:
                n_samples = result["samples_processed"]
                console.print(f"[green]✓[/green] Computed QC for {n_samples} samples")
                console.print(f"  Batch ID: {batch_id}")
                console.print(f"  Pass: {result['samples_pass']}")
                console.print(f"  Fail: {result['samples_fail']}")
                console.print(f"  Mean call rate: {result['mean_call_rate']:.4f}")
            else:
                status = "[green]PASS[/green]" if result.get("qc_pass") else "[red]FAIL[/red]"
                console.print(f"Sample QC: {status}")
                console.print(f"  Sample ID: {result['sample_id']}")
                console.print(f"  Call rate: {result['call_rate']:.4f}")
                if result.get("het_hom_ratio"):
                    console.print(f"  Het/Hom ratio: {result['het_hom_ratio']:.4f}")
                if result.get("ti_tv_ratio"):
                    console.print(f"  Ti/Tv ratio: {result['ti_tv_ratio']:.4f}")
                if result.get("sex_inferred"):
                    console.print(f"  Inferred sex: {result['sex_inferred']}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@app.command("refresh-views")
def refresh_views(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    concurrent: bool = typer.Option(
        True, "--concurrent/--no-concurrent", help="Use CONCURRENTLY to avoid blocking reads"
    ),
    create: bool = typer.Option(False, "--create", help="Create views if they don't exist"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Refresh PRS materialized views.

    Refreshes pre-computed materialized views for common PRS query patterns:
    - prs_candidate_variants: HapMap3 variants passing QC filters
    - variant_qc_summary: Aggregate QC counts
    - chromosome_variant_counts: Per-chromosome summary

    Use --concurrent (default) to refresh without blocking reads.
    Use --create to create views if they don't exist.

    Example:
        vcf-pg-loader refresh-views --db postgresql://localhost/variants
        vcf-pg-loader refresh-views --create --no-concurrent
    """
    setup_logging(verbose, quiet)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .views.prs_views import PRSViewsManager

    async def run_refresh() -> dict[str, float]:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            mgr = PRSViewsManager()

            views_exist = await mgr.verify_prs_views(conn)
            if not views_exist:
                if create:
                    if not quiet:
                        console.print("Creating PRS materialized views...")
                    await mgr.create_prs_materialized_views(conn)
                else:
                    console.print(
                        "[red]Error: PRS views do not exist. Use --create to create them.[/red]"
                    )
                    raise typer.Exit(1)

            if not quiet:
                mode = "concurrently" if concurrent else "blocking"
                console.print(f"Refreshing PRS views ({mode})...")

            timings = await mgr.refresh_prs_views(conn, concurrent=concurrent)
            return timings
        finally:
            await conn.close()

    try:
        timings = asyncio.run(run_refresh())

        if json_output:
            import json as json_module

            console.print(json_module.dumps(timings, indent=2))
        elif not quiet:
            console.print("[green]✓[/green] PRS views refreshed")
            for view_name, elapsed in timings.items():
                console.print(f"  {view_name}: {elapsed:.3f}s")
            total = sum(timings.values())
            console.print(f"  Total: {total:.3f}s")
    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


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


retention_app = typer.Typer(help="Audit log retention policy management (HIPAA 164.316(b)(2)(i))")
audit_app.add_typer(retention_app, name="retention")


@retention_app.command("init")
def retention_init(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Initialize audit retention schema.

    Creates the audit_retention_policy table with HIPAA-compliant defaults.

    \b
    HIPAA Reference: 164.316(b)(2)(i) - 6-Year Minimum Retention
    """
    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            from .audit.retention import AuditRetentionManager

            manager = AuditRetentionManager()
            await manager.create_retention_schema(conn)
            console.print("[green]✓[/green] Audit retention schema initialized")
            console.print("  Default policy: 6-year retention per HIPAA")
        finally:
            await conn.close()

    asyncio.run(run())


@retention_app.command("status")
def retention_status(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Show audit log retention status.

    Displays current retention policy, compliance status, and partition info.

    \b
    HIPAA Reference: 164.316(b)(2)(i) - 6-Year Minimum Retention
    """
    import json as json_module

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            from .audit.retention import AuditRetentionManager

            manager = AuditRetentionManager()
            return await manager.get_retention_status(conn)
        finally:
            await conn.close()

    try:
        status = asyncio.run(run())
        if json_output:
            console.print(json_module.dumps(status.to_dict(), indent=2))
        else:
            console.print("[bold]Audit Retention Status[/bold]")
            console.print()
            if status.has_policy:
                if status.is_compliant:
                    console.print("[green]✓ HIPAA Compliant[/green]")
                else:
                    console.print("[red]✗ NOT HIPAA Compliant[/red]")
                console.print(f"  Retention Period: {status.retention_years} years")
                enforcement = (
                    "[green]Enabled[/green]"
                    if status.enforcement_enabled
                    else "[red]Disabled[/red]"
                )
                console.print(f"  Enforcement: {enforcement}")
            else:
                console.print("[yellow]No retention policy configured[/yellow]")
            console.print()
            console.print("[bold]Partition Info[/bold]")
            console.print(f"  Active Partitions: {status.partition_count}")
            console.print(f"  Archived Partitions: {status.archived_partition_count}")
            if status.oldest_partition_date:
                console.print(f"  Oldest Partition: {status.oldest_partition_date.isoformat()}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@retention_app.command("set-policy")
def retention_set_policy(
    years: Annotated[int, typer.Argument(help="Retention period in years (minimum 6)")],
    user_id: Annotated[
        int | None, typer.Option("--user-id", "-u", help="User ID setting policy")
    ] = None,
    notes: Annotated[str | None, typer.Option("--notes", "-n", help="Policy notes")] = None,
    no_enforce: bool = typer.Option(
        False, "--no-enforce", help="Disable minimum enforcement (not recommended)"
    ),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Set audit log retention policy.

    Configures the minimum retention period for audit logs.
    HIPAA requires at least 6 years retention.

    \b
    HIPAA Reference: 164.316(b)(2)(i) - 6-Year Minimum Retention
    """
    import json as json_module

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            from .audit.retention import AuditRetentionManager

            manager = AuditRetentionManager()
            return await manager.set_retention_policy(
                conn,
                retention_years=years,
                enforce_minimum=not no_enforce,
                created_by=user_id,
                notes=notes,
            )
        finally:
            await conn.close()

    try:
        policy = asyncio.run(run())
        if json_output:
            output = {
                "policy_id": policy.policy_id,
                "retention_years": policy.retention_years,
                "enforce_minimum": policy.enforce_minimum,
                "is_compliant": policy.is_compliant(),
                "created_at": policy.created_at.isoformat(),
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            console.print("[green]✓[/green] Retention policy updated")
            console.print(f"  Policy ID: {policy.policy_id}")
            console.print(f"  Retention: {policy.retention_years} years")
            enforcement = (
                "[green]Enabled[/green]" if policy.enforce_minimum else "[yellow]Disabled[/yellow]"
            )
            console.print(f"  Enforcement: {enforcement}")
            if policy.is_compliant():
                console.print("[green]  ✓ HIPAA Compliant[/green]")
            else:
                console.print("[red]  ✗ NOT HIPAA Compliant[/red]")
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@retention_app.command("verify")
def retention_verify(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Verify retention policy integrity.

    Checks that retention policy is properly configured and enforced.

    \b
    HIPAA Reference: 164.316(b)(2)(i) - 6-Year Minimum Retention
    """
    import json as json_module

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            from .audit.retention import AuditRetentionManager

            manager = AuditRetentionManager()
            return await manager.verify_retention_integrity(conn)
        finally:
            await conn.close()

    try:
        is_valid, issues = asyncio.run(run())
        if json_output:
            output = {
                "is_valid": is_valid,
                "issues": issues,
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            if is_valid:
                console.print("[green]✓[/green] Retention policy integrity verified")
                console.print("  All checks passed")
            else:
                console.print("[red]✗[/red] Retention policy issues detected")
                for issue in issues:
                    console.print(f"  - {issue}")
                raise typer.Exit(1)
    except typer.Exit:
        raise
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


mfa_app = typer.Typer(help="Multi-factor authentication (HIPAA 164.312(d))")
auth_app.add_typer(mfa_app, name="mfa")


@mfa_app.command("enroll")
def mfa_enroll(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Start MFA enrollment for current user.

    HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication
    """
    from .auth import Authenticator, SessionStorage
    from .auth.mfa import MFAManager

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

    async def run_enroll():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return None, "Session expired"

            manager = MFAManager()
            enrollment = await manager.enroll(conn, user_id=session.user_id)
            return enrollment, None
        finally:
            await conn.close()

    try:
        enrollment, error = asyncio.run(run_enroll())
        if error:
            console.print(f"[red]{error}[/red]")
            raise typer.Exit(1)

        if not quiet:
            console.print("[green]MFA enrollment started[/green]")
            console.print("\n[bold]Provisioning URI:[/bold]")
            console.print(f"  {enrollment.provisioning_uri}")
            console.print("\n[bold]Recovery codes (save these securely):[/bold]")
            for code in enrollment.recovery_codes:
                console.print(f"  {code}")
            console.print(
                "\n[yellow]Run 'vcf-pg-loader auth mfa confirm' with a TOTP code to complete enrollment[/yellow]"
            )

    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@mfa_app.command("confirm")
def mfa_confirm(
    code: Annotated[
        str, typer.Option("--code", "-c", prompt=True, help="TOTP code from authenticator")
    ],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Confirm MFA enrollment with TOTP code.

    HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication
    """
    from .auth import Authenticator, SessionStorage
    from .auth.mfa import MFAManager

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

    async def run_confirm():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = MFAManager()
            success = await manager.confirm_enrollment(conn, user_id=session.user_id, code=code)
            return success, None if success else "Invalid code"
        finally:
            await conn.close()

    try:
        success, error = asyncio.run(run_confirm())
        if success:
            if not quiet:
                console.print("[green]✓[/green] MFA enrollment confirmed")
        else:
            console.print(f"[red]Failed: {error}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@mfa_app.command("status")
def mfa_status(
    username: Annotated[
        str | None, typer.Option("--username", "-u", help="Username (admin only)")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Show MFA status for current user or specified user.

    HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication
    """
    import json as json_module

    from .auth import Authenticator, SessionStorage, UserManager
    from .auth.mfa import MFAManager

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

    async def run_status():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return None, "Session expired"

            target_user_id = session.user_id
            if username:
                user_manager = UserManager()
                target_user = await user_manager.get_user_by_username(conn, username)
                if not target_user:
                    return None, f"User '{username}' not found"
                target_user_id = target_user.user_id

            manager = MFAManager()
            status = await manager.get_status(conn, user_id=target_user_id)
            return status, None
        finally:
            await conn.close()

    try:
        status, error = asyncio.run(run_status())
        if error:
            console.print(f"[red]{error}[/red]")
            raise typer.Exit(1)

        if status is None:
            console.print("[yellow]MFA not configured[/yellow]")
            return

        if json_output:
            console.print(
                json_module.dumps(
                    {
                        "user_id": status.user_id,
                        "mfa_enabled": status.mfa_enabled,
                        "enrolled_at": status.enrolled_at.isoformat()
                        if status.enrolled_at
                        else None,
                        "recovery_codes_remaining": status.recovery_codes_remaining,
                    }
                )
            )
        else:
            if not quiet:
                enabled_str = (
                    "[green]Enabled[/green]" if status.mfa_enabled else "[red]Disabled[/red]"
                )
                console.print(f"MFA Status: {enabled_str}")
                if status.enrolled_at:
                    console.print(f"Enrolled: {status.enrolled_at.isoformat()}")
                console.print(f"Recovery codes remaining: {status.recovery_codes_remaining}")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@mfa_app.command("disable")
def mfa_disable(
    username: Annotated[
        str, typer.Option("--username", "-u", prompt=True, help="Username to disable MFA for")
    ],
    reason: Annotated[
        str, typer.Option("--reason", "-r", prompt=True, help="Reason for disabling MFA")
    ],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Disable MFA for a user (admin only).

    HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication
    """
    from .auth import Authenticator, SessionStorage, UserManager
    from .auth.mfa import MFAManager

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

            user_manager = UserManager()
            target_user = await user_manager.get_user_by_username(conn, username)
            if not target_user:
                return False, f"User '{username}' not found"

            manager = MFAManager()
            success = await manager.disable(
                conn,
                user_id=target_user.user_id,
                disabled_by=session.user_id,
                reason=reason,
            )
            return success, None if success else "Failed to disable MFA"
        finally:
            await conn.close()

    try:
        success, error = asyncio.run(run_disable())
        if success:
            if not quiet:
                console.print(f"[green]✓[/green] MFA disabled for {username}")
        else:
            console.print(f"[red]Failed: {error}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@mfa_app.command("verify")
def mfa_verify(
    code: Annotated[str, typer.Option("--code", "-c", prompt=True, help="TOTP code to verify")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Verify a TOTP code for current user.

    HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication
    """
    from .auth import Authenticator, SessionStorage
    from .auth.mfa import MFAManager

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

    async def run_verify():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = MFAManager()
            is_valid = await manager.verify_code(conn, user_id=session.user_id, code=code)
            return is_valid, None if is_valid else "Invalid code"
        finally:
            await conn.close()

    try:
        is_valid, error = asyncio.run(run_verify())
        if is_valid:
            if not quiet:
                console.print("[green]✓[/green] Code is valid")
        else:
            console.print(f"[red]Invalid: {error}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@mfa_app.command("regenerate-codes")
def mfa_regenerate_codes(
    code: Annotated[
        str, typer.Option("--code", "-c", prompt=True, help="Current TOTP code for verification")
    ],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Regenerate recovery codes (requires TOTP verification).

    HIPAA Citation: 45 CFR 164.312(d) - Person or Entity Authentication
    """
    from .auth import Authenticator, SessionStorage
    from .auth.mfa import MFAManager

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

    async def run_regenerate():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return None, "Session expired"

            manager = MFAManager()
            codes = await manager.regenerate_recovery_codes(
                conn, user_id=session.user_id, code=code
            )
            return codes, None
        finally:
            await conn.close()

    try:
        codes, error = asyncio.run(run_regenerate())
        if codes is None:
            console.print(f"[red]Failed: {error or 'Invalid TOTP code'}[/red]")
            raise typer.Exit(1)

        if not quiet:
            console.print("[green]✓[/green] Recovery codes regenerated")
            console.print("\n[bold]New recovery codes (save these securely):[/bold]")
            for rc in codes:
                console.print(f"  {rc}")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


emergency_app = typer.Typer(help="Emergency access procedures (HIPAA 164.312(a)(2)(ii))")
auth_app.add_typer(emergency_app, name="emergency")


@emergency_app.command("init")
def emergency_init(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Initialize emergency access schema.

    HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
    """
    from .auth.schema import AuthSchemaManager

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_init():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            schema_manager = AuthSchemaManager()
            await schema_manager.create_emergency_access_schema(conn)
            return True
        finally:
            await conn.close()

    try:
        asyncio.run(run_init())
        if not quiet:
            console.print("[green]✓[/green] Emergency access schema initialized")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@emergency_app.command("grant")
def emergency_grant(
    username: Annotated[
        str, typer.Option("--username", "-u", prompt=True, help="Username to grant access")
    ],
    justification: Annotated[
        str, typer.Option("--justification", "-j", prompt=True, help="Justification (min 20 chars)")
    ],
    emergency_type: Annotated[
        str, typer.Option("--type", "-t", help="Emergency type")
    ] = "patient_emergency",
    duration: Annotated[int, typer.Option("--duration", "-d", help="Duration in minutes")] = 60,
    db_url: Annotated[str | None, typer.Option("--db", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Grant emergency access to a user.

    HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
    """
    from .auth import Authenticator, SessionStorage, UserManager
    from .auth.emergency_access import EmergencyAccessManager, EmergencyType

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

    async def run_grant():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return None, "Session expired"

            user_manager = UserManager()
            target_user = await user_manager.get_user_by_username(conn, username)
            if not target_user:
                return None, f"User '{username}' not found"

            try:
                etype = EmergencyType(emergency_type)
            except ValueError:
                valid_types = [e.value for e in EmergencyType]
                return None, f"Invalid emergency type. Valid: {valid_types}"

            manager = EmergencyAccessManager()
            token_obj = await manager.grant_access(
                conn,
                user_id=target_user.user_id,
                justification=justification,
                emergency_type=etype,
                duration_minutes=duration,
                granted_by=session.user_id,
            )
            return token_obj, None
        finally:
            await conn.close()

    try:
        token_obj, error = asyncio.run(run_grant())
        if error:
            console.print(f"[red]{error}[/red]")
            raise typer.Exit(1)

        if not quiet:
            console.print("[green]✓[/green] Emergency access granted")
            console.print(f"  Token ID: {token_obj.token_id}")
            console.print(f"  User: {username}")
            console.print(f"  Expires: {token_obj.expires_at.isoformat()}")
            console.print(f"  Minutes remaining: {token_obj.minutes_remaining():.0f}")

    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@emergency_app.command("revoke")
def emergency_revoke(
    token_id: Annotated[str, typer.Option("--token", "-t", prompt=True, help="Token ID to revoke")],
    reason: Annotated[
        str, typer.Option("--reason", "-r", prompt=True, help="Reason for revocation")
    ],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Revoke an emergency access token.

    HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
    """
    from .auth import Authenticator, SessionStorage
    from .auth.emergency_access import EmergencyAccessManager

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
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = EmergencyAccessManager()
            success = await manager.revoke_access(
                conn,
                token_id=UUID(token_id),
                revoked_by=session.user_id,
                reason=reason,
            )
            return success, None if success else "Token not found or already revoked"
        finally:
            await conn.close()

    try:
        success, error = asyncio.run(run_revoke())
        if success:
            if not quiet:
                console.print("[green]✓[/green] Emergency access revoked")
        else:
            console.print(f"[red]Failed: {error}[/red]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@emergency_app.command("list")
def emergency_list(
    username: Annotated[
        str | None, typer.Option("--username", "-u", help="Filter by username")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List active emergency access tokens.

    HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
    """
    import json as json_module

    from .auth import Authenticator, SessionStorage, UserManager
    from .auth.emergency_access import EmergencyAccessManager

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
                return None, "Session expired"

            user_id = None
            if username:
                user_manager = UserManager()
                target_user = await user_manager.get_user_by_username(conn, username)
                if not target_user:
                    return None, f"User '{username}' not found"
                user_id = target_user.user_id

            manager = EmergencyAccessManager()
            tokens = await manager.get_active_tokens(conn, user_id=user_id)
            return tokens, None
        finally:
            await conn.close()

    try:
        tokens, error = asyncio.run(run_list())
        if error:
            console.print(f"[red]{error}[/red]")
            raise typer.Exit(1)

        if not tokens:
            if not quiet:
                console.print("[yellow]No active emergency tokens[/yellow]")
            return

        if json_output:
            console.print(
                json_module.dumps(
                    [
                        {
                            "token_id": str(t.token_id),
                            "user_id": t.user_id,
                            "emergency_type": t.emergency_type.value,
                            "granted_at": t.granted_at.isoformat(),
                            "expires_at": t.expires_at.isoformat(),
                            "minutes_remaining": t.minutes_remaining(),
                            "requires_review": t.requires_review,
                        }
                        for t in tokens
                    ],
                    indent=2,
                )
            )
        else:
            if not quiet:
                console.print(f"[bold]Active Emergency Tokens ({len(tokens)}):[/bold]")
                for t in tokens:
                    console.print(f"\n  Token: {t.token_id}")
                    console.print(f"  User ID: {t.user_id}")
                    console.print(f"  Type: {t.emergency_type.value}")
                    console.print(f"  Expires: {t.expires_at.isoformat()}")
                    console.print(f"  Minutes remaining: {t.minutes_remaining():.0f}")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@emergency_app.command("pending-reviews")
def emergency_pending_reviews(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """List emergency tokens pending post-incident review.

    HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
    """
    import json as json_module

    from .auth import Authenticator, SessionStorage
    from .auth.emergency_access import EmergencyAccessManager

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

    async def run_pending():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return None, "Session expired"

            manager = EmergencyAccessManager()
            pending = await manager.get_pending_reviews(conn)
            return pending, None
        finally:
            await conn.close()

    try:
        pending, error = asyncio.run(run_pending())
        if error:
            console.print(f"[red]{error}[/red]")
            raise typer.Exit(1)

        if not pending:
            if not quiet:
                console.print("[green]No pending reviews[/green]")
            return

        if json_output:
            console.print(json_module.dumps(pending, indent=2, default=str))
        else:
            if not quiet:
                console.print(f"[bold]Pending Reviews ({len(pending)}):[/bold]")
                for p in pending:
                    console.print(f"\n  Token: {p['token_id']}")
                    console.print(f"  User: {p.get('username', p['user_id'])}")
                    console.print(f"  Type: {p['emergency_type']}")
                    console.print(f"  Access count: {p.get('access_count', 0)}")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@emergency_app.command("complete-review")
def emergency_complete_review(
    token_id: Annotated[str, typer.Option("--token", "-t", prompt=True, help="Token ID to review")],
    notes: Annotated[str, typer.Option("--notes", "-n", prompt=True, help="Review notes")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Complete post-incident review for emergency access.

    HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
    """
    from .auth import Authenticator, SessionStorage
    from .auth.emergency_access import EmergencyAccessManager

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

    async def run_review():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            auth = Authenticator(jwt_secret=_get_jwt_secret())
            session = await auth.validate_session(conn, token)
            if not session:
                return False, "Session expired"

            manager = EmergencyAccessManager()
            success = await manager.complete_review(
                conn,
                token_id=UUID(token_id),
                reviewed_by=session.user_id,
                review_notes=notes,
            )
            return success, None if success else "Token not found or already reviewed"
        finally:
            await conn.close()

    try:
        success, error = asyncio.run(run_review())
        if success:
            if not quiet:
                console.print("[green]✓[/green] Review completed")
        else:
            console.print(f"[red]Failed: {error}[/red]")
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


phi_app = typer.Typer(help="PHI anonymization management (HIPAA 164.514(b))")
app.add_typer(phi_app, name="phi")


@phi_app.command("lookup")
def phi_lookup(
    anonymous_id: Annotated[str, typer.Argument(..., help="Anonymous UUID to look up")],
    reason: Annotated[str | None, typer.Option("--reason", "-r", help="Reason for lookup")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Reverse lookup: get original sample ID from anonymous UUID.

    This operation is audited. All lookups are logged to phi_vault.reverse_lookup_audit.
    Requires phi:lookup permission.
    """
    from .phi import SampleAnonymizer

    try:
        lookup_uuid = UUID(anonymous_id)
    except ValueError:
        console.print(f"[red]Error: Invalid UUID format: {anonymous_id}[/red]")
        raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_lookup():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            anonymizer = SampleAnonymizer(pool=pool)
            original_id = await anonymizer.reverse_lookup(
                lookup_uuid,
                requester_id=1,
                reason=reason,
            )
            return original_id
        finally:
            await pool.close()

    try:
        original_id = asyncio.run(run_lookup())

        if original_id:
            console.print(f"[bold]Original ID:[/bold] {original_id}")
        else:
            console.print(f"[yellow]No mapping found for {anonymous_id}[/yellow]")
            raise typer.Exit(1)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@phi_app.command("export-mapping")
def phi_export_mapping(
    batch_id: Annotated[str, typer.Argument(..., help="Load batch UUID to export mappings for")],
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Output file path")] = None,
    format: Annotated[
        str, typer.Option("--format", "-f", help="Output format (json, csv)")
    ] = "json",
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Export sample ID mappings for a load batch.

    Exports the mapping between anonymous IDs and metadata for authorized users.
    Note: Original IDs are NOT exported for security; use 'phi lookup' for individual lookups.
    """
    import json as json_module

    try:
        batch_uuid = UUID(batch_id)
    except ValueError:
        console.print(f"[red]Error: Invalid UUID format: {batch_id}[/red]")
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
            rows = await conn.fetch(
                """
                SELECT anonymous_id, source_file, created_at
                FROM phi_vault.sample_id_mapping
                WHERE load_batch_id = $1
                ORDER BY created_at
                """,
                batch_uuid,
            )
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    try:
        mappings = asyncio.run(run_export())

        if not mappings:
            console.print(f"[yellow]No mappings found for batch {batch_id}[/yellow]")
            raise typer.Exit(1)

        for m in mappings:
            m["anonymous_id"] = str(m["anonymous_id"])
            m["created_at"] = m["created_at"].isoformat()

        if format == "json":
            content = json_module.dumps(mappings, indent=2)
        elif format == "csv":
            import csv
            import io

            output_io = io.StringIO()
            writer = csv.DictWriter(
                output_io, fieldnames=["anonymous_id", "source_file", "created_at"]
            )
            writer.writeheader()
            writer.writerows(mappings)
            content = output_io.getvalue()
        else:
            console.print(f"[red]Unknown format: {format}. Use 'json' or 'csv'[/red]")
            raise typer.Exit(1)

        if output:
            with open(output, "w") as f:
                f.write(content)
            if not quiet:
                console.print(f"[green]✓[/green] Exported {len(mappings)} mappings to {output}")
        else:
            console.print(content)

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@phi_app.command("stats")
def phi_stats(
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Show PHI anonymization statistics."""
    import json as json_module

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
            mapping_stats = await conn.fetchrow("SELECT * FROM phi_vault.v_mapping_stats")
            lookup_stats = await conn.fetchrow("SELECT * FROM phi_vault.v_lookup_stats")
            return dict(mapping_stats) if mapping_stats else {}, dict(
                lookup_stats
            ) if lookup_stats else {}
        finally:
            await conn.close()

    try:
        mapping_stats, lookup_stats = asyncio.run(run_stats())

        for key in ["oldest_mapping", "newest_mapping"]:
            if key in mapping_stats and mapping_stats[key]:
                mapping_stats[key] = mapping_stats[key].isoformat()
        for key in ["first_lookup", "last_lookup"]:
            if key in lookup_stats and lookup_stats[key]:
                lookup_stats[key] = lookup_stats[key].isoformat()

        if json_output:
            output = {"mappings": mapping_stats, "lookups": lookup_stats}
            console.print(json_module.dumps(output, indent=2))
        else:
            console.print("[bold]Sample ID Mappings[/bold]")
            console.print(f"  Total mappings: {mapping_stats.get('total_mappings', 0):,}")
            console.print(f"  Unique files: {mapping_stats.get('unique_files', 0)}")
            console.print(f"  Total batches: {mapping_stats.get('total_batches', 0)}")
            console.print(f"  Encrypted: {mapping_stats.get('encrypted_count', 0):,}")
            if mapping_stats.get("oldest_mapping"):
                console.print(f"  Oldest: {mapping_stats['oldest_mapping']}")
            if mapping_stats.get("newest_mapping"):
                console.print(f"  Newest: {mapping_stats['newest_mapping']}")

            console.print()
            console.print("[bold]Reverse Lookups[/bold]")
            console.print(f"  Total lookups: {lookup_stats.get('total_lookups', 0):,}")
            console.print(f"  Unique requesters: {lookup_stats.get('unique_requesters', 0)}")
            console.print(f"  Failed lookups: {lookup_stats.get('failed_lookups', 0)}")
            if lookup_stats.get("first_lookup"):
                console.print(f"  First: {lookup_stats['first_lookup']}")
            if lookup_stats.get("last_lookup"):
                console.print(f"  Last: {lookup_stats['last_lookup']}")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@phi_app.command("generate-key")
def phi_generate_key() -> None:
    """Generate a new PHI encryption key.

    The key should be stored securely and set as VCF_PG_LOADER_PHI_KEY environment variable.
    """
    from .phi import PHIEncryptor

    key = PHIEncryptor.generate_key()
    key_b64 = PHIEncryptor.key_to_base64(key)

    console.print("[bold]Generated PHI Encryption Key:[/bold]")
    console.print(f"  {key_b64}")
    console.print()
    console.print("[yellow]Store this key securely![/yellow]")
    console.print("Set as environment variable:")
    console.print(f"  export VCF_PG_LOADER_PHI_KEY='{key_b64}'")


@phi_app.command("scan")
def phi_scan(
    vcf_path: Path = typer.Argument(..., help="Path to VCF file to scan"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Scan a VCF file for potential PHI in headers.

    Checks for patterns like patient IDs, MRNs, file paths, dates, and institution names.
    """
    import json

    from .phi import PHIScanner

    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    scanner = PHIScanner()
    result = scanner.scan_vcf_for_phi(vcf_path)

    if json_output:
        output = {
            "has_phi": result.has_phi,
            "risk_level": result.risk_level,
            "summary": result.summary,
            "findings": result.findings,
        }
        console.print(json.dumps(output, indent=2))
    else:
        if result.has_phi:
            console.print(f"[yellow]⚠[/yellow]  PHI detected (risk level: {result.risk_level})")
            console.print()
            console.print("[bold]Summary:[/bold]")
            for pattern_type, count in result.summary.items():
                console.print(f"  {pattern_type}: {count}")
            console.print()
            console.print("[bold]Findings:[/bold]")
            for finding in result.findings[:10]:
                console.print(f"  Line {finding['line']}: [{finding['type']}] {finding['value']}")
            if len(result.findings) > 10:
                console.print(f"  ... and {len(result.findings) - 10} more")
        else:
            console.print("[green]✓[/green] No PHI detected in VCF headers")


@phi_app.command("sanitize")
def phi_sanitize(
    vcf_path: Path = typer.Argument(..., help="Path to VCF file to sanitize"),
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output file path (default: stdout)"),
    ] = None,
    preview: bool = typer.Option(False, "--preview", "-p", help="Preview changes without writing"),
) -> None:
    """Sanitize a VCF file by removing PHI from headers.

    Creates a new VCF file with sanitized headers. Original file is not modified.
    """
    import gzip

    from .phi import VCFHeaderSanitizer

    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    sanitizer = VCFHeaderSanitizer()
    is_gzipped = str(vcf_path).endswith(".gz")
    opener = gzip.open if is_gzipped else open

    with opener(vcf_path, "rt") as f:
        header_lines = []
        data_lines = []
        in_header = True
        for line in f:
            if in_header and line.startswith("#"):
                header_lines.append(line.rstrip())
            else:
                in_header = False
                data_lines.append(line)

    header_text = "\n".join(header_lines)
    result = sanitizer.sanitize_header(header_text)

    if preview:
        console.print("[bold]Sanitization Preview:[/bold]")
        console.print(f"  Items to sanitize: {len(result.removed_items)}")
        console.print()
        if result.removed_items:
            console.print("[bold]Changes:[/bold]")
            for item in result.removed_items[:20]:
                console.print(
                    f"  Line {item.line_number} [{item.pattern_matched}]: "
                    f"{item.original_value[:40]}... → {item.sanitized_value}"
                )
            if len(result.removed_items) > 20:
                console.print(f"  ... and {len(result.removed_items) - 20} more")
        return

    sanitized_content = "\n".join(result.sanitized_lines) + "\n" + "".join(data_lines)

    if output:
        out_opener = gzip.open if str(output).endswith(".gz") else open
        with out_opener(output, "wt") as f:
            f.write(sanitized_content)
        console.print(f"[green]✓[/green] Sanitized VCF written to {output}")
        console.print(f"  Removed {len(result.removed_items)} PHI items")
    else:
        console.print(sanitized_content)


@phi_app.command("report")
def phi_report(
    batch_id: Annotated[str, typer.Argument(..., help="Load batch UUID")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Show PHI sanitization report for a load batch.

    Displays what PHI was detected and sanitized during the load.
    """
    import json

    try:
        batch_uuid = UUID(batch_id)
    except ValueError:
        console.print(f"[red]Error: Invalid UUID format: {batch_id}[/red]")
        raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    async def run_report():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            rows = await conn.fetch(
                """
                SELECT event_time, action, details
                FROM hipaa_audit_log
                WHERE resource_id = $1
                  AND action = 'header_sanitization'
                ORDER BY event_time DESC
                """,
                str(batch_uuid),
            )
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    try:
        reports = asyncio.run(run_report())

        if not reports:
            console.print(f"[yellow]No sanitization report found for batch {batch_id}[/yellow]")
            raise typer.Exit(1)

        if json_output:
            for report in reports:
                report["event_time"] = report["event_time"].isoformat()
            console.print(json.dumps(reports, indent=2))
        else:
            for report in reports:
                details = report.get("details", {})
                console.print(f"[bold]Sanitization Report[/bold] ({report['event_time']})")
                console.print(f"  Source: {details.get('source_file', 'N/A')}")
                console.print(f"  PHI Detected: {details.get('phi_detected', False)}")
                console.print(f"  Risk Level: {details.get('risk_level', 'N/A')}")
                console.print(f"  Items Sanitized: {details.get('items_sanitized', 0)}")
                if details.get("summary"):
                    console.print("  Summary:")
                    for pattern, count in details["summary"].items():
                        console.print(f"    {pattern}: {count}")

    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@phi_app.command("detect")
def phi_detect(
    vcf_path: Path = typer.Argument(..., help="Path to VCF file to scan"),
    sample_rate: float = typer.Option(
        1.0, "--sample-rate", "-s", help="Sample rate for records (0.0-1.0)"
    ),
    max_records: Annotated[
        int | None, typer.Option("--max-records", "-m", help="Max records to scan")
    ] = None,
    scan_headers: bool = typer.Option(
        True, "--scan-headers/--no-scan-headers", help="Scan header lines"
    ),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Perform full PHI detection scan on a VCF file.

    Scans headers, INFO fields, FORMAT fields, and sample data for potential PHI
    using configurable patterns with severity levels.
    """
    import json

    from .phi import PHIDetector

    if not vcf_path.exists():
        console.print(f"[red]Error: VCF file not found: {vcf_path}[/red]")
        raise typer.Exit(1)

    detector = PHIDetector()
    report = detector.scan_vcf_stream(
        vcf_path,
        sample_rate=sample_rate,
        max_records=max_records,
        scan_headers=scan_headers,
    )

    if json_output:
        output = {
            "has_phi": report.has_phi,
            "risk_level": report.risk_level,
            "records_scanned": report.records_scanned,
            "records_total": report.records_total,
            "sample_rate": report.sample_rate,
            "summary": report.summary,
            "severity_summary": report.severity_summary,
            "detections": [
                {
                    "pattern": d.pattern_name,
                    "severity": d.severity,
                    "location": d.location,
                    "line": d.line_number,
                    "masked_value": d.masked_value,
                    "context": d.context[:100] if d.context else None,
                    "false_positive_hints": d.false_positive_hints,
                }
                for d in report.detections
            ],
        }
        console.print(json.dumps(output, indent=2))
    else:
        if report.has_phi:
            console.print(f"[yellow]⚠[/yellow]  PHI detected (risk level: {report.risk_level})")
            console.print(
                f"  Records scanned: {report.records_scanned:,} / {report.records_total:,}"
            )
            console.print()
            console.print("[bold]Summary by pattern:[/bold]")
            for pattern_type, count in report.summary.items():
                console.print(f"  {pattern_type}: {count}")
            console.print()
            console.print("[bold]Summary by severity:[/bold]")
            for severity, count in report.severity_summary.items():
                console.print(f"  {severity}: {count}")
            console.print()
            console.print("[bold]Detections (first 20):[/bold]")
            for detection in report.detections[:20]:
                hints = (
                    f" (hints: {', '.join(detection.false_positive_hints)})"
                    if detection.false_positive_hints
                    else ""
                )
                console.print(
                    f"  [{detection.severity}] {detection.pattern_name} at {detection.location}"
                    f" (line {detection.line_number}): {detection.masked_value}{hints}"
                )
            if len(report.detections) > 20:
                console.print(f"  ... and {len(report.detections) - 20} more")
            raise typer.Exit(1)
        else:
            console.print("[green]✓[/green] No PHI detected")
            console.print(
                f"  Records scanned: {report.records_scanned:,} / {report.records_total:,}"
            )


patterns_app = typer.Typer(help="Manage PHI detection patterns")
phi_app.add_typer(patterns_app, name="patterns")


@patterns_app.command("list")
def phi_patterns_list(
    severity: Annotated[
        str | None, typer.Option("--severity", "-s", help="Filter by severity")
    ] = None,
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """List all registered PHI detection patterns."""
    import json

    from .phi import PHIPatternRegistry

    registry = PHIPatternRegistry()

    if severity:
        patterns = registry.get_patterns_by_severity(severity)
    else:
        patterns = registry.patterns

    if json_output:
        output = [
            {
                "name": p.name,
                "severity": p.severity,
                "description": p.description,
                "pattern": p.pattern.pattern,
                "false_positive_hints": p.false_positive_hints,
            }
            for p in patterns
        ]
        console.print(json.dumps(output, indent=2))
    else:
        console.print(f"[bold]PHI Detection Patterns ({len(patterns)} total)[/bold]")
        console.print()
        for p in sorted(patterns, key=lambda x: (x.severity, x.name)):
            severity_color = {
                "critical": "red",
                "high": "yellow",
                "medium": "blue",
                "low": "dim",
            }.get(p.severity, "white")
            console.print(f"  [{severity_color}]{p.severity:8}[/{severity_color}] {p.name}")
            console.print(f"             {p.description}")
            if p.false_positive_hints:
                console.print(f"             [dim]Hints: {', '.join(p.false_positive_hints)}[/dim]")


@patterns_app.command("test")
def phi_patterns_test(
    pattern: str = typer.Option(..., "--pattern", "-p", help="Regex pattern to test"),
    input_text: str = typer.Option(..., "--input", "-i", help="Text to test against"),
    case_insensitive: bool = typer.Option(
        False, "--ignore-case", "-I", help="Case insensitive matching"
    ),
) -> None:
    """Test a regex pattern against input text."""
    import re

    flags = re.IGNORECASE if case_insensitive else 0
    try:
        compiled = re.compile(pattern, flags)
    except re.error as e:
        console.print(f"[red]Invalid regex pattern: {e}[/red]")
        raise typer.Exit(1) from None

    matches = list(compiled.finditer(input_text))

    if matches:
        console.print(f"[green]✓[/green] Found {len(matches)} match(es):")
        for i, match in enumerate(matches, 1):
            console.print(f"  {i}. '{match.group()}' at position {match.start()}-{match.end()}")
    else:
        console.print("[yellow]No matches found[/yellow]")


@patterns_app.command("add")
def phi_patterns_add(
    name: str = typer.Option(..., "--name", "-n", help="Pattern name"),
    pattern: str = typer.Option(..., "--pattern", "-p", help="Regex pattern"),
    severity: str = typer.Option(
        ..., "--severity", "-s", help="Severity: critical, high, medium, low"
    ),
    description: str = typer.Option("", "--description", "-d", help="Pattern description"),
    config_path: Path = typer.Option(
        "phi_patterns.toml", "--config", "-c", help="Config file to write to"
    ),
) -> None:
    """Add a custom PHI pattern to a configuration file."""
    import re

    import tomli_w

    if severity not in ("critical", "high", "medium", "low"):
        console.print(
            f"[red]Invalid severity: {severity}. Must be one of: critical, high, medium, low[/red]"
        )
        raise typer.Exit(1)

    try:
        re.compile(pattern)
    except re.error as e:
        console.print(f"[red]Invalid regex pattern: {e}[/red]")
        raise typer.Exit(1) from None

    if config_path.exists():
        with open(config_path, "rb") as f:
            import tomllib

            data = tomllib.load(f)
    else:
        data = {"patterns": []}

    if "patterns" not in data:
        data["patterns"] = []

    data["patterns"].append(
        {
            "name": name,
            "pattern": pattern,
            "severity": severity,
            "description": description,
            "false_positive_hints": [],
        }
    )

    with open(config_path, "wb") as f:
        tomli_w.dump(data, f)

    console.print(f"[green]✓[/green] Pattern '{name}' added to {config_path}")


security_app = typer.Typer(help="Security and encryption management (HIPAA 164.312(a)(2)(iv))")
app.add_typer(security_app, name="security")


@security_app.command("check-encryption")
def security_check_encryption(
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Check PHI encryption status and configuration.

    Verifies that encryption is properly configured and reports on
    encrypted vs unencrypted data in the database.
    """
    import json as json_module

    from .phi import check_encryption_status

    status = check_encryption_status()

    db_stats = None
    if db_url or os.environ.get("POSTGRES_URL") or os.environ.get("PGHOST"):
        try:
            resolved_db_url = _resolve_database_url(db_url, quiet)
            if resolved_db_url:

                async def get_db_stats():
                    conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
                    try:
                        row = await conn.fetchrow(
                            """
                            SELECT
                                COUNT(*) as total,
                                COUNT(*) FILTER (WHERE original_id_encrypted IS NOT NULL) as encrypted,
                                COUNT(*) FILTER (WHERE original_id_encrypted IS NULL) as unencrypted
                            FROM phi_vault.sample_id_mapping
                            """
                        )
                        return dict(row) if row else None
                    except Exception:
                        return None
                    finally:
                        await conn.close()

                db_stats = asyncio.run(get_db_stats())
        except Exception:
            pass

    if json_output:
        output = {
            "enabled": status.enabled,
            "algorithm": status.algorithm,
            "key_source": status.key_source.value if status.key_source else None,
            "key_id": status.key_id,
            "library_version": status.library_version,
        }
        if db_stats:
            output["database"] = db_stats
        console.print(json_module.dumps(output, indent=2))
    else:
        console.print("[bold]PHI Encryption Status[/bold]")
        if status.enabled:
            console.print("  Status: [green]Enabled[/green]")
            console.print(f"  Algorithm: {status.algorithm}")
            console.print(
                f"  Key Source: {status.key_source.value if status.key_source else 'unknown'}"
            )
            if status.key_id:
                console.print(f"  Key ID: {status.key_id}")
            console.print(f"  Library: cryptography {status.library_version}")
        else:
            console.print("  Status: [red]Disabled[/red]")
            console.print("  Set VCF_PG_LOADER_PHI_KEY environment variable to enable.")
            console.print(f"  Library: cryptography {status.library_version}")

        if db_stats:
            console.print()
            console.print("[bold]Database Encryption Stats[/bold]")
            console.print(f"  Total mappings: {db_stats['total']:,}")
            console.print(f"  Encrypted: {db_stats['encrypted']:,}")
            console.print(f"  Unencrypted: {db_stats['unencrypted']:,}")
            if db_stats["total"] > 0:
                pct = (db_stats["encrypted"] / db_stats["total"]) * 100
                console.print(f"  Coverage: {pct:.1f}%")


@security_app.command("generate-key")
def security_generate_key(
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Write key to file")] = None,
    raw: bool = typer.Option(False, "--raw", help="Output only the key, no formatting"),
) -> None:
    """Generate a new 256-bit AES encryption key.

    The key is output as base64 and can be used with VCF_PG_LOADER_PHI_KEY.
    """
    from .phi import PHIEncryptor

    key = PHIEncryptor.generate_key()
    key_b64 = PHIEncryptor.key_to_base64(key)

    if output:
        output.write_text(key_b64 + "\n")
        output.chmod(0o600)
        if not raw:
            console.print(f"[green]✓[/green] Key written to {output}")
            console.print("  Permissions set to 0600")
            console.print()
            console.print("To use this key:")
            console.print(f'  export VCF_PG_LOADER_PHI_KEY_FILE="{output.absolute()}"')
    else:
        if raw:
            console.print(key_b64)
        else:
            console.print("[bold]Generated Encryption Key[/bold]")
            console.print()
            console.print(f"  {key_b64}")
            console.print()
            console.print("To use this key:")
            console.print(f'  export VCF_PG_LOADER_PHI_KEY="{key_b64}"')
            console.print()
            console.print(
                "[yellow]Warning:[/yellow] Store this key securely. Loss of key = loss of data."
            )


@security_app.command("rotate-key")
def security_rotate_key(
    new_key: Annotated[
        str, typer.Option("--new-key", "-n", help="New encryption key (base64)")
    ] = None,
    new_key_file: Annotated[
        Path | None, typer.Option("--new-key-file", help="File containing new key")
    ] = None,
    batch_size: int = typer.Option(1000, "--batch-size", "-b", help="Rows per batch"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be rotated without changing data"
    ),
) -> None:
    """Rotate encryption key for all PHI data.

    Re-encrypts all data with a new key. The old key is read from
    VCF_PG_LOADER_PHI_KEY environment variable.

    This operation is atomic per batch and can be interrupted safely.
    """
    from .phi import KeyRotator, PHIEncryptor

    if new_key is None and new_key_file is None:
        console.print("[red]Error: Provide --new-key or --new-key-file[/red]")
        raise typer.Exit(1)

    try:
        old_encryptor = PHIEncryptor()
        if not old_encryptor.is_available:
            console.print("[red]Error: Old key not available. Set VCF_PG_LOADER_PHI_KEY.[/red]")
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error loading old key: {e}[/red]")
        raise typer.Exit(1) from None

    try:
        if new_key_file:
            new_key_bytes = PHIEncryptor.key_from_base64(new_key_file.read_text().strip())
        else:
            new_key_bytes = PHIEncryptor.key_from_base64(new_key)
        new_encryptor = PHIEncryptor(key=new_key_bytes)
    except Exception as e:
        console.print(f"[red]Error loading new key: {e}[/red]")
        raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required for key rotation[/red]")
        raise typer.Exit(1)

    async def run_rotation():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM phi_vault.sample_id_mapping WHERE original_id_encrypted IS NOT NULL"
            )

            if count == 0:
                return 0

            if dry_run:
                return count

            rotator = KeyRotator(old_encryptor, new_encryptor)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Rotating keys...", total=count)

                def update_progress(processed, total):
                    progress.update(task, completed=processed)

                rotated = await rotator.rotate_table(
                    conn,
                    batch_size=batch_size,
                    progress_callback=update_progress,
                )
                return rotated
        finally:
            await conn.close()

    try:
        if dry_run:
            count = asyncio.run(run_rotation())
            console.print(f"[bold]Dry Run:[/bold] Would rotate {count:,} encrypted rows")
            console.print("Run without --dry-run to perform rotation.")
        else:
            rotated = asyncio.run(run_rotation())
            if rotated == 0:
                console.print("[yellow]No encrypted rows found to rotate[/yellow]")
            else:
                console.print(f"[green]✓[/green] Rotated {rotated:,} rows")
                console.print()
                console.print("[bold]Next steps:[/bold]")
                console.print("1. Update VCF_PG_LOADER_PHI_KEY with new key")
                console.print("2. Verify data access works with new key")
                console.print("3. Securely delete old key")
    except Exception as e:
        console.print(f"[red]Error during key rotation: {e}[/red]")
        raise typer.Exit(1) from None


encryption_app = typer.Typer(help="Database encryption key management (HIPAA 164.312(a)(2)(iv))")
security_app.add_typer(encryption_app, name="encryption")


@encryption_app.command("init")
def encryption_init(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Initialize encryption schema.

    Creates encryption_keys, encryption_key_rotations, and encrypted_data_registry
    tables for managing database encryption keys.

    \b
    HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption
    """
    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            from .security import SecuritySchemaManager

            manager = SecuritySchemaManager()
            await manager.create_encryption_schema(conn)
            console.print("[green]✓[/green] Encryption schema initialized")
        finally:
            await conn.close()

    asyncio.run(run())


@encryption_app.command("create-key")
def encryption_create_key(
    name: Annotated[str, typer.Argument(help="Key name (must be unique)")],
    purpose: Annotated[
        str,
        typer.Option(
            "--purpose",
            "-p",
            help="Key purpose: data_encryption, phi_encryption, backup_encryption, transport_encryption",
        ),
    ] = "data_encryption",
    expires_days: Annotated[
        int | None, typer.Option("--expires", "-e", help="Key expiration in days")
    ] = None,
    user_id: Annotated[int | None, typer.Option("--user-id", "-u", help="Creating user ID")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Create a new database encryption key.

    Keys are stored encrypted with the master key (VCF_PG_LOADER_MASTER_KEY).
    The master key must be set before creating keys.

    \b
    HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption
    NIST SP 800-111 - Guide to Storage Encryption Technologies
    """
    import json as json_module

    from .security import EncryptionManager
    from .security.encryption import KeyPurpose

    try:
        key_purpose = KeyPurpose(purpose)
    except ValueError:
        valid = [p.value for p in KeyPurpose]
        console.print(f"[red]Error: Invalid purpose. Valid options: {valid}[/red]")
        raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    try:
        manager = EncryptionManager()
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print(
            "Set VCF_PG_LOADER_MASTER_KEY environment variable (base64-encoded 256-bit key)"
        )
        raise typer.Exit(1) from None

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            key = await manager.create_key(
                conn,
                key_name=name,
                purpose=key_purpose,
                expires_days=expires_days,
                created_by=user_id,
            )
            return key
        finally:
            await conn.close()

    try:
        key = asyncio.run(run())
        if json_output:
            output = {
                "key_id": str(key.key_id),
                "key_name": key.key_name,
                "key_version": key.key_version,
                "purpose": key.purpose.value,
                "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                "created_at": key.created_at.isoformat(),
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            console.print("[green]✓[/green] Encryption key created")
            console.print(f"  Key ID: {key.key_id}")
            console.print(f"  Name: {key.key_name}")
            console.print(f"  Version: {key.key_version}")
            console.print(f"  Purpose: {key.purpose.value}")
            if key.expires_at:
                console.print(f"  Expires: {key.expires_at.isoformat()}")
    except Exception as e:
        console.print(f"[red]Error creating key: {e}[/red]")
        raise typer.Exit(1) from None


@encryption_app.command("list-keys")
def encryption_list_keys(
    purpose: Annotated[
        str | None,
        typer.Option("--purpose", "-p", help="Filter by purpose"),
    ] = None,
    include_retired: bool = typer.Option(False, "--include-retired", help="Include retired keys"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """List database encryption keys.

    Shows all encryption keys managed by the system.

    \b
    HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption
    """
    import json as json_module

    from .security import EncryptionManager
    from .security.encryption import KeyPurpose

    key_purpose = None
    if purpose:
        try:
            key_purpose = KeyPurpose(purpose)
        except ValueError:
            valid = [p.value for p in KeyPurpose]
            console.print(f"[red]Error: Invalid purpose. Valid options: {valid}[/red]")
            raise typer.Exit(1) from None

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    try:
        manager = EncryptionManager()
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print(
            "Set VCF_PG_LOADER_MASTER_KEY environment variable (base64-encoded 256-bit key)"
        )
        raise typer.Exit(1) from None

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            return await manager.list_keys(
                conn,
                purpose=key_purpose,
                include_retired=include_retired,
            )
        finally:
            await conn.close()

    try:
        keys = asyncio.run(run())
        if json_output:
            output = []
            for key in keys:
                output.append(
                    {
                        "key_id": str(key.key_id),
                        "key_name": key.key_name,
                        "key_version": key.key_version,
                        "purpose": key.purpose.value,
                        "is_active": key.is_active,
                        "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                        "retired_at": key.retired_at.isoformat() if key.retired_at else None,
                        "use_count": key.use_count,
                        "created_at": key.created_at.isoformat(),
                    }
                )
            console.print(json_module.dumps(output, indent=2))
        else:
            if not keys:
                console.print("[yellow]No encryption keys found[/yellow]")
                return

            console.print("[bold]Encryption Keys[/bold]")
            console.print()
            for key in keys:
                status = "[green]active[/green]" if key.is_active else "[dim]inactive[/dim]"
                if key.retired_at:
                    status = "[red]retired[/red]"
                console.print(f"  {key.key_name} (v{key.key_version}) [{status}]")
                console.print(f"    ID: {key.key_id}")
                console.print(f"    Purpose: {key.purpose.value}")
                console.print(f"    Uses: {key.use_count}")
                if key.expires_at:
                    console.print(f"    Expires: {key.expires_at.isoformat()}")
                console.print()
    except Exception as e:
        console.print(f"[red]Error listing keys: {e}[/red]")
        raise typer.Exit(1) from None


@encryption_app.command("rotate")
def encryption_rotate_key(
    name: Annotated[str, typer.Argument(help="Key name to rotate")],
    reason: Annotated[str, typer.Option("--reason", "-r", help="Reason for rotation")] = None,
    user_id: Annotated[
        int, typer.Option("--user-id", "-u", help="User ID performing rotation")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Rotate a database encryption key.

    Creates a new version of the key, retiring the old version.
    Existing data encrypted with the old key can still be decrypted.

    \b
    HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption
    NIST SP 800-57 - Key Management Guidelines
    """
    import json as json_module

    from .security import EncryptionManager

    if not reason:
        console.print("[red]Error: --reason is required for audit trail[/red]")
        raise typer.Exit(1)

    if not user_id:
        console.print("[red]Error: --user-id is required[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    try:
        manager = EncryptionManager()
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print(
            "Set VCF_PG_LOADER_MASTER_KEY environment variable (base64-encoded 256-bit key)"
        )
        raise typer.Exit(1) from None

    async def run():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            return await manager.rotate_key(
                conn,
                key_name=name,
                rotated_by=user_id,
                reason=reason,
            )
        finally:
            await conn.close()

    try:
        key = asyncio.run(run())
        if json_output:
            output = {
                "key_id": str(key.key_id),
                "key_name": key.key_name,
                "key_version": key.key_version,
                "purpose": key.purpose.value,
                "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                "created_at": key.created_at.isoformat(),
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            console.print("[green]✓[/green] Encryption key rotated")
            console.print(f"  Key ID: {key.key_id}")
            console.print(f"  Name: {key.key_name}")
            console.print(f"  New Version: {key.key_version}")
            console.print(f"  Purpose: {key.purpose.value}")
    except Exception as e:
        console.print(f"[red]Error rotating key: {e}[/red]")
        raise typer.Exit(1) from None


data_app = typer.Typer(help="Data management and HIPAA-compliant disposal (164.530(j))")
app.add_typer(data_app, name="data")


@data_app.command("dispose")
def data_dispose(
    batch_id: Annotated[UUID | None, typer.Option("--batch-id", "-b", help="Batch UUID")] = None,
    sample_id: Annotated[UUID | None, typer.Option("--sample-id", "-s", help="Sample UUID")] = None,
    reason: Annotated[str, typer.Option("--reason", "-r", help="Reason for disposal")] = None,
    user_id: Annotated[int, typer.Option("--user-id", "-u", help="Authorizing user ID")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    require_second_auth: bool = typer.Option(
        True, "--require-second-auth/--no-second-auth", help="Require two-person authorization"
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Request secure disposal of PHI data.

    Disposes all data for a batch (--batch-id) or sample (--sample-id).
    Creates a disposal request that may require second authorization.

    \b
    HIPAA Reference: 164.530(j) - Retention and Disposal
    """
    import json as json_module

    from .data import DataDisposal

    if not batch_id and not sample_id:
        console.print("[red]Error: Provide --batch-id or --sample-id[/red]")
        raise typer.Exit(1)

    if batch_id and sample_id:
        console.print("[red]Error: Provide only one of --batch-id or --sample-id[/red]")
        raise typer.Exit(1)

    if not reason:
        console.print("[red]Error: --reason is required[/red]")
        raise typer.Exit(1)

    if not user_id:
        console.print("[red]Error: --user-id is required[/red]")
        raise typer.Exit(1)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_disposal():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            disposal = DataDisposal(pool, require_two_person_auth=require_second_auth)

            if batch_id:
                result = await disposal.dispose_batch(
                    batch_id=batch_id,
                    reason=reason,
                    authorized_by=user_id,
                )
            else:
                result = await disposal.dispose_sample(
                    sample_anonymous_id=sample_id,
                    reason=reason,
                    authorized_by=user_id,
                )

            return result
        finally:
            await pool.close()

    try:
        result = asyncio.run(run_disposal())

        if json_output:
            output = {
                "disposal_id": str(result.disposal_id),
                "type": result.disposal_type.value,
                "target": result.target_identifier,
                "status": result.status.value,
                "variants_disposed": result.variants_disposed,
                "mappings_disposed": result.mappings_disposed,
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            console.print("[bold]Disposal Request Created[/bold]")
            console.print(f"  ID: {result.disposal_id}")
            console.print(f"  Type: {result.disposal_type.value}")
            console.print(f"  Target: {result.target_identifier}")
            console.print(f"  Status: {result.status.value}")

            if result.status.value == "pending":
                console.print()
                console.print("[yellow]Second authorization required.[/yellow]")
                console.print(
                    f"Run: vcf-pg-loader data authorize "
                    f"--disposal-id {result.disposal_id} --user-id <id>"
                )
            elif result.status.value == "completed":
                console.print(f"  Variants disposed: {result.variants_disposed:,}")
                console.print(f"  Mappings disposed: {result.mappings_disposed:,}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@data_app.command("authorize")
def data_authorize(
    disposal_id: Annotated[UUID, typer.Option("--disposal-id", "-i", help="Disposal UUID")],
    user_id: Annotated[int, typer.Option("--user-id", "-u", help="Authorizing user ID")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Provide second authorization for a pending disposal.

    Required when two-person authorization is enabled.
    The second authorizer must be different from the first.
    """
    from .data import DataDisposal

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_auth():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            disposal = DataDisposal(pool)
            return await disposal.authorize_disposal(disposal_id, user_id)
        finally:
            await pool.close()

    try:
        asyncio.run(run_auth())
        console.print(f"[green]✓[/green] Disposal {disposal_id} authorized")
        console.print(f"Run: vcf-pg-loader data execute --disposal-id {disposal_id} --user-id <id>")
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@data_app.command("execute")
def data_execute(
    disposal_id: Annotated[UUID, typer.Option("--disposal-id", "-i", help="Disposal UUID")],
    user_id: Annotated[int, typer.Option("--user-id", "-u", help="Executor user ID")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Execute an authorized disposal.

    Permanently deletes data. This action cannot be undone.
    """
    import json as json_module

    from .data import DataDisposal

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_execute():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            disposal = DataDisposal(pool)
            return await disposal.execute_disposal(disposal_id, user_id)
        finally:
            await pool.close()

    try:
        result = asyncio.run(run_execute())

        if json_output:
            output = {
                "disposal_id": str(result.disposal_id),
                "status": result.status.value,
                "variants_disposed": result.variants_disposed,
                "mappings_disposed": result.mappings_disposed,
                "executed_at": result.executed_at.isoformat() if result.executed_at else None,
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            console.print(f"[green]✓[/green] Disposal {disposal_id} executed")
            console.print(f"  Variants disposed: {result.variants_disposed:,}")
            console.print(f"  Mappings disposed: {result.mappings_disposed:,}")
            console.print()
            console.print(
                f"Verify: vcf-pg-loader data verify-disposal "
                f"--disposal-id {disposal_id} --user-id <id>"
            )

    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@data_app.command("verify-disposal")
def data_verify_disposal(
    disposal_id: Annotated[UUID, typer.Option("--disposal-id", "-i", help="Disposal UUID")],
    user_id: Annotated[int, typer.Option("--user-id", "-u", help="Verifier user ID")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Verify data was properly disposed.

    Checks that no data remains for the disposed target.
    Required before generating a certificate of destruction.
    """
    import json as json_module

    from .data import DataDisposal

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_verify():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            disposal = DataDisposal(pool)
            return await disposal.verify_disposal(disposal_id, user_id)
        finally:
            await pool.close()

    try:
        result = asyncio.run(run_verify())

        if json_output:
            output = {
                "disposal_id": str(result.disposal_id),
                "passed": result.passed,
                "remaining_variants": result.remaining_variants,
                "expected_deleted": result.expected_deleted,
                "verified_at": result.verified_at.isoformat() if result.verified_at else None,
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            if result.passed:
                console.print("[green]✓[/green] Verification PASSED")
                console.print(f"  Expected deleted: {result.expected_deleted:,}")
                console.print(f"  Remaining: {result.remaining_variants}")
                console.print()
                console.print(
                    f"Generate certificate: vcf-pg-loader data certificate "
                    f"--disposal-id {disposal_id}"
                )
            else:
                console.print("[red]✗[/red] Verification FAILED")
                console.print(f"  Expected deleted: {result.expected_deleted:,}")
                console.print(f"  Remaining: {result.remaining_variants:,}")

    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@data_app.command("certificate")
def data_certificate(
    disposal_id: Annotated[UUID, typer.Option("--disposal-id", "-i", help="Disposal UUID")],
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Output file path")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Generate a certificate of destruction.

    Requires the disposal to be verified (verification status = passed).
    The certificate includes a cryptographic hash for integrity verification.
    """
    from .data import DataDisposal

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_certificate():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            disposal = DataDisposal(pool)
            return await disposal.generate_disposal_certificate(disposal_id)
        finally:
            await pool.close()

    try:
        cert = asyncio.run(run_certificate())
        cert_json = cert.to_json()

        if output:
            output.write_text(cert_json)
            console.print(f"[green]✓[/green] Certificate written to {output}")
            console.print(f"  Hash: {cert.certificate_hash}")
        else:
            console.print(cert_json)

    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@data_app.command("list-disposals")
def data_list_disposals(
    start_date: Annotated[
        str | None, typer.Option("--start-date", help="Start date (YYYY-MM-DD)")
    ] = None,
    end_date: Annotated[
        str | None, typer.Option("--end-date", help="End date (YYYY-MM-DD)")
    ] = None,
    status: Annotated[str | None, typer.Option("--status", "-s", help="Filter by status")] = None,
    limit: int = typer.Option(100, "--limit", "-l", help="Max records to return"),
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """List disposal records with optional filtering."""
    import json as json_module
    from datetime import datetime

    from .data import DataDisposal, DisposalStatus

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    start_dt = datetime.fromisoformat(start_date) if start_date else None
    end_dt = datetime.fromisoformat(end_date) if end_date else None
    status_enum = DisposalStatus(status) if status else None

    async def run_list():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            disposal = DataDisposal(pool)
            return await disposal.list_disposals(
                start_date=start_dt,
                end_date=end_dt,
                status=status_enum,
                limit=limit,
            )
        finally:
            await pool.close()

    try:
        records = asyncio.run(run_list())

        if json_output:

            def serialize(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                if isinstance(obj, UUID):
                    return str(obj)
                return obj

            output = []
            for r in records:
                output.append({k: serialize(v) for k, v in r.items()})
            console.print(json_module.dumps(output, indent=2, default=str))
        else:
            if not records:
                console.print("No disposal records found")
            else:
                console.print(f"[bold]Disposal Records ({len(records)})[/bold]")
                for r in records:
                    console.print(f"\n  ID: {r['disposal_id']}")
                    console.print(f"  Type: {r['disposal_type']}")
                    console.print(f"  Target: {r['target_identifier']}")
                    console.print(f"  Status: {r['execution_status']}")
                    console.print(f"  Reason: {r['reason']}")
                    if r.get("authorized_by_name"):
                        console.print(f"  Authorized by: {r['authorized_by_name']}")
                    if r.get("variants_disposed"):
                        console.print(f"  Variants disposed: {r['variants_disposed']:,}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@data_app.command("check-retention")
def data_check_retention(
    days_ahead: int = typer.Option(
        90, "--days", "-d", help="Days ahead to check for expiring data"
    ),
    db_url: Annotated[str | None, typer.Option("--db", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Check for data past retention period or expiring soon.

    Shows expired data and data approaching expiration based on retention policies.
    """
    import json as json_module

    from .data import RetentionPolicy

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_check():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            policy = RetentionPolicy(pool)
            return await policy.generate_expiration_report(days_ahead)
        finally:
            await pool.close()

    try:
        report = asyncio.run(run_check())

        if json_output:
            output = {
                "generated_at": report.generated_at.isoformat(),
                "total_expired_variants": report.total_expired_variants,
                "total_expiring_variants": report.total_expiring_variants,
                "expired_batches": [
                    {
                        "batch_id": str(e.load_batch_id),
                        "file": e.vcf_file_path,
                        "expires_at": e.expires_at.isoformat(),
                        "variant_count": e.variant_count,
                    }
                    for e in report.expired_batches
                ],
                "expiring_soon": [
                    {
                        "batch_id": str(e.load_batch_id),
                        "file": e.vcf_file_path,
                        "expires_at": e.expires_at.isoformat(),
                        "variant_count": e.variant_count,
                    }
                    for e in report.expiring_soon
                ],
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            console.print("[bold]Retention Report[/bold]")
            console.print(f"Generated: {report.generated_at.strftime('%Y-%m-%d %H:%M')}")
            console.print()

            if report.expired_batches:
                console.print(f"[red]Expired Data ({len(report.expired_batches)} batches)[/red]")
                console.print(f"  Total variants: {report.total_expired_variants:,}")
                for e in report.expired_batches[:5]:
                    console.print(f"  - {e.vcf_file_path}: {e.variant_count:,} variants")
                if len(report.expired_batches) > 5:
                    console.print(f"  ... and {len(report.expired_batches) - 5} more")
            else:
                console.print("[green]No expired data[/green]")

            console.print()

            if report.expiring_soon:
                console.print(
                    f"[yellow]Expiring Soon ({len(report.expiring_soon)} batches, "
                    f"within {days_ahead} days)[/yellow]"
                )
                console.print(f"  Total variants: {report.total_expiring_variants:,}")
                for e in report.expiring_soon[:5]:
                    days_left = (e.expires_at - report.generated_at).days
                    console.print(
                        f"  - {e.vcf_file_path}: {e.variant_count:,} variants ({days_left} days)"
                    )
            else:
                console.print(f"[green]No data expiring within {days_ahead} days[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@data_app.command("cancel")
def data_cancel(
    disposal_id: Annotated[UUID, typer.Option("--disposal-id", "-i", help="Disposal UUID")],
    user_id: Annotated[int, typer.Option("--user-id", "-u", help="Cancelling user ID")],
    reason: Annotated[str, typer.Option("--reason", "-r", help="Cancellation reason")],
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Cancel a pending disposal request.

    Only pending or authorized disposals can be cancelled.
    Completed disposals cannot be cancelled.
    """
    from .data import DataDisposal

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_cancel():
        pool = await asyncpg.create_pool(resolved_db_url, ssl=_get_ssl_param())
        try:
            disposal = DataDisposal(pool)
            return await disposal.cancel_disposal(disposal_id, user_id, reason)
        finally:
            await pool.close()

    try:
        cancelled = asyncio.run(run_cancel())
        if cancelled:
            console.print(f"[green]✓[/green] Disposal {disposal_id} cancelled")
        else:
            console.print(
                f"[yellow]Disposal {disposal_id} could not be cancelled "
                "(may already be executed)[/yellow]"
            )

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


compliance_app = typer.Typer(help="HIPAA compliance validation and reporting")
app.add_typer(compliance_app, name="compliance")


@compliance_app.command("check")
def compliance_check(
    check_id: Annotated[str | None, typer.Option("--id", "-i", help="Specific check ID")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Run HIPAA compliance checks.

    Validates database configuration against HIPAA Security Rule requirements.
    Returns non-zero exit code if critical or high severity checks fail.

    \b
    Examples:
        vcf-pg-loader compliance check
        vcf-pg-loader compliance check --id TLS_ENABLED
        vcf-pg-loader compliance check --json
    """
    import json as json_module

    from .compliance import ComplianceValidator, ReportExporter, ReportFormat

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_check():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            validator = ComplianceValidator(conn)
            if check_id:
                result = await validator.run_check(check_id)
                return result, None
            else:
                report = await validator.run_all_checks()
                return None, report
        finally:
            await conn.close()

    try:
        result, report = asyncio.run(run_check())
        exporter = ReportExporter()

        if result:
            if json_output:
                console.print(json_module.dumps(result.to_dict(), indent=2))
            else:
                status_symbol = "✓" if result.status.value == "pass" else "✗"
                status_color = "green" if result.status.value == "pass" else "red"
                console.print(
                    f"[{status_color}]{status_symbol}[/{status_color}] "
                    f"{result.check.name}: {result.message}"
                )
                if result.remediation:
                    console.print(f"  Remediation: {result.remediation}")

            if result.status.value == "fail" and result.check.severity.value in (
                "critical",
                "high",
            ):
                raise typer.Exit(1)
        else:
            if json_output:
                console.print(exporter.export(report, ReportFormat.JSON))
            else:
                console.print("\n[bold]HIPAA Compliance Check Results[/bold]\n")
                for r in report.results:
                    if r.status.value == "pass":
                        symbol, color = "✓", "green"
                    elif r.status.value == "fail":
                        symbol, color = "✗", "red"
                    elif r.status.value == "warn":
                        symbol, color = "!", "yellow"
                    else:
                        symbol, color = "-", "dim"

                    console.print(f"[{color}]{symbol}[/{color}] {r.check.name}: {r.message}")
                    if r.remediation and r.status.value != "pass":
                        console.print(f"    [dim]→ {r.remediation}[/dim]")

                console.print()
                console.print(
                    f"Summary: {report.passed_count} passed, {report.failed_count} failed, "
                    f"{report.warned_count} warnings"
                )

                if report.is_compliant:
                    console.print("[green]✓ System is HIPAA compliant[/green]")
                else:
                    console.print("[red]✗ System is NOT HIPAA compliant[/red]")

            exit_code = exporter.get_exit_code(report)
            if exit_code != 0:
                raise typer.Exit(exit_code)

    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        if not isinstance(e, SystemExit):
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None
        raise


@compliance_app.command("report")
def compliance_report(
    format: Annotated[
        str, typer.Option("--format", "-f", help="Output format (json, html)")
    ] = "json",
    output: Annotated[Path, typer.Option("--output", "-o", help="Output file path")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
) -> None:
    """Generate a HIPAA compliance report.

    Creates a detailed report of all compliance checks for documentation.

    \b
    Examples:
        vcf-pg-loader compliance report --format json --output report.json
        vcf-pg-loader compliance report --format html --output report.html
    """
    from .compliance import ComplianceValidator, ReportExporter, ReportFormat

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    format_map = {
        "json": ReportFormat.JSON,
        "html": ReportFormat.HTML,
        "text": ReportFormat.TEXT,
    }

    if format.lower() not in format_map:
        console.print(f"[red]Error: Unknown format '{format}'. Use: json, html, text[/red]")
        raise typer.Exit(1)

    async def run_report():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            validator = ComplianceValidator(conn)
            return await validator.run_all_checks()
        finally:
            await conn.close()

    try:
        report = asyncio.run(run_report())
        exporter = ReportExporter()
        report_format = format_map[format.lower()]
        content = exporter.export(report, report_format)

        if output:
            output.write_text(content)
            if not quiet:
                console.print(f"[green]✓[/green] Report written to {output}")
        else:
            console.print(content)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@compliance_app.command("status")
def compliance_status(
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Quick compliance status summary.

    Shows a brief overview of compliance status without detailed check output.
    """
    import json as json_module

    from .compliance import ComplianceValidator

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None

    if not resolved_db_url:
        console.print("[red]Error: Database connection required[/red]")
        raise typer.Exit(1)

    async def run_status():
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            validator = ComplianceValidator(conn)
            return await validator.run_all_checks()
        finally:
            await conn.close()

    try:
        report = asyncio.run(run_status())

        if json_output:
            output = {
                "is_compliant": report.is_compliant,
                "passed": report.passed_count,
                "failed": report.failed_count,
                "warned": report.warned_count,
                "skipped": report.skipped_count,
                "timestamp": report.timestamp.isoformat(),
            }
            console.print(json_module.dumps(output, indent=2))
        else:
            status = (
                "[green]COMPLIANT[/green]" if report.is_compliant else "[red]NON-COMPLIANT[/red]"
            )
            console.print(f"Status: {status}")
            console.print(
                f"Checks: {report.passed_count} passed, {report.failed_count} failed, "
                f"{report.warned_count} warnings"
            )

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


export_app = typer.Typer(help="Export data to PRS tool formats")
app.add_typer(export_app, name="export")


@export_app.command("plink-score")
def export_plink_score_cmd(
    study_id: Annotated[int, typer.Option("--study-id", "-s", help="GWAS study ID to export")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Output file path")],
    hapmap3_only: bool = typer.Option(False, "--hapmap3-only", help="Restrict to HapMap3 variants"),
    min_info: Annotated[
        float | None, typer.Option("--min-info", help="Minimum imputation INFO score")
    ] = None,
    min_maf: Annotated[float | None, typer.Option("--min-maf", help="Minimum MAF")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Export GWAS summary statistics in PLINK 2.0 --score format.

    Output format:
        SNP     A1      BETA
        rs123   A       0.05

    Example:
        vcf-pg-loader export plink-score --study-id 1 --output scores.txt
        vcf-pg-loader export plink-score -s 1 -o scores.txt --hapmap3-only --min-info 0.8
    """
    setup_logging(verbose, quiet)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .export.prs_formats import VariantFilter, export_plink_score

    async def run_export() -> int:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            variant_filter = VariantFilter(
                hapmap3_only=hapmap3_only,
                min_info=min_info,
                min_maf=min_maf,
            )
            return await export_plink_score(conn, study_id, output, variant_filter)
        finally:
            await conn.close()

    try:
        count = asyncio.run(run_export())
        if not quiet:
            console.print(f"[green]✓[/green] Exported {count:,} variants to {output}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@export_app.command("prs-cs")
def export_prs_cs_cmd(
    study_id: Annotated[int, typer.Option("--study-id", "-s", help="GWAS study ID to export")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Output file path")],
    use_se: bool = typer.Option(
        True, "--use-se/--use-p", help="Include SE (default) or P-value in last column"
    ),
    hapmap3_only: bool = typer.Option(False, "--hapmap3-only", help="Restrict to HapMap3 variants"),
    min_info: Annotated[
        float | None, typer.Option("--min-info", help="Minimum imputation INFO score")
    ] = None,
    min_maf: Annotated[float | None, typer.Option("--min-maf", help="Minimum MAF")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Export GWAS summary statistics in PRS-CS format.

    Output format (with --use-se):
        SNP     A1      A2      BETA    SE
        rs123   A       G       0.05    0.01

    Output format (with --use-p):
        SNP     A1      A2      BETA    P
        rs123   A       G       0.05    1e-8

    Example:
        vcf-pg-loader export prs-cs --study-id 1 --output prscs.txt
        vcf-pg-loader export prs-cs -s 1 -o prscs.txt --use-p --hapmap3-only
    """
    setup_logging(verbose, quiet)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .export.prs_formats import VariantFilter, export_prs_cs

    async def run_export() -> int:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            variant_filter = VariantFilter(
                hapmap3_only=hapmap3_only,
                min_info=min_info,
                min_maf=min_maf,
            )
            return await export_prs_cs(conn, study_id, output, use_se, variant_filter)
        finally:
            await conn.close()

    try:
        count = asyncio.run(run_export())
        if not quiet:
            console.print(f"[green]✓[/green] Exported {count:,} variants to {output}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@export_app.command("ldpred2")
def export_ldpred2_cmd(
    study_id: Annotated[int, typer.Option("--study-id", "-s", help="GWAS study ID to export")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Output file path")],
    hapmap3_only: bool = typer.Option(False, "--hapmap3-only", help="Restrict to HapMap3 variants"),
    min_info: Annotated[
        float | None, typer.Option("--min-info", help="Minimum imputation INFO score")
    ] = None,
    min_maf: Annotated[float | None, typer.Option("--min-maf", help="Minimum MAF")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Export GWAS summary statistics in LDpred2 bigsnpr format.

    Output format:
        chr     pos     a0      a1      beta    beta_se n_eff
        1       12345   G       A       0.05    0.01    50000

    The n_eff (effective sample size) is computed automatically:
    - For case-control: n_eff = 4 / (1/n_cases + 1/n_controls)
    - For quantitative traits: n_eff = sample_size

    Example:
        vcf-pg-loader export ldpred2 --study-id 1 --output ldpred2.txt
        vcf-pg-loader export ldpred2 -s 1 -o ldpred2.txt --hapmap3-only
    """
    setup_logging(verbose, quiet)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .export.prs_formats import VariantFilter, export_ldpred2

    async def run_export() -> int:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            variant_filter = VariantFilter(
                hapmap3_only=hapmap3_only,
                min_info=min_info,
                min_maf=min_maf,
            )
            return await export_ldpred2(conn, study_id, output, variant_filter)
        finally:
            await conn.close()

    try:
        count = asyncio.run(run_export())
        if not quiet:
            console.print(f"[green]✓[/green] Exported {count:,} variants to {output}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


@export_app.command("prsice2")
def export_prsice2_cmd(
    study_id: Annotated[int, typer.Option("--study-id", "-s", help="GWAS study ID to export")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Output file path")],
    hapmap3_only: bool = typer.Option(False, "--hapmap3-only", help="Restrict to HapMap3 variants"),
    min_info: Annotated[
        float | None, typer.Option("--min-info", help="Minimum imputation INFO score")
    ] = None,
    min_maf: Annotated[float | None, typer.Option("--min-maf", help="Minimum MAF")] = None,
    db_url: Annotated[str | None, typer.Option("--db", "-d", help="PostgreSQL URL")] = None,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Export GWAS summary statistics in PRSice-2 format.

    Output format:
        SNP     A1      A2      BETA    SE      P
        rs123   A       G       0.05    0.01    1e-8

    Example:
        vcf-pg-loader export prsice2 --study-id 1 --output prsice2.txt
        vcf-pg-loader export prsice2 -s 1 -o prsice2.txt --hapmap3-only --min-maf 0.01
    """
    setup_logging(verbose, quiet)

    try:
        resolved_db_url = _resolve_database_url(db_url, quiet)
    except CredentialValidationError as e:
        console.print(f"[red]Security Error: {e}[/red]")
        raise typer.Exit(1) from None
    if resolved_db_url is None:
        raise typer.Exit(1)

    from .export.prs_formats import VariantFilter, export_prsice2

    async def run_export() -> int:
        conn = await asyncpg.connect(resolved_db_url, ssl=_get_ssl_param())
        try:
            variant_filter = VariantFilter(
                hapmap3_only=hapmap3_only,
                min_info=min_info,
                min_maf=min_maf,
            )
            return await export_prsice2(conn, study_id, output, variant_filter)
        finally:
            await conn.close()

    try:
        count = asyncio.run(run_export())
        if not quiet:
            console.print(f"[green]✓[/green] Exported {count:,} variants to {output}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from None


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
