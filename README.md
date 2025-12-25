# vcf-pg-loader

[![CI](https://github.com/Zacharyr41/vcf-pg-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/Zacharyr41/vcf-pg-loader/actions/workflows/ci.yml)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](http://bioconda.github.io/recipes/vcf-pg-loader/README.html)
[![PyPI version](https://badge.fury.io/py/vcf-pg-loader.svg)](https://badge.fury.io/py/vcf-pg-loader)

High-performance VCF to PostgreSQL loader with clinical-grade compliance.

## Features

- **Streaming VCF parsing** with cyvcf2 for memory-efficient processing
- **Variant normalization** using the vt algorithm (left-align and trim)
- **Number=A/R/G field handling** - proper per-ALT extraction during multi-allelic decomposition
- **Binary COPY protocol** via asyncpg for maximum insert performance
- **Chromosome-partitioned tables** for efficient region queries
- **Human and non-human genome support** - chromosome enum for human, TEXT for others
- **Audit trail** with load batch tracking and validation
- **CLI interface** with Typer for easy operation
- **TOML configuration** - file-based configuration with CLI overrides
- **Progress reporting** - real-time progress bar with `rich`
- **Structured logging** - configurable verbosity levels
- **Retry logic** - exponential backoff for transient database failures
- **Docker support** - multi-stage Dockerfile and docker-compose for development
- **Zero-config database** - auto-managed PostgreSQL via Docker, no setup required

## Installation

### Bioconda (Recommended)

```bash
conda install -c conda-forge -c bioconda vcf-pg-loader
```

### PyPI

```bash
pip install vcf-pg-loader
```

### Quick Install Script

```bash
curl -fsSL https://raw.githubusercontent.com/Zacharyr41/vcf-pg-loader/main/install.sh | bash
```

This installs vcf-pg-loader and all dependencies (Python, Docker) automatically.

### From Source

```bash
git clone https://github.com/Zacharyr41/vcf-pg-loader.git
cd vcf-pg-loader
uv pip install -e ".[dev]"
```

### Nextflow Module

For use in Nextflow pipelines, the `vcfpgloader/load` module is available:

```nextflow
include { VCFPGLOADER } from './modules/local/vcfpgloader/load/main'

workflow {
    VCFPGLOADER(
        ch_vcf,           // tuple val(meta), path(vcf), path(tbi)
        params.db_host,
        params.db_port,
        params.db_name,
        params.db_user,
        params.db_schema
    )
}
```

The module uses `PGPASSWORD` as a Nextflow secret. See [nf-core modules](https://github.com/nf-core/modules) for integration into nf-core pipelines.

### Verify Installation

```bash
vcf-pg-loader doctor
```

## Quick Start

### Zero-Config Mode (Easiest)

No PostgreSQL setup required - vcf-pg-loader manages a local database automatically:

```bash
# Load a VCF file (auto-starts PostgreSQL in Docker)
vcf-pg-loader load sample.vcf.gz

# Check database status
vcf-pg-loader db status

# Open psql shell to query data
vcf-pg-loader db shell
```

### With Your Own PostgreSQL

```bash
# Initialize database schema
vcf-pg-loader init-db --db postgresql://user:pass@localhost/variants

# Load a VCF file
vcf-pg-loader load sample.vcf.gz --db postgresql://user:pass@localhost/variants

# Validate a completed load
vcf-pg-loader validate <load-batch-id> --db postgresql://user:pass@localhost/variants
```

### Additional Options

```bash
# Load without normalization
vcf-pg-loader load sample.vcf.gz --no-normalize

# Load non-human VCF (e.g., SARS-CoV-2)
vcf-pg-loader load sarscov2.vcf.gz --no-human-genome

# Initialize for non-human genomes
vcf-pg-loader init-db --db postgresql://... --no-human-genome
```

## CLI Commands

### `load`

Load a VCF file into PostgreSQL.

```bash
vcf-pg-loader load <vcf_path> [OPTIONS]

Options:
  --db, -d                        PostgreSQL connection URL (omit for auto-managed DB)
  --batch, -b                     Records per batch [default: 50000]
  --workers, -w                   Parallel workers [default: 8]
  --normalize/--no-normalize      Normalize variants using vt algorithm [default: normalize]
  --drop-indexes/--keep-indexes   Drop indexes during load [default: drop-indexes]
  --human-genome/--no-human-genome  Use human chromosome enum type [default: human-genome]
  --config, -c                    TOML configuration file
  --verbose, -v                   Enable verbose logging (DEBUG level)
  --quiet, -q                     Suppress non-error output
  --progress/--no-progress        Show progress bar [default: progress]
  --force, -f                     Force reload even if file was already loaded
```

When `--db` is omitted, vcf-pg-loader automatically uses a managed PostgreSQL container.

**Normalization**: When enabled (default), variants are left-aligned and trimmed following the vt algorithm. This ensures consistent representation across different variant callers.

**Genome Type**: Human genome mode uses a PostgreSQL enum for chromosomes (chr1-22, X, Y, M) which provides validation and efficient storage. Non-human mode uses TEXT to support arbitrary chromosome/contig names.

### `validate`

Validate a completed load by checking record counts and duplicates.

```bash
vcf-pg-loader validate <load_batch_id> [OPTIONS]

Options:
  --db, -d    PostgreSQL connection URL
```

### `init-db`

Initialize the database schema (tables, indexes, extensions).

```bash
vcf-pg-loader init-db [OPTIONS]

Options:
  --db, -d                          PostgreSQL connection URL
  --human-genome/--no-human-genome  Use human chromosome enum type [default: human-genome]
```

**Important**: The genome type must match between `init-db` and `load` commands. Use `--no-human-genome` for both when loading non-human VCFs.

### `benchmark`

Run performance benchmarks on VCF parsing and loading.

```bash
vcf-pg-loader benchmark [OPTIONS]

Options:
  --vcf, -f        Path to VCF file (uses built-in fixture if omitted)
  --synthetic, -s  Generate synthetic VCF with N variants
  --db, -d         PostgreSQL URL (omit for parsing-only benchmark)
  --batch, -b      Batch size [default: 50000]
  --normalize/--no-normalize  Test with/without normalization
  --json           Output results as JSON (for CI integration)
  --quiet, -q      Minimal output
```

**Examples:**

```bash
# Quick benchmark with built-in fixture (~2.6K variants)
vcf-pg-loader benchmark

# Generate and benchmark 100K synthetic variants
vcf-pg-loader benchmark --synthetic 100000

# Benchmark a specific VCF file
vcf-pg-loader benchmark --vcf /path/to/sample.vcf.gz

# Full benchmark including database loading
vcf-pg-loader benchmark --synthetic 50000 --db postgresql://localhost/variants

# JSON output for CI/scripting
vcf-pg-loader benchmark --synthetic 10000 --json
```

**Sample output:**

```
Benchmark Results (synthetic)
  Variants: 100,000
  Batch size: 50,000
  Normalized: True

Parsing: 100,000 variants in 0.94s (106,000/sec)
```

### `doctor`

Check system dependencies and diagnose issues.

```bash
vcf-pg-loader doctor

# Example output:
Dependency Check
  Python         3.12.4   OK
  cyvcf2         0.30.22  OK
  asyncpg        0.29.0   OK
  Docker         24.0.5   OK
  Docker daemon  running  OK
```

### `db`

Manage the local PostgreSQL database (Docker-based).

```bash
vcf-pg-loader db start   # Start PostgreSQL container
vcf-pg-loader db stop    # Stop the container
vcf-pg-loader db status  # Show running status and connection URL
vcf-pg-loader db url     # Print connection URL (for scripts)
vcf-pg-loader db shell   # Open psql shell
vcf-pg-loader db reset   # Remove container and all data
```

## Architecture

### Components

1. **VCFHeaderParser** - Parses VCF headers via cyvcf2's native API to extract INFO/FORMAT field definitions
2. **VCFStreamingParser** - Memory-efficient streaming iterator that yields batches of `VariantRecord` objects
3. **VariantParser** - Handles per-variant parsing with Number=A/R/G field extraction for multi-allelic decomposition
4. **VCFLoader** - Orchestrates loading with asyncpg binary COPY protocol
5. **SchemaManager** - Manages PostgreSQL schema creation and index management

### Data Flow

```
VCF File → VCFStreamingParser → Batch Buffer → asyncpg COPY → PostgreSQL
                ↓
         VCFHeaderParser (field metadata)
                ↓
         VariantParser (Number=A/R/G extraction)
```

## Citations and Acknowledgments

This project was inspired by and builds upon several foundational tools in the genomics community:

### Primary References

**Slivar** - Rapid variant filtering:
> Pedersen, B.S., Brown, J.M., Dashnow, H. et al. Effective variant filtering and expected
> candidate variant yield in studies of rare human disease. *npj Genom. Med.* 6, 60 (2021).
> https://doi.org/10.1038/s41525-021-00227-3

**GEMINI** - Original SQL-based VCF database:
> Paila, U., Chapman, B.A., Kirchner, R., & Quinlan, A.R. GEMINI: Integrative Exploration
> of Genetic Variation and Genome Annotations. *PLoS Comput Biol* 9(7): e1003153 (2013).
> https://doi.org/10.1371/journal.pcbi.1003153

**cyvcf2** - Python VCF parsing:
> Pedersen, B.S. & Quinlan, A.R. cyvcf2: fast, flexible variant analysis with Python.
> *Bioinformatics* 33(12), 1867–1869 (2017). https://doi.org/10.1093/bioinformatics/btx057

### Supporting Tools

- **vcf2db**: https://github.com/quinlan-lab/vcf2db
- **VCF Format**: Danecek et al. (2011) https://doi.org/10.1093/bioinformatics/btr330
- **bcftools/HTSlib**: Danecek et al. (2021) https://doi.org/10.1093/gigascience/giab008
- **GIAB Benchmarks**: Zook et al. (2019) https://doi.org/10.1038/s41587-019-0074-6

## Configuration

vcf-pg-loader supports TOML configuration files for persistent settings:

```toml
# vcf-pg-loader.toml
[vcf_pg_loader]
batch_size = 25000
workers = 16
normalize = true
drop_indexes = true
human_genome = true
log_level = "INFO"
```

Use with the `--config` flag:

```bash
vcf-pg-loader load sample.vcf.gz --config vcf-pg-loader.toml
```

CLI arguments override config file values.

## Docker

### Using Docker Compose (recommended for development)

```bash
# Start PostgreSQL and run a load
docker-compose up -d postgres
docker-compose run vcf-pg-loader load /data/sample.vcf.gz --db postgresql://vcfloader:vcfloader@postgres:5432/variants

# Or build and run standalone
docker build -t vcf-pg-loader .
docker run vcf-pg-loader --help
```

### Docker Compose Services

- `postgres`: PostgreSQL 16 with health checks
- `vcf-pg-loader`: The loader application

Mount your VCF files to `/data` in the container.

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=vcf_pg_loader

# Run only unit tests (skip integration)
uv run pytest -m "not integration"
```

### Code Quality

```bash
# Lint
uv run ruff check src tests

# Type check
uv run mypy src
```

## Documentation

- [CLI Reference](docs/cli-reference.md) - Complete command-line documentation
- [Genomics Concepts](docs/genomics-concepts.md) - Understanding VCF data for non-geneticists
- [Glossary of Terms](docs/glossary-of-terms.md) - Technical terminology reference
- [Architecture](docs/architecture.md) - Detailed system design and implementation

## License

MIT - See [LICENSE](LICENSE) for details.
