# CLI Reference

Complete command-line interface documentation for vcf-pg-loader.

## Global Options

```bash
vcf-pg-loader [OPTIONS] COMMAND [ARGS]...
```

| Option | Description |
|--------|-------------|
| `--install-completion` | Install shell completion for bash/zsh/fish |
| `--show-completion` | Show completion script to customize |
| `--help` | Show help and exit |

### Shell Completion

Enable tab completion for commands and options:

```bash
# Install completion (detects your shell automatically)
vcf-pg-loader --install-completion

# Restart your shell or source your profile
source ~/.zshrc  # or ~/.bashrc
```

---

## Commands

### `load`

Load a VCF file into PostgreSQL.

```bash
vcf-pg-loader load [OPTIONS] VCF_PATH
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `VCF_PATH` | Yes | Path to VCF file (.vcf or .vcf.gz) |

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | auto | PostgreSQL URL or 'auto' for managed DB |
| `--batch` | `-b` | 50000 | Records per batch |
| `--workers` | `-w` | 8 | Parallel workers |
| `--normalize` | | Yes | Normalize variants (left-align, trim) |
| `--no-normalize` | | | Skip normalization |
| `--drop-indexes` | | Yes | Drop indexes during load for speed |
| `--keep-indexes` | | | Keep indexes during load |
| `--human-genome` | | Yes | Use chromosome enum (chr1-22, X, Y, M) |
| `--no-human-genome` | | | Use TEXT for arbitrary contig names |
| `--force` | `-f` | | Reload even if file was already loaded |
| `--config` | `-c` | | Path to TOML configuration file |
| `--verbose` | `-v` | | Enable DEBUG level logging |
| `--quiet` | `-q` | | Suppress non-error output |
| `--progress` | | Yes | Show progress bar |
| `--no-progress` | | | Hide progress bar |

#### Examples

```bash
# Simplest: auto-managed database
vcf-pg-loader load sample.vcf.gz

# With your own PostgreSQL
vcf-pg-loader load sample.vcf.gz --db postgresql://user:pass@localhost/variants

# High-throughput settings
vcf-pg-loader load large.vcf.gz --batch 100000 --workers 16

# Non-human genome (e.g., viral, bacterial)
vcf-pg-loader load sarscov2.vcf.gz --no-human-genome

# Skip normalization for pre-normalized data
vcf-pg-loader load normalized.vcf.gz --no-normalize

# Quiet mode for scripts
vcf-pg-loader load sample.vcf.gz --quiet --no-progress

# Force reload of previously loaded file
vcf-pg-loader load sample.vcf.gz --force

# Use configuration file
vcf-pg-loader load sample.vcf.gz --config settings.toml
```

---

### `init-db`

Initialize database schema (tables, indexes, extensions).

```bash
vcf-pg-loader init-db [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--human-genome` | | Yes | Use chromosome enum type |
| `--no-human-genome` | | | Use TEXT for chromosomes |

#### Examples

```bash
# Standard human genome schema
vcf-pg-loader init-db --db postgresql://localhost/variants

# Non-human genome schema
vcf-pg-loader init-db --db postgresql://localhost/variants --no-human-genome
```

**Note**: The genome type must match between `init-db` and `load` commands.

---

### `validate`

Validate a completed load by checking record counts and duplicates.

```bash
vcf-pg-loader validate [OPTIONS] LOAD_BATCH_ID
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `LOAD_BATCH_ID` | Yes | UUID of the load batch to validate |

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |

#### Examples

```bash
vcf-pg-loader validate 550e8400-e29b-41d4-a716-446655440000 --db postgresql://localhost/variants
```

---

### `benchmark`

Run performance benchmarks on VCF parsing and database loading.

```bash
vcf-pg-loader benchmark [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--vcf` | `-f` | | Path to VCF file to benchmark |
| `--synthetic` | `-s` | | Generate N synthetic variants |
| `--db` | `-d` | | PostgreSQL URL (omit for parse-only) |
| `--batch` | `-b` | 50000 | Batch size |
| `--normalize` | | Yes | Include normalization |
| `--no-normalize` | | | Skip normalization |
| `--json` | | | Output results as JSON |
| `--quiet` | `-q` | | Minimal output |

#### Examples

```bash
# Quick benchmark with built-in fixture
vcf-pg-loader benchmark

# Benchmark with synthetic data
vcf-pg-loader benchmark --synthetic 100000

# Benchmark specific file
vcf-pg-loader benchmark --vcf /path/to/sample.vcf.gz

# Full benchmark including database
vcf-pg-loader benchmark --synthetic 50000 --db postgresql://localhost/variants

# JSON output for CI
vcf-pg-loader benchmark --synthetic 10000 --json
```

#### Output

```
Benchmark Results (synthetic)
  Variants: 100,000
  Batch size: 50,000
  Normalized: True

Parsing: 100,000 variants in 0.94s (106,000/sec)
Loading: 100,000 variants in 2.31s (43,290/sec)
```

---

### `doctor`

Check system dependencies and diagnose configuration issues.

```bash
vcf-pg-loader doctor
```

#### Output

```
Dependency Check
  Python         3.12.4   OK
  cyvcf2         0.31.0   OK
  asyncpg        0.29.0   OK
  Docker         24.0.5   OK
  Docker daemon  running  OK
```

If a check fails, the tool provides installation instructions for your platform.

---

### `db`

Manage the local PostgreSQL database (Docker-based).

```bash
vcf-pg-loader db COMMAND [OPTIONS]
```

#### Subcommands

##### `db start`

Start the managed PostgreSQL container.

```bash
vcf-pg-loader db start [OPTIONS]
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--port` | `-p` | 5432 | Host port to expose |

```bash
# Start on default port
vcf-pg-loader db start

# Start on custom port
vcf-pg-loader db start --port 5433
```

##### `db stop`

Stop the managed PostgreSQL container.

```bash
vcf-pg-loader db stop
```

##### `db status`

Show status of the managed database.

```bash
vcf-pg-loader db status
```

Output:
```
Database Status
  Container: vcf-pg-loader-db
  Status: running
  Port: 5432
  URL: postgresql://vcfloader:vcfloader@localhost:5432/variants
```

##### `db url`

Print the database connection URL (useful for scripts).

```bash
vcf-pg-loader db url
```

Output:
```
postgresql://vcfloader:vcfloader@localhost:5432/variants
```

##### `db shell`

Open an interactive psql shell to the managed database.

```bash
vcf-pg-loader db shell
```

##### `db reset`

Stop and remove the database container and all data.

```bash
vcf-pg-loader db reset
```

**Warning**: This permanently deletes all loaded variant data.

---

## PRS Research Commands

### `import-gwas`

Import GWAS summary statistics following the GWAS-SSF standard.

```bash
vcf-pg-loader import-gwas [OPTIONS] FILE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `FILE` | Yes | Path to summary statistics file (TSV) |

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--study-id` | | | GWAS Catalog accession (e.g., GCST90012345) |
| `--trait` | | | Trait name |
| `--sample-size` | | | Total sample size |
| `--n-cases` | | | Number of cases (case-control) |
| `--n-controls` | | | Number of controls |
| `--genome-build` | | GRCh38 | Reference genome build |

#### Examples

```bash
vcf-pg-loader import-gwas sumstats.tsv \
    --study-id GCST90012345 \
    --trait "Type 2 Diabetes" \
    --sample-size 898130 \
    --n-cases 74124 \
    --n-controls 824006 \
    --db postgresql://localhost/prs_db
```

---

### `list-studies`

List loaded GWAS studies.

```bash
vcf-pg-loader list-studies [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |

---

### `import-pgs`

Import PGS Catalog scoring file.

```bash
vcf-pg-loader import-pgs [OPTIONS] FILE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `FILE` | Yes | Path to PGS Catalog scoring file |

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |

#### Examples

```bash
vcf-pg-loader import-pgs PGS000001_hmPOS_GRCh38.txt \
    --db postgresql://localhost/prs_db
```

---

### `list-pgs`

List loaded PGS scores with match statistics.

```bash
vcf-pg-loader list-pgs [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |

---

### `download-reference`

Download reference panel data (HapMap3) from authoritative sources.

```bash
vcf-pg-loader download-reference [OPTIONS] PANEL_TYPE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `PANEL_TYPE` | Yes | Reference panel type (currently: `hapmap3`) |

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--build` | `-b` | `grch38` | Genome build (`grch37` or `grch38`) |
| `--output` | `-o` | `~/.vcf-pg-loader/references` | Output directory |
| `--force` | `-f` | `false` | Force re-download even if cached |
| `--quiet` | `-q` | `false` | Suppress non-error output |
| `--verbose` | `-v` | `false` | Verbose output |

#### Examples

```bash
# Download HapMap3 for GRCh38 (default)
vcf-pg-loader download-reference hapmap3

# Download for GRCh37
vcf-pg-loader download-reference hapmap3 --build grch37

# Force re-download
vcf-pg-loader download-reference hapmap3 --force

# Custom output directory
vcf-pg-loader download-reference hapmap3 --output /path/to/refs
```

---

### `load-reference`

Load a reference panel (e.g., HapMap3) into the database.

```bash
vcf-pg-loader load-reference [OPTIONS] PANEL_TYPE [FILE]
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `PANEL_TYPE` | Yes | Reference panel type (`hapmap3`, `ld-blocks`) |
| `FILE` | No | Path to reference file (uses cached download if omitted) |

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--build` | `-b` | `grch38` | Genome build (`grch37` or `grch38`) |
| `--population` | `-p` | | Population for LD blocks (EUR, AFR, EAS, SAS) |
| `--quiet` | `-q` | `false` | Suppress non-error output |
| `--verbose` | `-v` | `false` | Verbose output |

#### Examples

```bash
# Load HapMap3 using cached download
vcf-pg-loader load-reference hapmap3 --db postgresql://localhost/prs_db

# Load from custom file
vcf-pg-loader load-reference hapmap3 /path/to/hapmap3.tsv --db postgresql://localhost/prs_db

# Load LD blocks for European population
vcf-pg-loader load-reference ld-blocks --population EUR --db postgresql://localhost/prs_db
```

---

### `annotate-ld-blocks`

Annotate variants with LD block IDs.

```bash
vcf-pg-loader annotate-ld-blocks [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--population` | | EUR | Population for LD blocks |

#### Examples

```bash
vcf-pg-loader annotate-ld-blocks \
    --population EUR \
    --db postgresql://localhost/prs_db
```

---

### `import-frequencies`

Import population allele frequencies.

```bash
vcf-pg-loader import-frequencies [OPTIONS] FILE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `FILE` | Yes | Path to frequency file (VCF or TSV) |

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--source` | | Required | Source name (e.g., "gnomAD_v3") |

---

### `compute-sample-qc`

Compute sample-level QC metrics.

```bash
vcf-pg-loader compute-sample-qc [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--batch-id` | | | Specific batch to process |

---

### `refresh-views`

Refresh materialized views.

```bash
vcf-pg-loader refresh-views [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--concurrent` | | Yes | Use CONCURRENTLY (non-blocking) |

---

## Export Commands

### `export-plink-score`

Export GWAS statistics in PLINK 2.0 --score format.

```bash
vcf-pg-loader export-plink-score [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--study-id` | | Required | Study ID to export |
| `--output` | `-o` | Required | Output file path |
| `--hapmap3-only` | | No | Filter to HapMap3 variants |
| `--min-info` | | | Minimum INFO score |
| `--min-maf` | | | Minimum MAF |

#### Output Format

```
SNP     A1      BETA
rs123   A       0.05
```

---

### `export-prs-cs`

Export GWAS statistics in PRS-CS format.

```bash
vcf-pg-loader export-prs-cs [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--study-id` | | Required | Study ID to export |
| `--output` | `-o` | Required | Output file path |
| `--use-se` | | Yes | Include standard error (vs p-value) |
| `--hapmap3-only` | | No | Filter to HapMap3 variants |
| `--min-info` | | | Minimum INFO score |
| `--min-maf` | | | Minimum MAF |

#### Output Format

```
SNP     A1      A2      BETA    SE
rs123   A       G       0.05    0.01
```

---

### `export-ldpred2`

Export GWAS statistics in LDpred2 (bigsnpr) format.

```bash
vcf-pg-loader export-ldpred2 [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--study-id` | | Required | Study ID to export |
| `--output` | `-o` | Required | Output file path |
| `--hapmap3-only` | | No | Filter to HapMap3 variants |
| `--min-info` | | | Minimum INFO score |

#### Output Format

```
chr     pos     a0      a1      beta    beta_se n_eff
1       12345   G       A       0.05    0.01    50000
```

---

### `export-prsice2`

Export GWAS statistics in PRSice-2 format.

```bash
vcf-pg-loader export-prsice2 [OPTIONS]
```

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | Required | PostgreSQL connection URL |
| `--study-id` | | Required | Study ID to export |
| `--output` | `-o` | Required | Output file path |
| `--hapmap3-only` | | No | Filter to HapMap3 variants |
| `--min-info` | | | Minimum INFO score |

#### Output Format

```
SNP     A1      A2      BETA    SE      P
rs123   A       G       0.05    0.01    1e-8
```

---

## Configuration File

vcf-pg-loader supports TOML configuration files for persistent settings.

### Format

```toml
[vcf_pg_loader]
batch_size = 50000
workers = 8
normalize = true
drop_indexes = true
human_genome = true
log_level = "INFO"
```

### Available Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `batch_size` | int | 50000 | Records per batch |
| `workers` | int | 8 | Parallel workers |
| `normalize` | bool | true | Normalize variants |
| `drop_indexes` | bool | true | Drop indexes during load |
| `human_genome` | bool | true | Use chromosome enum |
| `log_level` | string | "INFO" | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Usage

```bash
vcf-pg-loader load sample.vcf.gz --config settings.toml
```

CLI arguments override config file values.

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VCF_PG_LOADER_DB` | Default database URL |
| `VCF_PG_LOADER_LOG_LEVEL` | Logging level |

---

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |
| 3 | Database connection error |
| 4 | File not found |

---

## Troubleshooting

### Docker not running

```
Error: Docker daemon not running
```

**Solution**: Start Docker Desktop (macOS/Windows) or the Docker service (Linux):
```bash
# macOS: Open Docker Desktop from Applications

# Linux
sudo systemctl start docker
```

### Permission denied on Docker socket

```
Error: Permission denied while trying to connect to Docker daemon
```

**Solution**: Add your user to the docker group:
```bash
sudo usermod -aG docker $USER
# Log out and back in
```

### Port already in use

```
Error: Port 5432 is already in use
```

**Solution**: Use a different port:
```bash
vcf-pg-loader db start --port 5433
vcf-pg-loader load sample.vcf.gz --db postgresql://vcfloader:vcfloader@localhost:5433/variants
```

### cyvcf2 installation fails

cyvcf2 requires htslib. On macOS:
```bash
brew install htslib
pip install cyvcf2
```

On Ubuntu/Debian:
```bash
sudo apt-get install libhts-dev
pip install cyvcf2
```

### Out of memory during large loads

Reduce batch size and workers:
```bash
vcf-pg-loader load large.vcf.gz --batch 10000 --workers 4
```

### Slow loading performance

1. Ensure indexes are dropped during load (default)
2. Increase batch size for large files
3. Use more workers if CPU allows
4. Disable progress bar for marginal speedup: `--no-progress`
