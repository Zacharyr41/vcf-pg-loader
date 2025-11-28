# vcf-pg-loader

High-performance VCF to PostgreSQL loader with clinical-grade compliance.

## Features

- **Streaming VCF parsing** with cyvcf2 for memory-efficient processing
- **Number=A/R/G field handling** - proper per-ALT extraction during multi-allelic decomposition
- **Binary COPY protocol** via asyncpg for maximum insert performance
- **Chromosome-partitioned tables** for efficient region queries
- **Audit trail** with load batch tracking and validation
- **CLI interface** with Typer for easy operation

## Installation

```bash
# Install with uv (recommended)
uv pip install -e ".[dev]"

# Or with pip
pip install -e ".[dev]"
```

## Quick Start

```bash
# Initialize database schema
vcf-pg-loader init-db --db postgresql://user:pass@localhost/variants

# Load a VCF file
vcf-pg-loader load sample.vcf.gz --db postgresql://user:pass@localhost/variants

# Validate a completed load
vcf-pg-loader validate <load-batch-id> --db postgresql://user:pass@localhost/variants
```

## CLI Commands

### `load`

Load a VCF file into PostgreSQL.

```bash
vcf-pg-loader load <vcf_path> [OPTIONS]

Options:
  --db, -d        PostgreSQL connection URL [default: postgresql://localhost/variants]
  --batch, -b     Records per batch [default: 50000]
  --workers, -w   Parallel workers [default: 8]
  --normalize/--no-normalize    Normalize variants [default: normalize]
  --drop-indexes/--keep-indexes Drop indexes during load [default: drop-indexes]
```

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
  --db, -d    PostgreSQL connection URL
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

## High-Level Architecture Plan
 ```
┌─────────────────────────────────────────────────────────────────┐
│                        VCF File(s)                               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Header Parser                                                   │
│  - Extract INFO/FORMAT definitions for schema inference          │
│  - Build contig name→index mapping                               │
│  - Parse CSQ/ANN field structure from ##INFO lines               │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  Worker 1        │ │  Worker 2        │ │  Worker N        │
│  (chr1, chr4...) │ │  (chr2, chr5...) │ │  (chr3, chr6...) │
│                  │ │                  │ │                  │
│  ┌────────────┐  │ │  ┌────────────┐  │ │  ┌────────────┐  │
│  │  cyvcf2    │  │ │  │  cyvcf2    │  │ │  │  cyvcf2    │  │
│  │  Parser    │  │ │  │  Parser    │  │ │  │  Parser    │  │
│  └─────┬──────┘  │ │  └─────┬──────┘  │ │  └─────┬──────┘  │
│        │         │ │        │         │ │        │         │
│  ┌─────▼──────┐  │ │  ┌─────▼──────┐  │ │  ┌─────▼──────┐  │
│  │ Normalizer │  │ │  │ Normalizer │  │ │  │ Normalizer │  │
│  └─────┬──────┘  │ │  └─────┬──────┘  │ │  └─────┬──────┘  │
│        │         │ │        │         │ │        │         │
│  ┌─────▼──────┐  │ │  ┌─────▼──────┐  │ │  ┌─────▼──────┐  │
│  │  Batch     │  │ │  │  Batch     │  │ │  │  Batch     │  │
│  │  Buffer    │  │ │  │  Buffer    │  │ │  │  Buffer    │  │
│  │  (50K rows)│  │ │  │  (50K rows)│  │ │  │  (50K rows)│  │
│  └─────┬──────┘  │ │  └─────┬──────┘  │ │  └─────┬──────┘  │
└────────┼─────────┘ └────────┼─────────┘ └────────┼─────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  asyncpg Connection Pool (min=4, max=16)                        │
│  Binary COPY protocol                                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  PostgreSQL (indexes dropped during load)                        │
│  - Partitioned by chromosome                                     │
│  - Unlogged tables during bulk load (optional)                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Post-Load Phase                                                 │
│  1. CREATE INDEX CONCURRENTLY (parallel per partition)           │
│  2. VACUUM ANALYZE                                               │
│  3. Validation: count verification, duplicate check              │
└─────────────────────────────────────────────────────────────────┘
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

- [Genomics Concepts](docs/genomics-concepts.md) - Understanding VCF data for non-geneticists
- [Glossary of Terms](docs/glossary-of-terms.md) - Technical terminology reference
- [Architecture](docs/architecture.md) - Detailed system design and implementation

## License

LGPL-2.1 - See [LICENSE](LICENSE) for details.
