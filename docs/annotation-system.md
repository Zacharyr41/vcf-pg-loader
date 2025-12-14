# SQL-Based Variant Annotation System

vcf-pg-loader provides a PostgreSQL-based variant annotation system inspired by
[echtvar](https://github.com/brentp/echtvar). Unlike echtvar's compact binary
format, this system uses SQL JOINs for annotation lookup, enabling complex queries
and integration with existing PostgreSQL workflows.

## Overview

The annotation system allows you to:

1. **Load population databases** (gnomAD, ClinVar, etc.) as reference tables
2. **Annotate query VCFs** with allele frequencies, clinical significance, etc.
3. **Filter variants** using echtvar-compatible expressions
4. **Execute ad-hoc SQL** queries against annotation data

## Quick Start

```bash
# Load gnomAD as an annotation source
vcf-pg-loader load-annotation gnomad.vcf.gz \
  --name gnomad_v3 \
  --config gnomad.json \
  --version v3.1.2

# Load your query VCF
vcf-pg-loader load sample.vcf.gz

# Annotate with filtering
vcf-pg-loader annotate <batch-id> \
  --source gnomad_v3 \
  --filter "gnomad_af < 0.01"
```

## Architecture

### Database Schema

Each annotation source creates a dedicated table:

```sql
-- Registry table (tracks all loaded sources)
CREATE TABLE annotation_sources (
    source_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    source_type VARCHAR(50),
    version VARCHAR(50),
    vcf_path TEXT,
    field_config JSONB NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    variant_count BIGINT DEFAULT 0
);

-- Per-source annotation table (e.g., anno_gnomad_v3)
CREATE TABLE anno_gnomad_v3 (
    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    gnomad_af REAL,
    gnomad_ac INTEGER,
    gnomad_an INTEGER,
    gnomad_nhomalt INTEGER,
    PRIMARY KEY (chrom, pos, ref, alt)
);
```

### Annotation Lookup

Annotation uses SQL LEFT JOINs for efficient variant-to-annotation matching:

```sql
SELECT v.chrom, v.pos, v.ref, v.alt, a.gnomad_af
FROM variants v
LEFT JOIN anno_gnomad_v3 a
  ON v.chrom = a.chrom
  AND v.pos = a.pos
  AND v.ref = a.ref
  AND v.alt = a.alt
WHERE v.load_batch_id = 'your-batch-id'
```

## CLI Commands

### load-annotation

Load an annotation VCF as a reference database:

```bash
vcf-pg-loader load-annotation <vcf-path> \
  --name <source-name> \
  --config <config.json> \
  [--version <version>] \
  [--type <source-type>] \
  [--db <postgresql-url>]
```

Options:
- `--name, -n`: Required. Unique name for this annotation source
- `--config, -c`: Required. JSON field configuration file
- `--version, -v`: Version string (e.g., "v3.1.2")
- `--type, -t`: Source type (e.g., "population", "pathogenicity")

### list-annotations

List all loaded annotation sources:

```bash
vcf-pg-loader list-annotations [--db <url>] [--json]
```

### annotate

Annotate loaded variants:

```bash
vcf-pg-loader annotate <batch-id> \
  --source <source-name> \
  [--filter <expression>] \
  [--output <file>] \
  [--format tsv|json] \
  [--limit <n>]
```

Options:
- `--source, -s`: Annotation source(s) to use (can be repeated)
- `--filter, -f`: Filter expression (echtvar-style syntax)
- `--output, -o`: Output file (stdout if omitted)
- `--format`: Output format (tsv or json)
- `--limit, -l`: Limit number of results

### annotation-query

Execute ad-hoc SQL queries:

```bash
vcf-pg-loader annotation-query \
  --sql "SELECT * FROM anno_gnomad LIMIT 10" \
  [--format tsv|json]
```

## Filter Expressions

The system supports echtvar-compatible filter expressions:

```
gnomad_af < 0.01                        # Rare variants
gnomad_af < 0.01 && clinvar_sig == 'Pathogenic'  # Rare + pathogenic
gnomad_af < 0.01 || gnomad_af IS NULL   # Rare or novel
```

### Supported Operators

| Expression | SQL Translation |
|------------|-----------------|
| `&&` | `AND` |
| `\|\|` | `OR` |
| `==` | `=` |
| `!=` | `<>` |
| `<`, `<=`, `>`, `>=` | Same |
| `IS NULL` | `IS NULL` |
| `IS NOT NULL` | `IS NOT NULL` |

## Python API

```python
import asyncpg
from vcf_pg_loader.annotator import VariantAnnotator
from vcf_pg_loader.annotation_loader import AnnotationLoader
from vcf_pg_loader.annotation_config import load_field_config

# Load an annotation source
conn = await asyncpg.connect("postgresql://localhost/variants")
fields = load_field_config("gnomad.json")
loader = AnnotationLoader()
await loader.load_annotation_source(
    vcf_path=Path("gnomad.vcf.gz"),
    source_name="gnomad_v3",
    field_config=fields,
    conn=conn,
)

# Annotate variants
annotator = VariantAnnotator(conn)
results = await annotator.annotate_variants(
    sources=["gnomad_v3"],
    load_batch_id="your-batch-id",
    filter_expr="gnomad_af < 0.01",
)
```

## Comparison with echtvar

| Feature | echtvar | vcf-pg-loader |
|---------|---------|---------------|
| Storage format | Custom binary (.echtvar) | PostgreSQL tables |
| Annotation speed | ~1M variants/sec | ~100K variants/sec |
| Query flexibility | Filter expressions only | Full SQL |
| Multiple sources | Requires separate files | Single database |
| Integration | Standalone tool | SQL ecosystem |

### When to use each

**Use echtvar when:**
- Maximum annotation speed is critical
- Working with VCF pipelines end-to-end
- Memory/storage efficiency is important

**Use vcf-pg-loader when:**
- You need complex SQL queries
- Integrating with existing PostgreSQL infrastructure
- Combining annotation with other database operations
- Building dashboards or APIs on variant data

## Attribution

The annotation system design and filter expression syntax are inspired by
[echtvar](https://github.com/brentp/echtvar) by Brent Pedersen, licensed under MIT.

Test patterns in `tests/vendored/echtvar/` are derived from echtvar's test suite.
See `tests/vendored/echtvar/ATTRIBUTION.md` for full attribution.
