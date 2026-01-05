# Architecture: VCF-to-PostgreSQL Loader

This document describes the internal architecture of the vcf-pg-loader system, covering the key components, data flow, and design decisions.

---

## Overview

The loader is designed for high-throughput ingestion of VCF files into PostgreSQL, purpose-built for **Polygenic Risk Score (PRS) research workflows**. Key design goals:

1. **Memory efficiency** - Stream processing without loading entire files
2. **Correctness** - Proper handling of VCF edge cases (multi-allelics, Number=A/R/G fields)
3. **Performance** - Binary COPY protocol, index management, batch processing
4. **Auditability** - Complete load tracking with validation support
5. **PRS-optimized** - Schema and workflows designed for polygenic risk score analysis

---

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              CLI Layer                                   │
│  cli.py: Typer-based commands (load, validate, init-db)                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            Loader Layer                                  │
│  loader.py: VCFLoader orchestrates the loading pipeline                 │
│  - Connection pool management                                            │
│  - Batch coordination                                                    │
│  - Audit trail management                                                │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
┌───────────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐
│   VCF Parsing Layer   │ │  Schema Layer   │ │   Data Models Layer     │
│                       │ │                 │ │                         │
│  vcf_parser.py:       │ │  schema.py:     │ │  models.py:             │
│  - VCFHeaderParser    │ │  - SchemaManager│ │  - VariantRecord        │
│  - VCFStreamingParser │ │  - Table DDL    │ │  - LoadConfig           │
│  - VariantParser      │ │  - Index mgmt   │ │                         │
└───────────────────────┘ └─────────────────┘ └─────────────────────────┘
```

---

## Key Components

### 1. VCFHeaderParser (`vcf_parser.py`)

Parses VCF header metadata using cyvcf2's native API.

**Responsibilities:**
- Extract INFO field definitions (ID, Number, Type, Description)
- Extract FORMAT field definitions
- Parse sample names
- Parse contig information
- Extract VEP CSQ field structure if present

**Key Methods:**
```python
parser = VCFHeaderParser()
parser.parse_from_vcf(vcf)           # Parse all header info
parser.get_info_field("AC")          # Get metadata for INFO field
parser.get_format_field("GT")        # Get metadata for FORMAT field
parser.samples                       # List of sample names
parser.contigs                       # Dict of contig info
```

**Why cyvcf2 integration matters:**
The original implementation parsed header lines as raw strings. Using cyvcf2's `header_iter()` and `get_header_type()` provides:
- Consistent parsing across VCF versions
- Proper handling of quoted descriptions
- Direct access to field metadata without regex

---

### 2. VCFStreamingParser (`vcf_parser.py`)

Memory-efficient iterator that streams through VCF files yielding batches.

**Responsibilities:**
- Open VCF file via cyvcf2
- Parse header on initialization
- Yield batches of `VariantRecord` objects
- Track variant and record counts
- Manage file handle lifecycle

**Key Methods:**
```python
parser = VCFStreamingParser("sample.vcf.gz", batch_size=10000)

for batch in parser.iter_batches():
    # batch is List[VariantRecord]
    process(batch)

print(f"Variants: {parser.variant_count}")
print(f"Records: {parser.record_count}")  # Higher due to multi-allelic decomposition
```

**Design Decision: Batch Size**
Default batch size of 10,000 records balances:
- Memory usage (each record ~1KB)
- COPY efficiency (larger batches = fewer round trips)
- Progress granularity (users see updates)

---

### 3. VariantParser (`vcf_parser.py`)

Handles per-variant parsing with Number=A/R/G field extraction.

**The Multi-Allelic Problem:**

VCF allows multiple ALT alleles per record:
```
chr1  100  .  A  G,T  .  PASS  AC=10,5;AF=0.1,0.05
```

When decomposed into separate database rows, each row needs its own values:
- Row 1 (A→G): AC=10, AF=0.1
- Row 2 (A→T): AC=5, AF=0.05

**Number Specifications:**
| Number | Meaning | Example | Extraction |
|--------|---------|---------|------------|
| `A` | One per ALT allele | AC, AF | `value[alt_idx]` |
| `R` | One per allele (REF + ALTs) | AD | `[value[0], value[alt_idx+1]]` |
| `G` | One per genotype | PL | Extract 3 values for biallelic |
| `1` | Single value | DP | Pass through unchanged |
| `.` | Variable | CSQ | Pass through unchanged |

**Implementation:**
```python
def _extract_number_a(self, value, alt_idx, n_alts):
    """Extract value for this ALT from a Number=A field."""
    if isinstance(value, (list, tuple)):
        return value[alt_idx] if alt_idx < len(value) else None
    return value

def _extract_number_r(self, value, alt_idx, n_alts):
    """Extract REF + this ALT values from a Number=R field."""
    if isinstance(value, (list, tuple)) and len(value) >= n_alts + 1:
        return [value[0], value[alt_idx + 1]]
    return value
```

---

### 4. VCFLoader (`loader.py`)

Orchestrates the complete loading pipeline.

**Responsibilities:**
- Manage asyncpg connection pool
- Coordinate streaming parser and batch insertion
- Handle index drop/recreate for performance
- Maintain audit trail

**Binary COPY Protocol:**

Uses asyncpg's `copy_records_to_table()` for maximum performance:
```python
await conn.copy_records_to_table(
    "variants",
    records=records,  # List of tuples
    columns=[...],
)
```

This bypasses SQL parsing and uses PostgreSQL's binary format, achieving 100K+ rows/second on typical hardware.

**Audit Trail:**

Every load creates an audit record:
```sql
INSERT INTO variant_load_audit (
    load_batch_id,      -- UUID for this load
    vcf_file_path,      -- Source file
    vcf_file_md5,       -- Checksum for verification
    samples_count,      -- Number of samples
    status              -- started/completed/failed
)
```

---

### 5. SchemaManager (`schema.py`)

Manages PostgreSQL schema lifecycle.

**Table Design:**

The `variants` table is partitioned by chromosome:
```sql
CREATE TABLE variants (
    variant_id BIGINT GENERATED ALWAYS AS IDENTITY,
    chrom chromosome_type NOT NULL,
    pos_range int8range NOT NULL,  -- For range queries
    pos BIGINT NOT NULL,
    ...
) PARTITION BY LIST (chrom);
```

**Why Partitioning:**
- Queries typically filter by chromosome
- Partition pruning eliminates scanning irrelevant data
- Parallel index creation per partition
- Easier maintenance (VACUUM per partition)

**Index Strategy:**
```sql
-- Region queries (GiST on range type)
CREATE INDEX idx_variants_region ON variants USING GiST (chrom, pos_range);

-- Gene lookup with covering columns
CREATE INDEX idx_variants_gene ON variants (gene)
    INCLUDE (pos, ref, alt, impact_severity);

-- rsID lookup (hash for equality only)
CREATE INDEX idx_variants_rsid ON variants USING HASH (rs_id);
```

---

## Data Flow

### Load Operation

```
1. CLI invokes VCFLoader.load_vcf(path)
2. VCFLoader creates VCFStreamingParser
3. VCFStreamingParser parses header → VCFHeaderParser
4. For each variant in VCF:
   a. VariantParser.parse_variant() decomposes multi-allelics
   b. Number=A/R/G fields extracted per-ALT
   c. Records added to batch buffer
5. When batch full:
   a. VCFLoader.copy_batch() sends to PostgreSQL
   b. asyncpg uses binary COPY protocol
6. After all batches:
   a. Indexes recreated (if dropped)
   b. Audit record updated to 'completed'
```

### Validate Operation

```
1. CLI invokes validation with load_batch_id
2. Query variant_load_audit for expected count
3. Query COUNT(*) FROM variants WHERE load_batch_id = ?
4. Query for duplicates (GROUP BY chrom, pos, ref, alt)
5. Report pass/fail
```

---

## Testing Strategy

### Unit Tests
- `test_vcf_parser.py` - Header parsing, array sizing
- `test_vcf_header_cyvcf2.py` - cyvcf2 integration
- `test_number_arg_extraction.py` - Number=A/R/G extraction
- `test_streaming_parser.py` - Batch iteration
- `test_cli.py` - CLI commands (mocked)

### Integration Tests
- `test_schema.py` - PostgreSQL schema creation (testcontainers)
- `test_loader.py` - Full load pipeline (testcontainers)

### Test Data
Test VCF fixtures sourced from [slivar](https://github.com/brentp/slivar):
- `with_annotations.vcf` - BCSQ annotations, multiple samples
- `multiallelic.vcf` - Multi-allelic variants for decomposition testing

---

## Performance Considerations

### Memory
- Streaming parser: Only one batch in memory at a time
- Default 10K records/batch ≈ 10MB working set

### Database
- Binary COPY: 10-100x faster than INSERT
- Index drop/recreate: Faster than incremental updates during bulk load
- Connection pool: Reuse connections, avoid handshake overhead

### Future Optimizations
- Parallel chromosome loading (worker pool)
- Compressed VCF streaming (already supported by cyvcf2)
- Unlogged tables during load (with conversion after)

---

## PRS Research Components

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CLI Layer (PRS Commands)                         │
│  import-gwas, import-pgs, load-reference, compute-sample-qc, export-*   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│   GWAS Module       │  │   PRS Module        │  │   Reference Module  │
│                     │  │                     │  │                     │
│  gwas/loader.py     │  │  prs/loader.py      │  │  references/*.py    │
│  gwas/schema.py     │  │  prs/schema.py      │  │  - hapmap3.py       │
│  gwas/models.py     │  │  prs/pgs_catalog.py │  │  - ld_blocks.py     │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
          │                         │                         │
          └─────────────────────────┼─────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         QC & Validation Layer                            │
│  qc/variant_qc.py, qc/sample_qc.py, validation/sql_functions.py         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Export & Views Layer                                │
│  export/prs_formats.py, views/prs_views.py                              │
└─────────────────────────────────────────────────────────────────────────┘
```

### GWAS Module (`gwas/`)

Handles GWAS summary statistics following the GWAS-SSF standard.

**Components:**
- `schema.py` - Creates `studies` and `gwas_summary_stats` tables
- `loader.py` - Parses and loads summary statistics files
- `models.py` - Data models for GWAS records

**Supported Formats:**
- Tab-separated values with standard column headers
- Automatic column mapping (BETA/OR, CHROM/CHR, etc.)
- P-value and effect size validation

### PRS Module (`prs/`)

Manages PGS Catalog scoring files.

**Components:**
- `schema.py` - Creates `pgs_scores` and `prs_weights` tables
- `loader.py` - Loads PGS Catalog scoring files
- `pgs_catalog.py` - PGS Catalog file format parser
- `models.py` - PRS weight data models

**Variant Matching:**
1. First pass: Match by chromosome + position + alleles
2. Unmatched variants stored with `variant_id = NULL`
3. Statistics reported: loaded count, matched count, match rate

### Reference Module (`references/`)

Manages reference panels and LD block definitions.

**Components:**
- `hapmap3.py` - HapMap3 SNP set loader
- `ld_blocks.py` - Berisa & Pickrell LD block loader
- `schema.py` - Creates `reference_panels` and `ld_blocks` tables

**HapMap3 Integration:**
- ~1.1M variants commonly used for PRS methods
- Adds `in_hapmap3` boolean column to variants table
- Required for PRS-CS, LDpred2, and similar methods

### QC Module (`qc/`)

Computes quality control metrics at load time and on-demand.

**Variant QC (`variant_qc.py`):**
- Genotype counts (n_het, n_hom_ref, n_hom_alt)
- Allele frequencies (AAF, MAF, MAC)
- Hardy-Weinberg equilibrium exact test

**Sample QC (`sample_qc.py`):**
- Call rate
- Het/hom ratio
- Ti/Tv ratio
- Sex inference from X chromosome
- F inbreeding coefficient

### Genotypes Module (`genotypes/`)

Stores individual-level genotype data with imputation support.

**Key Features:**
- Hash-partitioned by `sample_id` (16 partitions)
- Dosage and GP (genotype probability) support
- Generated `passes_adj` column for GATK-style filtering
- Efficient PRS calculation via dosage-weighted sums

### Views Module (`views/`)

Pre-computed materialized views for common query patterns.

**Views:**
- `prs_candidate_variants` - HapMap3 + QC filters
- `variant_qc_summary` - Aggregate statistics
- `chromosome_variant_counts` - Per-chromosome breakdown

**Concurrent Refresh:**
```python
await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY prs_candidate_variants")
```

### Export Module (`export/`)

Exports data to downstream PRS tools.

**Supported Formats:**
| Format | Tool | Columns |
|--------|------|---------|
| PLINK score | PLINK 2.0 | SNP, A1, BETA |
| PRS-CS | PRS-CS | SNP, A1, A2, BETA, SE/P |
| LDpred2 | bigsnpr | chr, pos, a0, a1, beta, beta_se, n_eff |
| PRSice-2 | PRSice-2 | SNP, A1, A2, BETA, SE, P |

---

## PRS Data Flows

### GWAS Import Flow

```
1. CLI: vcf-pg-loader import-gwas sumstats.tsv --study-id GCST...
2. GWASLoader parses header, detects column mapping
3. Study metadata inserted into `studies` table
4. For each row:
   a. Parse effect allele, beta/OR, SE, p-value
   b. Match to variants by chrom + pos + alleles
   c. Insert into `gwas_summary_stats`
5. Create indexes on p-value, study_id
```

### PGS Catalog Import Flow

```
1. CLI: vcf-pg-loader import-pgs PGS000001.txt
2. PRSLoader parses PGS Catalog header (pgs_id, trait, build)
3. Score metadata inserted into `pgs_scores` table
4. For each weight:
   a. Parse effect allele, weight, position
   b. Match to variants table
   c. Insert into `prs_weights`
5. Report: loaded/matched/total variants
```

### PRS Calculation Flow

```sql
SELECT
    g.sample_id,
    SUM(w.effect_weight * g.dosage) as prs_raw
FROM genotypes g
JOIN prs_weights w ON g.variant_id = w.variant_id
JOIN prs_candidate_variants c ON g.variant_id = c.variant_id
WHERE w.pgs_id = 'PGS000018'
GROUP BY g.sample_id;
```

### Export to PRS Tools Flow

```
1. CLI: vcf-pg-loader export-prs-cs --study-id 1 --output gwas.txt
2. Query gwas_summary_stats joined with variants
3. Apply filters (HapMap3, MAF, INFO)
4. Format output per tool specification
5. Write to file with appropriate headers
```

---

## SQL Functions

Custom PostgreSQL functions for in-database computation:

### HWE Exact Test

```sql
SELECT hwe_exact_test(100, 50, 10) as hwe_p;
-- Returns: 0.023 (two-sided p-value)
```

Implementation follows Wigginton et al. (2005).

### Allele Frequency from Dosages

```sql
SELECT af_from_dosages(ARRAY[0.0, 1.0, 1.5, 2.0]) as af;
-- Returns: 0.5625
```

### Effective Sample Size

```sql
SELECT n_eff(10000, 50000) as n_eff;
-- Returns: 16666.67
```

Used for case-control studies in PRS methods.

### Allele Harmonization

```sql
SELECT alleles_match('A', 'G', 'T', 'C') as matches;
-- Returns: TRUE (strand flip: A=T, G=C)
```

Handles direct match, allele swap, and strand flip.
