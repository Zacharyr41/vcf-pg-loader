# Genotypes Tables

Tables for storing individual-level genotype data with support for imputation dosages.

## genotypes

Per-sample genotype calls with imputation quality metrics. Hash-partitioned by `sample_id` for parallel query execution.

### Schema

```sql
CREATE TABLE genotypes (
    variant_id BIGINT NOT NULL,
    sample_id INTEGER NOT NULL REFERENCES samples(sample_id),
    gt VARCHAR(20) NOT NULL,
    phased BOOLEAN DEFAULT FALSE,
    gq SMALLINT,
    dp INTEGER,
    ad INTEGER[],
    dosage FLOAT,
    gp FLOAT[],
    allele_balance REAL,
    passes_adj BOOLEAN GENERATED ALWAYS AS (
        COALESCE(gq >= 20, TRUE) AND
        COALESCE(dp >= 10, TRUE) AND
        (gt NOT IN ('0/1', '0|1', '1|0') OR COALESCE(allele_balance >= 0.2, TRUE))
    ) STORED,
    PRIMARY KEY (variant_id, sample_id),
    CONSTRAINT valid_dosage CHECK (dosage IS NULL OR (dosage >= 0 AND dosage <= 2))
) PARTITION BY HASH (sample_id);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `variant_id` | BIGINT | FK to variants table |
| `sample_id` | INTEGER | FK to samples table |
| `gt` | VARCHAR(20) | Genotype call (e.g., "0/1", "1\|1") |
| `phased` | BOOLEAN | TRUE if genotype is phased |
| `gq` | SMALLINT | Genotype quality (Phred-scaled) |
| `dp` | INTEGER | Read depth at position |
| `ad` | INTEGER[] | Allelic depths [REF, ALT] |
| `dosage` | FLOAT | ALT allele dosage (0-2 scale) |
| `gp` | FLOAT[] | Genotype probabilities [P(0/0), P(0/1), P(1/1)] |
| `allele_balance` | REAL | ALT reads / total reads for hets |
| `passes_adj` | BOOLEAN | Generated column for GATK-style ADJ filter |

### Generated Column: passes_adj

The `passes_adj` column implements GATK-style genotype filtering:

```sql
passes_adj = (
    GQ >= 20 AND
    DP >= 10 AND
    (not heterozygous OR allele_balance >= 0.2)
)
```

This allows efficient filtering without repeated computation:

```sql
SELECT * FROM genotypes WHERE passes_adj = TRUE;
```

### Hash Partitioning

The table uses 16 hash partitions for parallel query execution:

```sql
CREATE TABLE genotypes_p0 PARTITION OF genotypes
    FOR VALUES WITH (MODULUS 16, REMAINDER 0);
-- ... through genotypes_p15
```

Benefits:
- Parallel query execution across partitions
- Even data distribution regardless of sample count
- Efficient per-sample lookups

### Indexes

```sql
CREATE INDEX idx_genotypes_adj ON genotypes(variant_id)
    WHERE passes_adj = TRUE;

CREATE INDEX idx_genotypes_dosage ON genotypes(variant_id, dosage)
    WHERE dosage IS NOT NULL;

CREATE INDEX idx_genotypes_sample ON genotypes(sample_id);

CREATE INDEX idx_genotypes_variant ON genotypes(variant_id);
```

### Usage Examples

#### Calculate allele frequency from dosages

```sql
SELECT
    v.variant_id,
    v.rs_id,
    af_from_dosages(ARRAY_AGG(g.dosage)) as af_dosage
FROM variants v
JOIN genotypes g ON v.variant_id = g.variant_id
WHERE g.dosage IS NOT NULL
GROUP BY v.variant_id, v.rs_id;
```

#### Get high-quality genotypes

```sql
SELECT
    g.sample_id,
    g.gt,
    g.gq,
    g.dp
FROM genotypes g
WHERE g.variant_id = 12345
    AND g.passes_adj = TRUE;
```

#### Calculate sample-level statistics

```sql
SELECT
    sample_id,
    COUNT(*) as n_calls,
    COUNT(*) FILTER (WHERE gt LIKE '%1%') as n_variant,
    AVG(gq) as mean_gq,
    AVG(dp) as mean_dp
FROM genotypes
GROUP BY sample_id;
```

#### PRS calculation with dosages

```sql
SELECT
    g.sample_id,
    SUM(w.effect_weight * g.dosage) as prs
FROM genotypes g
JOIN prs_weights w ON g.variant_id = w.variant_id
WHERE w.pgs_id = 'PGS000018'
    AND g.dosage IS NOT NULL
GROUP BY g.sample_id;
```

## Dosage vs Hard Calls

For PRS calculation, dosages are preferred over hard calls:

| Method | Formula | When to Use |
|--------|---------|-------------|
| Hard call | count ALT alleles (0, 1, or 2) | High-quality sequencing |
| Dosage | expected ALT count [0-2] | Imputed data |

Dosages preserve uncertainty from imputation:
- `dosage = 0.95` means ~95% confident heterozygous
- More accurate than rounding to `1`

### Dosage from Genotype Probabilities

If `GP` is available but `dosage` is not:

```sql
UPDATE genotypes
SET dosage = gp[2] + 2 * gp[3]
WHERE dosage IS NULL AND gp IS NOT NULL;
```

## Imputation Quality

Imputed genotypes should be filtered by INFO score (from variants table):

```sql
SELECT g.*
FROM genotypes g
JOIN variants v ON g.variant_id = v.variant_id
WHERE v.info_score >= 0.8;
```

## Related Tables

- [variants](./index.md) - Variant definitions
- [samples](./index.md) - Sample metadata
- [sample_qc](./qc-tables.md) - Per-sample QC metrics
- [prs_weights](./prs-tables.md) - PRS effect weights
