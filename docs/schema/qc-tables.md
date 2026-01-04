# QC Tables

Tables for storing quality control metrics at the sample and variant level.

## sample_qc

Per-sample quality control metrics computed from genotype data.

### Schema

```sql
CREATE TABLE sample_qc (
    sample_id VARCHAR(100) PRIMARY KEY,
    call_rate FLOAT,
    n_called INTEGER,
    n_snp INTEGER,
    n_het INTEGER,
    n_hom_var INTEGER,
    het_hom_ratio FLOAT,
    ti_tv_ratio FLOAT,
    n_singleton INTEGER,
    f_inbreeding FLOAT,
    mean_dp FLOAT,
    mean_gq FLOAT,
    sex_inferred VARCHAR(10),
    sex_reported VARCHAR(10),
    sex_concordant BOOLEAN,
    contamination_estimate FLOAT,
    batch_id INTEGER,
    qc_pass BOOLEAN GENERATED ALWAYS AS (
        call_rate >= 0.99 AND
        COALESCE(contamination_estimate < 0.025, TRUE) AND
        COALESCE(sex_concordant, TRUE)
    ) STORED,
    computed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_call_rate CHECK (call_rate >= 0 AND call_rate <= 1),
    CONSTRAINT valid_contamination CHECK (
        contamination_estimate IS NULL OR
        (contamination_estimate >= 0 AND contamination_estimate <= 1)
    )
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `sample_id` | VARCHAR(100) | Sample identifier |
| `call_rate` | FLOAT | Fraction of variants with genotype call |
| `n_called` | INTEGER | Number of variants with calls |
| `n_snp` | INTEGER | Number of SNP calls |
| `n_het` | INTEGER | Number of heterozygous calls |
| `n_hom_var` | INTEGER | Number of homozygous variant calls |
| `het_hom_ratio` | FLOAT | n_het / n_hom_var |
| `ti_tv_ratio` | FLOAT | Transitions / transversions |
| `n_singleton` | INTEGER | Variants where sample is only carrier |
| `f_inbreeding` | FLOAT | Inbreeding coefficient |
| `mean_dp` | FLOAT | Mean read depth |
| `mean_gq` | FLOAT | Mean genotype quality |
| `sex_inferred` | VARCHAR(10) | Sex inferred from X het rate |
| `sex_reported` | VARCHAR(10) | Reported sex |
| `sex_concordant` | BOOLEAN | Inferred matches reported |
| `contamination_estimate` | FLOAT | Estimated contamination fraction |
| `batch_id` | INTEGER | Load batch identifier |
| `qc_pass` | BOOLEAN | Generated: passes all QC criteria |
| `computed_at` | TIMESTAMPTZ | When metrics were computed |

### Generated Column: qc_pass

Implements standard QC thresholds:

```sql
qc_pass = (
    call_rate >= 0.99 AND
    contamination_estimate < 0.025 AND
    sex_concordant = TRUE
)
```

### QC Thresholds

Based on common practices (e.g., gnomAD, UK Biobank):

| Metric | Threshold | Rationale |
|--------|-----------|-----------|
| Call rate | >= 99% | Sample quality |
| Contamination | < 2.5% | Sample swap/mix |
| Sex concordance | TRUE | Sample swap |
| F coefficient | -0.2 to 0.2 | Contamination/inbreeding |

### Indexes

```sql
CREATE INDEX idx_sample_qc_pass ON sample_qc(qc_pass);
CREATE INDEX idx_sample_qc_batch ON sample_qc(batch_id);
CREATE INDEX idx_sample_qc_call_rate ON sample_qc(call_rate);
CREATE INDEX idx_sample_qc_batch_pass ON sample_qc(batch_id, qc_pass);
```

### Usage Examples

#### Get failing samples

```sql
SELECT sample_id, call_rate, contamination_estimate, sex_concordant
FROM sample_qc
WHERE qc_pass = FALSE
ORDER BY call_rate ASC;
```

#### QC summary statistics

```sql
SELECT
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE qc_pass) as passing,
    ROUND(100.0 * COUNT(*) FILTER (WHERE qc_pass) / COUNT(*), 1) as pass_rate,
    AVG(call_rate) as mean_call_rate,
    AVG(het_hom_ratio) as mean_het_hom,
    AVG(ti_tv_ratio) as mean_ti_tv
FROM sample_qc;
```

## sample_qc_summary

Materialized view for batch-level QC aggregation.

### Schema

```sql
CREATE MATERIALIZED VIEW sample_qc_summary AS
SELECT
    batch_id,
    COUNT(*) as n_samples,
    COUNT(*) FILTER (WHERE qc_pass) as n_pass,
    COUNT(*) FILTER (WHERE NOT qc_pass) as n_fail,
    AVG(call_rate) as mean_call_rate,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY call_rate) as median_call_rate,
    MIN(call_rate) as min_call_rate,
    MAX(call_rate) as max_call_rate,
    AVG(het_hom_ratio) as mean_het_hom_ratio,
    AVG(ti_tv_ratio) as mean_ti_tv_ratio,
    AVG(f_inbreeding) as mean_f_inbreeding,
    COUNT(*) FILTER (WHERE sex_concordant = FALSE) as n_sex_discordant,
    COUNT(*) FILTER (WHERE contamination_estimate >= 0.025) as n_contaminated
FROM sample_qc
GROUP BY batch_id;
```

## population_frequencies

Multi-ancestry allele frequencies from external sources (gnomAD, 1000 Genomes).

### Schema

```sql
CREATE TABLE population_frequencies (
    id SERIAL PRIMARY KEY,
    variant_id BIGINT,
    source VARCHAR(20) NOT NULL,
    population VARCHAR(10) NOT NULL,
    subset VARCHAR(20) DEFAULT 'all',
    ac INTEGER,
    an INTEGER,
    af DOUBLE PRECISION,
    hom_count INTEGER,
    faf_95 DOUBLE PRECISION,
    UNIQUE (variant_id, source, population, subset)
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Auto-incrementing primary key |
| `variant_id` | BIGINT | FK to variants table |
| `source` | VARCHAR(20) | Frequency source (gnomAD_v3, 1KG_phase3) |
| `population` | VARCHAR(10) | Population code (NFE, AFR, EAS, etc.) |
| `subset` | VARCHAR(20) | Data subset (all, controls, non_neuro) |
| `ac` | INTEGER | Allele count |
| `an` | INTEGER | Allele number (total chromosomes) |
| `af` | DOUBLE PRECISION | Allele frequency |
| `hom_count` | INTEGER | Homozygous individual count |
| `faf_95` | DOUBLE PRECISION | Filtering allele frequency (95% CI) |

### Population Codes

| Code | Population |
|------|------------|
| NFE | Non-Finnish European |
| FIN | Finnish |
| AFR | African/African American |
| AMR | Latino/Admixed American |
| EAS | East Asian |
| SAS | South Asian |
| ASJ | Ashkenazi Jewish |
| OTH | Other |

### Indexes

```sql
CREATE INDEX idx_popfreq_lookup ON population_frequencies(variant_id, population);
CREATE INDEX idx_popfreq_af ON population_frequencies(af);
CREATE INDEX idx_popfreq_source ON population_frequencies(source);
CREATE INDEX idx_popfreq_rare ON population_frequencies(population, af)
    WHERE af < 0.01;
```

### Usage Examples

#### Get population-specific frequencies

```sql
SELECT
    v.rs_id,
    p.population,
    p.af
FROM variants v
JOIN population_frequencies p ON v.variant_id = p.variant_id
WHERE v.rs_id = 'rs12345'
    AND p.source = 'gnomAD_v3'
ORDER BY p.population;
```

#### Find population-specific rare variants

```sql
SELECT
    v.variant_id,
    v.rs_id,
    MAX(p.af) FILTER (WHERE p.population = 'NFE') as af_nfe,
    MAX(p.af) FILTER (WHERE p.population = 'AFR') as af_afr
FROM variants v
JOIN population_frequencies p ON v.variant_id = p.variant_id
WHERE p.source = 'gnomAD_v3'
GROUP BY v.variant_id, v.rs_id
HAVING MAX(p.af) FILTER (WHERE p.population = 'NFE') < 0.01
    AND MAX(p.af) FILTER (WHERE p.population = 'AFR') >= 0.05;
```

## Variant QC Metrics

Core variant QC metrics are stored directly in the `variants` table:

| Column | Description |
|--------|-------------|
| `info_score` | Imputation quality (INFO/R2) |
| `call_rate` | Fraction of samples with calls |
| `hwe_p` | Hardy-Weinberg equilibrium p-value |
| `aaf` | Alternate allele frequency |
| `maf` | Minor allele frequency |
| `mac` | Minor allele count |

### HWE Exact Test

The schema includes a SQL function for HWE calculation:

```sql
SELECT hwe_exact_test(n_hom_ref, n_het, n_hom_alt) as hwe_p
FROM variant_genotype_counts;
```

## CLI Commands

### Compute sample QC

```bash
vcf-pg-loader compute-sample-qc --db postgresql://localhost/prs_db
```

### Import population frequencies

```bash
vcf-pg-loader import-frequencies gnomad_v3.vcf.gz \
    --source gnomAD_v3 \
    --db postgresql://localhost/prs_db
```

## Related Tables

- [genotypes](./genotypes-tables.md) - Source data for QC computation
- [variants](./index.md) - Variant-level QC metrics
- [prs_candidate_variants](./views.md) - Pre-filtered by QC criteria
