# GWAS Tables

Tables for storing GWAS summary statistics following the [GWAS-SSF](https://www.ebi.ac.uk/gwas/docs/methods/summary-statistics) standard format.

## studies

Metadata table for GWAS studies.

### Schema

```sql
CREATE TABLE studies (
    study_id SERIAL PRIMARY KEY,
    study_accession VARCHAR(50) UNIQUE,
    trait_name TEXT,
    trait_ontology_id VARCHAR(50),
    publication_pmid VARCHAR(20),
    sample_size INTEGER,
    n_cases INTEGER,
    n_controls INTEGER,
    genome_build VARCHAR(10),
    analysis_software TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `study_id` | SERIAL | Auto-incrementing primary key |
| `study_accession` | VARCHAR(50) | GWAS Catalog accession (e.g., GCST90012345) |
| `trait_name` | TEXT | Human-readable trait name |
| `trait_ontology_id` | VARCHAR(50) | EFO ontology term |
| `publication_pmid` | VARCHAR(20) | PubMed ID of publication |
| `sample_size` | INTEGER | Total sample size |
| `n_cases` | INTEGER | Number of cases (case-control only) |
| `n_controls` | INTEGER | Number of controls (case-control only) |
| `genome_build` | VARCHAR(10) | Reference genome (GRCh37/GRCh38) |
| `analysis_software` | TEXT | Software used for association testing |
| `created_at` | TIMESTAMPTZ | Record creation timestamp |

### Example Data

| study_id | study_accession | trait_name | sample_size | n_cases | n_controls |
|----------|-----------------|------------|-------------|---------|------------|
| 1 | GCST90012345 | Type 2 Diabetes | 898130 | 74124 | 824006 |
| 2 | GCST90002357 | Height | 253288 | NULL | NULL |

## gwas_summary_stats

Per-variant GWAS association results following GWAS-SSF standard.

### Schema

```sql
CREATE TABLE gwas_summary_stats (
    id SERIAL PRIMARY KEY,
    variant_id BIGINT,
    study_id INTEGER REFERENCES studies(study_id),
    effect_allele VARCHAR(255) NOT NULL,
    other_allele VARCHAR(255),
    beta DOUBLE PRECISION,
    odds_ratio DOUBLE PRECISION,
    standard_error DOUBLE PRECISION,
    p_value DOUBLE PRECISION NOT NULL,
    effect_allele_frequency DOUBLE PRECISION,
    n_total INTEGER,
    n_cases INTEGER,
    info_score DOUBLE PRECISION,
    is_effect_allele_alt BOOLEAN,
    UNIQUE (variant_id, study_id)
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Auto-incrementing primary key |
| `variant_id` | BIGINT | FK to variants table (NULL if unmatched) |
| `study_id` | INTEGER | FK to studies table |
| `effect_allele` | VARCHAR(255) | Allele tested for association |
| `other_allele` | VARCHAR(255) | Reference allele |
| `beta` | DOUBLE PRECISION | Effect size (quantitative traits) |
| `odds_ratio` | DOUBLE PRECISION | Odds ratio (binary traits) |
| `standard_error` | DOUBLE PRECISION | Standard error of effect estimate |
| `p_value` | DOUBLE PRECISION | Association p-value |
| `effect_allele_frequency` | DOUBLE PRECISION | EAF in study population |
| `n_total` | INTEGER | Per-variant sample size |
| `n_cases` | INTEGER | Per-variant case count |
| `info_score` | DOUBLE PRECISION | Imputation quality score |
| `is_effect_allele_alt` | BOOLEAN | TRUE if effect allele is ALT |

### Indexes

```sql
CREATE INDEX idx_gwas_pvalue ON gwas_summary_stats (p_value)
    WHERE p_value < 5e-8;

CREATE INDEX idx_gwas_study_id ON gwas_summary_stats (study_id);

CREATE INDEX idx_gwas_variant_id ON gwas_summary_stats (variant_id)
    WHERE variant_id IS NOT NULL;

CREATE INDEX idx_gwas_study_pvalue ON gwas_summary_stats (study_id, p_value);
```

### Usage Examples

#### Get genome-wide significant hits

```sql
SELECT
    v.chrom,
    v.pos,
    v.rs_id,
    g.effect_allele,
    g.beta,
    g.p_value
FROM gwas_summary_stats g
JOIN variants v ON g.variant_id = v.variant_id
WHERE g.study_id = 1
    AND g.p_value < 5e-8
ORDER BY g.p_value;
```

#### Calculate effective sample size

```sql
SELECT
    study_accession,
    trait_name,
    sample_size,
    n_cases,
    n_controls,
    n_eff(n_cases, n_controls) as effective_n
FROM studies
WHERE n_cases IS NOT NULL;
```

#### Manhattan plot data

```sql
SELECT
    v.chrom,
    v.pos,
    -LOG10(g.p_value) as neg_log_p
FROM gwas_summary_stats g
JOIN variants v ON g.variant_id = v.variant_id
WHERE g.study_id = 1
ORDER BY v.chrom, v.pos;
```

#### Filter by imputation quality

```sql
SELECT COUNT(*)
FROM gwas_summary_stats
WHERE study_id = 1
    AND info_score >= 0.8
    AND p_value < 5e-8;
```

## GWAS-SSF Standard

The schema follows the [GWAS Summary Statistics Format](https://www.ebi.ac.uk/gwas/docs/methods/summary-statistics) standard:

| GWAS-SSF Field | Column |
|----------------|--------|
| `chromosome` | via `variant_id` |
| `base_pair_location` | via `variant_id` |
| `effect_allele` | `effect_allele` |
| `other_allele` | `other_allele` |
| `beta` | `beta` |
| `odds_ratio` | `odds_ratio` |
| `standard_error` | `standard_error` |
| `p_value` | `p_value` |
| `effect_allele_frequency` | `effect_allele_frequency` |
| `variant_id` (rsID) | via `variant_id` |

## CLI Commands

### Import GWAS summary statistics

```bash
vcf-pg-loader import-gwas sumstats.tsv \
    --study-id GCST90012345 \
    --trait "Type 2 Diabetes" \
    --sample-size 898130 \
    --n-cases 74124 \
    --n-controls 824006 \
    --db postgresql://localhost/prs_db
```

### List loaded studies

```bash
vcf-pg-loader list-studies --db postgresql://localhost/prs_db
```

## Related Tables

- [variants](./index.md) - Linked via `variant_id`
- [prs_weights](./prs-tables.md) - PRS weights often derived from GWAS
- [prs_candidate_variants](./views.md) - Pre-filtered variants with GWAS stats
