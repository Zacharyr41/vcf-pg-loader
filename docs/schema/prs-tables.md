# PRS Tables

Tables for storing Polygenic Risk Score weights from the [PGS Catalog](https://www.pgscatalog.org/).

## pgs_scores

Metadata table for PGS Catalog scoring files.

### Schema

```sql
CREATE TABLE pgs_scores (
    pgs_id VARCHAR(20) PRIMARY KEY,
    trait_name TEXT,
    trait_ontology_id VARCHAR(50),
    publication_pmid VARCHAR(20),
    n_variants INTEGER,
    genome_build VARCHAR(10),
    weight_type VARCHAR(20),
    reporting_ancestry TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `pgs_id` | VARCHAR(20) | PGS Catalog accession (e.g., PGS000001) |
| `trait_name` | TEXT | Human-readable trait name |
| `trait_ontology_id` | VARCHAR(50) | EFO/MONDO ontology term |
| `publication_pmid` | VARCHAR(20) | PubMed ID of source publication |
| `n_variants` | INTEGER | Number of variants in scoring file |
| `genome_build` | VARCHAR(10) | Reference genome (GRCh37/GRCh38) |
| `weight_type` | VARCHAR(20) | Type of weights (beta, OR, etc.) |
| `reporting_ancestry` | TEXT | Ancestry of discovery GWAS |
| `created_at` | TIMESTAMPTZ | Record creation timestamp |

### Example Data

| pgs_id | trait_name | n_variants | genome_build |
|--------|------------|------------|--------------|
| PGS000001 | Breast Cancer | 77 | GRCh37 |
| PGS000018 | Type 2 Diabetes | 6917 | GRCh38 |
| PGS000337 | Coronary Artery Disease | 1745268 | GRCh38 |

## prs_weights

Per-variant effect weights from PGS Catalog scoring files.

### Schema

```sql
CREATE TABLE prs_weights (
    id SERIAL PRIMARY KEY,
    variant_id BIGINT,
    pgs_id VARCHAR(20) REFERENCES pgs_scores(pgs_id),
    effect_allele VARCHAR(255) NOT NULL,
    effect_weight DOUBLE PRECISION NOT NULL,
    is_interaction BOOLEAN DEFAULT FALSE,
    is_haplotype BOOLEAN DEFAULT FALSE,
    is_dominant BOOLEAN DEFAULT FALSE,
    is_recessive BOOLEAN DEFAULT FALSE,
    allele_frequency DOUBLE PRECISION,
    locus_name VARCHAR(100),
    chr_name VARCHAR(10),
    chr_position BIGINT,
    rsid VARCHAR(20),
    other_allele VARCHAR(255),
    UNIQUE (variant_id, pgs_id)
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Auto-incrementing primary key |
| `variant_id` | BIGINT | FK to variants table (NULL if unmatched) |
| `pgs_id` | VARCHAR(20) | FK to pgs_scores table |
| `effect_allele` | VARCHAR(255) | Allele associated with effect |
| `effect_weight` | DOUBLE PRECISION | Effect size (beta or log(OR)) |
| `is_interaction` | BOOLEAN | Part of gene-gene interaction |
| `is_haplotype` | BOOLEAN | Haplotype-based weight |
| `is_dominant` | BOOLEAN | Dominant inheritance model |
| `is_recessive` | BOOLEAN | Recessive inheritance model |
| `allele_frequency` | DOUBLE PRECISION | Effect allele frequency in discovery |
| `locus_name` | VARCHAR(100) | Gene or locus identifier |
| `chr_name` | VARCHAR(10) | Chromosome (for unmatched variants) |
| `chr_position` | BIGINT | Position (for unmatched variants) |
| `rsid` | VARCHAR(20) | dbSNP rsID |
| `other_allele` | VARCHAR(255) | Non-effect allele |

### Indexes

```sql
CREATE INDEX idx_prs_pgsid ON prs_weights(pgs_id);

CREATE INDEX idx_prs_variant_id ON prs_weights(variant_id)
    WHERE variant_id IS NOT NULL;

CREATE INDEX idx_prs_position ON prs_weights(chr_name, chr_position)
    WHERE chr_name IS NOT NULL AND chr_position IS NOT NULL;
```

### Usage Examples

#### List all loaded PGS scores with match rates

```sql
SELECT
    s.pgs_id,
    s.trait_name,
    s.n_variants as expected,
    COUNT(w.id) as loaded,
    COUNT(w.variant_id) as matched,
    ROUND(100.0 * COUNT(w.variant_id) / NULLIF(s.n_variants, 0), 1) as match_pct
FROM pgs_scores s
LEFT JOIN prs_weights w ON s.pgs_id = w.pgs_id
GROUP BY s.pgs_id, s.trait_name, s.n_variants
ORDER BY s.pgs_id;
```

#### Calculate PRS for a sample

```sql
SELECT
    g.sample_id,
    w.pgs_id,
    SUM(w.effect_weight * g.dosage) as prs_raw
FROM genotypes g
JOIN prs_weights w ON g.variant_id = w.variant_id
WHERE w.pgs_id = 'PGS000018'
GROUP BY g.sample_id, w.pgs_id;
```

#### Find unmatched variants for a PGS score

```sql
SELECT chr_name, chr_position, rsid, effect_allele, other_allele
FROM prs_weights
WHERE pgs_id = 'PGS000001'
    AND variant_id IS NULL
ORDER BY chr_name, chr_position;
```

## CLI Commands

### Import PGS Catalog scoring file

```bash
vcf-pg-loader import-pgs PGS000001_hmPOS_GRCh38.txt \
    --db postgresql://localhost/prs_db
```

### List loaded PGS scores

```bash
vcf-pg-loader list-pgs --db postgresql://localhost/prs_db
```

## Related Tables

- [variants](./index.md) - Linked via `variant_id`
- [gwas_summary_stats](./gwas-tables.md) - GWAS results for the same traits
- [reference_panels](./reference-tables.md) - HapMap3 for filtering
