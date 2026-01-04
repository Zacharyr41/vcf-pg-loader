# Materialized Views

Pre-computed query results for common PRS access patterns. Use `REFRESH MATERIALIZED VIEW CONCURRENTLY` to update without blocking reads.

## prs_candidate_variants

Pre-filtered variants meeting standard PRS QC criteria. This is the primary view for PRS workflows.

### Definition

```sql
CREATE MATERIALIZED VIEW prs_candidate_variants AS
SELECT
    v.variant_id,
    v.chrom,
    v.pos,
    v.ref,
    v.alt,
    v.rs_id,
    v.info_score,
    v.call_rate,
    v.hwe_p,
    v.maf,
    v.aaf,
    v.in_hapmap3,
    v.ld_block_id,
    v.load_batch_id,
    pf.af AS gnomad_nfe_af,
    ss.beta,
    ss.standard_error,
    ss.p_value
FROM variants v
LEFT JOIN population_frequencies pf
    ON v.variant_id = pf.variant_id
    AND pf.population = 'NFE'
    AND pf.source = 'gnomAD_v3'
LEFT JOIN gwas_summary_stats ss
    ON v.variant_id = ss.variant_id
WHERE v.in_hapmap3 = TRUE
    AND v.info_score >= 0.6
    AND v.call_rate >= 0.98
    AND v.hwe_p > 1e-6
    AND v.maf >= 0.01;
```

### Filter Criteria

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| HapMap3 | TRUE | Required by PRS-CS, LDpred2 |
| INFO score | >= 0.6 | Imputation quality |
| Call rate | >= 98% | Genotyping completeness |
| HWE p-value | > 1e-6 | Population genetics QC |
| MAF | >= 1% | Common variants only |

### Indexes

```sql
CREATE UNIQUE INDEX idx_prs_candidates_pk
    ON prs_candidate_variants(variant_id);

CREATE INDEX idx_prs_candidates_pos
    ON prs_candidate_variants(chrom, pos);
```

### Usage Examples

#### Get PRS-ready variant count

```sql
SELECT COUNT(*) FROM prs_candidate_variants;
```

#### Export for PRS-CS

```sql
SELECT rs_id, ref, alt, beta, p_value
FROM prs_candidate_variants
WHERE beta IS NOT NULL
ORDER BY chrom, pos;
```

#### Join with PRS weights

```sql
SELECT
    c.variant_id,
    c.rs_id,
    w.effect_weight,
    c.maf
FROM prs_candidate_variants c
JOIN prs_weights w ON c.variant_id = w.variant_id
WHERE w.pgs_id = 'PGS000018';
```

## variant_qc_summary

Aggregate QC statistics across all variants.

### Definition

```sql
CREATE MATERIALIZED VIEW variant_qc_summary AS
SELECT
    1 as id,
    COUNT(*) as total_variants,
    COUNT(*) FILTER (WHERE in_hapmap3 = TRUE) as hapmap3_variants,
    COUNT(*) FILTER (WHERE info_score >= 0.6) as high_info_variants,
    COUNT(*) FILTER (WHERE call_rate >= 0.98) as high_callrate_variants,
    COUNT(*) FILTER (WHERE hwe_p > 1e-6) as hwe_pass_variants,
    COUNT(*) FILTER (WHERE maf >= 0.01) as common_variants,
    COUNT(*) FILTER (
        WHERE in_hapmap3 = TRUE
        AND info_score >= 0.6
        AND call_rate >= 0.98
        AND hwe_p > 1e-6
        AND maf >= 0.01
    ) as prs_ready_variants
FROM variants;
```

### Usage

```sql
SELECT * FROM variant_qc_summary;
```

Returns a single row with counts:

| Column | Description |
|--------|-------------|
| `total_variants` | All loaded variants |
| `hapmap3_variants` | Variants in HapMap3 |
| `high_info_variants` | INFO >= 0.6 |
| `high_callrate_variants` | Call rate >= 98% |
| `hwe_pass_variants` | HWE p > 1e-6 |
| `common_variants` | MAF >= 1% |
| `prs_ready_variants` | Pass all filters |

## chromosome_variant_counts

Per-chromosome variant counts with PRS-ready breakdown.

### Definition

```sql
CREATE MATERIALIZED VIEW chromosome_variant_counts AS
SELECT
    chrom,
    COUNT(*) as n_variants,
    COUNT(*) FILTER (WHERE in_hapmap3 = TRUE) as n_hapmap3,
    COUNT(*) FILTER (WHERE in_hapmap3 = TRUE AND info_score >= 0.6) as n_prs_ready
FROM variants
GROUP BY chrom;
```

### Usage

```sql
SELECT * FROM chromosome_variant_counts
ORDER BY
    CASE WHEN chrom ~ '^chr\d+$' THEN regexp_replace(chrom, 'chr', '')::int
         WHEN chrom = 'chrX' THEN 23
         WHEN chrom = 'chrY' THEN 24
         WHEN chrom = 'chrM' THEN 25
         ELSE 100
    END;
```

## sample_qc_summary

Batch-level sample QC aggregation.

### Definition

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

## variant_ld_block_summary

Block-level variant aggregation for LD-aware analysis.

### Definition

```sql
CREATE VIEW variant_ld_block_summary AS
SELECT
    lb.block_id,
    lb.chrom,
    lb.start_pos,
    lb.end_pos,
    lb.population,
    COUNT(v.variant_id) as n_variants,
    COUNT(v.variant_id) FILTER (WHERE v.in_hapmap3) as n_hapmap3
FROM ld_blocks lb
LEFT JOIN variants v ON v.ld_block_id = lb.block_id
GROUP BY lb.block_id, lb.chrom, lb.start_pos, lb.end_pos, lb.population;
```

## Refreshing Views

### Manual Refresh

```sql
-- Non-blocking refresh (requires unique index)
REFRESH MATERIALIZED VIEW CONCURRENTLY prs_candidate_variants;
REFRESH MATERIALIZED VIEW CONCURRENTLY variant_qc_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY chromosome_variant_counts;
```

### CLI Command

```bash
vcf-pg-loader refresh-views --db postgresql://localhost/prs_db
```

### Refresh Timing

Refresh after:
- Loading new VCF data
- Updating HapMap3 annotations
- Importing GWAS summary statistics
- Computing variant QC metrics

### Concurrent Refresh Requirements

`REFRESH MATERIALIZED VIEW CONCURRENTLY` requires:
1. A unique index on the view
2. No active transactions holding locks

Benefits:
- Reads continue during refresh
- No downtime for queries

## Performance Tips

### Query the View, Not the Base Tables

```sql
-- Fast (uses pre-filtered view)
SELECT * FROM prs_candidate_variants WHERE chrom = 'chr1';

-- Slower (filters at query time)
SELECT * FROM variants
WHERE in_hapmap3 = TRUE
    AND info_score >= 0.6
    AND call_rate >= 0.98
    AND hwe_p > 1e-6
    AND maf >= 0.01
    AND chrom = 'chr1';
```

### Monitor View Freshness

```sql
SELECT
    schemaname,
    matviewname,
    ispopulated,
    pg_size_pretty(pg_relation_size(matviewname::regclass)) as size
FROM pg_matviews
WHERE matviewname LIKE 'prs_%' OR matviewname LIKE '%_summary';
```

## Related Tables

- [variants](./index.md) - Source data for variant views
- [sample_qc](./qc-tables.md) - Source for sample QC summary
- [ld_blocks](./reference-tables.md) - Source for LD block summary
