# PRS Workflows

End-to-end workflows for Polygenic Risk Score (PRS) analysis using vcf-pg-loader.

## Overview

vcf-pg-loader provides a complete data infrastructure for PRS research, from raw VCF files to export for downstream PRS calculation tools.

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│  VCF Files  │───▶│  vcf-pg-     │───▶│  PostgreSQL │───▶│  PRS Tools   │
│  (imputed)  │    │  loader      │    │  Database   │    │  (PRS-CS,    │
└─────────────┘    └──────────────┘    └─────────────┘    │   LDpred2)   │
                          │                   ▲           └──────────────┘
                          │                   │
                   ┌──────▼──────┐    ┌───────┴───────┐
                   │ GWAS Stats  │    │ Reference     │
                   │ PGS Catalog │    │ Panels        │
                   └─────────────┘    └───────────────┘
```

## Complete PRS Pipeline

### Prerequisites

1. PostgreSQL database (local or managed)
2. Imputed genotype VCF files
3. GWAS summary statistics
4. Reference panel (HapMap3)

### Step 1: Initialize Database

```bash
# Start managed PostgreSQL (recommended for development)
vcf-pg-loader db start

# Or initialize your own PostgreSQL
vcf-pg-loader init-db --db postgresql://user:pass@localhost/prs_db
```

### Step 2: Load Imputed Genotypes

Load VCF files containing imputed genotypes with dosages:

```bash
vcf-pg-loader load imputed_chr1.vcf.gz \
    --db postgresql://localhost/prs_db

# Load all chromosomes
for chr in {1..22}; do
    vcf-pg-loader load imputed_chr${chr}.vcf.gz \
        --db postgresql://localhost/prs_db
done
```

The loader automatically extracts:
- Imputation INFO scores
- Dosages (DS field)
- Genotype probabilities (GP field)
- Call rate and HWE statistics

### Step 3: Load Reference Panel

Download and load HapMap3 SNPs used by PRS-CS and LDpred2:

```bash
# Download HapMap3 reference (~1.1M variants)
vcf-pg-loader download-reference hapmap3 --build grch38

# Load into database
vcf-pg-loader load-reference hapmap3 --build grch38 \
    --db postgresql://localhost/prs_db
```

This flags matching variants with `in_hapmap3 = TRUE`.

### Step 4: Import GWAS Summary Statistics

Import GWAS results for your trait of interest:

```bash
vcf-pg-loader import-gwas t2d_gwas_sumstats.tsv \
    --study-id GCST90012345 \
    --trait "Type 2 Diabetes" \
    --sample-size 898130 \
    --n-cases 74124 \
    --n-controls 824006 \
    --db postgresql://localhost/prs_db
```

### Step 5: Load and Annotate LD Blocks

Download and load LD block definitions for Bayesian PRS methods:

```bash
# Download LD blocks from Berisa & Pickrell (2016)
vcf-pg-loader download-reference ld-blocks --population eur

# Load into database
vcf-pg-loader load-reference ld-blocks --population EUR --build grch37 \
    --db postgresql://localhost/prs_db

# Annotate variants with LD block assignments
vcf-pg-loader annotate-ld-blocks \
    --population EUR \
    --db postgresql://localhost/prs_db
```

Note: LD blocks are only available for GRCh37. Available populations: EUR, AFR, ASN.

### Step 6: Compute Sample QC

Run quality control on samples:

```bash
vcf-pg-loader compute-sample-qc --db postgresql://localhost/prs_db
```

### Step 7: Refresh Materialized Views

Update pre-computed views for fast queries:

```bash
vcf-pg-loader refresh-views --db postgresql://localhost/prs_db
```

> **Note**: The `prs_candidate_variants` materialized view requires GWAS data (it joins with `gwas_summary_stats`). Ensure you have imported GWAS summary statistics in Step 4 before refreshing views. If no GWAS data is available, the view will be empty but the command will succeed.

### Step 8: Export for PRS Tools

Export data in the format required by your PRS method:

```bash
# For PRS-CS
vcf-pg-loader export-prs-cs \
    --study-id 1 \
    --output t2d_prscs.txt \
    --hapmap3-only \
    --min-info 0.6 \
    --db postgresql://localhost/prs_db

# For LDpred2
vcf-pg-loader export-ldpred2 \
    --study-id 1 \
    --output t2d_ldpred2.txt \
    --hapmap3-only \
    --db postgresql://localhost/prs_db
```

---

## Workflow: Using Pre-Computed PRS Weights

If you already have PRS weights from PGS Catalog:

### Step 1: Load Genotypes

```bash
vcf-pg-loader load imputed.vcf.gz --db postgresql://localhost/prs_db
```

### Step 2: Import PGS Weights

```bash
vcf-pg-loader import-pgs PGS000018_hmPOS_GRCh38.txt \
    --db postgresql://localhost/prs_db
```

### Step 3: Calculate PRS

Query the database directly:

```sql
SELECT
    g.sample_id,
    SUM(w.effect_weight * g.dosage) as prs_raw
FROM genotypes g
JOIN prs_weights w ON g.variant_id = w.variant_id
WHERE w.pgs_id = 'PGS000018'
    AND g.dosage IS NOT NULL
GROUP BY g.sample_id;
```

---

## Workflow: Multi-Ancestry PRS

### Step 1: Load Population Frequencies

```bash
vcf-pg-loader import-frequencies gnomad_v3.vcf.gz \
    --source gnomAD_v3 \
    --db postgresql://localhost/prs_db
```

### Step 2: Query Population-Specific Data

```sql
-- Get variants with EUR frequency < 1% but AFR frequency > 5%
SELECT v.rs_id, v.chrom, v.pos
FROM variants v
JOIN population_frequencies pf_eur
    ON v.variant_id = pf_eur.variant_id
    AND pf_eur.population = 'NFE'
JOIN population_frequencies pf_afr
    ON v.variant_id = pf_afr.variant_id
    AND pf_afr.population = 'AFR'
WHERE pf_eur.af < 0.01 AND pf_afr.af > 0.05;
```

---

## Quality Control Workflow

### Variant QC Summary

```sql
SELECT * FROM variant_qc_summary;
```

Returns:
| Metric | Count |
|--------|-------|
| total_variants | 1,234,567 |
| hapmap3_variants | 1,100,000 |
| high_info_variants | 1,180,000 |
| prs_ready_variants | 950,000 |

### Sample QC Summary

```sql
SELECT
    batch_id,
    n_samples,
    n_pass,
    n_fail,
    mean_call_rate
FROM sample_qc_summary;
```

### Identify Failed Samples

```sql
SELECT sample_id, call_rate, sex_concordant
FROM sample_qc
WHERE qc_pass = FALSE
ORDER BY call_rate;
```

---

## Best Practices

### 1. Use Imputed Data

PRS methods work best with imputed genotypes:
- Higher variant coverage (especially for rare variants)
- Dosages capture imputation uncertainty
- Better match rates to GWAS variants

### 2. Apply Standard QC Filters

The `prs_candidate_variants` view applies standard filters:
- HapMap3 variants only
- INFO score >= 0.6
- Call rate >= 98%
- HWE p-value > 1e-6
- MAF >= 1%

### 3. Use Ancestry-Matched LD Reference

Match LD blocks and reference panel to your population:

```bash
vcf-pg-loader annotate-ld-blocks --population EUR  # For European samples
vcf-pg-loader annotate-ld-blocks --population AFR  # For African samples
```

### 4. Verify Variant Match Rates

Check how many PRS variants match your data:

```bash
vcf-pg-loader list-pgs --db postgresql://localhost/prs_db
```

Target match rates:
- HapMap3-based methods: 90%+
- Genome-wide PRS: 70%+

### 5. Standardize Scores

After calculating raw PRS, standardize to a reference population:

```sql
SELECT
    sample_id,
    prs_raw,
    (prs_raw - AVG(prs_raw) OVER()) / STDDEV(prs_raw) OVER() as prs_z
FROM sample_prs;
```

---

## Troubleshooting

### Low Variant Match Rate

**Symptom**: Only 50% of PRS variants match

**Solutions**:
1. Check genome build (GRCh37 vs GRCh38)
2. Verify chromosome naming (chr1 vs 1)
3. Check allele coding (A/G vs T/C strand flip)

### Missing Dosages

**Symptom**: `dosage IS NULL` for many genotypes

**Solutions**:
1. Ensure VCF has DS or GP fields
2. Check imputation software output format
3. Use hard calls as fallback:

```sql
SELECT
    sample_id,
    SUM(w.effect_weight *
        CASE gt
            WHEN '0/0' THEN 0
            WHEN '0/1' THEN 1
            WHEN '1/1' THEN 2
        END) as prs
FROM genotypes g
JOIN prs_weights w ON g.variant_id = w.variant_id
GROUP BY sample_id;
```

### Slow Queries

**Symptom**: PRS calculation takes hours

**Solutions**:
1. Use materialized views:
   ```sql
   SELECT * FROM prs_candidate_variants WHERE ...
   ```
2. Partition by chromosome for parallel processing
3. Pre-filter to HapMap3 variants
4. Refresh views after data changes:
   ```bash
   vcf-pg-loader refresh-views --db ...
   ```

---

## Integration with External Tools

### PRS-CS

```bash
# Export GWAS summary stats
vcf-pg-loader export-prs-cs \
    --study-id 1 \
    --output t2d_sumstats.txt \
    --hapmap3-only \
    --db postgresql://localhost/prs_db

# Run PRS-CS (external tool)
python PRScs.py \
    --ref_dir=/path/to/ldblk_1kg_eur \
    --bim_prefix=target_data \
    --sst_file=t2d_sumstats.txt \
    --n_gwas=898130 \
    --out_dir=./prscs_output
```

### LDpred2 (R)

```r
library(bigsnpr)

# Load exported summary stats
sumstats <- fread("t2d_ldpred2.txt")

# Run LDpred2
beta_auto <- snp_ldpred2_auto(
  corr, df_beta = sumstats,
  h2_init = 0.3, sparse = TRUE
)
```

### PLINK 2.0

```bash
# Export in PLINK format
vcf-pg-loader export-plink-score \
    --study-id 1 \
    --output t2d_score.txt \
    --db postgresql://localhost/prs_db

# Calculate PRS with PLINK
plink2 --bfile target \
    --score t2d_score.txt \
    --out prs_results
```

---

## Reference

### Related Documentation

- [CLI Reference](./cli-reference.md) - Complete command documentation
- [Schema Reference](./schema/index.md) - Database schema details
- [Architecture](./architecture.md) - System design

### External Resources

- [PGS Catalog](https://www.pgscatalog.org/) - Published PRS weights
- [GWAS Catalog](https://www.ebi.ac.uk/gwas/) - GWAS summary statistics
- [PRS-CS](https://github.com/getian107/PRScs) - Bayesian PRS method
- [LDpred2](https://privefl.github.io/bigsnpr/articles/LDpred2.html) - R package for PRS
