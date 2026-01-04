# Database Schema

This document provides an overview of the vcf-pg-loader database schema. For detailed documentation of each table, see the [schema reference](./schema/index.md).

## Entity Relationship Diagram

```mermaid
erDiagram
    variants ||--o{ variant_load_audit : "load_batch_id"
    variants ||--o{ genotypes : "variant_id"
    variants ||--o{ gwas_summary_stats : "variant_id"
    variants ||--o{ prs_weights : "variant_id"
    variants ||--o{ population_frequencies : "variant_id"
    variants }o--o| ld_blocks : "ld_block_id"

    samples ||--o{ genotypes : "sample_id"
    samples ||--o| sample_qc : "sample_id"

    studies ||--o{ gwas_summary_stats : "study_id"
    pgs_scores ||--o{ prs_weights : "pgs_id"

    variants {
        bigint variant_id PK
        chromosome_type chrom PK
        bigint pos
        text ref
        text alt
        text rs_id
        real info_score
        real call_rate
        real hwe_p
        real aaf
        real maf
        int mac
        boolean in_hapmap3
        int ld_block_id FK
        uuid load_batch_id FK
    }

    variant_load_audit {
        bigserial audit_id PK
        uuid load_batch_id UK
        text vcf_file_path
        char vcf_file_hash
        bigint variants_loaded
        varchar status
    }

    samples {
        integer sample_id PK
        varchar external_id UK
        varchar family_id
        smallint sex
        smallint phenotype
    }

    genotypes {
        bigint variant_id PK
        integer sample_id PK
        varchar gt
        float dosage
        float[] gp
        boolean passes_adj
    }

    studies {
        serial study_id PK
        varchar study_accession UK
        text trait_name
        integer sample_size
        integer n_cases
        integer n_controls
    }

    gwas_summary_stats {
        serial id PK
        bigint variant_id FK
        integer study_id FK
        varchar effect_allele
        double beta
        double standard_error
        double p_value
    }

    pgs_scores {
        varchar pgs_id PK
        text trait_name
        integer n_variants
        varchar genome_build
    }

    prs_weights {
        serial id PK
        bigint variant_id FK
        varchar pgs_id FK
        varchar effect_allele
        double effect_weight
    }

    reference_panels {
        varchar panel_name PK
        varchar chrom PK
        bigint position PK
        varchar a1 PK
        varchar a2 PK
    }

    ld_blocks {
        serial block_id PK
        varchar chrom
        bigint start_pos
        bigint end_pos
        varchar population
    }

    population_frequencies {
        serial id PK
        bigint variant_id FK
        varchar source
        varchar population
        double af
    }

    sample_qc {
        varchar sample_id PK
        float call_rate
        float het_hom_ratio
        boolean qc_pass
    }
```

## Table Categories

### Core Tables

| Table | Description | Documentation |
|-------|-------------|---------------|
| `variants` | Main variant storage, partitioned by chromosome | [Schema Overview](./schema/index.md) |
| `variant_load_audit` | Load tracking and validation | [Schema Overview](./schema/index.md) |
| `samples` | Sample metadata | [Schema Overview](./schema/index.md) |

### PRS Research Tables

| Table | Description | Documentation |
|-------|-------------|---------------|
| `pgs_scores` | PGS Catalog score metadata | [PRS Tables](./schema/prs-tables.md) |
| `prs_weights` | Per-variant PRS effect weights | [PRS Tables](./schema/prs-tables.md) |
| `studies` | GWAS study metadata | [GWAS Tables](./schema/gwas-tables.md) |
| `gwas_summary_stats` | GWAS association results (GWAS-SSF) | [GWAS Tables](./schema/gwas-tables.md) |

### Reference Data

| Table | Description | Documentation |
|-------|-------------|---------------|
| `reference_panels` | HapMap3 and other SNP sets | [Reference Tables](./schema/reference-tables.md) |
| `ld_blocks` | LD block definitions (Berisa & Pickrell) | [Reference Tables](./schema/reference-tables.md) |

### Individual-Level Data

| Table | Description | Documentation |
|-------|-------------|---------------|
| `genotypes` | Per-sample genotypes with dosages | [Genotypes Tables](./schema/genotypes-tables.md) |
| `population_frequencies` | Multi-ancestry allele frequencies | [QC Tables](./schema/qc-tables.md) |

### Quality Control

| Table | Description | Documentation |
|-------|-------------|---------------|
| `sample_qc` | Per-sample QC metrics | [QC Tables](./schema/qc-tables.md) |

### Materialized Views

| View | Description | Documentation |
|------|-------------|---------------|
| `prs_candidate_variants` | Pre-filtered PRS-ready variants | [Views](./schema/views.md) |
| `variant_qc_summary` | Aggregate QC statistics | [Views](./schema/views.md) |
| `chromosome_variant_counts` | Per-chromosome counts | [Views](./schema/views.md) |
| `sample_qc_summary` | Batch-level QC summary | [Views](./schema/views.md) |

## Partitioning

### variants table (List Partitioning)

```sql
PARTITION BY LIST (chrom)
```

- `variants_chr1` through `variants_chr22`
- `variants_chrx`, `variants_chry`, `variants_chrm`
- `variants_default`

### genotypes table (Hash Partitioning)

```sql
PARTITION BY HASH (sample_id)
```

- 16 partitions: `genotypes_p0` through `genotypes_p15`

## SQL Functions

| Function | Description |
|----------|-------------|
| `hwe_exact_test(n_aa, n_ab, n_bb)` | Hardy-Weinberg equilibrium p-value |
| `af_from_dosages(dosages[])` | Allele frequency from dosage array |
| `n_eff(n_cases, n_controls)` | Effective sample size |
| `alleles_match(ref1, alt1, ref2, alt2)` | Allele harmonization |

## Detailed Documentation

See the [Schema Reference](./schema/index.md) for complete table definitions, indexes, and usage examples.
