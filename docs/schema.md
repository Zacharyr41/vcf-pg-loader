# Database Schema

## Entity Relationship Diagram

```mermaid
erDiagram
    variants {
        bigint variant_id PK
        chromosome_type chrom PK
        int8range pos_range
        bigint pos
        bigint end_pos
        text ref
        text alt
        real qual
        text[] filter
        varchar variant_type
        text rs_id
        varchar gene
        varchar transcript
        varchar hgvs_c
        varchar hgvs_p
        varchar consequence
        varchar impact
        boolean is_coding
        boolean is_lof
        real af_gnomad
        real af_gnomad_popmax
        real af_1kg
        real cadd_phred
        varchar clinvar_sig
        varchar clinvar_review
        jsonb info
        jsonb vep_annotations
        varchar sample_id FK
        uuid load_batch_id FK
        timestamptz created_at
    }

    variant_load_audit {
        bigserial audit_id PK
        uuid load_batch_id UK
        text vcf_file_path
        char vcf_file_hash
        bigint vcf_file_size
        timestamptz load_started_at
        timestamptz load_completed_at
        varchar reference_genome
        varchar vep_version
        varchar snpeff_version
        date clinvar_version
        varchar gnomad_version
        bigint total_variants_in_file
        bigint variants_loaded
        bigint variants_skipped
        integer samples_count
        varchar status
        varchar loaded_by
        text error_message
        boolean is_reload
        uuid previous_load_id
    }

    samples {
        integer sample_id PK
        varchar external_id UK
        varchar family_id
        smallint sex
        smallint phenotype
        jsonb metadata
        timestamptz created_at
    }

    annotation_sources {
        serial source_id PK
        varchar name UK
        varchar source_type
        varchar version
        text vcf_path
        jsonb field_config
        timestamptz loaded_at
        bigint variant_count
    }

    anno_source {
        chromosome_type chrom PK
        bigint pos PK
        text ref PK
        text alt PK
        text dynamic_fields
    }

    variants ||--o{ variant_load_audit : "load_batch_id"
    variants }o--|| samples : "sample_id → external_id"
    annotation_sources ||--o{ anno_source : "registry tracks"
    variants ||--o{ anno_source : "lookup via chrom,pos,ref,alt"
```

## Tables

### variants
Main storage for VCF variant records. Partitioned by chromosome (`PARTITION BY LIST (chrom)`) for query performance. Human genome mode creates partitions for chr1-22, chrX, chrY, chrM, plus a default partition.

### variant_load_audit
Audit trail tracking each VCF file load operation. Records file checksums, variant counts, annotation versions, and load status for compliance and debugging.

### samples
Sample metadata including family relationships, sex, and phenotype. Referenced by variants via `external_id`.

### annotation_sources
Registry of loaded annotation sources (gnomAD, ClinVar, etc.). Stores field configurations as JSONB for dynamic table creation.

### anno_{source_name}
Dynamic tables created per annotation source. Each contains (chrom, pos, ref, alt) as composite primary key plus source-specific fields. Used for variant annotation lookups.
