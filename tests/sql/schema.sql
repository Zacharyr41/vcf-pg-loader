-- Schema for vcf-pg-loader variant storage
CREATE SCHEMA IF NOT EXISTS variants;

-- Core variants table
CREATE TABLE IF NOT EXISTS variants.variant (
    id BIGSERIAL PRIMARY KEY,
    sample_id VARCHAR(255) NOT NULL,
    chrom VARCHAR(50) NOT NULL,
    pos INTEGER NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    qual REAL,
    filter VARCHAR(255),
    info JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT variant_unique UNIQUE (sample_id, chrom, pos, ref, alt)
);

-- Genotypes table (per-sample data)
CREATE TABLE IF NOT EXISTS variants.genotype (
    id BIGSERIAL PRIMARY KEY,
    variant_id BIGINT REFERENCES variants.variant(id) ON DELETE CASCADE,
    sample_name VARCHAR(255) NOT NULL,
    gt VARCHAR(10),
    gq INTEGER,
    dp INTEGER,
    ad INTEGER[],
    pl INTEGER[],
    CONSTRAINT genotype_unique UNIQUE (variant_id, sample_name)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_variant_sample ON variants.variant(sample_id);
CREATE INDEX IF NOT EXISTS idx_variant_position ON variants.variant(chrom, pos);
CREATE INDEX IF NOT EXISTS idx_variant_info ON variants.variant USING GIN (info);
CREATE INDEX IF NOT EXISTS idx_genotype_sample ON variants.genotype(sample_name);
