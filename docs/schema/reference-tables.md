# Reference Tables

Tables for storing reference panels and LD block definitions used in PRS methods.

## reference_panels

SNP reference sets used by PRS methods. The primary use case is HapMap3 variants, which are required by methods like PRS-CS and LDpred2.

### Schema

```sql
CREATE TABLE reference_panels (
    panel_name VARCHAR(50) NOT NULL,
    rsid VARCHAR(20),
    chrom VARCHAR(2) NOT NULL,
    position BIGINT NOT NULL,
    a1 VARCHAR(10) NOT NULL,
    a2 VARCHAR(10) NOT NULL,
    PRIMARY KEY (panel_name, chrom, position, a1, a2)
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `panel_name` | VARCHAR(50) | Reference panel identifier (e.g., "hapmap3") |
| `rsid` | VARCHAR(20) | dbSNP rsID (optional) |
| `chrom` | VARCHAR(2) | Chromosome (1-22, X, Y) |
| `position` | BIGINT | Genomic position |
| `a1` | VARCHAR(10) | First allele |
| `a2` | VARCHAR(10) | Second allele |

### Indexes

```sql
CREATE INDEX idx_refpanel_rsid ON reference_panels(rsid)
    WHERE rsid IS NOT NULL;

CREATE INDEX idx_refpanel_lookup ON reference_panels(panel_name, chrom, position);
```

### HapMap3 Reference Panel

HapMap3 SNPs (~1.1M variants) are commonly used for:
- **PRS-CS**: Bayesian PRS using continuous shrinkage priors
- **LDpred2**: LD-aware polygenic prediction
- **SBayesR**: Summary-based Bayesian PRS
- **Quality control**: High-quality, well-characterized variants

#### Downloading HapMap3 Data

The easiest way to get HapMap3 data is using the CLI:

```bash
# Download HapMap3 for GRCh38 (default)
vcf-pg-loader download-reference hapmap3

# Download for GRCh37
vcf-pg-loader download-reference hapmap3 --build grch37

# Force re-download
vcf-pg-loader download-reference hapmap3 --force

# Custom output directory
vcf-pg-loader download-reference hapmap3 --output /path/to/refs
```

Files are cached at `~/.vcf-pg-loader/references/` by default. The data is sourced from LDpred2's authoritative HapMap3+ variant list on figshare.

#### Loading HapMap3 into the Database

After downloading, load into the database:

```bash
# Uses cached download automatically
vcf-pg-loader load-reference hapmap3 --build grch38 --db postgresql://localhost/prs_db

# Or provide your own file
vcf-pg-loader load-reference hapmap3 /path/to/hapmap3.tsv --db postgresql://localhost/prs_db
```

### Usage Examples

#### Check HapMap3 coverage

```sql
SELECT
    panel_name,
    COUNT(*) as n_variants,
    COUNT(DISTINCT chrom) as n_chromosomes
FROM reference_panels
GROUP BY panel_name;
```

#### Flag variants in HapMap3

```sql
UPDATE variants v
SET in_hapmap3 = TRUE
FROM reference_panels r
WHERE r.panel_name = 'hapmap3'
    AND v.chrom = r.chrom
    AND v.pos = r.position
    AND (
        (v.ref = r.a1 AND v.alt = r.a2) OR
        (v.ref = r.a2 AND v.alt = r.a1)
    );
```

#### Get HapMap3 variants per chromosome

```sql
SELECT chrom, COUNT(*) as n_variants
FROM reference_panels
WHERE panel_name = 'hapmap3'
GROUP BY chrom
ORDER BY
    CASE WHEN chrom ~ '^\d+$' THEN chrom::int ELSE 100 END;
```

## ld_blocks

Linkage disequilibrium block definitions from Berisa & Pickrell (2016). Used by Bayesian PRS methods to partition the genome.

### Schema

```sql
CREATE TABLE ld_blocks (
    block_id SERIAL PRIMARY KEY,
    chrom VARCHAR(2) NOT NULL,
    start_pos BIGINT NOT NULL,
    end_pos BIGINT NOT NULL,
    population VARCHAR(10) NOT NULL,
    source VARCHAR(50) DEFAULT 'Berisa_Pickrell_2016',
    genome_build VARCHAR(10) DEFAULT 'GRCh37',
    n_snps_1kg INTEGER,
    UNIQUE (source, population, chrom, start_pos, genome_build)
);
```

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `block_id` | SERIAL | Auto-incrementing primary key |
| `chrom` | VARCHAR(2) | Chromosome |
| `start_pos` | BIGINT | Block start position |
| `end_pos` | BIGINT | Block end position |
| `population` | VARCHAR(10) | Population (EUR, AFR, EAS, etc.) |
| `source` | VARCHAR(50) | Source of LD blocks |
| `genome_build` | VARCHAR(10) | Reference genome |
| `n_snps_1kg` | INTEGER | 1000 Genomes SNP count in block |

### Indexes

```sql
CREATE INDEX idx_ldblock_region ON ld_blocks
    USING GIST (chrom, int8range(start_pos, end_pos, '[]'));

CREATE INDEX idx_ldblock_population ON ld_blocks (population, genome_build);
```

### Berisa & Pickrell LD Blocks

Reference:
> Berisa T, Pickrell JK. Approximately independent linkage disequilibrium blocks in human populations. Bioinformatics. 2016;32(2):283-285.

The LD blocks partition the genome into approximately independent regions:

| Population | Blocks | Mean Size |
|------------|--------|-----------|
| EUR | 1703 | ~1.7 Mb |
| AFR | 2583 | ~1.2 Mb |
| ASN | 1445 | ~2.0 Mb |

#### Downloading LD Block Data

The easiest way to get LD block data is using the CLI:

```bash
# Download LD blocks for European population (default)
vcf-pg-loader download-reference ld-blocks --population eur

# Download for African population
vcf-pg-loader download-reference ld-blocks --population afr

# Download for Asian population
vcf-pg-loader download-reference ld-blocks --population asn

# Force re-download
vcf-pg-loader download-reference ld-blocks --population eur --force
```

Files are cached at `~/.vcf-pg-loader/references/` by default. The data is sourced from the ldetect-data repository on Bitbucket.

**Note:** LD blocks are only available for GRCh37/hg19. GRCh38 coordinates would require liftover.

#### Loading LD Blocks into the Database

After downloading, load into the database:

```bash
# Uses cached download automatically
vcf-pg-loader load-reference ld-blocks --population EUR --build grch37 --db postgresql://localhost/prs_db

# Or provide your own file
vcf-pg-loader load-reference ld-blocks /path/to/blocks.bed --population EUR --db postgresql://localhost/prs_db
```

### Usage Examples

#### Annotate variants with LD blocks

```sql
UPDATE variants v
SET ld_block_id = lb.block_id
FROM ld_blocks lb
WHERE lb.population = 'EUR'
    AND v.chrom = lb.chrom
    AND v.pos >= lb.start_pos
    AND v.pos <= lb.end_pos;
```

#### Count variants per LD block

```sql
SELECT
    lb.block_id,
    lb.chrom,
    lb.start_pos,
    lb.end_pos,
    COUNT(v.variant_id) as n_variants,
    COUNT(v.variant_id) FILTER (WHERE v.in_hapmap3) as n_hapmap3
FROM ld_blocks lb
LEFT JOIN variants v ON v.ld_block_id = lb.block_id
WHERE lb.population = 'EUR'
GROUP BY lb.block_id, lb.chrom, lb.start_pos, lb.end_pos
ORDER BY lb.chrom, lb.start_pos;
```

#### Find block containing a position

```sql
SELECT *
FROM ld_blocks
WHERE population = 'EUR'
    AND chrom = '6'
    AND int8range(start_pos, end_pos, '[]') @> 32000000::bigint;
```

## variant_ld_block_summary View

Pre-computed block-level variant aggregation:

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

## CLI Commands

### Load reference panel

```bash
vcf-pg-loader load-reference hapmap3_snps.tsv \
    --panel-name hapmap3 \
    --db postgresql://localhost/prs_db
```

### Annotate variants with LD blocks

```bash
vcf-pg-loader annotate-ld-blocks \
    --population EUR \
    --db postgresql://localhost/prs_db
```

## Related Tables

- [variants](./index.md) - `in_hapmap3` flag and `ld_block_id` FK
- [prs_weights](./prs-tables.md) - Often filtered to HapMap3 variants
- [prs_candidate_variants](./views.md) - Includes HapMap3 filter
