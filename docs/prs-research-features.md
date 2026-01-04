# Feature Assessment: Supporting PRS Research Methodology in vcf-pg-loader

## Executive Summary

After analyzing Peer's PRS methodology for hypertensive disorders of pregnancy and the current vcf-pg-loader architecture, I've identified **10 feature candidates** that would support this research while remaining within scope. The key insight is that vcf-pg-loader occupies the **data storage and retrieval layer** of the genomics pipeline—it should focus on making data available in queryable form for downstream tools (SAIGE, REGENIE, PRS-CS, etc.) rather than performing the analysis itself.

---

## Feature Candidates (In-Scope)

### 1. HapMap3 Variant Set Reference Table

**Relevance to Peer's Methodology:**
PRS-CS explicitly restricts analysis to ~1.3 million HapMap3 SNPs for three reasons: computational tractability, high imputation quality across populations, and cross-study compatibility. The methodology document states this is essential for PRS construction.

**Why In-Scope:**
This is reference annotation data—exactly what the existing annotation loader infrastructure handles. HapMap3 is a static variant catalog, analogous to gnomAD or ClinVar.

**Architecture Integration:**

```python
# New file: src/vcf_pg_loader/hapmap3.py

from dataclasses import dataclass

@dataclass
class HapMap3Variant:
    chrom: str
    pos: int
    rs_id: str
    ref: str
    alt: str
    ld_block_id: int  # Berisa & Pickrell block assignment

class HapMap3ReferenceLoader:
    """Load HapMap3 variant catalog as annotation reference."""

    async def load_hapmap3(
        self,
        conn: asyncpg.Connection,
        source_file: Path,  # BIM file or VCF
        ld_block_file: Path | None = None,  # Berisa & Pickrell blocks
    ) -> int:
        """Load HapMap3 variants with LD block assignments."""
        ...
```

**Schema:**
```sql
CREATE TABLE hapmap3_variants (
    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    rs_id TEXT,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    ld_block_id INTEGER,  -- 1-1703 per Berisa & Pickrell
    PRIMARY KEY (chrom, pos, ref, alt)
);

CREATE INDEX idx_hapmap3_rsid ON hapmap3_variants(rs_id);
CREATE INDEX idx_hapmap3_ld_block ON hapmap3_variants(ld_block_id);
```

**Query Example:**
```sql
-- Find variants in my dataset that are in HapMap3
SELECT v.* FROM variants v
INNER JOIN hapmap3_variants h
ON v.chrom = h.chrom AND v.pos = h.pos AND v.ref = h.ref AND v.alt = h.alt;

-- Count HapMap3 overlap per LD block
SELECT h.ld_block_id, COUNT(*)
FROM variants v
INNER JOIN hapmap3_variants h ON ...
GROUP BY h.ld_block_id;
```

---

### 2. Imputation Quality Score (INFO/R²) First-Class Support

**Relevance to Peer's Methodology:**
Post-imputation QC requires filtering on INFO score ≥0.6. The methodology explicitly mentions that Michigan Imputation Server outputs R² scores, and variants below threshold are excluded from downstream analysis.

**Why In-Scope:**
INFO/R² is a standard VCF INFO field that the tool already parses. This feature promotes it to first-class status with dedicated column, indexing, and CLI filtering.

**Architecture Integration:**

Extend `VariantRecord` in `models.py`:
```python
@dataclass
class VariantRecord:
    # ... existing fields ...

    # Imputation quality metrics
    imputation_r2: float | None = None
    imputation_info: float | None = None
    imputed: bool = False
```

Extend schema in `schema.py`:
```sql
-- Add to variants table
imputation_r2 REAL,
imputation_info REAL,
imputed BOOLEAN DEFAULT FALSE,
```

**CLI Enhancement:**
```bash
# Load with imputation quality filtering
vcf-pg-loader load imputed.vcf.gz --min-info-score 0.6

# The tool would skip variants below threshold and report:
# "Loaded 2,847,293 variants (skipped 423,108 with INFO < 0.6)"
```

**Indexing:**
```sql
CREATE INDEX idx_variants_imputation_quality
ON variants (imputation_r2)
WHERE imputed = TRUE;
```

---

### 3. Minor Allele Frequency (MAF) and Minor Allele Count (MAC) Computation

**Relevance to Peer's Methodology:**
- QC filtering requires MAF > 1% for common variant imputation
- Post-imputation filtering uses MAC ≥50
- SAIGE uses MAC-dependent variance ratio estimation (separate ratios for MAC categories 1, 2, 3, 4, 5, 6-10, 11-20, >20)

**Why In-Scope:**
This is a computed field derived from loaded data. When genotype data is available (multi-sample VCF), MAF/MAC can be computed during loading. For annotation sources (gnomAD), AF is already extracted.

**Architecture Integration:**

For loaded sample data:
```python
def compute_maf_mac(genotypes: list[str]) -> tuple[float, int]:
    """Compute MAF and MAC from sample genotypes.

    Args:
        genotypes: List of GT strings like "0/1", "1/1", "./."

    Returns:
        (minor_allele_frequency, minor_allele_count)
    """
    ref_count = 0
    alt_count = 0

    for gt in genotypes:
        if gt in ("./.", ".|.", "."):
            continue
        alleles = gt.replace("|", "/").split("/")
        for a in alleles:
            if a == "0":
                ref_count += 1
            elif a.isdigit():
                alt_count += 1

    total = ref_count + alt_count
    if total == 0:
        return 0.0, 0

    alt_freq = alt_count / total
    maf = min(alt_freq, 1 - alt_freq)
    mac = min(alt_count, ref_count)

    return maf, mac
```

**Schema Extension:**
```sql
ALTER TABLE variants ADD COLUMN maf REAL;
ALTER TABLE variants ADD COLUMN mac INTEGER;

CREATE INDEX idx_variants_maf ON variants(maf) WHERE maf IS NOT NULL;
CREATE INDEX idx_variants_mac_categories ON variants(
    CASE
        WHEN mac = 1 THEN 1
        WHEN mac = 2 THEN 2
        WHEN mac <= 5 THEN 3
        WHEN mac <= 10 THEN 4
        WHEN mac <= 20 THEN 5
        ELSE 6
    END
);
```

---

### 4. Hardy-Weinberg Equilibrium (HWE) P-value Computation

**Relevance to Peer's Methodology:**
Variant-level QC excludes markers with HWE P-value < 1×10⁻⁴. This tests whether observed genotype frequencies match expectations under random mating—significant deviation indicates genotyping errors or population stratification.

**Why In-Scope:**
HWE is a standard QC metric computable from genotype counts. It's a data quality annotation derived during loading.

**Architecture Integration:**

```python
# New file: src/vcf_pg_loader/qc/hwe.py

from scipy import stats

def compute_hwe_pvalue(
    n_hom_ref: int,
    n_het: int,
    n_hom_alt: int
) -> float:
    """Compute Hardy-Weinberg equilibrium exact test p-value.

    Uses the method of Wigginton et al. (2005) for exact test.

    Args:
        n_hom_ref: Count of homozygous reference genotypes (0/0)
        n_het: Count of heterozygous genotypes (0/1)
        n_hom_alt: Count of homozygous alt genotypes (1/1)

    Returns:
        HWE p-value (two-sided)
    """
    n = n_hom_ref + n_het + n_hom_alt
    if n == 0:
        return 1.0

    # Expected under HWE
    p = (2 * n_hom_ref + n_het) / (2 * n)
    q = 1 - p

    expected_hom_ref = n * p * p
    expected_het = 2 * n * p * q
    expected_hom_alt = n * q * q

    observed = [n_hom_ref, n_het, n_hom_alt]
    expected = [expected_hom_ref, expected_het, expected_hom_alt]

    # Chi-square test (df=1 for biallelic)
    chi2, pvalue = stats.chisquare(observed, expected, ddof=1)
    return pvalue


def hwe_filter_threshold(pvalue: float, threshold: float = 1e-4) -> bool:
    """Return True if variant passes HWE filter."""
    return pvalue >= threshold
```

**Schema:**
```sql
ALTER TABLE variants ADD COLUMN hwe_pvalue REAL;

CREATE INDEX idx_variants_hwe_fail
ON variants(hwe_pvalue)
WHERE hwe_pvalue < 1e-4;
```

**CLI:**
```bash
vcf-pg-loader load multi_sample.vcf.gz --compute-hwe --min-hwe 1e-4
```

---

### 5. GWAS Summary Statistics Schema and Loading

**Relevance to Peer's Methodology:**
The entire PRS pipeline consumes GWAS summary statistics:
- Meta-analysis (METAL) combines summary statistics across cohorts
- PRS-CS uses summary statistics as input for Bayesian posterior estimation
- LDSC estimates heritability from summary statistics
- The formula: weights wᵢ = 1/SEᵢ², combined effect β̂ = Σ(wᵢ × β̂ᵢ) / Σwᵢ

**Why In-Scope:**
Summary statistics are variant-indexed data, structurally similar to annotation databases. Loading GWAS summary statistics enables the database to serve as a unified repository for PRS construction inputs.

**Architecture Integration:**

```python
# New file: src/vcf_pg_loader/gwas/summary_stats.py

@dataclass
class GWASSummaryRecord:
    chrom: str
    pos: int
    ref: str
    alt: str
    rs_id: str | None

    # Effect estimates
    beta: float
    se: float
    pvalue: float

    # Optional fields
    odds_ratio: float | None = None
    z_score: float | None = None

    # Sample info
    n_cases: int | None = None
    n_controls: int | None = None
    n_total: int | None = None

    # Allele frequencies
    eaf: float | None = None  # Effect allele frequency

    # Meta-analysis fields
    heterogeneity_pvalue: float | None = None
    i_squared: float | None = None

class SummaryStatsLoader:
    """Load GWAS summary statistics in various formats."""

    SUPPORTED_FORMATS = ["metal", "ldsc", "prscs", "generic"]

    async def load_summary_stats(
        self,
        conn: asyncpg.Connection,
        file_path: Path,
        study_name: str,
        file_format: str = "generic",
        column_mapping: dict[str, str] | None = None,
    ) -> int:
        """Load summary statistics file into database."""
        ...
```

**Schema:**
```sql
CREATE TABLE gwas_summary_stats (
    study_id SERIAL,
    study_name VARCHAR(100) NOT NULL,

    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    rs_id TEXT,

    -- Effect estimates
    beta REAL NOT NULL,
    se REAL NOT NULL,
    pvalue DOUBLE PRECISION NOT NULL,

    -- Optional
    odds_ratio REAL,
    z_score REAL,

    -- Sample sizes
    n_cases INTEGER,
    n_controls INTEGER,
    n_total INTEGER,

    -- Allele frequency
    eaf REAL,

    -- Meta-analysis
    heterogeneity_pvalue REAL,
    i_squared REAL,

    PRIMARY KEY (study_name, chrom, pos, ref, alt)
);

CREATE INDEX idx_gwas_pvalue ON gwas_summary_stats(pvalue);
CREATE INDEX idx_gwas_rsid ON gwas_summary_stats(rs_id);

-- Registry table
CREATE TABLE gwas_studies (
    study_id SERIAL PRIMARY KEY,
    study_name VARCHAR(100) UNIQUE NOT NULL,
    phenotype VARCHAR(255),
    ancestry VARCHAR(50),
    n_cases INTEGER,
    n_controls INTEGER,
    lambda_gc REAL,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

### 6. PRS Weights Table Schema and Loading

**Relevance to Peer's Methodology:**
PRS-CS outputs posterior effect sizes (weights) for each variant. These weights are then applied to compute individual-level polygenic scores: `PRS = Σ βⱼ × Gⱼ` where βⱼ is the weight and Gⱼ is the genotype dosage.

**Why In-Scope:**
PRS weights are variant-indexed annotations. Storing them enables SQL-based PRS computation against loaded genotypes.

**Architecture Integration:**

```python
@dataclass
class PRSWeight:
    chrom: str
    pos: int
    ref: str
    alt: str
    effect_allele: str
    weight: float

class PRSWeightLoader:
    """Load PRS weight files (PRS-CS output, PGS Catalog, etc.)."""

    SUPPORTED_FORMATS = ["prscs", "pgs_catalog", "ldpred2", "generic"]

    async def load_prs_weights(
        self,
        conn: asyncpg.Connection,
        file_path: Path,
        score_name: str,
        file_format: str = "generic",
    ) -> int:
        ...
```

**Schema:**
```sql
CREATE TABLE prs_weights (
    score_id INTEGER REFERENCES prs_scores(score_id),

    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,

    effect_allele CHAR(1) NOT NULL,
    weight DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (score_id, chrom, pos, ref, alt)
);

CREATE TABLE prs_scores (
    score_id SERIAL PRIMARY KEY,
    score_name VARCHAR(100) UNIQUE NOT NULL,
    phenotype VARCHAR(255),
    ancestry VARCHAR(50),
    n_variants INTEGER,
    prs_method VARCHAR(50),  -- 'PRS-CS', 'LDpred2', etc.
    global_phi REAL,  -- PRS-CS phi parameter
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Query Example:**
```sql
-- Compute PRS for a sample by joining genotypes with weights
SELECT
    s.sample_id,
    p.score_name,
    SUM(
        CASE
            WHEN v.alt = w.effect_allele THEN
                (CASE g.gt WHEN '1/1' THEN 2 WHEN '0/1' THEN 1 ELSE 0 END) * w.weight
            ELSE 0
        END
    ) as prs
FROM genotypes g
JOIN variants v ON g.variant_id = v.variant_id
JOIN prs_weights w ON v.chrom = w.chrom AND v.pos = w.pos AND v.ref = w.ref AND v.alt = w.alt
JOIN prs_scores p ON w.score_id = p.score_id
JOIN samples s ON g.sample_id = s.sample_id
WHERE p.score_name = 'HDP_PRS_v1'
GROUP BY s.sample_id, p.score_name;
```

---

### 7. Genotype Data Storage (FORMAT Fields: GT, GQ, DP, AD)

**Relevance to Peer's Methodology:**
Sample-level QC requires:
- GQ ≥ 20 (genotype quality)
- DP ≥ 10 (read depth)
- AB ≥ 0.2 for heterozygotes (allele balance, computed from AD)

The ADJ genotype quality criteria from gnomAD are essential for high-quality variant calling.

**Why In-Scope:**
FORMAT fields are core VCF data. Currently vcf-pg-loader only stores sample_id, not genotypes. Extending to FORMAT fields enables sample-level QC and PRS computation.

**Architecture Integration:**

```python
# Extend models.py
@dataclass
class GenotypeRecord:
    variant_id: int
    sample_id: str

    # Core genotype
    gt: str  # e.g., "0/1", "1|1"
    phased: bool

    # Quality metrics
    gq: int | None = None
    dp: int | None = None
    ad: list[int] | None = None  # Allelic depths [ref, alt]

    # Derived
    allele_balance: float | None = None  # AD[1] / sum(AD)

    @property
    def passes_adj(self) -> bool:
        """Check if genotype passes gnomAD ADJ criteria."""
        if self.gq is not None and self.gq < 20:
            return False
        if self.dp is not None and self.dp < 10:
            return False
        if self.gt in ("0/1", "0|1", "1|0") and self.allele_balance is not None:
            if self.allele_balance < 0.2:
                return False
        return True
```

**Schema:**
```sql
CREATE TABLE genotypes (
    variant_id BIGINT NOT NULL,
    sample_id INTEGER REFERENCES samples(sample_id),

    gt VARCHAR(20) NOT NULL,
    phased BOOLEAN DEFAULT FALSE,

    gq SMALLINT,
    dp INTEGER,
    ad INTEGER[],
    allele_balance REAL,

    passes_adj BOOLEAN GENERATED ALWAYS AS (
        COALESCE(gq >= 20, TRUE) AND
        COALESCE(dp >= 10, TRUE) AND
        (gt NOT IN ('0/1', '0|1', '1|0') OR COALESCE(allele_balance >= 0.2, TRUE))
    ) STORED,

    PRIMARY KEY (variant_id, sample_id)
) PARTITION BY HASH (sample_id);

CREATE INDEX idx_genotypes_adj ON genotypes(variant_id) WHERE passes_adj = TRUE;
```

**CLI:**
```bash
vcf-pg-loader load multi_sample.vcf.gz --store-genotypes --adj-filter
```

---

### 8. Call Rate Computation (Sample-level and Variant-level)

**Relevance to Peer's Methodology:**
QC thresholds:
- Sample call rate ≥ 99%
- Variant call rate ≥ 99%

**Why In-Scope:**
Call rate is a materialized view/computed statistic over loaded data. It's a natural extension once genotype data is stored.

**Architecture Integration:**

```sql
-- Materialized view for sample call rates
CREATE MATERIALIZED VIEW sample_call_rates AS
SELECT
    sample_id,
    COUNT(*) as total_variants,
    COUNT(*) FILTER (WHERE gt NOT LIKE '.%') as called_variants,
    COUNT(*) FILTER (WHERE gt NOT LIKE '.%')::FLOAT / COUNT(*) as call_rate
FROM genotypes
GROUP BY sample_id;

-- Materialized view for variant call rates
CREATE MATERIALIZED VIEW variant_call_rates AS
SELECT
    variant_id,
    COUNT(*) as total_samples,
    COUNT(*) FILTER (WHERE gt NOT LIKE '.%') as called_samples,
    COUNT(*) FILTER (WHERE gt NOT LIKE '.%')::FLOAT / COUNT(*) as call_rate
FROM genotypes
GROUP BY variant_id;

-- Index for QC filtering
CREATE INDEX idx_variant_low_call_rate ON variant_call_rates(variant_id)
WHERE call_rate < 0.99;
```

**Python:**
```python
async def compute_qc_metrics(conn: asyncpg.Connection) -> dict:
    """Compute sample and variant call rates after loading."""

    await conn.execute("REFRESH MATERIALIZED VIEW sample_call_rates")
    await conn.execute("REFRESH MATERIALIZED VIEW variant_call_rates")

    low_call_samples = await conn.fetchval(
        "SELECT COUNT(*) FROM sample_call_rates WHERE call_rate < 0.99"
    )
    low_call_variants = await conn.fetchval(
        "SELECT COUNT(*) FROM variant_call_rates WHERE call_rate < 0.99"
    )

    return {
        "samples_below_99_call_rate": low_call_samples,
        "variants_below_99_call_rate": low_call_variants,
    }
```

---

### 9. Population/Ancestry Annotation per Sample

**Relevance to Peer's Methodology:**
- Different imputation reference panels for different ancestries (TOPMed is 50% non-European)
- Beagle Ne parameter tuned for population (Ne=20,000 for Finnish vs 1,000,000 for outbred)
- Population stratification controlled via principal components

**Why In-Scope:**
Sample metadata is already in the `samples` table. Ancestry is a critical sample attribute for stratified analysis.

**Architecture Integration:**

Extend `samples` table:
```sql
ALTER TABLE samples ADD COLUMN ancestry VARCHAR(50);
ALTER TABLE samples ADD COLUMN ancestry_probabilities JSONB;
-- e.g., {"EUR": 0.85, "AFR": 0.10, "AMR": 0.05}
ALTER TABLE samples ADD COLUMN principal_components REAL[];  -- PC1-PC20

CREATE INDEX idx_samples_ancestry ON samples(ancestry);
```

**Ancestry Loading:**
```python
async def load_ancestry_assignments(
    conn: asyncpg.Connection,
    ancestry_file: Path,  # e.g., from KING, ADMIXTURE, or PCA-based inference
) -> int:
    """Load ancestry assignments for samples."""
    ...
```

**Query Example:**
```sql
-- Get variant frequencies stratified by ancestry
SELECT
    v.chrom, v.pos, v.ref, v.alt,
    s.ancestry,
    COUNT(*) FILTER (WHERE g.gt IN ('0/1', '1/1')) as alt_carrier_count,
    COUNT(*) as total_samples
FROM variants v
JOIN genotypes g ON v.variant_id = g.variant_id
JOIN samples s ON g.sample_id = s.sample_id
WHERE v.gene = 'FLT1'  -- Preeclampsia gene
GROUP BY v.chrom, v.pos, v.ref, v.alt, s.ancestry;
```

---

### 10. LD Block Annotation (Berisa & Pickrell)

**Relevance to Peer's Methodology:**
PRS-CS partitions the genome into 1,703 largely independent LD blocks for computational tractability. The Gibbs sampler updates effects within blocks: `βℓ | rest ~ N(μℓ, Σℓ)`.

**Why In-Scope:**
LD blocks are a static genomic reference, like HapMap3 variants or gene annotations. Tagging variants with LD block enables block-aware queries.

**Architecture Integration:**

```sql
CREATE TABLE ld_blocks (
    block_id SERIAL PRIMARY KEY,
    chrom chromosome_type NOT NULL,
    start_pos BIGINT NOT NULL,
    end_pos BIGINT NOT NULL,

    -- Optional metadata
    n_snps INTEGER,
    block_set VARCHAR(50) DEFAULT 'berisa_pickrell_eur',  -- EUR, AFR, EAS blocks differ

    UNIQUE (block_set, chrom, start_pos)
);

CREATE INDEX idx_ld_blocks_region ON ld_blocks
    USING GIST (chrom, int8range(start_pos, end_pos));

-- Add block annotation to variants
ALTER TABLE variants ADD COLUMN ld_block_id INTEGER REFERENCES ld_blocks(block_id);

-- Or use a join view
CREATE VIEW variants_with_ld_block AS
SELECT v.*, lb.block_id as ld_block_id
FROM variants v
LEFT JOIN ld_blocks lb ON v.chrom = lb.chrom
    AND v.pos >= lb.start_pos AND v.pos < lb.end_pos
    AND lb.block_set = 'berisa_pickrell_eur';
```

---

## Features Explicitly OUT OF SCOPE

The following are **not recommended** because they are fundamentally different computational tools:

| Feature | Why Out of Scope |
|---------|------------------|
| **Imputation** (EAGLE, Minimac4) | Computationally intensive phasing/imputation requiring specialized algorithms |
| **GWAS Association Testing** (SAIGE, REGENIE) | Statistical analysis requiring mixed models, SPA, Firth regression |
| **Meta-Analysis** (METAL) | Summary statistic combination tool |
| **PRS Calculation** (PRS-CS) | Bayesian Gibbs sampling, MCMC inference |
| **LD Score Computation** | O(M²) genome-wide calculation |
| **Phasing** | HMM-based haplotype inference |
| **Colocalization** (coloc) | Bayesian hypothesis testing |

These tools should **consume data from** vcf-pg-loader, not be implemented within it.

---

## Implementation Priority Recommendation

Based on impact and implementation complexity:

| Priority | Feature | Impact | Complexity |
|----------|---------|--------|------------|
| 1 | Imputation Quality Score (INFO/R²) | High | Low |
| 2 | GWAS Summary Statistics Loading | High | Medium |
| 3 | HapMap3 Variant Set | High | Low |
| 4 | PRS Weights Table | High | Medium |
| 5 | Genotype Data Storage | High | High |
| 6 | MAF/MAC Computation | Medium | Low |
| 7 | HWE P-value | Medium | Low |
| 8 | LD Block Annotation | Medium | Low |
| 9 | Call Rate Computation | Medium | Low (after genotypes) |
| 10 | Ancestry Annotation | Medium | Low |

---

## Summary

vcf-pg-loader can significantly support PRS research methodology by:

1. **Storing imputation quality metrics** for post-imputation QC filtering
2. **Loading GWAS summary statistics** as structured data for meta-analysis and PRS tools
3. **Maintaining reference catalogs** (HapMap3, LD blocks) for variant subsetting
4. **Storing PRS weights** to enable SQL-based score computation
5. **Capturing genotype data** with QC metrics (GQ, DP, AD) for sample-level filtering
6. **Computing derived QC metrics** (MAF, MAC, HWE, call rates) during loading

The tool remains a **data loading and storage layer**, leaving computational analysis (imputation, GWAS, PRS calculation) to specialized tools that query the database.
