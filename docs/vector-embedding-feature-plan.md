# Vector Embedding Features for vcf-pg-loader: Complete Feature Specification

## Executive Summary

This document outlines a comprehensive feature set for adding genomic machine learning capabilities to vcf-pg-loader. The features are designed to integrate with the Pe'er research ecosystem at Columbia/MSK, with specific attention to Itsik Pe'er's population genetics work, Dana Pe'er's single-cell methods, and Kushal Dey's variant-to-function frameworks.

The core insight driving this design: **variant embeddings become powerful when contextualized by population structure (IBD), cell state (metacells), and functional predictions (V2F scores)**. A VCF-to-PostgreSQL loader is uniquely positioned to unify these data types through SQL JOINs.

---

## Part 1: Core Embedding Infrastructure

### 1.1 DNABERT-2 Variant Embeddings

#### What This Feature Does

Every variant in a VCF file gets transformed into a 768-dimensional vector that captures its sequence context. This enables similarity-based queries ("find variants with similar sequence signatures") and serves as input features for downstream ML models.

The embedding process:
1. Extract ~1000bp flanking sequence around each variant from the reference genome
2. Create two sequences: one with REF allele, one with ALT allele
3. Pass both through DNABERT-2 (117M parameter transformer trained on multi-species genomes)
4. Store the **difference embedding** (alt_embedding - ref_embedding), which captures the variant's effect on sequence representation

#### Why DNABERT-2 Over Alternatives

| Model | Parameters | Context | Output Dim | Rationale |
|-------|-----------|---------|------------|-----------|
| **DNABERT-2** | 117M | ~10kb | 768 | Best balance of efficiency and performance; BPE tokenization handles variants naturally |
| Nucleotide Transformer | 500M-2.5B | 12kb | 1024 | Larger but diminishing returns for variant embedding |
| HyenaDNA | 1.6M-6.6M | 1M bp | 256 | Overkill context length; designed for ultra-long-range, not local variant effects |
| Enformer | 249M | 200kb | 3072 | Specialized for expression prediction, not general embeddings |

DNABERT-2's Byte Pair Encoding (BPE) tokenization is particularly advantageous for variants—it doesn't suffer from the k-mer boundary issues of older models, and a single nucleotide change doesn't shift the entire tokenization.

#### Architecture Integration

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Embedding Pipeline                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   VCF File ──► VCFStreamingParser ──► VariantRecord                     │
│                                              │                           │
│                                              ▼                           │
│   Reference ──► pyfaidx.Fasta ──► SequenceExtractor                     │
│   (hg38.fa)                              │                               │
│                                          ▼                               │
│                              ┌─────────────────────┐                     │
│                              │   ref_seq, alt_seq  │                     │
│                              └──────────┬──────────┘                     │
│                                         │                                │
│                                         ▼                                │
│                              ┌─────────────────────┐                     │
│                              │     DNABERT-2       │                     │
│                              │  (GPU-accelerated)  │                     │
│                              └──────────┬──────────┘                     │
│                                         │                                │
│                              ┌──────────┴──────────┐                     │
│                              ▼                     ▼                     │
│                         ref_embed            alt_embed                   │
│                         (768-dim)            (768-dim)                   │
│                              │                     │                     │
│                              └──────────┬──────────┘                     │
│                                         │                                │
│                                         ▼                                │
│                              diff_embed = alt - ref                      │
│                                         │                                │
│                                         ▼                                │
│                              ┌─────────────────────┐                     │
│                              │  pgvector column    │                     │
│                              │  variants.embedding │                     │
│                              └─────────────────────┘                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

#### New Module: `src/vcf_pg_loader/embedding/`

**`sequence_extractor.py`**
```python
from pyfaidx import Fasta
from dataclasses import dataclass

@dataclass
class VariantSequences:
    ref_seq: str
    alt_seq: str
    chrom: str
    center_pos: int
    flank_size: int

class SequenceExtractor:
    """Extract flanking sequences around variants for embedding generation."""

    def __init__(self, reference_path: str, flank_size: int = 500):
        self.reference = Fasta(reference_path)
        self.flank_size = flank_size

    def extract(self, chrom: str, pos: int, ref: str, alt: str) -> VariantSequences:
        """Extract ref and alt sequences centered on variant."""
        # Handle chr prefix normalization
        chrom_key = chrom.replace('chr', '') if chrom.startswith('chr') else chrom
        if chrom_key not in self.reference.keys():
            chrom_key = f'chr{chrom_key}'

        # 0-based coordinates for pyfaidx
        start = max(0, pos - 1 - self.flank_size)
        end = pos - 1 + len(ref) + self.flank_size

        full_seq = str(self.reference[chrom_key][start:end])

        # Position of variant within extracted sequence
        var_start = pos - 1 - start

        # Construct ref and alt sequences
        ref_seq = full_seq  # Already contains reference
        alt_seq = full_seq[:var_start] + alt + full_seq[var_start + len(ref):]

        return VariantSequences(
            ref_seq=ref_seq,
            alt_seq=alt_seq,
            chrom=chrom,
            center_pos=pos,
            flank_size=self.flank_size
        )
```

**`dnabert2.py`**
```python
import torch
from transformers import AutoTokenizer, AutoModel
import numpy as np

class DNABERT2Embedder:
    """Generate embeddings using DNABERT-2 foundation model."""

    MODEL_ID = "zhihan1996/DNABERT-2-117M"
    EMBEDDING_DIM = 768

    def __init__(self, device: str | None = None, batch_size: int = 32):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.batch_size = batch_size

        self.tokenizer = AutoTokenizer.from_pretrained(
            self.MODEL_ID, trust_remote_code=True
        )
        self.model = AutoModel.from_pretrained(
            self.MODEL_ID, trust_remote_code=True
        ).to(self.device)
        self.model.eval()

    def embed_sequence(self, sequence: str) -> np.ndarray:
        """Generate embedding for a single sequence using mean pooling."""
        with torch.no_grad():
            inputs = self.tokenizer(
                sequence,
                return_tensors="pt",
                truncation=True,
                max_length=512
            ).to(self.device)

            outputs = self.model(**inputs)
            hidden_states = outputs.last_hidden_state

            # Mean pooling (consistently outperforms CLS token)
            embedding = torch.mean(hidden_states[0], dim=0)
            return embedding.cpu().numpy()

    def embed_variant(self, ref_seq: str, alt_seq: str) -> np.ndarray:
        """Generate difference embedding for a variant."""
        ref_embed = self.embed_sequence(ref_seq)
        alt_embed = self.embed_sequence(alt_seq)
        return alt_embed - ref_embed

    def embed_batch(self, sequences: list[tuple[str, str]]) -> list[np.ndarray]:
        """Batch embedding generation for efficiency."""
        # Flatten to single list, process, then compute differences
        all_seqs = []
        for ref_seq, alt_seq in sequences:
            all_seqs.extend([ref_seq, alt_seq])

        embeddings = []
        with torch.no_grad():
            for i in range(0, len(all_seqs), self.batch_size):
                batch = all_seqs[i:i + self.batch_size]
                inputs = self.tokenizer(
                    batch,
                    return_tensors="pt",
                    padding=True,
                    truncation=True,
                    max_length=512
                ).to(self.device)

                outputs = self.model(**inputs)
                batch_embeds = torch.mean(outputs.last_hidden_state, dim=1)
                embeddings.extend(batch_embeds.cpu().numpy())

        # Reconstruct difference embeddings
        diff_embeddings = []
        for i in range(0, len(embeddings), 2):
            diff_embeddings.append(embeddings[i + 1] - embeddings[i])

        return diff_embeddings
```

#### Schema Changes

```sql
-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Add embedding column to variants table
ALTER TABLE variants ADD COLUMN embedding vector(768);

-- IVFFlat index for approximate nearest neighbor search
-- lists = sqrt(n_rows) is a good starting point
CREATE INDEX idx_variants_embedding_ivfflat
ON variants USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 1000);

-- For exact search on smaller result sets
CREATE INDEX idx_variants_embedding_hnsw
ON variants USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- Track embedding generation metadata
ALTER TABLE variant_load_audit ADD COLUMN embeddings_generated BOOLEAN DEFAULT FALSE;
ALTER TABLE variant_load_audit ADD COLUMN embedding_model VARCHAR(100);
ALTER TABLE variant_load_audit ADD COLUMN embedding_flank_size INTEGER;
```

#### CLI Integration

```bash
# Generate embeddings during load
vcf-pg-loader load sample.vcf.gz --embed --reference /path/to/hg38.fa

# Generate embeddings for already-loaded variants
vcf-pg-loader embed <load_batch_id> --reference /path/to/hg38.fa

# Options
vcf-pg-loader embed <load_batch_id> \
    --reference /path/to/hg38.fa \
    --flank-size 500 \
    --batch-size 64 \
    --device cuda:0
```

---

### 1.2 Similarity Search Queries

#### What This Feature Does

Once variants have embeddings, you can find variants with similar sequence contexts. This enables:
- Finding functionally similar variants across genes
- Identifying variants that might have similar effects despite different positions
- Clustering variants by sequence signature

#### Query Capabilities

**Find similar variants:**
```sql
-- Find 10 most similar variants to a given variant
SELECT v2.chrom, v2.pos, v2.ref, v2.alt, v2.gene,
       1 - (v1.embedding <=> v2.embedding) AS similarity
FROM variants v1, variants v2
WHERE v1.variant_id = $1
  AND v1.variant_id != v2.variant_id
ORDER BY v1.embedding <=> v2.embedding
LIMIT 10;
```

**Find variants similar to a query embedding:**
```sql
-- Find variants similar to a provided embedding vector
SELECT chrom, pos, ref, alt, gene, consequence,
       1 - (embedding <=> $1::vector) AS similarity
FROM variants
WHERE embedding IS NOT NULL
ORDER BY embedding <=> $1::vector
LIMIT 20;
```

**Cluster variants by embedding:**
```sql
-- Find variants within a similarity threshold
SELECT v1.variant_id, v2.variant_id,
       1 - (v1.embedding <=> v2.embedding) AS similarity
FROM variants v1
CROSS JOIN variants v2
WHERE v1.variant_id < v2.variant_id
  AND v1.embedding <=> v2.embedding < 0.3  -- cosine distance < 0.3
  AND v1.gene = 'BRCA1';
```

#### Python API

```python
class EmbeddingQueryMixin:
    """Mixin for embedding-based variant queries."""

    async def find_similar_variants(
        self,
        variant_id: int,
        limit: int = 10,
        min_similarity: float = 0.5,
        same_gene: bool = False
    ) -> list[dict]:
        """Find variants with similar embeddings."""
        gene_filter = "AND v2.gene = v1.gene" if same_gene else ""

        query = f"""
            SELECT v2.chrom, v2.pos, v2.ref, v2.alt,
                   v2.gene, v2.consequence,
                   1 - (v1.embedding <=> v2.embedding) AS similarity
            FROM variants v1, variants v2
            WHERE v1.variant_id = $1
              AND v1.variant_id != v2.variant_id
              AND v2.embedding IS NOT NULL
              {gene_filter}
              AND (1 - (v1.embedding <=> v2.embedding)) >= $2
            ORDER BY v1.embedding <=> v2.embedding
            LIMIT $3
        """
        rows = await self.conn.fetch(query, variant_id, min_similarity, limit)
        return [dict(row) for row in rows]

    async def find_variants_by_embedding(
        self,
        embedding: np.ndarray,
        limit: int = 10,
        filters: dict | None = None
    ) -> list[dict]:
        """Find variants similar to a provided embedding vector."""
        where_clauses = ["embedding IS NOT NULL"]
        params = [embedding.tolist()]

        if filters:
            if "gene" in filters:
                params.append(filters["gene"])
                where_clauses.append(f"gene = ${len(params)}")
            if "impact" in filters:
                params.append(filters["impact"])
                where_clauses.append(f"impact = ${len(params)}")

        params.append(limit)

        query = f"""
            SELECT chrom, pos, ref, alt, gene, consequence, impact,
                   1 - (embedding <=> $1::vector) AS similarity
            FROM variants
            WHERE {' AND '.join(where_clauses)}
            ORDER BY embedding <=> $1::vector
            LIMIT ${len(params)}
        """
        rows = await self.conn.fetch(query, *params)
        return [dict(row) for row in rows]
```

---

## Part 2: Population Genetics Integration

### 2.1 IBD Segment Storage and Queries

#### Why This Matters for the Pe'er Ecosystem

Itsik Pe'er's foundational work on GERMLINE and IBD detection provides crucial population context for variant analysis. IBD segments reveal:
- **Shared ancestry**: Variants in IBD regions are inherited from a common ancestor
- **Population structure**: IBD patterns distinguish populations and subpopulations
- **Rare variant sharing**: IBD explains why unrelated individuals share rare variants

By storing IBD segments alongside variants, we can answer questions like:
- "Are these two individuals' shared variants due to IBD or independent mutation?"
- "Which variants in this patient are in regions of extended homozygosity?"
- "What's the IBD-weighted frequency of this variant in the cohort?"

#### IBD Data Sources

| Tool | Output Format | Key Fields |
|------|--------------|------------|
| GERMLINE | `.match` | sample1, sample2, chrom, start, end, cM_length, n_snps |
| hap-IBD | `.ibd.gz` | id1, hap1, id2, hap2, chrom, start, end, cM_length |
| iLASH | `.ibd` | Similar to hap-IBD |
| TRUFFLE | `.segments` | sample pair, segment coordinates |

#### Schema Design

```sql
-- IBD segments table
CREATE TABLE ibd_segments (
    segment_id BIGSERIAL PRIMARY KEY,

    -- Sample identifiers (link to samples table)
    sample1_id VARCHAR(255) NOT NULL,
    sample1_haplotype SMALLINT,  -- 1 or 2, NULL if unphased
    sample2_id VARCHAR(255) NOT NULL,
    sample2_haplotype SMALLINT,

    -- Genomic coordinates
    chrom chromosome_type NOT NULL,
    start_pos BIGINT NOT NULL,
    end_pos BIGINT NOT NULL,
    segment_range int8range NOT NULL,  -- For GiST index

    -- Segment metrics
    length_cm REAL NOT NULL,
    length_bp BIGINT GENERATED ALWAYS AS (end_pos - start_pos) STORED,
    n_markers INTEGER,

    -- Metadata
    source VARCHAR(50) NOT NULL,  -- 'germline', 'hap_ibd', 'ilash'
    source_version VARCHAR(50),
    quality_score REAL,

    -- Audit
    load_batch_id UUID NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Spatial index for region queries
CREATE INDEX idx_ibd_region
ON ibd_segments USING GiST (chrom, segment_range);

-- Sample pair lookups
CREATE INDEX idx_ibd_sample_pair
ON ibd_segments (sample1_id, sample2_id);

-- Reverse pair lookups (IBD is symmetric)
CREATE INDEX idx_ibd_sample_pair_reverse
ON ibd_segments (sample2_id, sample1_id);

-- Length-based queries (find long IBD segments)
CREATE INDEX idx_ibd_length
ON ibd_segments (length_cm DESC);

-- HBD (homozygosity-by-descent) within individuals
CREATE TABLE hbd_segments (
    segment_id BIGSERIAL PRIMARY KEY,
    sample_id VARCHAR(255) NOT NULL,
    chrom chromosome_type NOT NULL,
    start_pos BIGINT NOT NULL,
    end_pos BIGINT NOT NULL,
    segment_range int8range NOT NULL,
    length_cm REAL NOT NULL,
    source VARCHAR(50) NOT NULL,
    load_batch_id UUID NOT NULL
);

CREATE INDEX idx_hbd_region
ON hbd_segments USING GiST (chrom, segment_range);
```

#### IBD Parser Module

```python
# src/vcf_pg_loader/ibd/parser.py

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
import gzip

@dataclass
class IBDSegment:
    sample1_id: str
    sample1_hap: int | None
    sample2_id: str
    sample2_hap: int | None
    chrom: str
    start_pos: int
    end_pos: int
    length_cm: float
    n_markers: int | None

class GERMLINEParser:
    """Parse GERMLINE .match output files."""

    def parse(self, path: Path) -> Iterator[IBDSegment]:
        opener = gzip.open if str(path).endswith('.gz') else open
        with opener(path, 'rt') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                fields = line.strip().split('\t')
                # GERMLINE format: fam1 id1 fam2 id2 chrom start end snp1 snp2 ... cM
                yield IBDSegment(
                    sample1_id=fields[1],
                    sample1_hap=None,
                    sample2_id=fields[3],
                    sample2_hap=None,
                    chrom=fields[4],
                    start_pos=int(fields[5]),
                    end_pos=int(fields[6]),
                    length_cm=float(fields[-1]),
                    n_markers=int(fields[9]) if len(fields) > 9 else None
                )

class HapIBDParser:
    """Parse hap-IBD .ibd.gz output files."""

    def parse(self, path: Path) -> Iterator[IBDSegment]:
        with gzip.open(path, 'rt') as f:
            for line in f:
                fields = line.strip().split('\t')
                # hap-IBD format: id1 hap1 id2 hap2 chrom start end cM
                yield IBDSegment(
                    sample1_id=fields[0],
                    sample1_hap=int(fields[1]),
                    sample2_id=fields[2],
                    sample2_hap=int(fields[3]),
                    chrom=fields[4],
                    start_pos=int(fields[5]),
                    end_pos=int(fields[6]),
                    length_cm=float(fields[7]),
                    n_markers=None
                )
```

#### IBD-Aware Queries

```python
# src/vcf_pg_loader/ibd/queries.py

class IBDQueryMixin:
    """Mixin for IBD-aware variant queries."""

    async def get_variants_in_ibd_region(
        self,
        sample1: str,
        sample2: str,
        min_length_cm: float = 2.0
    ) -> list[dict]:
        """Get variants shared between two samples that fall within IBD segments."""
        query = """
            SELECT DISTINCT v.chrom, v.pos, v.ref, v.alt, v.gene,
                   i.length_cm, i.start_pos AS ibd_start, i.end_pos AS ibd_end
            FROM variants v
            JOIN ibd_segments i ON v.chrom = i.chrom
                AND v.pos_range && i.segment_range
            WHERE ((i.sample1_id = $1 AND i.sample2_id = $2)
                OR (i.sample1_id = $2 AND i.sample2_id = $1))
              AND i.length_cm >= $3
              AND v.sample_id IN ($1, $2)
            ORDER BY v.chrom, v.pos
        """
        return [dict(r) for r in await self.conn.fetch(query, sample1, sample2, min_length_cm)]

    async def get_ibd_context_for_variant(
        self,
        chrom: str,
        pos: int,
        sample_id: str
    ) -> list[dict]:
        """Find all IBD segments containing this variant position for a sample."""
        query = """
            SELECT sample1_id, sample2_id, start_pos, end_pos, length_cm, source
            FROM ibd_segments
            WHERE chrom = $1
              AND segment_range @> $2
              AND (sample1_id = $3 OR sample2_id = $3)
            ORDER BY length_cm DESC
        """
        return [dict(r) for r in await self.conn.fetch(query, chrom, pos, sample_id)]

    async def get_hbd_variants(
        self,
        sample_id: str,
        min_length_cm: float = 1.0
    ) -> list[dict]:
        """Get variants in runs of homozygosity for a sample."""
        query = """
            SELECT v.chrom, v.pos, v.ref, v.alt, v.gene,
                   h.length_cm, h.start_pos AS hbd_start, h.end_pos AS hbd_end
            FROM variants v
            JOIN hbd_segments h ON v.chrom = h.chrom
                AND v.pos_range && h.segment_range
            WHERE v.sample_id = $1
              AND h.sample_id = $1
              AND h.length_cm >= $2
            ORDER BY h.length_cm DESC, v.pos
        """
        return [dict(r) for r in await self.conn.fetch(query, sample_id, min_length_cm)]

    async def compute_ibd_weighted_frequency(
        self,
        chrom: str,
        pos: int,
        ref: str,
        alt: str
    ) -> dict:
        """Compute allele frequency accounting for IBD sharing.

        Standard AF overcounts variants in highly related individuals.
        IBD-weighted AF adjusts for this by identifying IBD clusters.
        """
        # This is a simplified version - full implementation would use
        # connected components of IBD graph
        query = """
            WITH variant_carriers AS (
                SELECT DISTINCT sample_id
                FROM variants
                WHERE chrom = $1 AND pos = $2 AND ref = $3 AND alt = $4
            ),
            ibd_pairs AS (
                SELECT DISTINCT
                    LEAST(sample1_id, sample2_id) AS s1,
                    GREATEST(sample1_id, sample2_id) AS s2
                FROM ibd_segments
                WHERE chrom = $1 AND segment_range @> $2
            ),
            carrier_ibd AS (
                SELECT vc1.sample_id AS carrier1, vc2.sample_id AS carrier2
                FROM variant_carriers vc1
                JOIN variant_carriers vc2 ON vc1.sample_id < vc2.sample_id
                JOIN ibd_pairs ip ON
                    (ip.s1 = vc1.sample_id AND ip.s2 = vc2.sample_id)
            )
            SELECT
                COUNT(DISTINCT sample_id) AS raw_carrier_count,
                COUNT(DISTINCT sample_id) - COUNT(DISTINCT carrier1) AS adjusted_count
            FROM variant_carriers
            LEFT JOIN carrier_ibd ON sample_id = carrier1
        """
        row = await self.conn.fetchrow(query, chrom, pos, ref, alt)
        return dict(row)
```

#### CLI Commands

```bash
# Load IBD segments
vcf-pg-loader load-ibd segments.ibd.gz --source hap-ibd --version 1.0
vcf-pg-loader load-ibd matches.match.gz --source germline

# Query IBD relationships
vcf-pg-loader query ibd-shared SAMPLE001 SAMPLE002 --min-length 3.0
vcf-pg-loader query hbd-regions SAMPLE001 --min-length 2.0

# Find variants in IBD regions
vcf-pg-loader query variants-in-ibd SAMPLE001 SAMPLE002 --output variants.tsv
```

---

### 2.2 Population Structure Metadata

#### What This Feature Does

Store population assignments and ancestry principal components for each sample, enabling:
- Population-stratified queries
- Ancestry-matched control selection
- Population-specific allele frequency calculations

#### Schema

```sql
-- Extend samples table with population data
ALTER TABLE samples ADD COLUMN population VARCHAR(50);
ALTER TABLE samples ADD COLUMN superpopulation VARCHAR(50);
ALTER TABLE samples ADD COLUMN ancestry_pcs REAL[];  -- First N principal components

-- Population reference data
CREATE TABLE population_reference (
    population_code VARCHAR(50) PRIMARY KEY,
    population_name VARCHAR(255),
    superpopulation VARCHAR(50),
    description TEXT,
    sample_count INTEGER
);

-- Pre-computed population allele frequencies
CREATE TABLE population_allele_frequencies (
    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    population VARCHAR(50) NOT NULL,
    allele_frequency REAL NOT NULL,
    allele_count INTEGER,
    allele_number INTEGER,
    PRIMARY KEY (chrom, pos, ref, alt, population)
);

CREATE INDEX idx_pop_af_lookup
ON population_allele_frequencies (chrom, pos, ref, alt);
```

#### Population-Stratified Queries

```python
async def find_similar_variants_in_population(
    self,
    variant_id: int,
    population: str,
    limit: int = 10
) -> list[dict]:
    """Find similar variants, restricted to samples from a specific population."""
    query = """
        SELECT v2.chrom, v2.pos, v2.ref, v2.alt, v2.gene,
               1 - (v1.embedding <=> v2.embedding) AS similarity,
               s.population
        FROM variants v1
        JOIN variants v2 ON v1.variant_id != v2.variant_id
        JOIN samples s ON v2.sample_id = s.external_id
        WHERE v1.variant_id = $1
          AND s.population = $2
          AND v2.embedding IS NOT NULL
        ORDER BY v1.embedding <=> v2.embedding
        LIMIT $3
    """
    return [dict(r) for r in await self.conn.fetch(query, variant_id, population, limit)]

async def get_population_specific_af(
    self,
    chrom: str,
    pos: int,
    ref: str,
    alt: str
) -> dict[str, float]:
    """Get allele frequencies across all populations."""
    query = """
        SELECT population, allele_frequency
        FROM population_allele_frequencies
        WHERE chrom = $1 AND pos = $2 AND ref = $3 AND alt = $4
    """
    rows = await self.conn.fetch(query, chrom, pos, ref, alt)
    return {r['population']: r['allele_frequency'] for r in rows}
```

---

## Part 3: Variant-to-Function Integration

### 3.1 Kushal Dey V2F Score Framework

#### Why This Matters

Kushal Dey's lab has developed a **consensus variant-to-function (V2F) score** that integrates multiple data types to predict which variants affect gene function. This is directly relevant to interpreting VCF data in the context of disease.

Key publications:
- "A consensus variant-to-function score" (bioRxiv 2024, in review at Nature Genetics)
- "Linking regulatory variants to target genes" (Nature Genetics 2025)

By storing V2F scores alongside variants, users can:
- Prioritize variants by predicted functional impact
- Link noncoding variants to their target genes
- Filter variants by cell-type-specific effects

#### Schema

```sql
-- V2F scores table (follows annotation pattern)
CREATE TABLE anno_v2f_scores (
    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,

    -- Core V2F score
    v2f_consensus REAL,           -- Main consensus score
    v2f_percentile REAL,          -- Genome-wide percentile

    -- Target gene predictions
    target_gene VARCHAR(100),      -- Predicted target gene
    gene_distance INTEGER,         -- Distance to TSS
    link_score REAL,              -- Variant-gene link score

    -- Cell type context (nullable - some variants are ubiquitous)
    cell_type VARCHAR(100),
    tissue VARCHAR(100),

    -- Component scores (for interpretability)
    component_scores JSONB,        -- {"abc_score": 0.5, "cicero": 0.3, ...}

    -- Evidence metadata
    evidence_sources TEXT[],       -- ['ABC', 'CICERO', 'EpiMap', ...]
    n_evidence_sources INTEGER,

    PRIMARY KEY (chrom, pos, ref, alt, COALESCE(cell_type, ''))
);

CREATE INDEX idx_v2f_gene ON anno_v2f_scores (target_gene);
CREATE INDEX idx_v2f_score ON anno_v2f_scores (v2f_consensus DESC);
CREATE INDEX idx_v2f_cell_type ON anno_v2f_scores (cell_type) WHERE cell_type IS NOT NULL;
```

#### Integration with Existing Annotation System

The V2F loader follows the same pattern as `annotation_loader.py`:

```python
# src/vcf_pg_loader/v2f/loader.py

class V2FScoreLoader:
    """Load V2F scores as an annotation source."""

    SOURCE_NAME = "v2f_consensus"

    async def load_v2f_scores(
        self,
        conn: asyncpg.Connection,
        score_file: Path,
        version: str | None = None
    ) -> int:
        """Load V2F scores from TSV file."""
        # Register as annotation source
        await self.schema_manager.register_source(
            conn,
            source_name=self.SOURCE_NAME,
            fields=self._get_field_configs(),
            source_type="functional",
            version=version
        )

        # Stream load scores
        count = 0
        async with conn.transaction():
            # ... batch loading logic
            pass

        return count
```

#### Query Integration

```python
async def get_high_impact_v2f_variants(
    self,
    gene: str,
    min_score: float = 0.5,
    cell_type: str | None = None
) -> list[dict]:
    """Find variants with high V2F scores affecting a gene."""
    query = """
        SELECT v.chrom, v.pos, v.ref, v.alt, v.consequence,
               s.v2f_consensus, s.target_gene, s.cell_type,
               s.evidence_sources
        FROM variants v
        JOIN anno_v2f_scores s ON
            v.chrom = s.chrom AND v.pos = s.pos
            AND v.ref = s.ref AND v.alt = s.alt
        WHERE s.target_gene = $1
          AND s.v2f_consensus >= $2
          AND ($3::text IS NULL OR s.cell_type = $3)
        ORDER BY s.v2f_consensus DESC
    """
    return [dict(r) for r in await self.conn.fetch(query, gene, min_score, cell_type)]
```

---

### 3.2 Enformer Long-Range Predictions

#### Why This Matters

Enformer (DeepMind, Nature Methods 2021) predicts gene expression from sequence using a 200kb context window. It captures **long-range enhancer-promoter interactions** that determine which noncoding variants affect gene expression.

For regulatory variant interpretation, Enformer predictions answer:
- "Does this variant change predicted expression in any tissue?"
- "Which tissues are most affected by this variant?"
- "Is this variant in a predicted enhancer?"

#### Data Source

DeepMind provides **precomputed Enformer predictions** for all variants in the 1000 Genomes Project. This is the practical approach (running Enformer on-demand is computationally expensive).

#### Schema

```sql
-- Enformer predictions (compact representation)
CREATE TABLE anno_enformer (
    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,

    -- Summary statistics (for filtering)
    max_abs_effect REAL,          -- Max |effect| across all tracks
    n_significant_tracks INTEGER, -- Tracks with |effect| > threshold

    -- Tissue-level effects (selected important tissues)
    effect_liver REAL,
    effect_brain REAL,
    effect_heart REAL,
    effect_blood REAL,
    effect_lung REAL,
    effect_kidney REAL,

    -- Full predictions (optional, for detailed analysis)
    -- 5313 tracks is too many for columns; store as array or JSONB
    track_effects REAL[],         -- All 5313 track effects

    -- Metadata
    enformer_version VARCHAR(20),

    PRIMARY KEY (chrom, pos, ref, alt)
);

-- Index for finding high-effect variants
CREATE INDEX idx_enformer_max_effect
ON anno_enformer (max_abs_effect DESC);

-- Partial index for variants with strong tissue effects
CREATE INDEX idx_enformer_liver_effect
ON anno_enformer (effect_liver)
WHERE ABS(effect_liver) > 0.1;
```

#### Query Examples

```python
async def get_expression_altering_variants(
    self,
    gene: str,
    tissue: str = "liver",
    min_effect: float = 0.1
) -> list[dict]:
    """Find variants predicted to alter expression in a tissue."""
    effect_column = f"effect_{tissue.lower()}"

    query = f"""
        SELECT v.chrom, v.pos, v.ref, v.alt, v.gene,
               e.{effect_column} AS predicted_effect,
               e.max_abs_effect
        FROM variants v
        JOIN anno_enformer e ON
            v.chrom = e.chrom AND v.pos = e.pos
            AND v.ref = e.ref AND v.alt = e.alt
        WHERE v.gene = $1
          AND ABS(e.{effect_column}) >= $2
        ORDER BY ABS(e.{effect_column}) DESC
    """
    return [dict(r) for r in await self.conn.fetch(query, gene, min_effect)]
```

---

## Part 4: Single-Cell Context Integration

### 4.1 SEACells Metacell Linkage

#### Why This Matters

Dana Pe'er's SEACells algorithm identifies **metacells**—groups of cells sharing transcriptional states. By linking variants to metacells through their target genes, we can answer:

- "In which cell states is this variant's target gene expressed?"
- "Does this variant affect a gene specific to a particular cell type?"
- "Which metacells should I examine for this variant's effects?"

This is particularly powerful when combined with V2F scores that predict variant-gene links.

#### Schema

```sql
-- Metacell definitions
CREATE TABLE metacells (
    metacell_id VARCHAR(100) PRIMARY KEY,
    dataset_id VARCHAR(100) NOT NULL,

    -- Cell type annotation
    cell_type VARCHAR(100),
    cell_type_confidence REAL,

    -- Tissue/condition context
    tissue VARCHAR(100),
    condition VARCHAR(100),
    sample_source VARCHAR(255),

    -- Metacell embedding (SEACells diffusion components)
    embedding vector(50),

    -- Size metrics
    n_cells INTEGER,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Gene expression per metacell
CREATE TABLE metacell_expression (
    metacell_id VARCHAR(100) NOT NULL REFERENCES metacells(metacell_id),
    gene VARCHAR(100) NOT NULL,

    -- Expression metrics
    mean_expression REAL NOT NULL,
    pct_expressed REAL,           -- % of cells expressing
    specificity_score REAL,       -- How specific to this metacell

    PRIMARY KEY (metacell_id, gene)
);

CREATE INDEX idx_metacell_expr_gene ON metacell_expression (gene);
CREATE INDEX idx_metacell_expr_high ON metacell_expression (mean_expression DESC);

-- ATAC accessibility per metacell (optional, for multiome data)
CREATE TABLE metacell_accessibility (
    metacell_id VARCHAR(100) NOT NULL REFERENCES metacells(metacell_id),
    peak_id VARCHAR(100) NOT NULL,  -- chr:start-end
    chrom chromosome_type NOT NULL,
    peak_range int8range NOT NULL,
    accessibility_score REAL NOT NULL,

    PRIMARY KEY (metacell_id, peak_id)
);

CREATE INDEX idx_metacell_atac_region
ON metacell_accessibility USING GiST (chrom, peak_range);
```

#### Query: Variant Cell Context

```python
async def get_variant_cell_context(
    self,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
    min_expression: float = 1.0
) -> list[dict]:
    """Find metacells where a variant's target gene is expressed.

    Combines variant -> V2F target gene -> metacell expression.
    """
    query = """
        WITH variant_genes AS (
            -- Get target genes from V2F scores
            SELECT DISTINCT target_gene
            FROM anno_v2f_scores
            WHERE chrom = $1 AND pos = $2 AND ref = $3 AND alt = $4
              AND target_gene IS NOT NULL

            UNION

            -- Also include directly annotated gene
            SELECT gene
            FROM variants
            WHERE chrom = $1 AND pos = $2 AND ref = $3 AND alt = $4
              AND gene IS NOT NULL
        )
        SELECT m.metacell_id, m.cell_type, m.tissue, m.condition,
               e.gene, e.mean_expression, e.specificity_score
        FROM variant_genes vg
        JOIN metacell_expression e ON e.gene = vg.target_gene
        JOIN metacells m ON m.metacell_id = e.metacell_id
        WHERE e.mean_expression >= $5
        ORDER BY e.specificity_score DESC, e.mean_expression DESC
    """
    return [dict(r) for r in await self.conn.fetch(
        query, chrom, pos, ref, alt, min_expression
    )]

async def find_variants_affecting_cell_type(
    self,
    cell_type: str,
    min_v2f_score: float = 0.3,
    min_expression: float = 2.0
) -> list[dict]:
    """Find variants predicted to affect genes important in a cell type."""
    query = """
        SELECT DISTINCT v.chrom, v.pos, v.ref, v.alt,
               s.target_gene, s.v2f_consensus,
               m.cell_type, e.mean_expression, e.specificity_score
        FROM variants v
        JOIN anno_v2f_scores s ON
            v.chrom = s.chrom AND v.pos = s.pos
            AND v.ref = s.ref AND v.alt = s.alt
        JOIN metacell_expression e ON e.gene = s.target_gene
        JOIN metacells m ON m.metacell_id = e.metacell_id
        WHERE m.cell_type = $1
          AND s.v2f_consensus >= $2
          AND e.mean_expression >= $3
        ORDER BY s.v2f_consensus * e.specificity_score DESC
        LIMIT 100
    """
    return [dict(r) for r in await self.conn.fetch(
        query, cell_type, min_v2f_score, min_expression
    )]
```

#### Metacell Similarity via Embeddings

```python
async def find_similar_metacells(
    self,
    metacell_id: str,
    limit: int = 10
) -> list[dict]:
    """Find metacells with similar transcriptional states."""
    query = """
        SELECT m2.metacell_id, m2.cell_type, m2.tissue,
               1 - (m1.embedding <=> m2.embedding) AS similarity
        FROM metacells m1, metacells m2
        WHERE m1.metacell_id = $1
          AND m1.metacell_id != m2.metacell_id
          AND m2.embedding IS NOT NULL
        ORDER BY m1.embedding <=> m2.embedding
        LIMIT $2
    """
    return [dict(r) for r in await self.conn.fetch(query, metacell_id, limit)]
```

---

## Part 5: Unified Query Interface

### Combined Analysis Queries

The power of storing all these data types together is the ability to write unified queries that span variants, embeddings, population structure, functional predictions, and cell context.

#### Example: Complete Variant Context Report

```python
async def get_complete_variant_context(
    self,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
    sample_id: str | None = None
) -> dict:
    """Get comprehensive context for a variant.

    Combines:
    - Basic variant annotation
    - Similar variants (by embedding)
    - IBD context (if sample provided)
    - V2F functional predictions
    - Enformer expression predictions
    - Cell type context
    """
    result = {}

    # Basic info
    result['variant'] = await self._get_variant_info(chrom, pos, ref, alt)

    # Embedding-based similar variants
    if result['variant'].get('embedding'):
        result['similar_variants'] = await self.find_similar_variants(
            result['variant']['variant_id'], limit=5
        )

    # IBD context
    if sample_id:
        result['ibd_segments'] = await self.get_ibd_context_for_variant(
            chrom, pos, sample_id
        )

    # V2F predictions
    result['v2f_predictions'] = await self.conn.fetch("""
        SELECT target_gene, v2f_consensus, cell_type, evidence_sources
        FROM anno_v2f_scores
        WHERE chrom = $1 AND pos = $2 AND ref = $3 AND alt = $4
        ORDER BY v2f_consensus DESC
    """, chrom, pos, ref, alt)

    # Enformer predictions
    result['enformer'] = await self.conn.fetchrow("""
        SELECT max_abs_effect, effect_liver, effect_brain,
               effect_heart, effect_blood
        FROM anno_enformer
        WHERE chrom = $1 AND pos = $2 AND ref = $3 AND alt = $4
    """, chrom, pos, ref, alt)

    # Cell context
    result['cell_context'] = await self.get_variant_cell_context(
        chrom, pos, ref, alt
    )

    return result
```

---

## Implementation Roadmap

### Phase 1: Core Infrastructure (Weeks 1-3)
1. pgvector extension integration in schema.py
2. pyfaidx sequence extractor module
3. DNABERT-2 embedding generator with GPU batching
4. Embedding storage and basic similarity search
5. CLI commands: `--embed`, `vcf-pg-loader embed`

### Phase 2: Population Genetics (Weeks 4-5)
6. IBD segment schema and parsers (GERMLINE, hap-IBD)
7. IBD loader with batch support
8. IBD-aware variant queries
9. Population metadata on samples table
10. CLI commands: `load-ibd`, `query ibd-shared`

### Phase 3: Functional Predictions (Weeks 6-7)
11. V2F score schema and loader
12. Enformer prediction schema and loader
13. Integrated functional queries
14. CLI commands: `load-v2f`, `load-enformer`

### Phase 4: Single-Cell Integration (Weeks 8-9)
15. Metacell schema and loader
16. Expression matrix loader
17. Variant-to-cell-context queries
18. Metacell similarity search

### Phase 5: Polish and Documentation (Week 10)
19. Unified query interface
20. Performance optimization (index tuning)
21. Documentation and examples
22. Test coverage

---

## Dependency Additions

```toml
# pyproject.toml additions
[project.optional-dependencies]
embedding = [
    "torch>=2.0.0",
    "transformers>=4.40.0",
    "pyfaidx>=0.8.0",
    "pgvector>=0.3.0",
    "kipoiseq>=0.7.0",
]
```

---

## Research Project Alignment Matrix

| Feature | Itsik Pe'er (Pop Gen) | Dana Pe'er (Single-Cell) | Kushal Dey (V2F) |
|---------|:---------------------:|:------------------------:|:----------------:|
| DNABERT-2 embeddings | ✓ | ✓ | ✓ |
| Similarity search | ✓ | ✓ | ✓ |
| IBD segment storage | **★★★** | | ✓ |
| IBD-aware queries | **★★★** | | ✓ |
| Population stratification | **★★★** | | ✓ |
| V2F scores | | ✓ | **★★★** |
| Enformer predictions | | ✓ | **★★★** |
| Metacell integration | | **★★★** | ✓ |
| Cell-type context | | **★★★** | ✓ |

**★★★** = Core to that lab's research focus

---

## Conclusion

This feature set transforms vcf-pg-loader from a VCF loading tool into a **genomic knowledge graph** that unifies:

1. **Sequence representation** (DNABERT-2 embeddings)
2. **Population structure** (IBD segments, ancestry)
3. **Functional predictions** (V2F scores, Enformer)
4. **Cellular context** (SEACells metacells)

The PostgreSQL + pgvector foundation enables efficient querying across all these dimensions using standard SQL, with the annotation pattern already established in the codebase providing a template for extension.
