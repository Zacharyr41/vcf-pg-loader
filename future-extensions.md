# Future Extensions: Embedding-Based Variant Similarity Search

This document explores how embedding-based similarity search could integrate with vcf-pg-loader, enabling AI-powered variant analysis capabilities.

## The Core Question: What Makes Variants "Similar"?

There are fundamentally different notions of variant similarity, each requiring different embedding strategies:

| Similarity Type | Input for Embedding | Model | Dimensions | Use Case |
|----------------|---------------------|-------|------------|----------|
| **Sequence context** | 100bp flanking + variant | Nucleotide Transformer | 768 | Find variants in similar genomic contexts |
| **Protein effect** | Mutant protein region | ESM-2 | 1280 | Find variants with similar protein impact |
| **Functional profile** | Gene + consequence + scores | Learned or engineered | 64-256 | Find functionally similar variants |
| **HGVS text** | "p.Arg248Trp" | Sentence transformer | 384 | Find variants with similar notation |

## How It Maps to the Current Schema

The `variants` table already has the building blocks:

```
┌─────────────────────────────────────────────────────────────────┐
│                        variants table                           │
├─────────────────────────────────────────────────────────────────┤
│ Core identity: chrom, pos, ref, alt                             │
│ ─────────────────────────────────────────────────────────────── │
│ Annotation fields that could inform embeddings:                 │
│   • gene, transcript, consequence, impact                       │
│   • hgvs_c, hgvs_p (variant notation - embeddable text)        │
│   • af_gnomad, cadd_phred, clinvar_sig (numeric features)      │
│   • is_coding, is_lof (binary flags)                           │
│   • info JSONB, vep_annotations JSONB (rich structured data)   │
└─────────────────────────────────────────────────────────────────┘
```

## The Partitioning Complication

The schema uses `PARTITION BY LIST (chrom)` with primary key `(chrom, variant_id)`. This means `variant_id` is **not globally unique** - it's only unique within a chromosome partition.

An embedding table has two design options:

### Option A: Composite foreign key

```sql
CREATE TABLE variant_embeddings (
    chrom chromosome_type NOT NULL,
    variant_id BIGINT NOT NULL,
    model_id INTEGER REFERENCES embedding_models(id),
    embedding vector(768),
    PRIMARY KEY (chrom, variant_id, model_id),
    FOREIGN KEY (chrom, variant_id) REFERENCES variants(chrom, variant_id)
);
```

- Maintains referential integrity
- Similarity queries must include `chrom` or scan all partitions

### Option B: Denormalized with variant coordinates (Recommended)

```sql
CREATE TABLE variant_embeddings (
    id BIGSERIAL PRIMARY KEY,
    chrom chromosome_type NOT NULL,
    pos BIGINT NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    model_id INTEGER,
    embedding vector(768)
);
CREATE INDEX idx_emb_hnsw ON variant_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX idx_emb_variant ON variant_embeddings (chrom, pos, ref, alt);
```

- No FK constraint (embedding table is independent)
- Can find similar variants across chromosomes in one query
- Joins back to `variants` via natural key

**Option B is better for similarity search** because the primary use case is "find similar variants anywhere in the genome."

## Query Patterns

### Basic similarity search

```sql
-- Given a variant, find similar ones
WITH query_emb AS (
    SELECT embedding FROM variant_embeddings
    WHERE chrom = 'chr17' AND pos = 41244000 AND ref = 'G' AND alt = 'A'
)
SELECT ve.chrom, ve.pos, ve.ref, ve.alt,
       v.gene, v.consequence, v.clinvar_sig,
       1 - (ve.embedding <=> (SELECT embedding FROM query_emb)) as similarity
FROM variant_embeddings ve
JOIN variants v ON ve.chrom = v.chrom AND ve.pos = v.pos
                AND ve.ref = v.ref AND ve.alt = v.alt
WHERE ve.embedding <=> (SELECT embedding FROM query_emb) < 0.3
ORDER BY ve.embedding <=> (SELECT embedding FROM query_emb)
LIMIT 20;
```

### Fuzzy annotation transfer

```sql
-- Find annotations for similar variants when exact match doesn't exist
WITH query_var AS (
    SELECT * FROM variant_embeddings
    WHERE chrom = 'chr1' AND pos = 12345 AND ref = 'A' AND alt = 'G'
),
similar_annotated AS (
    SELECT ve.*, a.af, a.cadd_score,
           1 - (ve.embedding <=> qv.embedding) as sim
    FROM variant_embeddings ve
    CROSS JOIN query_var qv
    JOIN anno_gnomad a ON ve.chrom = a.chrom AND ve.pos = a.pos
                       AND ve.ref = a.ref AND ve.alt = a.alt
    WHERE ve.embedding <=> qv.embedding < 0.2
    ORDER BY ve.embedding <=> qv.embedding
    LIMIT 5
)
SELECT AVG(af) as imputed_af, AVG(cadd_score) as imputed_cadd
FROM similar_annotated;
```

## Embedding Generation: The Reference Genome Problem

For sequence-based embeddings (the most biologically meaningful), you need genomic context:

```
                    Reference Genome (GRCh38)
                    ─────────────────────────
Position:  ...41243950                    41244050...
Sequence:  ...ACGTACGTACGTACGT[G]ACGTACGTACGTACGT...
                              ↓
                         Variant: G→A
                              ↓
           Extract: 50bp upstream + [REF or ALT] + 50bp downstream
                              ↓
                    Nucleotide Transformer
                              ↓
                    768-dim embedding
```

### Options for handling reference genome

1. **User provides FASTA path** - most flexible, user's responsibility
2. **Download on demand** - `vcf-pg-loader embed --download-reference`
3. **Use Ensembl REST API** - slower but no local storage
4. **Store context during load** - add `sequence_context` column to variants table

Option 1 is cleanest - consistent with requiring user to provide VCF path.

## The Simpler Path: Annotation-Based Embeddings

Create useful embeddings **without external data** using existing variant annotations:

```python
def create_annotation_embedding(v: VariantRecord) -> np.ndarray:
    """Create embedding from existing annotation fields."""
    features = []

    # Consequence encoding (50 VEP consequences → 50 dims)
    features.extend(one_hot(v.consequence, VEP_CONSEQUENCES))

    # Impact encoding (4 dims)
    features.extend(one_hot(v.impact, ['HIGH', 'MODERATE', 'LOW', 'MODIFIER']))

    # Variant type (3 dims)
    features.extend(one_hot(v.variant_type, ['snp', 'indel', 'mnp']))

    # Numeric features (normalized, 4 dims)
    features.append(min(v.af_gnomad or 0, 1.0))
    features.append((v.cadd_phred or 0) / 50.0)
    features.append(1.0 if v.is_coding else 0.0)
    features.append(1.0 if v.is_lof else 0.0)

    return np.array(features, dtype=np.float32)  # ~60 dimensions
```

This creates a **fully self-contained** similarity measure that answers: "which variants have similar functional profiles?"

## CLI Integration

Natural extension of the existing command structure:

```bash
# Generate embeddings for a loaded batch
vcf-pg-loader embed <batch-id> --model annotation  # Uses existing fields
vcf-pg-loader embed <batch-id> --model esm2 --reference /path/to/GRCh38.fa

# Find similar variants
vcf-pg-loader similar chr17:41244000:G:A --top 10 --min-similarity 0.8

# Annotate with fuzzy matching
vcf-pg-loader annotate <batch-id> --source gnomad --fuzzy --similarity 0.9

# Cluster variants
vcf-pg-loader cluster <batch-id> --method hdbscan --output clusters.tsv
```

## Integration with VariantAnnotator

The existing JOIN pattern in `annotator.py` extends naturally:

```python
# Current: exact match
LEFT JOIN anno_gnomad a ON v.chrom = a.chrom AND v.pos = a.pos ...

# Extended: similarity-aware
LEFT JOIN LATERAL (
    SELECT * FROM anno_gnomad a
    JOIN variant_embeddings ae ON a.chrom = ae.chrom AND a.pos = ae.pos ...
    WHERE ae.embedding <=> ve.embedding < $threshold
    ORDER BY ae.embedding <=> ve.embedding
    LIMIT 1
) a_fuzzy ON true
```

## Scale & Performance Considerations

| Metric | Current System | With Embeddings |
|--------|----------------|-----------------|
| Variants per WGS | ~5M | Same |
| Storage per variant | ~500 bytes | +3KB (768 * 4 bytes) |
| Total per genome | ~2.5GB | ~17GB |
| Index type | B-tree, GiST | +HNSW |
| HNSW memory | - | ~1.5x embedding size |
| Similarity query | - | <10ms with HNSW |

## What This Enables

1. **VUS classification support**: "This VUS has embedding similarity 0.92 to ClinVar pathogenic variants"

2. **Novel variant annotation**: "No exact match in gnomAD, but 3 similar variants have AF < 0.001"

3. **Cohort clustering**: "These 47 variants cluster together - shared functional mechanism?"

4. **Cross-sample deduplication**: "Variants from different callers with similarity > 0.99 are likely same variant"

5. **Rare disease prioritization**: "Rank candidate variants by similarity to known disease-causing mutations"

## The Minimal Useful Implementation

The smallest useful addition would be:

1. **pgvector extension** (one line in `schema.py`)
2. **Annotation-based embedding** (no external deps, ~100 lines)
3. **`similar` CLI command** (uses existing Typer patterns)

This gives similarity search without GPUs, reference genomes, or new infrastructure - just PostgreSQL.

## Phased Implementation Roadmap

### Phase 1: pgvector Foundation (3 days)

- Add `vector` extension to `schema.py:create_extensions()`
- Create `embedding_schema.py` with model registry and embeddings table
- Add `vcf-pg-loader embed <batch-id> --model annotation` CLI command
- Unit tests with mocked embeddings

### Phase 2: Similarity Search (1 week)

- Add `vcf-pg-loader similar <variant_id> --top 10` command
- Implement cosine similarity queries via pgvector operators
- Add `--threshold` and `--gene` filters
- Integration tests with testcontainers + pgvector

### Phase 3: Sequence-Based Embeddings (1-2 weeks)

- Add reference genome FASTA handling
- Integrate Nucleotide Transformer or ESM-2
- Add `--model esm2` and `--model nucleotide-transformer` options
- GPU inference support (optional)

### Phase 4: Advanced Features (2-4 weeks)

- Redis caching layer for embedding lookups
- Background workers (arq) for async embedding generation
- Fuzzy annotation matching in `annotate` command
- Variant clustering with HDBSCAN

## New Dependencies

```toml
# Phase 1-2 (minimal)
pgvector = ">=0.2.0"

# Phase 3 (sequence embeddings)
torch = ">=2.0.0"
transformers = ">=4.36.0"

# Phase 4 (optional infrastructure)
redis = ">=5.0.0"
arq = ">=0.25.0"
hdbscan = ">=0.8.33"
```

## References

- [pgvector](https://github.com/pgvector/pgvector) - Vector similarity search for PostgreSQL
- [ESM-2](https://github.com/facebookresearch/esm) - Protein language model
- [Nucleotide Transformer](https://github.com/instadeepai/nucleotide-transformer) - DNA foundation model
- [SHEPHERD](https://github.com/mims-harvard/shepherd) - Rare disease diagnosis with knowledge graphs
