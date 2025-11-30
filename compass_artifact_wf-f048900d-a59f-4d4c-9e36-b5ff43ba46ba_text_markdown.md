# TDD strategy for a VCF-to-PostgreSQL loader addressing nf-core gaps

A production-ready VCF-to-PostgreSQL loader targeting **500K+ variants/second** must build its test suite around three pillars: edge cases derived from real documented bugs (vcf2db Number=A handling, GEMINI multi-allelic failures), validation against GIAB reference materials (HG002 v4.2.1 with 3.37M SNVs, 525K indels), and performance benchmarks grounded in published genomics tool comparisons (GenESysV achieved 694 variants/second on 1000 Genomes data; your 500K target requires PostgreSQL COPY optimization and parallel loading).

The nf-core ecosystem provides immediate test infrastructure: minimal VCFs from `nf-core/test-datasets` for unit tests, sarek's germline pipeline configurations for integration tests, and raredisease's trio structures for family-aware query validation—all with exact URLs for CI reproducibility.

## nf-core test-datasets provide immediate CI infrastructure

The nf-core test-datasets repository follows the principle "as small as possible, as large as necessary," offering graduated complexity for TDD stages.

**Unit test fixtures (smallest, fastest):**
```
https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/sarscov2/illumina/vcf/test.vcf
https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/sarscov2/illumina/vcf/test.vcf.gz
```
These SARS-CoV-2 VCFs contain ~10-50 variants against a 30kb genome, enabling sub-second test execution for parsing correctness.

**Integration test fixtures (human variants):**
```
https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/dbsnp_146.hg38.vcf.gz
https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/gnomAD.r2.1.1.vcf.gz
https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/mills_and_1000G.indels.vcf.gz
https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/genome/vcf/syntheticvcf_short.vcf.gz
```

The **nf-core/sarek** pipeline uses these exact files in its CI test profiles, configured at `conf/test.config`:
- `dbsnp`: dbSNP v146 (known variant reference, rsID handling)
- `germline_resource`: gnomAD r2.1.1 (population frequency fields)
- `known_indels`: Mills and 1000G (indel normalization testing)

For family analysis, **nf-core/raredisease** provides trio samplesheet structures expecting pedigree columns (`paternal_id`, `maternal_id`, `case_id`) that your loader's schema must support. The pipeline's test data includes rank model configurations (`rank_model_snv.ini`, `rank_model_sv.ini`) demonstrating compound heterozygosity scoring requirements.

## GIAB v4.2.1 provides ground truth for validation assertions

The Genome in a Bottle Ashkenazi Trio (HG002, HG003, HG004) is the gold standard for variant database validation, with v4.2.1 representing the current recommended benchmark.

**Exact download URLs:**
```
# HG002 (son) - primary benchmark sample
https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz

# High-confidence regions
https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed
```

**Expected variant counts for test assertions:**

| Sample | SNVs | Indels | Total |
|--------|------|--------|-------|
| HG002 | 3,367,208 | 525,545 | ~3.89M |
| HG003 | 3,430,611 | 569,180 | ~4.0M |
| HG004 | 3,454,689 | 576,301 | ~4.03M |

**Mendelian consistency test targets:** The Ashkenazi trio shows only **2,502 violations out of 4,968,730 variants (0.05%)**. After loading all three samples, a correct implementation should identify ~1,177 SNV and ~284 indel potential de novo mutations, with remaining violations (~715) predominantly indels in segmental duplications. This provides a concrete assertion: `mendelian_error_rate < 0.001` after quality filtering.

Version v4.2.1 adds **336,713 SNVs and 50,213 indels** compared to v3.3.2, including 53.7% of segmental duplications/low-mappability regions (vs 24.3%). The stratification BED files at `ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/genome-stratifications/v3.1/` enable targeted testing of difficult regions (homopolymers >6bp, GC-extreme regions, tandem repeats).

## Documented bugs define mandatory edge case tests

Real-world parsing failures from vcf2db, GEMINI, and variant caller outputs reveal specific edge cases your loader must handle.

**Number=A field handling (vcf2db issue #14):**
vcf2db skips INFO fields with `Number=A` (one value per alternate allele), producing warnings like `skipping 'AC' because it has Number=A`. Test case:
```vcf
chr1	100	.	A	T,C	.	PASS	AC=5,3;AF=0.5,0.3	GT	1/2
```
Your loader must correctly parse both AC values and associate them with the correct alternate alleles after any decomposition.

**Multi-allelic handling (GEMINI issues #161, #193, #405):**
GEMINI misclassifies multi-nucleotide polymorphisms:
```vcf
chr1	805101	.	CG	AA,CT	.	PASS	.	GT	1/2
```
This gets incorrectly classified as `type=None, sub_type=indel unknown` instead of MNP. Your variant type classification must correctly identify SNPs, MNPs, insertions, and deletions across multi-allelic sites.

**Spanning deletion (*) allele (GATK edge case):**
```vcf
chr1	14604	.	A	G,*	20	.	.	GT:AD:DP	1/2:6,19,10:36
```
Many tools fail on the `*` allele representing overlapping deletions. HTSJDK throws `Duplicate allele added to VariantContext: *`; bcftools warns `Symbolic alleles other than <DEL> are currently not supported: <*>`. Your loader must handle `*` without crashing.

**Strelka2 non-standard allele counts:**
Strelka2 uses `AU`, `CU`, `GU`, `TU` fields instead of `AD` for SNVs, and `TAR`/`TIR` for indels. Test file:
```vcf
chr1	4345650	.	C	T	.	PASS	SOMATIC;SGT=CC->CT	DP:FDP:AU:CU:GU:TU	63:1:0,0:62,63:0,0:0,0
```
Your loader should either convert to standard AD format or store caller-specific fields in a JSONB column.

**VEP CSQ parsing with multiple transcripts:**
```vcf
chr1	500	.	G	A	.	PASS	CSQ=A|missense_variant|HIGH|GENE1|ENSG001|...,A|synonymous_variant|LOW|GENE1|ENSG001|...
```
Each variant maps to multiple transcripts (pipe-delimited, comma-separated entries). The `&` character separates multiple consequences per transcript (e.g., `splice_donor_variant&non_coding_transcript_variant`). Test that all transcript annotations load correctly, not just the first.

**Type coercion failure (vcf2db #51):**
dbNSFP annotations contain comma-separated float values in String-typed fields:
```
VEST3_score=0.123,0.122
```
This causes `sqlalchemy.exc.StatementError: could not convert string to float`. Your loader must respect VCF header type declarations rather than inferring types from values.

## Test patterns from cyvcf2, bcftools, and Hail inform structure

Analysis of mature bioinformatics test suites reveals consistent patterns to adopt.

**cyvcf2 patterns (brentp/cyvcf2):**
- Test multiple input sources: file path, Path object, file descriptor, file-like object
- Cover strict genotype mode (`strict_gt`) for partially missing alleles like `0/.`, `./1`
- Include caller-specific test files: `test.snpeff.vcf`, `test.mnp.vcf`, `test.comp_het.3.vcf`

**bcftools patterns (samtools/bcftools):**
- Test VCF v4.0, v4.1, v4.2, v4.3 format compatibility
- Platform-specific tests (ARM unsigned char handling, 32-bit float precision)
- Round-trip tests: VCF → BCF → VCF with diff comparison

**Hail patterns (hail-is/hail):**
- Use 1000 Genomes chromosome subsets for scale testing
- Validate schema after import with `mt.describe()`
- Test reference genome mapping (GRCh37 vs GRCh38, `1` vs `chr1`)

**Recommended test file organization:**
```
tests/
├── fixtures/
│   ├── minimal.vcf           # 10 variants, basic structure
│   ├── multiallelic.vcf      # Multi-allele sites, spanning deletions
│   ├── missing_data.vcf      # Missing GT, QUAL, INFO values
│   ├── annotations/
│   │   ├── vep_csq.vcf       # VEP CSQ field parsing
│   │   └── snpeff_ann.vcf    # SnpEff ANN field parsing
│   ├── callers/
│   │   ├── gatk.vcf          # GATK HaplotypeCaller output
│   │   ├── deepvariant.vcf   # DeepVariant with VAF field
│   │   ├── strelka2.vcf      # Strelka2 AU/CU/GU/TU fields
│   │   └── freebayes.vcf     # FreeBayes RO/AO fields
│   └── giab/
│       └── HG002_subset.vcf  # 1000 variants from GIAB truth set
├── unit/
│   ├── test_parser.py
│   ├── test_schema.py
│   └── test_normalization.py
├── integration/
│   ├── test_loading.py
│   └── test_queries.py
└── performance/
    └── test_benchmarks.py
```

## Validation methodology follows GA4GH and clinical standards

The GA4GH Benchmarking Team (Nature Biotechnology 2019) established standard metrics that your test suite should compute and assert against.

**Required metrics (Tier 1):**
- `TRUTH.TP`: True positives (correctly loaded variants)
- `TRUTH.FN`: False negatives (variants in VCF missing from database)
- `QUERY.FP`: False positives (database entries not in source VCF)
- `METRIC.Recall`: TP/(TP+FN) — target **>99.9%** for SNVs
- `METRIC.Precision`: TP/(TP+FP) — target **>99.9%** for SNVs
- `METRIC.F1_Score`: Harmonic mean — target **>0.999**

**Integration with hap.py and vcfeval:**
After loading a VCF and re-exporting, compare against the original:
```bash
hap.py original.vcf.gz exported.vcf.gz \
    -f confident_regions.bed.gz \
    -r reference.fasta \
    -o roundtrip_validation
```
The F1 score must be 1.0 for lossless loading; any deviation indicates data corruption.

**Clinical validation thresholds (NY-CLEP requirements):**
Clinical laboratories must validate at least **200 SNVs and 200 indels** with orthogonal confirmation. Your test suite should include a validation mode that:
- Loads known truth set (GIAB)
- Computes sensitivity/specificity per variant type
- Generates audit-ready documentation

## Academic use cases define query test assertions

Research from published rare disease workflows (slivar paper, GEMINI papers) identifies specific queries your database must support efficiently.

**De novo mutation detection (trio analysis):**
```sql
SELECT v.chrom, v.pos, v.ref, v.alt, v.gene
FROM variants v
JOIN genotypes g_child ON v.variant_id = g_child.variant_id
JOIN genotypes g_mom ON v.variant_id = g_mom.variant_id
JOIN genotypes g_dad ON v.variant_id = g_dad.variant_id
WHERE g_child.gt = 'HET'
  AND g_mom.gt = 'HOM_REF'
  AND g_dad.gt = 'HOM_REF'
  AND g_child.depth >= 10
  AND v.gnomad_popmax_af < 0.001;
```
Expected results per slivar benchmarks: ~3 de novo candidates per WGS trio, ~1.4-3.5 per exome trio.

**Compound heterozygous detection:**
Requires identifying two different heterozygous variants in the same gene, one inherited from each parent. Expected: ~10 candidates per WGS trio.

**Cohort-level frequency (impossible with bcftools alone):**
```sql
SELECT v.chrom, v.pos, v.ref, v.alt,
       COUNT(CASE WHEN g.gt != 'HOM_REF' THEN 1 END) as carriers,
       COUNT(*) as total_samples
FROM variants v
JOIN genotypes g ON v.variant_id = g.variant_id
GROUP BY v.chrom, v.pos, v.ref, v.alt;
```
This cross-sample aggregation is a key database advantage over file-based tools and must execute in <5 minutes for 10M variants.

**Query latency targets:**

| Query Type | Target (warm cache) |
|------------|---------------------|
| Single variant lookup | <5ms |
| Chromosome region (100KB) | <50ms |
| Gene-level query | <50ms |
| Complex trio filter | <30 seconds |
| Cohort aggregation (10M variants) | <5 minutes |

## Performance benchmarks establish 500K/second feasibility

Published benchmarks and PostgreSQL optimization research establish the path to your 500K variants/second target.

**Reference benchmarks:**
- GEMINI: ~260 variants/second (SQLite, single-core)
- GenESysV: ~694 variants/second (PostgreSQL, 24-core)
- Bystro: 8,500-15,000 variants/second (custom LMDB backend)

**PostgreSQL COPY is essential:**
Single INSERT achieves ~1,000 rows/second. PostgreSQL COPY achieves **50,000-150,000 rows/second**. Your 500K target requires:
1. Binary COPY format (20-30% faster than text)
2. Optimal batch size: **30,000 rows** per COPY command
3. UNLOGGED staging tables (40-60% faster, no WAL overhead)
4. Parallel loading by chromosome (scale with core count)

**Index strategy:**
Create indexes AFTER bulk loading—index creation on existing data is 2-5x faster than maintaining indexes during insertion.

```sql
-- Essential indexes for genomic queries
CREATE INDEX idx_variants_chrom_pos ON variants (chrom, pos);
CREATE INDEX idx_variants_gene ON variants (gene);
CREATE INDEX idx_genotypes_sample ON genotypes (sample_id);
CREATE INDEX idx_genotypes_variant ON genotypes (variant_id);
```

**Hardware scaling expectations:**

| Hardware Tier | Expected Rate | Time for 10M variants |
|--------------|---------------|----------------------|
| Laptop (NVMe SSD) | 1,500-3,000/sec | 55-110 min |
| Workstation (32 cores) | 10,000-50,000/sec | 3-17 min |
| Server (optimized) | 100,000-500,000/sec | 20-100 sec |

Your 500K/second target is achievable on server hardware with:
- NVMe storage (100K+ IOPS)
- 64+ cores for parallel chromosome loading
- 128GB+ RAM for large shared_buffers
- Optimized postgresql.conf: `max_wal_size = '20GB'`, `maintenance_work_mem = '2GB'`

## GEMINI deprecation creates the market opportunity

GEMINI is now officially deprecated, with the README stating: "Gemini is largely replaced by slivar. We recommend to update your pipelines." However, slivar is a filtering tool, not a database—it doesn't provide:
- Multi-user concurrent access
- Ad-hoc SQL queries
- User annotations and audit trails
- Cross-family variant comparison at scale

**Key gaps your tool fills:**

| Feature | GEMINI | slivar | vcf-pg-loader Target |
|---------|--------|--------|---------------------|
| GRCh38 support | ❌ (hg19 only) | ✅ | ✅ |
| Multi-allelic handling | Buggy | ✅ | ✅ Correct from design |
| Number=A fields | Skipped | N/A | ✅ Full support |
| Loading speed | ~260/sec | N/A | 500K/sec |
| Concurrent users | Limited (SQLite) | N/A | ✅ PostgreSQL |
| Custom annotations | ❌ Static bundles | N/A | ✅ User-defined |

## Concrete TDD implementation roadmap

**Phase 1: Parser unit tests (Week 1-2)**
- Test fixtures from nf-core test-datasets (sarscov2 VCFs)
- Edge cases: Number=A, spanning deletions, missing values
- Caller-specific FORMAT field handling

**Phase 2: Database integration tests (Week 3-4)**
- Schema creation from VCF header
- COPY performance benchmarks (target: 50K/sec baseline)
- Index creation timing
- Round-trip validation (load → export → compare)

**Phase 3: Validation tests (Week 5-6)**
- GIAB HG002 loading with variant count assertions
- GIAB trio Mendelian consistency (<0.1% error rate)
- hap.py/vcfeval integration for F1 score verification

**Phase 4: Query tests (Week 7-8)**
- De novo, compound het, recessive query patterns
- Latency assertions against targets
- Concurrent query stress tests

**Phase 5: Performance optimization (Week 9-10)**
- Parallel loading benchmarks
- Batch size optimization
- Index strategy validation
- Target: 500K variants/second on reference hardware

This TDD strategy ensures your vcf-pg-loader addresses documented real-world failures (vcf2db #14, GEMINI multi-allelic bugs), validates against accepted truth sets (GIAB v4.2.1), and proves performance claims against published benchmarks—positioning it as the production-ready successor for academic genomics research workflows.