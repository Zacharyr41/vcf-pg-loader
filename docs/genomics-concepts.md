# Understanding VCF Data: A Guide for Non-Geneticists

This guide explains the genomics concepts implemented and tested in the VCF-to-PostgreSQL loader, using plain language and analogies.

---

## What is a VCF File?

**VCF (Variant Call Format)** is a standard file format for storing genetic variations. Think of it as a **"track changes" document for DNA**.

Instead of storing your complete genome (3 billion letters), a VCF file only records the places where your DNA differs from the "reference" human genome. It's like storing:

```
Line 100: Change "A" to "G"
Line 5000: Delete "TCG"
Line 12000: Insert "AATTC"
```

This makes the files much smaller and easier to work with—a person's entire genome variation can fit in a few hundred megabytes instead of terabytes.

---

## Variant Types

DNA is written in a 4-letter alphabet: **A, T, C, G**. Variations come in several forms:

### SNP (Single Nucleotide Polymorphism)
*Tested in: `test_normalizer.py::TestClassifyVariant::test_snp_classification`*

The simplest change—swapping one letter for another.

| Reference | Your DNA | What Happened |
|-----------|----------|---------------|
| A | G | Single letter swap |

**Analogy:** A typo that changes one letter in a word: "cat" → "bat"

### Indel (Insertion or Deletion)
*Tested in: `test_normalizer.py::TestClassifyVariant::test_deletion_classification`, `test_insertion_classification`*

Adding or removing one or more letters.

| Type | Reference | Your DNA | What Happened |
|------|-----------|----------|---------------|
| Deletion | ATG | A | Lost "TG" |
| Insertion | A | ATG | Gained "TG" |

**Analogy:**
- Deletion: "Saturday" → "Sturday" (lost letters)
- Insertion: "cat" → "carat" (added letters)

### MNP (Multi-Nucleotide Polymorphism)
*Tested in: `test_normalizer.py::TestClassifyVariant::test_mnp_classification`*

Multiple consecutive letters change at once.

| Reference | Your DNA | What Happened |
|-----------|----------|---------------|
| AT | GC | Two letters changed together |

**Analogy:** "cat" → "dog" — multiple letters changed, but the word length stayed the same.

### Structural Variant (SV)
*Tested in: `test_normalizer.py::TestClassifyVariant::test_sv_classification`*

Large-scale changes like big deletions, duplications, or inversions—often thousands of letters.

| Code | Meaning |
|------|---------|
| `<DEL>` | Large deletion |
| `<INS>` | Large insertion |
| `<DUP>` | Duplication (a section is copied) |
| `<INV>` | Inversion (a section is flipped backwards) |

**Analogy:** Instead of editing a word, you're cutting, copying, or flipping entire paragraphs.

---

## Variant Normalization

### Why Normalize?

The same genetic change can be written multiple ways:

| Representation | Position | Reference | Your DNA |
|----------------|----------|-----------|----------|
| Verbose | 100 | ATG | AG |
| Normalized | 100 | AT | A |

Both describe the same deletion of "T", but the normalized form is minimal and consistent. This is critical for:
- **Deduplication:** Ensuring the same variant isn't counted twice
- **Database lookups:** Finding matching variants across different studies
- **Clinical matching:** Comparing a patient's variants to known disease variants

### Right-Trimming
*Tested in: `test_normalizer.py::TestNormalizeVariant::test_right_trim_deletion`, `test_right_trim_insertion`*

Remove identical letters from the end of both reference and alternate alleles.

```
Before: REF=ATG, ALT=AG   (both end in "G")
After:  REF=AT,  ALT=A    (trailing "G" removed)
```

### Left-Trimming
*Tested in: `test_normalizer.py::TestNormalizeVariant::test_left_trim_parsimony`*

Remove identical letters from the start, shifting the position forward.

```
Before: Pos=100, REF=TAC, ALT=TGC  (both start with "T")
After:  Pos=101, REF=AC,  ALT=GC   (leading "T" removed, position shifted)
```

### Anchor Bases
*Tested in: `test_normalizer.py::TestNormalizeVariant::test_deletion_preserves_anchor`, `test_no_left_trim_when_min_length_is_one`*

For indels, we always keep at least one "anchor" base. This prevents ambiguity about where the change occurs.

```
Deletion: REF=TA, ALT=T  ← We stop here
         (Don't trim to REF=A, ALT="" because empty alleles are invalid)
```

### Multi-Allelic Decomposition
*Tested in: `test_normalizer.py::TestDecomposeMultiallelic`*

Sometimes one position has multiple possible variations. We split these into separate records for easier analysis.

```
Before: Position 100, REF=A, ALT=[G, T]  (one record, two alternatives)
After:  Position 100, REF=A, ALT=G       (record 1)
        Position 100, REF=A, ALT=T       (record 2)
```

**Why?** Each variant might have different clinical significance. Splitting them allows independent analysis.

---

## VCF File Structure

### Header Sections
*Tested in: `test_vcf_parser.py::TestVCFHeaderParser`*

VCF files begin with metadata headers that define what data each column contains:

#### INFO Fields
Metadata about the variant itself (shared across all samples).

```
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total read depth">
##INFO=<ID=DB,Number=0,Type=Flag,Description="Is in dbSNP">
```

| Field | What It Tells Us |
|-------|------------------|
| AC (Allele Count) | How many chromosomes carry this variant |
| AF (Allele Frequency) | Percentage of chromosomes with this variant |
| DP (Depth) | How many times this position was sequenced |
| DB | Whether this variant is in the dbSNP database |

#### FORMAT Fields
Per-sample data (each person's individual results).

```
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
```

| Field | What It Tells Us |
|-------|------------------|
| GT (Genotype) | Which alleles this person has (e.g., 0/1 = one reference, one variant) |
| AD (Allelic Depths) | Read counts for each allele |

### The "Number" Specification
*Tested in: `test_vcf_parser.py::TestArraySizing`*

The `Number` field tells us how many values to expect:

| Number | Meaning | Example (2 ALT alleles) |
|--------|---------|-------------------------|
| `1` | Exactly one value | `DP=50` |
| `A` | One per ALT allele | `AC=10,5` (2 values) |
| `R` | One per allele (REF + ALTs) | `AD=30,10,5` (3 values) |
| `G` | One per possible genotype | `PL=0,30,60,45,75,90` (6 values for 3 genotypes) |
| `.` | Variable/unknown | Could be any number |

#### Genotype Count Formula
*Tested in: `test_vcf_parser.py::TestArraySizing::test_number_g_sizing`*

For diploid organisms (like humans, with 2 copies of each chromosome):

```
Genotypes = (n_alleles × (n_alleles + 1)) / 2

With 1 ALT (2 alleles total): 3 genotypes (0/0, 0/1, 1/1)
With 2 ALTs (3 alleles total): 6 genotypes (0/0, 0/1, 0/2, 1/1, 1/2, 2/2)
```

---

## VEP Annotations (Consequence Predictions)

### What is VEP?
*Tested in: `test_vcf_parser.py::TestVCFHeaderParser::test_parse_vep_csq_header`*

**VEP (Variant Effect Predictor)** is a tool that predicts what effect a variant might have. It adds a `CSQ` (consequence) field to each variant with detailed annotations.

The CSQ field contains pipe-separated values:
```
CSQ=G|missense_variant|MODERATE|BRCA1|...|c.181A>G|p.Lys61Glu
```

### Key CSQ Fields
*Tested in: `test_variant_record.py::TestVariantParser::test_parse_variant_with_csq`*

| Field | Meaning | Example |
|-------|---------|---------|
| Allele | Which variant this annotation is for | G |
| Consequence | Type of effect | missense_variant |
| IMPACT | Severity level | MODERATE |
| SYMBOL | Gene name | BRCA1 |
| HGVSc | DNA-level notation | c.181A>G |
| HGVSp | Protein-level notation | p.Lys61Glu |

### HGVS Notation Explained

**HGVSc (coding DNA):** `c.181A>G`
- Position 181 in the coding sequence
- Changed from A to G

**HGVSp (protein):** `p.Lys61Glu`
- Position 61 in the protein
- Lysine (Lys) changed to Glutamate (Glu)

---

## Impact Levels

### Understanding Variant Impact
*Tested in: `test_type_mapping.py::TestGetWorstImpact`*

VEP assigns an impact level to each variant:

| Impact | Description | Example Consequences |
|--------|-------------|---------------------|
| **HIGH** | Probably breaks the gene | Stop gained, frameshift, splice site destroyed |
| **MODERATE** | Changes protein but might be tolerated | Missense (amino acid change) |
| **LOW** | Unlikely to change protein function | Synonymous (same amino acid), splice region |
| **MODIFIER** | Non-coding or unknown effect | Intergenic, intronic |

### Worst Impact Selection
When a variant affects multiple genes or transcripts, we report the most severe impact:

```python
impacts = ['LOW', 'HIGH', 'MODERATE']
worst = 'HIGH'  # HIGH takes precedence
```

**Priority order:** HIGH > MODERATE > LOW > MODIFIER

---

## Clinical Significance (ClinVar)

### What is ClinVar?
*Tested in: `test_type_mapping.py::TestNormalizeClinvarSignificance`*

ClinVar is a public database where labs submit their interpretations of whether variants cause disease.

### Classification Categories

| Classification | Meaning | Action |
|---------------|---------|--------|
| **Pathogenic** | Causes disease | Report to patient |
| **Likely Pathogenic** | >90% chance of causing disease | Report with caveats |
| **VUS** (Uncertain Significance) | Unknown impact | Don't use for clinical decisions |
| **Likely Benign** | >90% chance of being harmless | Generally not reported |
| **Benign** | Does not cause disease | Not reported |
| **Conflicting** | Labs disagree | Requires expert review |
| **Drug Response** | Affects medication metabolism | Pharmacogenomics |

### Handling Multiple Classifications
*Tested in: `test_type_mapping.py::TestNormalizeClinvarSignificance::test_multiple_values_pathogenic_wins`*

When multiple labs submit different classifications, we use the most clinically significant:

```
Input: "Benign,Pathogenic"
Output: "pathogenic"  (pathogenic wins because it requires action)
```

---

## Database Schema Design

### Chromosome Partitioning
*Tested in: `test_schema.py::TestSchemaManager::test_create_variants_table`*

The variants table is partitioned by chromosome for performance. Each chromosome gets its own partition:

```
variants (parent table)
├── variants_1  (chromosome 1)
├── variants_2  (chromosome 2)
├── ...
├── variants_22 (chromosome 22)
├── variants_x  (X chromosome)
├── variants_y  (Y chromosome)
└── variants_m  (mitochondrial)
```

**Why partition?**
- Queries filtering by chromosome only scan relevant partitions
- Parallel loading—different chromosomes can load simultaneously
- Easier data management—can drop/archive individual chromosomes

### Chromosome Type
*Tested in: `test_schema.py::TestSchemaManager::test_create_types`*

Chromosomes are stored as an enum type for:
- Data validation (can't insert "chr99")
- Storage efficiency (enum vs text)
- Query optimization

### Audit Trail
*Tested in: `test_schema.py::TestSchemaManager::test_create_audit_table`*

Every data load is tracked:

| Column | Purpose |
|--------|---------|
| audit_id | Unique identifier |
| load_batch_id | Groups related loads |
| vcf_file_path | Source file |
| vcf_file_md5 | Checksum for verification |

This enables:
- Reproducibility: Reload data if needed
- Debugging: Trace variants to source files
- Compliance: Audit trail for clinical data

---

## Data Type Mapping

### VCF to PostgreSQL Types
*Tested in: `test_type_mapping.py::TestGetPgType`, `TestInferColumnDefinition`*

VCF data types map to PostgreSQL as follows:

| VCF Type | Number=1 | Number=A/R/G/. |
|----------|----------|----------------|
| Integer | INTEGER | INTEGER[] |
| Float | REAL | REAL[] |
| String | TEXT | TEXT[] |
| Flag | BOOLEAN | — |
| Character | CHAR(1) | — |

**Why arrays?** Fields like `AF` (allele frequency) have one value per ALT allele. With 3 ALT alleles, you get 3 frequencies: `AF=[0.1, 0.05, 0.01]`

---

## Variant Records

### Core Fields
*Tested in: `test_variant_record.py::TestVariantRecord`*

Every variant record contains:

| Field | Description | Example |
|-------|-------------|---------|
| chrom | Chromosome | chr1 |
| pos | Position (1-based) | 12345 |
| ref | Reference allele | A |
| alt | Alternate allele | G |
| qual | Quality score | 30.0 |
| filter | QC status | ["PASS"] |
| rs_id | dbSNP identifier | rs123456 |

### Automatic Type Classification
*Tested in: `test_variant_record.py::TestVariantRecord::test_variant_type_classification`*

The `variant_type` property is computed automatically:

```python
record.ref = "A"
record.alt = "G"
record.variant_type  # → "snp"

record.ref = "ATG"
record.alt = "A"
record.variant_type  # → "indel"
```

### Handling Missing Values
*Tested in: `test_variant_record.py::TestVariantParser::test_parse_variant_missing_values`*

VCF uses special values for missing data:
- Quality: `-1` or `.` → `None`
- ID: `.` → `None`
- Filter: empty → `[]`

---

## Glossary

| Term | Definition |
|------|------------|
| **Allele** | One version of a genetic sequence at a position |
| **ALT** | Alternate allele (the variant) |
| **dbSNP** | Database of known genetic variants |
| **Diploid** | Having two copies of each chromosome (like humans) |
| **Genotype** | The combination of alleles a person has |
| **Indel** | Insertion or deletion variant |
| **MNP** | Multi-nucleotide polymorphism |
| **Normalize** | Standardize variant representation |
| **REF** | Reference allele (the "normal" sequence) |
| **SNP** | Single nucleotide polymorphism |
| **VCF** | Variant Call Format |
| **VEP** | Variant Effect Predictor |
| **VUS** | Variant of Uncertain Significance |

---

## Test File Reference

Each concept is verified by specific tests:

| Concept | Test File | Test Class/Method |
|---------|-----------|-------------------|
| Variant types | `test_normalizer.py` | `TestClassifyVariant` |
| Normalization | `test_normalizer.py` | `TestNormalizeVariant`, `TestIsNormalized` |
| Multi-allelic decomposition | `test_normalizer.py` | `TestDecomposeMultiallelic` |
| VCF header parsing | `test_vcf_parser.py` | `TestVCFHeaderParser` |
| Array sizing | `test_vcf_parser.py` | `TestArraySizing` |
| Type mapping | `test_type_mapping.py` | `TestGetPgType`, `TestInferColumnDefinition` |
| ClinVar significance | `test_type_mapping.py` | `TestNormalizeClinvarSignificance` |
| Impact levels | `test_type_mapping.py` | `TestGetWorstImpact` |
| Variant records | `test_variant_record.py` | `TestVariantRecord`, `TestVariantParser` |
| Database schema | `test_schema.py` | `TestSchemaManager` |
