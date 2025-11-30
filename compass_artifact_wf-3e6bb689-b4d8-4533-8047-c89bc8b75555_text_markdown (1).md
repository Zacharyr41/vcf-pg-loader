# Sourcing benchmarks for SnpEff ANN field parsing

Building robust tests for VCF annotation parsing requires **three foundational resources**: real-world annotated VCF files that expose edge cases, documented parsing failures from the SnpEff issue tracker, and reference implementations from established tools. This document provides concrete URLs, example ANN strings, and validation strategies to support a comprehensive test suite for a VCF-to-PostgreSQL database loader.

The ANN field format contains **16 pipe-delimited subfields**, but parsing complexity arises from multi-allelic variants, combined effects using the `&` separator, empty fields in intergenic variants, and version-specific format differences. Most parsing failures in the wild stem from these structural edge cases rather than simple field extraction.

---

## Public VCF files with SnpEff annotations

The most accessible test data comes directly from SnpEff's own repository. The **pcingola/SnpEff** GitHub contains several example VCFs designed for testing:

**Primary test files (require annotation):**
- `https://github.com/pcingola/SnpEff/raw/master/examples/test.chr22.vcf` — 1000 Genomes chr22 data (~9,962 variants), annotate with `java -Xmx4g -jar snpEff.jar GRCh37.75 test.chr22.vcf`
- `https://github.com/pcingola/SnpEff/raw/master/examples/cancer.vcf` — Cancer sample variants for somatic annotation testing
- `https://github.com/pcingola/SnpEff/raw/master/examples/cancer_pedigree.vcf` — Pedigree data with familial variants

**Pre-annotated SnpSift test files:**
- `https://github.com/pcingola/SnpSift/raw/master/test/extractFields_27.vcf` — Contains EFF annotations (older format) from SnpEff v4.1, includes MuTect calls with dbSNP and COSMIC annotations

The **nf-core/test-datasets** repository provides production-representative annotated VCFs:
- `https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/homo_sapiens/illumina/gatk/haplotypecaller_calls/test2_haplotc.ann.vcf.gz` — HaplotypeCaller output with SnpEff annotations
- SARS-CoV-2 test VCFs at `https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/genomics/sarscov2/illumina/vcf/test.vcf`

**GIAB benchmark VCFs** at `ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/release/AshkenazimTrio/HG002_NA24385_son/` provide truth sets for validation but require SnpEff annotation—they don't ship pre-annotated. The HG002 (NA24385) sample is the most extensively characterized benchmark genome.

---

## ANN field specification: the 16-field format

The official VCF header defines the ANN structure:

```
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 
'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | 
Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | 
CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
```

| Field | Name | Example | Can be empty |
|-------|------|---------|--------------|
| 1 | Allele | `T` | No |
| 2 | Annotation | `missense_variant` | No |
| 3 | Impact | `MODERATE` | No |
| 4 | Gene_Name | `BRCA1` | Yes (intergenic) |
| 5 | Gene_ID | `ENSG00000012048` | Yes |
| 6 | Feature_Type | `transcript` | Yes |
| 7 | Feature_ID | `ENST00000357654` | Yes |
| 8 | Transcript_BioType | `protein_coding` | Yes |
| 9 | Rank | `7/19` | Yes |
| 10 | HGVS.c | `c.1406G>A` | Yes |
| 11 | HGVS.p | `p.Gly469Glu` | Yes (non-coding) |
| 12 | cDNA.pos/length | `1666/2034` | Yes |
| 13 | CDS.pos/length | `1406/1674` | Yes |
| 14 | AA.pos/length | `469/557` | Yes |
| 15 | Distance | `11` | Yes (often empty) |
| 16 | Errors/Warnings | `WARNING_REF_DOES_NOT_MATCH_GENOME` | Yes |

**Critical format rules:** Multiple annotations for different transcripts are comma-separated. Multiple effects on the same transcript use `&` (e.g., `intron_variant&nc_transcript_variant`). Special characters should be converted to underscore or URL-encoded as `%XX`. The equals sign in HGVS notation (`p.(=)`) must use alternative notation like `p.(Leu54Leu)` since `=` is illegal in VCF INFO fields.

---

## Documented edge cases from SnpEff GitHub issues

The **pcingola/SnpEff** issue tracker reveals parsing pitfalls that should inform test cases:

**Issue #158 — Version incompatibility crashes parsing:** SnpSift 4.3g cannot parse VCF files annotated with SnpEff 4.2, throwing `java.lang.RuntimeException: Cannot parse EffectType 'PROTEIN_INTERACTION_LOCUS'`. The author's resolution: "SnpEff & SnpSift are compatible within same version. Re-annotate using the same version." This means your parser must handle unknown effect types gracefully.

**Issue #218 — Phantom gene annotations:** When using `-onlyTr` to filter transcripts, SnpEff still produces `intragenic_variant` annotations for overlapping genes without defined transcripts:
```
ANN=C|intron_variant|MODIFIER|MSH6|ENSG00000116062|transcript|ENST00000234420|...,
C|intragenic_variant|MODIFIER|FBXO11|ENSG00000138081|gene_variant|ENSG00000138081|||n.48033891delA||||||
```

**Issue #122 — Inconsistent field counts in CLOSEST:** The CLOSEST annotation field has variable sub-field counts without placeholders, making programmatic parsing fail. The author acknowledged: "The comma separated list after the pipe sign is not intended to have a fixed number of elements."

**Issue #255 — Commas in warning field cause phantom annotations:** Warning messages containing commas create parsing ambiguity: `WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS , T` appears to start a new annotation.

**Version format change (v4.1):** SnpEff switched from EFF to ANN format in version 4.1. The EFF format used 11-13 fields with parentheses: `EFF=STOP_GAINED(HIGH|NONSENSE|Cag/Tag|Q236*|749|NOC2L||CODING|NM_015658|)`. Use `-formatEff` flag to generate legacy format.

---

## How other tools parse ANN fields

Understanding peer implementations reveals common patterns and pitfalls:

**bcftools +split-vep** parses the header Description to determine field order, uses pipe delimiter, and handles missing values by filling with dots. Key flags: `-a ANN` specifies SnpEff format (vs CSQ for VEP), `-d` outputs per-transcript on new lines, `-s worst` selects most severe consequence. Test file: `https://github.com/samtools/bcftools/blob/develop/test/split-vep.vcf`. Issue #1686 documents that dashes in tag names (e.g., "M-CAP_score") cause failures—bcftools replaces with underscores.

**slivar** automatically detects CSQ/BCSQ/ANN from header and exposes `INFO.impactful` boolean and `INFO.highest_impact_order` integer. It checks all three annotation types if present. Custom severity ordering configurable via `SLIVAR_IMPACTFUL_ORDER` environment variable.

**vcf2db and GEMINI** expect either CSQ (VEP) or ANN (SnpEff). With older SnpEff versions, requires `-classic -formatEff` flags. GEMINI issue #215 documents failures with `-sequenceOntology` flag. Schema uses separate `variant_impacts` table with foreign keys to variants.

**SnpSift extractFields** provides the reference implementation with indexed access: `ANN[*].EFFECT`, `ANN[0].GENE`, `ANN[1].IMPACT`. The script `vcfEffOnePerLine.pl` splits one effect per line. Issue #46 notes that `ANN[*].RANK` returns "13" instead of "13/19" (truncates at `/`).

**cyvcf2** returns raw ANN string via `variant.INFO.get('ANN')`—no native parsing. Users must split manually on `|` and `,`, matching alleles to annotations themselves.

---

## Concrete test cases with example ANN strings

**Multi-allelic variants (two ALT alleles, separate annotations):**
```
1  889455  .  G  A,T  .  .  ANN=A|stop_gained|HIGH|NOC2L|ENSG00000188976|transcript|ENST00000327044|protein_coding|7/19|c.706C>T|p.Gln236*|756/2790|706/2250|236/749||,T|missense_variant|MODERATE|NOC2L|ENSG00000188976|transcript|ENST00000327044|protein_coding|7/19|c.706C>A|p.Gln236Lys|756/2790|706/2250|236/749||
```

**Combined consequences (& separator for same transcript):**
```
ANN=A|splice_donor_variant&intron_variant|HIGH|BRCA1|ENSG00000012048|transcript|ENST00000357654|protein_coding|10/22|c.4096+1G>A|||||
```

**Intergenic variant (empty gene fields, flanking genes in Gene_Name):**
```
ANN=A|intergenic_region|MODIFIER|CHR_START-DDX11L1|CHR_START-ENSG00000223972|intergenic_region|CHR_START-ENSG00000223972|||n.2->T||||||
```

**Structural variant with gene fusion:**
```
ANN=<DUP>|gene_fusion|HIGH|FGFR3&TACC3|ENSG00000068078&ENSG00000013810|gene_variant|ENSG00000013810|||||||||
```

**Variant with warning in field 16:**
```
ANN=T|missense_variant|MODERATE|MSH6|ENSG00000116062|transcript|ENST00000234420|protein_coding|9/9|c.4002-10delT||||||INFO_REALIGN_3_PRIME
```

**Very long ANN field (8 transcripts, single variant):**
```
ANN=A|stop_gained|HIGH|NOC2L|...|ENST00000327044|...,A|downstream_gene_variant|MODIFIER|NOC2L|...|ENST00000487214|...,A|downstream_gene_variant|MODIFIER|NOC2L|...|ENST00000469563|...,A|non_coding_exon_variant|MODIFIER|NOC2L|...|ENST00000477976|...[continues]
```

**Warning/error codes to handle:**
| Code | Meaning |
|------|---------|
| `WARNING_REF_DOES_NOT_MATCH_GENOME` | Database mismatch—critical |
| `WARNING_TRANSCRIPT_INCOMPLETE` | CDS length not multiple of 3 |
| `WARNING_TRANSCRIPT_NO_START_CODON` | Missing START codon |
| `INFO_REALIGN_3_PRIME` | Shifted for HGVS compliance |
| `INFO_COMPOUND_ANNOTATION` | Multiple variants combined |

---

## Validation and benchmarking strategies

**Validation approach for parsed ANN fields:**
1. **Field count validation** — Every ANN entry must have exactly 16 pipe-delimited subfields
2. **Allele matching** — First subfield must match one of the ALT alleles
3. **Impact enumeration** — Field 3 must be HIGH, MODERATE, LOW, or MODIFIER
4. **SO term validation** — Effect terms should be valid Sequence Ontology terms (see http://www.sequenceontology.org/)
5. **Round-trip testing** — Parse, serialize, re-parse, compare for equality

**VCF validation tools:**
- `vcf-validator` from VCFtools validates format compliance
- `gatk ValidateVariants -V input.vcf -R reference.fasta` checks reference consistency
- SnpSift extractFields as verification: `java -jar SnpSift.jar extractFields test.ann.vcf CHROM POS "ANN[*].EFFECT" "ANN[*].GENE"`

**Performance benchmarks from brentp/vcf-bench:**
| Format | cyvcf2 | PyVCF | htslib |
|--------|--------|-------|--------|
| BCF | 3.9s | N/A | 3.5s |
| VCF.gz | 20s | 16m49s | 18s |

cyvcf2 is **168x faster** than PyVCF for VCF parsing. BCF format is **5x faster** than VCF.gz.

**Recommended test file sizes:**
- Unit tests: 1,000 variants (~100KB)
- Benchmark suite: 30,000 variants (~5MB)  
- Stress testing: 500,000 variants (~50MB)

---

## Database schema for storing parsed annotations

The GEMINI database schema provides a proven model for one-to-many variant-to-annotation relationships:

```sql
CREATE TABLE variants (
    variant_id SERIAL PRIMARY KEY,
    chrom TEXT NOT NULL,
    pos INTEGER NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    qual REAL,
    filter TEXT
);

CREATE TABLE variant_annotations (
    annotation_id SERIAL PRIMARY KEY,
    variant_id INTEGER REFERENCES variants(variant_id),
    allele TEXT NOT NULL,
    effect TEXT NOT NULL,
    impact TEXT NOT NULL,
    gene_name TEXT,
    gene_id TEXT,
    feature_type TEXT,
    feature_id TEXT,
    transcript_biotype TEXT,
    rank TEXT,
    hgvs_c TEXT,
    hgvs_p TEXT,
    cdna_pos TEXT,
    cds_pos TEXT,
    aa_pos TEXT,
    distance TEXT,
    warnings TEXT
);

CREATE INDEX idx_annotations_variant ON variant_annotations(variant_id);
CREATE INDEX idx_annotations_gene ON variant_annotations(gene_name);
CREATE INDEX idx_annotations_impact ON variant_annotations(impact);
```

**Pre-processing pipeline before loading:**
```bash
# Decompose multi-allelic variants
vt decompose -s input.vcf -o decomposed.vcf

# Normalize indel representation
vt normalize -r reference.fa decomposed.vcf -o normalized.vcf

# Annotate with SnpEff
java -Xmx8g -jar snpEff.jar GRCh38.105 normalized.vcf > annotated.vcf
```

---

## Conclusion

The most valuable resources for building a robust ANN parser test suite are the **SnpSift test files** (pre-annotated with edge cases), the **SnpEff GitHub issue tracker** (documenting real-world parsing failures), and **bcftools +split-vep** as a reference implementation. Your parser should handle: variable transcript counts per variant, combined effects with `&`, empty fields in intergenic regions, warning codes in field 16, and graceful degradation for unknown effect types. The 16-field pipe-delimited format is stable since SnpEff 4.1, but legacy EFF format may still appear in older datasets. Performance testing against the brentp/vcf-bench methodology using BCF format provides realistic baseline expectations.