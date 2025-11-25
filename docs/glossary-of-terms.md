# Genetics and Bioinformatics Glossary

A comprehensive glossary of terms used in VCF processing, variant analysis, and genomics database systems.

---

## Table of Contents

- [Core Genetics Concepts](#core-genetics-concepts)
- [Variant Types](#variant-types)
- [File Formats](#file-formats)
- [VCF Structure](#vcf-structure)
- [Variant Annotation](#variant-annotation)
- [Clinical Classification](#clinical-classification)
- [Population Databases](#population-databases)
- [Analysis Patterns](#analysis-patterns)
- [Reference Data](#reference-data)
- [Tools and Platforms](#tools-and-platforms)
- [Database and Computing](#database-and-computing)

---

## Core Genetics Concepts

### Allele
One of two or more alternative forms of a genetic sequence at a particular position. In a VCF file, REF is the reference allele and ALT contains the alternate allele(s).

### Anchor Base
A single nucleotide retained during variant normalization to prevent empty alleles. For example, a deletion of "TG" at position 100 is represented as `REF=ATG, ALT=A` rather than `REF=TG, ALT=""`. The "A" is the anchor base.

### Autosomal
Relating to chromosomes 1-22 (non-sex chromosomes). Autosomal inheritance patterns affect males and females equally.

### Base Pair (bp)
A unit of measurement for DNA length. One base pair is a single nucleotide (A, T, C, or G). The human genome contains approximately 3 billion base pairs.

### Chromosome
A structure containing DNA that carries genetic information. Humans have 23 pairs: 22 autosomal pairs (chr1-chr22) plus sex chromosomes (chrX, chrY). The mitochondrial genome (chrM) is also analyzed.

### Coding Region
DNA sequences that are translated into proteins. Variants in coding regions are more likely to affect protein function.

### Diploid
Having two copies of each chromosome (one from each parent). Humans are diploid, meaning each position has two alleles.

### DNA (Deoxyribonucleic Acid)
The molecule that carries genetic information, written in a four-letter alphabet: Adenine (A), Thymine (T), Cytosine (C), and Guanine (G).

### Exome
The portion of the genome that codes for proteins (~1-2% of total DNA). Whole Exome Sequencing (WES) targets only these regions.

### Gene
A segment of DNA that contains instructions for making a protein or functional RNA. Humans have approximately 20,000 protein-coding genes.

### Genome
The complete set of genetic material in an organism. The human genome is approximately 3.2 billion base pairs.

### Genotype
The combination of alleles an individual has at a particular position. Common genotypes:
- **0/0** (HOM_REF): Homozygous reference (two copies of reference allele)
- **0/1** (HET): Heterozygous (one reference, one alternate allele)
- **1/1** (HOM_ALT): Homozygous alternate (two copies of alternate allele)
- **1/2**: Heterozygous for two different alternate alleles
- **./.*: Missing or unknown genotype

### Haplotype
A set of alleles inherited together on a single chromosome from one parent.

### Heterozygous (Het)
Having two different alleles at a genetic position (e.g., one A and one G).

### Homozygous (Hom)
Having two identical alleles at a genetic position (either both reference or both alternate).

### Intron
Non-coding DNA sequences within a gene that are removed during RNA processing. Variants in introns usually have lower impact.

### Locus (plural: Loci)
A specific position or region on a chromosome.

### Mendelian Inheritance
Patterns of genetic inheritance following Mendel's laws. Variants can be inherited in autosomal dominant, autosomal recessive, or X-linked patterns.

### Nucleotide
A single unit of DNA, consisting of one of four bases: A, T, C, or G.

### Ploidy
The number of sets of chromosomes in a cell. Humans are diploid (2 sets); some organisms are haploid (1) or polyploid (3+).

### Position (POS)
The 1-based coordinate indicating where a variant occurs on a chromosome.

### Proband
The individual being studied, typically the affected person in a family study. In trio analysis, the proband is usually the child.

### Transcript
An RNA copy of a gene. A single gene may have multiple transcripts due to alternative splicing, and variants can affect transcripts differently.

---

## Variant Types

### Complex Variant
A variant involving both insertion and deletion at the same position, or multiple nearby changes that should be considered together.

### De Novo Variant
A new mutation that appears in a child but is not present in either parent. These are particularly important in rare disease diagnosis. Expected rate: ~1-2 coding de novo variants per individual per generation.

### Indel (Insertion/Deletion)
A variant where nucleotides are added (insertion) or removed (deletion) compared to the reference.
- **Insertion**: `REF=A, ALT=ATG` (TG added)
- **Deletion**: `REF=ATG, ALT=A` (TG removed)

### MNP (Multi-Nucleotide Polymorphism)
Multiple adjacent nucleotides changed simultaneously. Example: `REF=AT, ALT=GC`. Also called MNV (Multi-Nucleotide Variant).

### Multi-Allelic Variant
A position with more than one alternate allele. Example: `REF=A, ALT=G,T` means some individuals have G and others have T at this position.

### SNP (Single Nucleotide Polymorphism)
The simplest variant type—a single nucleotide change. Example: `REF=A, ALT=G`. Also called SNV (Single Nucleotide Variant).

### Structural Variant (SV)
Large-scale genomic changes, typically >50 base pairs:
- **DEL**: Large deletion
- **INS**: Large insertion  
- **DUP**: Duplication
- **INV**: Inversion (sequence flipped)
- **BND**: Breakend (complex rearrangement)
- **CNV**: Copy Number Variant

### Symbolic Allele
A placeholder representing a structural variant rather than explicit sequence. Written in angle brackets: `<DEL>`, `<INS>`, `<DUP>`, `<INV>`, `<NON_REF>`.

---

## File Formats

### BAM (Binary Alignment Map)
A compressed binary format storing aligned sequencing reads. The human-readable version is SAM (Sequence Alignment Map).

### BED (Browser Extensible Data)
A tab-delimited format for genomic regions with chromosome, start position, and end position. Used to define target regions or high-confidence callable regions.

### CRAM
A highly compressed alternative to BAM format that references the genome to reduce file size.

### FASTA
A text format for nucleotide or protein sequences. Reference genomes are distributed in FASTA format (`.fa` or `.fna` files).

### FASTQ
A format for raw sequencing reads that includes quality scores for each base.

### gVCF (Genomic VCF)
An extended VCF format that includes information about non-variant positions (where the sample matches the reference). Essential for joint calling across multiple samples.

### PED (Pedigree File)
A tab-delimited file describing family relationships and phenotypes. Contains: Family ID, Individual ID, Father ID, Mother ID, Sex, and Phenotype. Required for trio and family analyses.

### VCF (Variant Call Format)
The standard format for storing genetic variants. Contains header metadata and tab-delimited variant records with chromosome, position, reference allele, alternate allele(s), quality, filter status, INFO fields, and per-sample genotypes.

---

## VCF Structure

### ALT Field
The alternate allele(s) observed at a position. Multiple alternates are comma-separated: `ALT=G,T`.

### FILTER Field
Quality control status. `PASS` indicates the variant passed all filters. Other values indicate specific filters that failed.

### FORMAT Field
Defines the per-sample data fields and their order. Example: `GT:DP:GQ` means genotype, depth, and genotype quality.

### INFO Field
Variant-level annotations shared across all samples. Contains key-value pairs like `DP=100;AF=0.5`.

### Number Specification
VCF header attribute defining array sizes:
- **Number=1**: Single value
- **Number=A**: One value per ALT allele
- **Number=R**: One value per allele (REF + ALTs)
- **Number=G**: One value per possible genotype
- **Number=.**: Variable/unbounded

### QUAL Field
Phred-scaled quality score indicating confidence in the variant call. Higher is better. QUAL=30 means 99.9% confidence.

### REF Field
The reference allele (sequence from the reference genome) at this position.

### rsID
A unique identifier from dbSNP for known variants. Format: `rs` followed by a number (e.g., rs123456).

---

## Variant Annotation

### CADD (Combined Annotation Dependent Depletion)
A score predicting variant deleteriousness by integrating multiple annotations. CADD Phred scores >15-20 suggest potential pathogenicity. Higher scores indicate more likely damaging variants.

### Consequence
The predicted effect of a variant on a gene/transcript:
- **frameshift_variant**: Indel changes reading frame
- **stop_gained**: Creates premature stop codon
- **missense_variant**: Changes amino acid
- **synonymous_variant**: No amino acid change
- **splice_site_variant**: Affects RNA splicing
- **intron_variant**: Within intron
- **intergenic_variant**: Between genes

### CSQ Field
VEP's consequence annotation field in VCF INFO. Contains pipe-delimited values for each transcript annotation.

### HGVS Notation
Standardized nomenclature for describing variants:
- **HGVSc** (coding DNA): `c.181A>G` = position 181, A changed to G
- **HGVSp** (protein): `p.Lys61Glu` = lysine at position 61 changed to glutamate
- **HGVSg** (genomic): `g.12345A>G` = genomic position

### Impact Severity
VEP's classification of variant effect severity:
- **HIGH**: Likely disruptive (frameshift, stop gained, splice site)
- **MODERATE**: Possibly disruptive (missense)
- **LOW**: Unlikely disruptive (synonymous)
- **MODIFIER**: Non-coding or unknown (intronic, intergenic)

### LoF (Loss of Function)
Variants predicted to eliminate gene function: frameshift, stop gained, splice site disruption, or start lost.

### PolyPhen (Polymorphism Phenotyping)
Predicts impact of amino acid substitutions on protein function. Scores range from 0 (benign) to 1 (damaging).

### SIFT (Sorting Intolerant From Tolerant)
Predicts whether an amino acid substitution affects protein function. Scores ≤0.05 are considered damaging.

### SnpEff
A variant annotation tool that adds predicted effects using the ANN field format.

### VEP (Variant Effect Predictor)
Ensembl's tool for predicting variant consequences. Adds the CSQ field to VCF files with detailed functional annotations.

---

## Clinical Classification

### ACMG/AMP Guidelines
Standards from the American College of Medical Genetics for classifying variant pathogenicity using evidence criteria (PS, PM, PP for pathogenic; BS, BP for benign).

### ALCOA Principles
Data integrity requirements for clinical data: Attributable, Legible, Contemporaneous, Original, Accurate.

### Benign
A variant classification indicating the change does not cause disease.

### CAP (College of American Pathologists)
Organization providing laboratory accreditation. CAP compliance requires quality standards and proficiency testing.

### CLIA (Clinical Laboratory Improvement Amendments)
US federal regulations for clinical laboratory testing. CLIA compliance requires >99% accuracy with complete audit trails.

### ClinVar
NCBI's public database of clinically interpreted variants. Contains assertions about variant-disease relationships submitted by clinical labs.

### Conflicting Interpretations
When different laboratories have submitted different pathogenicity classifications for the same variant in ClinVar.

### Drug Response
A ClinVar classification indicating a variant affects response to medication (pharmacogenomics).

### Likely Benign
A variant classification indicating >90% probability the change does not cause disease.

### Likely Pathogenic
A variant classification indicating >90% probability the change causes disease.

### Pathogenic
A variant classification indicating the change causes disease.

### Pharmacogenomics (PGx)
The study of how genetic variants affect drug response. Key genes include CYP2D6, CYP2C19, and DPYD.

### Risk Factor
A ClinVar classification for variants that increase disease susceptibility but don't directly cause disease.

### Star Allele
Nomenclature for pharmacogene haplotypes (e.g., CYP2D6*4). Used to predict drug metabolism phenotypes.

### VUS (Variant of Uncertain Significance)
A variant classification indicating insufficient evidence to determine pathogenicity. Cannot be used for clinical decision-making.

---

## Population Databases

### 1000 Genomes Project
A catalog of human genetic variation from 2,504 individuals across 26 populations. Provides population-specific allele frequencies.

### Allele Count (AC)
The number of chromosomes in a dataset carrying the alternate allele.

### Allele Frequency (AF)
The proportion of chromosomes carrying a variant. AF=0.01 means 1% of chromosomes have the variant. Used to filter rare variants.

### dbSNP
NCBI's database of known genetic variants. Assigns rsID identifiers to cataloged variants.

### gnomAD (Genome Aggregation Database)
The largest public collection of human variant frequencies, containing data from >140,000 individuals. Successor to ExAC.

### MAF (Minor Allele Frequency)
The frequency of the less common allele in a population. Often used as a filter threshold (e.g., MAF < 0.01 for rare variants).

### Popmax AF
The maximum allele frequency across all populations in gnomAD. Used for filtering to ensure variants are rare in all ancestry groups.

---

## Analysis Patterns

### Autosomal Dominant
Inheritance pattern where one mutant allele causes disease. Affected individuals are typically heterozygous.

### Autosomal Recessive
Inheritance pattern requiring two mutant alleles for disease. Affected individuals are homozygous or compound heterozygous.

### Burden Test
Statistical test comparing the aggregate frequency of rare variants in a gene between cases and controls.

### Compound Heterozygous
Having two different pathogenic variants in the same gene, one inherited from each parent. Causes autosomal recessive disease when both variants impair the gene.

### Joint Calling
Variant calling across multiple samples simultaneously to improve accuracy, especially for low-frequency variants.

### Rare Disease Trio Analysis
Analysis of a child (proband) and both parents to identify disease-causing variants. Enables detection of de novo, recessive, and compound heterozygous variants.

### Segregation Analysis
Tracking variant inheritance through a family to assess pathogenicity. Variants that segregate with disease are more likely pathogenic.

### Trio
A family unit of child plus both biological parents. The standard configuration for rare disease analysis.

### X-Linked Inheritance
Inheritance pattern for genes on the X chromosome. Males (XY) are more severely affected since they have only one X chromosome.

---

## Reference Data

### Alternate Contig
Additional sequences in a reference assembly representing known population variation at specific loci.

### Decoy Sequences
Additional sequences added to reference genomes to capture reads that would otherwise mismap. Improves variant calling accuracy.

### GRCh37 / hg19
The previous human reference genome assembly (2009). Still used by some legacy pipelines. UCSC calls it "hg19"; NCBI/Ensembl call it "GRCh37".

### GRCh38 / hg38
The current human reference genome assembly (2013). UCSC calls it "hg38"; NCBI/Ensembl call it "GRCh38". Current patch: GRCh38.p14.

### Genome Reference Consortium (GRC)
The organization maintaining and improving reference genome assemblies for human, mouse, zebrafish, and chicken.

### GIAB (Genome in a Bottle)
NIST consortium providing benchmark samples and high-confidence variant calls for validating sequencing pipelines. Key samples: HG001 (NA12878), HG002 (NA24385).

### Patch Release
Updates to a reference assembly that fix errors without changing chromosome coordinates. Example: GRCh38.p14.

### T2T (Telomere-to-Telomere)
A complete human genome assembly with no gaps, achieved in 2022. Includes centromeres and other previously unsequenced regions.

---

## Tools and Platforms

### bcftools
Command-line toolkit for VCF/BCF manipulation: filtering, merging, normalization, and statistics.

### BioContainers
Repository of containerized bioinformatics tools with automatic Docker/Singularity builds from Bioconda recipes.

### BWA (Burrows-Wheeler Aligner)
Widely used tool for aligning sequencing reads to a reference genome.

### Cromwell
Workflow execution engine for WDL (Workflow Description Language). Powers the Terra platform.

### cyvcf2
High-performance Python library for VCF parsing. Approximately 168x faster than PyVCF.

### DeepVariant
Google's deep learning-based variant caller using neural networks trained on sequencing data.

### GATK (Genome Analysis Toolkit)
Broad Institute's toolkit for variant discovery. The "Best Practices" pipelines are industry standards.

### GEMINI
A deprecated SQLite-based framework for variant analysis. Replaced by slivar due to scalability limitations.

### Glow
Databricks' genomics library for Apache Spark. Claims 10x efficiency improvements over Hail.

### Hail
Spark-based framework for large-scale genomic analysis. Powers gnomAD QC. Uses MatrixTable data structure.

### IGV (Integrative Genomics Viewer)
Desktop application for visualizing genomic data including alignments and variants.

### nf-core
Community curated collection of Nextflow pipelines following strict standards. Contains 113+ production-ready pipelines.

### Nextflow
Workflow manager for bioinformatics pipelines. Uses channel-based dataflow model and DSL2 syntax. Dominant in academic bioinformatics (~43% citation share).

### Seqera Platform / Tower
Commercial platform for managing Nextflow workflows at scale.

### slivar
Modern variant filtering tool using JavaScript expressions. Recommended replacement for GEMINI.

### Snakemake
Python-based workflow manager. Popular in academic settings, particularly among Python-focused teams.

### Terra
Broad Institute's cloud platform for genomic analysis. Uses WDL/Cromwell workflows.

### vcf2db
Tool for loading VCF files into SQLite/PostgreSQL databases. Current performance: ~1,200 variants/second.

### vcfanno
Fast tool for annotating VCF files from multiple annotation sources (~8,000 variants/second).

### vt (Variant Tool)
Toolkit for variant manipulation including normalization and decomposition.

### WDL (Workflow Description Language)
Domain-specific language for defining analysis workflows. Used with Cromwell execution engine.

---

## Database and Computing

### Batch Processing
Processing data in groups (batches) rather than individually. Spring Batch uses chunk-oriented processing with ItemReader → ItemProcessor → ItemWriter patterns.

### Binary COPY
PostgreSQL's fastest data loading method. Transfers data in binary format, bypassing text parsing. 10-50x faster than INSERT statements.

### COPY Protocol
PostgreSQL command for bulk data loading from files or streams.

### GiST Index
PostgreSQL's Generalized Search Tree index type. Supports range queries on int8range columns for genomic coordinate searches.

### int8range
PostgreSQL range type for 64-bit integers. Used for genomic position ranges enabling efficient overlap queries with operators like `&&` (overlaps) and `@>` (contains).

### JSONB
PostgreSQL's binary JSON type with indexing support. Used for flexible storage of variable annotation fields.

### Normalization (Database)
Organizing database tables to reduce redundancy. The variant schema uses a hybrid normalized design with denormalized columns for read performance.

### Normalization (Variant)
Standardizing variant representation to a canonical form: left-aligned and parsimonious (minimal representation). Ensures the same biological variant has one consistent representation.

### Partitioning
Dividing a large table into smaller pieces based on a column value. The variants table is partitioned by chromosome for query performance.

### pg_trgm
PostgreSQL extension for trigram-based text similarity. Enables fuzzy matching on HGVS notation.

### Phred Score
Logarithmic quality score where Q = -10 × log₁₀(P_error). Q30 means 99.9% accuracy (1 in 1000 error rate).

---

## Abbreviations Quick Reference

| Abbreviation | Full Term |
|--------------|-----------|
| AC | Allele Count |
| AF | Allele Frequency |
| ALT | Alternate Allele |
| BAM | Binary Alignment Map |
| bp | Base Pair |
| CADD | Combined Annotation Dependent Depletion |
| CNV | Copy Number Variant |
| DP | Read Depth |
| GQ | Genotype Quality |
| GWAS | Genome-Wide Association Study |
| HET | Heterozygous |
| HOM | Homozygous |
| HGVS | Human Genome Variation Society (nomenclature) |
| LoF | Loss of Function |
| MAF | Minor Allele Frequency |
| MNP | Multi-Nucleotide Polymorphism |
| PED | Pedigree File |
| PGx | Pharmacogenomics |
| QC | Quality Control |
| REF | Reference Allele |
| SNP | Single Nucleotide Polymorphism |
| SNV | Single Nucleotide Variant |
| SV | Structural Variant |
| VCF | Variant Call Format |
| VEP | Variant Effect Predictor |
| VUS | Variant of Uncertain Significance |
| WES | Whole Exome Sequencing |
| WGS | Whole Genome Sequencing |

---

## See Also

- [VCF Specification v4.3](https://samtools.github.io/hts-specs/VCFv4.3.pdf)
- [HGVS Nomenclature](https://varnomen.hgvs.org/)
- [ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/)
- [gnomAD](https://gnomad.broadinstitute.org/)
- [GIAB](https://www.nist.gov/programs-projects/genome-bottle)
- [GRC Human Reference](https://www.ncbi.nlm.nih.gov/grc/human)