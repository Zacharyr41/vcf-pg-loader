# vcf-pg-loader Demo Narration Script

*A 3Blue1Brown-inspired script for voiceover. Read at a measured, thoughtful pace with natural pauses. Emphasize words in **bold**.*

---

## Opening

Let me show you something.

Every day, sequencing machines around the world generate terabytes of genetic data. And buried in that data—sometimes—is the answer to why a child is sick, or why a treatment isn't working.

But here's the problem: finding that answer means searching through **millions** of genetic variants to find the **one or two** that actually matter.

Today, I want to show you a tool that makes that search dramatically faster.

---

## Part I: Background

### Section 1: VCF Files — From DNA to Data

**[Slide 1: What is a VCF file?]**

Let's start with the basics. What exactly are we working with?

A VCF file—that's Variant Call Format—is essentially a "track changes" document for DNA. Instead of storing your entire genome, which would be about three billion letters, it only stores the places where **your** DNA differs from the reference.

Think of it like this: instead of copying an entire book, you just note the typos.

**[Slide 2: The Journey]**

Here's how we get from a patient sample to a VCF file. Follow the numbers on the diagram.

**Step ①**: It starts with a DNA sample—usually blood or tissue from the patient.

**Step ②**: That sample goes into a sequencing machine. Illumina, PacBio, whatever technology. The machine reads millions of short fragments of DNA.

**Step ③**: Out comes a FASTQ file—billions of short reads, each about 150 base pairs long.

**Step ④**: Next, alignment. Software like BWA takes each read and figures out where it came from in the human reference genome.

**Step ⑤**: The result is a BAM or CRAM file—all those reads, now sorted and indexed by their position.

**Step ⑥**: Then variant calling. Software like GATK compares the aligned reads to the reference and identifies every position where this person's DNA is **different**.

**Step ⑦**: And finally, the output: a VCF file. Every variant, catalogued and ready for analysis.

**[Slide 3: Why VCF?]**

Now, why does this format matter? Why not just work with the full genome?

A full genome takes about 100 gigabytes. A VCF file? Maybe 100 to 500 megabytes. That's a **200x** reduction.

And that difference changes everything.

**Storage**: You can keep thousands of VCF files on a single hard drive. Try that with raw genomes and you need a data center.

**Speed**: Loading a VCF takes seconds. Loading a full genome? Minutes to hours. When you're iterating on analysis, that matters.

**Accessibility**: A researcher with a laptop can work with VCF files. No cluster required. No cloud bill. Just open it and start querying.

**Transfer**: Sending a VCF to a collaborator takes minutes. Sending a full genome? Better start that upload before lunch.

And it's standardized. Every major sequencing center, every research hospital, every diagnostic lab—they all speak VCF. Your analysis pipeline works everywhere.

**[Slide 4: Reference vs Sample]**

Let me show you what a variant actually looks like.

Here's the reference genome at position 12,345. It says "G." But this patient's DNA says "A." That single letter change? That's a variant. And that's what gets recorded in the VCF file.

---

### Section 2: Anatomy of a VCF File

**[Slide 5: VCF Header]**

Now let's look inside an actual VCF file.

At the top, you have a header. This defines what data you're about to see—what annotations are included, what format the sample data is in.

**[Slide 6: VCF Columns]**

Below the header, each line is a variant. You have chromosome, position, what the reference says, what this sample has instead, a quality score, and then the actual genotype data.

**[Slide 7: A Simple SNP]**

Here's a real example. Chromosome 1, position 12,345. Reference is A, alternate is G. Quality score of 99.5—that's high confidence. And the genotype is 0/1, which means this person has **one copy** of the reference and **one copy** of the variant.

**[Slide 8: Variant Types]**

Not all variants are single letter changes. You can have deletions, where bases are removed. Insertions, where bases are added. And larger structural variants that rearrange entire sections of DNA.

**[Slide 9: Deletion Example]**

Here's a deletion. The reference has ACTG—four bases. This patient just has A. The CTG got deleted.

**[Slide 10: Multi-allelic Variants]**

Sometimes, at a single position, you find **multiple** different variants in your cohort. These are called multi-allelic sites, and they need special handling during analysis.

**[Slide 11: INFO Field Numbers]**

One more technical detail that trips people up: the Number specification. When a field says Number=A, it means there's one value per alternate allele. Number=R means one value per **all** alleles, including reference.

**[Slide 12: Genotype Notation]**

And finally, genotypes. Zero-zero means homozygous reference—two copies of the normal sequence. Zero-one means heterozygous—one normal, one variant. One-one means homozygous variant—both copies are changed.

---

### Section 3: VCF in Rare Disease Research

**[Slide 13: The Challenge]**

Now here's where it gets interesting.

A typical person has 4 to 6 **million** variants compared to the reference. But if they have a rare genetic disease? Only **one to three** of those variants are actually causing the problem.

That's the needle in the haystack.

**[Slide 14: Impact Levels]**

So how do we narrow down millions to a handful?

We start with impact. Some variants completely break a gene—these are HIGH impact. Stop codons, frameshifts, splice site disruptions. Others change the protein but might be tolerated—MODERATE impact. And some don't change the protein at all—LOW impact or just modifiers.

**[Slide 15: Filtering Criteria]**

Then we filter on frequency. If a variant is common in the population—say, more than 1% of people have it—it's probably not causing a rare disease.

We also check ClinVar, a database of known disease-causing variants. And we look at computational predictions of how damaging a variant might be.

**[Slide 16: Inheritance Patterns]**

Inheritance matters too. Some diseases require variants from **both** parents—that's recessive. Some only need one copy—dominant. And some appear brand new in the child, not inherited at all—de novo.

**[Slide 17: Filtering Cascade]**

Put it all together and you get a filtering cascade. Start with 5 million variants. Remove the common ones—down to 50,000. Keep only the damaging ones—2,000. Match the inheritance pattern—100. Check the databases—maybe 5 to 20 candidates.

That's what a researcher manually reviews.

**[Slide 18: Why SQL?]**

And here's the key insight: researchers **iterate**. They try one set of filters, look at the results, then tweak the filters and try again.

If every tweak requires re-running an entire pipeline... that's hours of waiting. But if the data is in a database? A new query takes **seconds**.

---

## Part II: The Tool

### Section 4: Previous Tools

**[Slide 19: GEMINI]**

Now, this idea—putting VCF data into a SQL database—isn't new.

GEMINI came out in 2013 from the Quinlan Lab. It was brilliant for its time. Load a VCF into SQLite, run SQL queries. Revolutionary.

But it had limits. SQLite is single-user. Loading was slow. And the project was archived in 2019.

**[Slide 20: slivar]**

Then came slivar in 2021. Blazing fast streaming filter. Great for one-shot analysis. But no persistent storage. Every new query means reprocessing the original VCF.

**[Slide 21: The Gap]**

So there's a gap. We need GEMINI's query flexibility, slivar's speed, plus multi-user access, audit trails for clinical compliance, and the ability to add samples mid-study.

That's what vcf-pg-loader provides.

---

### Section 5: vcf-pg-loader Architecture

**[Slide 22: Tool Comparison]**

Let me show you the comparison.

GEMINI: SQLite, about 5,000 variants per second, limited scaling.
slivar: No database, streaming only.
vcf-pg-loader: PostgreSQL, over 100,000 variants per second, unlimited concurrent access.

**[Slide 23: Architecture]**

Here's how it works under the hood.

VCF files stream through cyvcf2—a fast C-based parser. Variants get normalized using the vt algorithm. Then they're loaded into PostgreSQL using the binary COPY protocol—that's the fastest way to insert data into Postgres.

**[Slide 24: Data Flow]**

The flow is simple: VCF in, streaming parser, normalization, binary COPY, PostgreSQL. Query ready.

**[Slide 25: Components]**

Four key components. The parser handles streaming and batching. The normalizer left-aligns indels and decomposes multi-allelics. Binary COPY uses asyncpg for maximum throughput. And the schema manager handles partitioning by chromosome.

**[Slide 26: Zero-Config]**

And here's the best part: you don't need to set up PostgreSQL yourself. Just run the command. vcf-pg-loader spins up a managed database in Docker, loads your data, and you're querying within minutes.

---

### Section 6: Research Pipeline Walkthrough

**[Slide 27: Pipeline Overview]**

Let me walk you through a real rare disease analysis.

You have a trio: child, mother, father. Three VCF files. You want to find de novo variants—mutations that appeared in the child but aren't in either parent.

**[Slide 28: Load the Data]**

Step one: load all three VCFs. One command per file. Takes maybe 30 seconds each.

**[Slide 29: Query for Candidates]**

Step two: write a SQL query. Give me variants in the proband that are rare, HIGH or MODERATE impact, and not classified as benign.

That query runs in seconds. Returns your candidates.

**[Slide 30: Compound Heterozygotes]**

Want to check for compound heterozygotes—two different damaging variants in the same gene? That's another SQL query. Group by gene, count heterozygous variants, filter for genes with two or more.

**[Slide 31: Adding Samples]**

Now here's where it gets powerful. Mid-study, a sibling's sample arrives. Just load it. No re-processing. The new sample is immediately queryable alongside the existing data.

**[Slide 32: Iterative Research]**

Compare the workflows. Traditional: new filter idea, re-run pipeline, wait hours, review results. With vcf-pg-loader: new filter idea, write SQL, execute in seconds, review results. Iterate as fast as you can think.

---

### Section 7: Performance & Compliance

**[Slide 33: Benchmarks]**

Let's talk numbers.

100,000 variants: about 1.2 seconds to load. A million variants: 11 seconds. Five million: under a minute. Sustained throughput of 90,000+ variants per second.

**[Slide 34: Why PostgreSQL?]**

Why PostgreSQL specifically?

Performance: binary protocol, parallel queries, advanced optimizer. Reliability: ACID compliance, point-in-time recovery. Ecosystem: every BI tool connects to Postgres. Cloud providers all offer managed Postgres.

**[Slide 35: Clinical Compliance]**

For clinical work, you need audit trails. Every load is tracked—timestamp, source file, MD5 checksum. You can trace any variant back to its origin. Role-based access control. SSL encryption. HIPAA-compatible infrastructure.

**[Slide 36: Validation]**

And validation is built in. Post-load verification, duplicate detection, batch IDs linking variants to source files. Everything is reproducible.

---

### Section 8: Future

**[Slide 37: The Vision]**

Let me show you where this is going.

Right now, we find exact matches. Variants that pass specific filters. But what if we could find **similar** cases?

**[Slide 38: Vector Architecture]**

Imagine embedding each variant profile as a vector. Gene context, consequence type, pathogenicity scores, even phenotype terms. Store these in pgvector—PostgreSQL's vector extension.

**[Slide 39: Applications]**

Now you can ask: show me patients with variant profiles **similar** to this undiagnosed case. Surface diagnoses from cases that looked like this one. Find research candidates with matching patterns.

It's a different way of thinking about genomic data.

**[Slide 40: Thank You]**

vcf-pg-loader is open source. Available on PyPI, Bioconda, and GitHub.

If you work with VCF data and you're tired of waiting for pipelines, give it a try. Load a file. Run a query. See how fast iteration can be.

Thank you.

---

## Recording Notes

- **Pacing**: Average 120-140 words per minute. Pause between sections.
- **Tone**: Curious, explanatory, building toward insight. Never rushed.
- **Emphasis**: Use natural emphasis on key terms (bolded above).
- **Pauses**: 1-2 seconds between paragraphs, 3-4 seconds between sections.
- **Music**: Subtle ambient background, similar to 3B1B style. Fade in during visuals, down during speech.

### Tools for Recording

- **Voice**: Record with a quality USB microphone in a quiet room
- **Editing**: Audacity (free) or Adobe Audition
- **Sync**: Use video editor to align audio with screen recording
- **Text-to-Speech Alternative**: ElevenLabs or other AI voice tools can produce natural narration from this script
