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

And it's standardized. The VCF format is maintained by the **GA4GH**—the Global Alliance for Genomics and Health. That's a consortium of over 600 organizations: research institutes, hospitals, pharma companies, governments. They agree on how genomic data should be formatted and shared.

So when you write code that reads a VCF from the Broad Institute, that same code works on a VCF from the UK Biobank, or from a hospital in Tokyo. Every major sequencing center, every research hospital, every diagnostic lab—they all speak VCF. Your analysis pipeline works everywhere.

**[Slide 4: Reference vs Sample]**

Let me show you what a variant actually looks like.

Here's the reference genome at position 12,345. It says "G." But this patient's DNA says "A." That single letter change? That's a variant. And that's what gets recorded in the VCF file.

---

### Section 2: Anatomy of a VCF File

**[Slide 5: VCF Header]**

Now let's look inside an actual VCF file.

At the top, you have a header. Lines starting with double hash marks. This defines what data you're about to see—what annotations are included, what format the sample data is in. Think of it as a data dictionary.

**[Slide 6: VCF Data Line]**

Below the header, each line is one variant. Let me walk you through each column.

**[Slide 7: CHROM & POS]**

First, the **location columns**. CHROM tells you which chromosome—1 through 22, X, Y, or mitochondrial. POS is the position on that chromosome, 1-based.

Together, chromosome and position uniquely identify where this variant is in the genome. It's like a street address for DNA.

**[Slide 8: ID]**

Next, the **ID column**. This is an identifier, often from dbSNP—a public database of known variants.

If you see "rs" followed by numbers, that's a dbSNP ID. It means this variant has been seen before. A dot means it's novel—no known ID.

**[Slide 9: REF & ALT]**

Now the heart of it: **REF and ALT**. REF is what the reference genome has at this position. ALT is what this sample has instead.

In this example, A goes to G. That's a single nucleotide change—a SNP. But ALT can have multiple values separated by commas, meaning we found multiple different variants at this position.

**[Slide 10: QUAL & FILTER]**

**Quality control columns**. QUAL is a confidence score—higher means more confident. 99.5 is excellent.

FILTER tells you if the variant passed quality checks. "PASS" means yes. Anything else—like "LowQual"—means it failed some filter and you should be cautious.

**[Slide 11: INFO]**

The **INFO column** is where variant-level annotations live. It's a semicolon-separated list of key-value pairs.

AC is allele count—how many chromosomes carry this variant. AF is allele frequency.

DP is depth—and this one's important. Remember those short reads from the sequencing machine? DP=30 means 30 of those reads happened to overlap this exact position in the genome.

Why does that matter? More reads means more confidence. If only 2 reads covered a position and one says A, one says G—is that a real variant or just sequencing error? Hard to tell. But if 30 reads cover it and 15 say A, 15 say G? That's a solid heterozygous call.

Think of it like witnesses to an event. One witness? Unreliable. Thirty witnesses agreeing on the details? Much more credible.

These INFO fields are defined in the header and vary by variant caller.

**[Slide 12: FORMAT & SAMPLE]**

Finally, **FORMAT and SAMPLE**. This is where per-sample data lives—the actual results for each person sequenced.

Here's the key insight: everything before this—CHROM, POS, REF, ALT, INFO—describes the **variant itself**. But FORMAT and SAMPLE describe **what each person has** at that variant.

**FORMAT** is like a column header for sample data. It says: "here are the fields I'm about to give you, in this order." GT:AD:GQ means "first genotype, then allelic depths, then genotype quality." Different variant callers include different fields, so FORMAT tells you how to parse what follows.

**SAMPLE** columns contain the actual values. If you sequenced a family of four, you'd have four sample columns—maybe named "proband," "mother," "father," "sibling." Each column has the same structure defined by FORMAT, but different values for each person.

The sample data here reads: 0/1:15,15:99. Let's decode it:

**GT** is genotype. Remember, humans are diploid—we have two copies of each chromosome, one inherited from mom, one from dad. GT tells you what alleles this person has on each copy.

The numbers refer to alleles: 0 means reference, 1 means the first alternate, 2 would mean a second alternate if there was one.

So 0/1 means one chromosome has the reference allele, the other has the alternate. We call that **heterozygous**—two different alleles. If it were 1/1, both copies carry the variant—**homozygous alternate**. 0/0 means neither copy has the variant—**homozygous reference**.

**AD** is allelic depth—how many sequencing reads supported each allele. AD=15,15 means 15 reads showed the reference A, and 15 reads showed the alternate G. A nice even split, which is exactly what you'd expect for a true heterozygous call. If it were 28,2? That might be a sequencing error rather than a real variant.

**GQ** is genotype quality—how confident are we in this specific genotype call? 99 is very high, essentially saying "we're nearly certain this person is heterozygous." This is different from QUAL, which measures confidence that a variant exists at all. GQ measures confidence in *which genotype* this individual has.

One more thing: these sample-level annotations come from the **variant caller**—software like GATK, DeepVariant, or Strelka. The caller looks at the aligned reads, counts alleles, applies statistical models, and outputs these values. Different callers may include different fields: some add PL for phred-scaled likelihoods, DP for per-sample depth, or SB for strand bias. The header defines what's available, and FORMAT tells you what's in each record.

**[Slide 13: A Simple SNP]**

Let's put it all together with a real example. Chromosome 1, position 12,345. This is a known variant, rs123456. Reference is A, alternate is G. High quality, passed filters. And the genotype is 0/1—heterozygous.

**[Slide 14: Variant Types]**

Not all variants are single letter changes. Let me walk through the main types.

**SNP**—Single Nucleotide Polymorphism. One letter changes to another. A becomes G, C becomes T. The simplest and most common type of variant. About 4 to 5 million SNPs in a typical genome.

**Insertion**. Extra bases get added where they weren't in the reference. Could be one base, could be hundreds. In the VCF, you'll see a short REF and a longer ALT—the extra bases are what got inserted.

**Deletion**. The opposite—bases that were in the reference are missing in this person. Longer REF, shorter ALT. The missing bases got deleted.

**Indel**. A catchall term for insertions and deletions together. Sometimes you'll see both happen at once—a few bases removed and different ones added in their place. Bioinformaticians call these "indels" because they're mechanistically similar.

**MNP**—Multi-Nucleotide Polymorphism. Two or more adjacent bases change together. Instead of A changing to G, maybe ACT changes to GGA. Rarer than SNPs, but important because they might affect codons differently than individual SNPs would.

**Structural Variants**. The big ones. Large deletions or duplications of hundreds to millions of bases. Inversions where a section of DNA gets flipped. Translocations where pieces from different chromosomes get swapped. These can be harder to detect and represent in VCF format, but they're often clinically significant.

**Copy Number Variants**—CNVs. A special case of structural variants where sections of the genome are duplicated or deleted. Instead of two copies of a gene, someone might have one or three or four. These are common and often affect gene expression.

Most analysis focuses on SNPs and small indels because they're the easiest to call confidently. But structural variants are getting more attention as long-read sequencing improves.

**[Slide 15: Deletion Example]**

Here's a deletion. The reference has ACTG—four bases. This patient just has A. The CTG got deleted. Notice the anchor base—we keep one base to mark the position.

**[Slide 16: Multi-allelic Variants]**

Sometimes, at a single position, you find **multiple** different variants. See the ALT column? A comma T. Two different alternates at the same spot.

These are called multi-allelic sites, and they need special handling. Most tools split them into separate records for analysis.

**[Slide 17: INFO Field Numbers]**

One more technical detail that trips people up: the Number specification in the header.

Number=1 means exactly one value. Number=A means one value per alternate allele. Number=R means one per **all** alleles, including reference. Number=G means one per possible genotype—that's where the math gets interesting.

**[Slide 18: Genotype Notation]**

And finally, let's decode genotypes. Zero-zero means homozygous reference—two copies of the normal sequence. Zero-one means heterozygous—one normal, one variant. One-one means homozygous alternate—both copies are changed.

Dot-slash-dot means no call—we couldn't determine the genotype at this position.

---

### Section 3: VCF in Rare Disease Research

**[Slide 19: The Challenge]**

Now here's where it gets interesting.

A typical person has 4 to 6 **million** variants compared to the reference. But if they have a rare genetic disease? Only **one to three** of those variants are actually causing the problem.

That's the needle in the haystack.

**[Slide 20: Impact Levels]**

So how do we narrow down millions to a handful?

We start with impact. Some variants completely break a gene—these are HIGH impact. Stop codons, frameshifts, splice site disruptions. Others change the protein but might be tolerated—MODERATE impact. And some don't change the protein at all—LOW impact or just modifiers.

**[Slide 21: Filtering Criteria]**

Then we filter on frequency. If a variant is common in the population—say, more than 1% of people have it—it's probably not causing a rare disease.

We also check ClinVar, a database of known disease-causing variants. And we look at computational predictions of how damaging a variant might be.

**[Slide 22: Inheritance Patterns]**

Inheritance matters too. Some diseases require variants from **both** parents—that's recessive. Some only need one copy—dominant. And some appear brand new in the child, not inherited at all—de novo.

**[Slide 23: Filtering Cascade]**

Put it all together and you get a filtering cascade. Start with 5 million variants. Remove the common ones—down to 50,000. Keep only the damaging ones—2,000. Match the inheritance pattern—100. Check the databases—maybe 5 to 20 candidates.

That's what a researcher manually reviews.

**[Slide 24: Why SQL?]**

And here's the key insight: researchers **iterate**. They try one set of filters, look at the results, then tweak the filters and try again.

If every tweak requires re-running an entire pipeline... that's hours of waiting. But if the data is in a database? A new query takes **seconds**.

---

## Part II: The Tool

### Section 4: Previous Tools

**[Slide 25: GEMINI]**

Now, this idea—putting VCF data into a SQL database—isn't new.

GEMINI came out in 2013 from the Quinlan Lab. It was brilliant for its time. Load a VCF into SQLite, run SQL queries. Revolutionary.

But it had limits. SQLite is single-user. Loading was slow. And the project was archived in 2019.

**[Slide 26: slivar]**

Then came slivar in 2021. Blazing fast streaming filter. Great for one-shot analysis. But no persistent storage. Every new query means reprocessing the original VCF.

**[Slide 27: The Gap]**

So there's a gap. We need GEMINI's query flexibility, slivar's speed, plus multi-user access, audit trails for clinical compliance, and the ability to add samples mid-study.

That's what vcf-pg-loader provides.

---

### Section 5: vcf-pg-loader Architecture

**[Slide 28: Tool Comparison]**

Let me show you the comparison.

GEMINI: SQLite, about 5,000 variants per second, limited scaling.
slivar: No database, streaming only.
vcf-pg-loader: PostgreSQL, over 100,000 variants per second, unlimited concurrent access.

**[Slide 29: Architecture]**

Here's how it works under the hood.

VCF files stream through cyvcf2—a fast C-based parser. Variants get normalized using the vt algorithm. Then they're loaded into PostgreSQL using the binary COPY protocol—that's the fastest way to insert data into Postgres.

**[Slide 30: Data Flow]**

The flow is simple: VCF in, streaming parser, normalization, binary COPY, PostgreSQL. Query ready.

**[Slide 31: Components]**

Four key components. The parser handles streaming and batching. The normalizer left-aligns indels and decomposes multi-allelics. Binary COPY uses asyncpg for maximum throughput. And the schema manager handles partitioning by chromosome.

**[Slide 32: Zero-Config]**

And here's the best part: you don't need to set up PostgreSQL yourself. Just run the command. vcf-pg-loader spins up a managed database in Docker, loads your data, and you're querying within minutes.

---

### Section 6: Research Pipeline Walkthrough

**[Slide 33: Pipeline Overview]**

Let me walk you through a real rare disease analysis.

You have a trio: child, mother, father. Three VCF files. You want to find de novo variants—mutations that appeared in the child but aren't in either parent.

**[Slide 34: Load the Data]**

Step one: load all three VCFs. One command per file. Takes maybe 30 seconds each.

**[Slide 35: Query for Candidates]**

Step two: write a SQL query. Give me variants in the proband that are rare, HIGH or MODERATE impact, and not classified as benign.

That query runs in seconds. Returns your candidates.

**[Slide 36: Compound Heterozygotes]**

Want to check for compound heterozygotes—two different damaging variants in the same gene? That's another SQL query. Group by gene, count heterozygous variants, filter for genes with two or more.

**[Slide 37: Adding Samples]**

Now here's where it gets powerful. Mid-study, a sibling's sample arrives. Just load it. No re-processing. The new sample is immediately queryable alongside the existing data.

**[Slide 38: Iterative Research]**

Compare the workflows. Traditional: new filter idea, re-run pipeline, wait hours, review results. With vcf-pg-loader: new filter idea, write SQL, execute in seconds, review results. Iterate as fast as you can think.

---

### Section 7: Performance & Compliance

**[Slide 39: Benchmarks]**

Let's talk numbers.

100,000 variants: about 1.2 seconds to load. A million variants: 11 seconds. Five million: under a minute. Sustained throughput of 90,000+ variants per second.

**[Slide 40: Why PostgreSQL?]**

Why PostgreSQL specifically?

Performance: binary protocol, parallel queries, advanced optimizer. Reliability: ACID compliance, point-in-time recovery. Ecosystem: every BI tool connects to Postgres. Cloud providers all offer managed Postgres.

**[Slide 41: Clinical Compliance]**

For clinical work, you need audit trails. Every load is tracked—timestamp, source file, MD5 checksum. You can trace any variant back to its origin. Role-based access control. SSL encryption. HIPAA-compatible infrastructure.

**[Slide 42: Validation]**

And validation is built in. Post-load verification, duplicate detection, batch IDs linking variants to source files. Everything is reproducible.

---

### Section 8: Future

**[Slide 43: The Vision]**

Let me show you where this is going.

Right now, we find exact matches. Variants that pass specific filters. But what if we could find **similar** cases?

**[Slide 44: Vector Architecture]**

Imagine embedding each variant profile as a vector. Gene context, consequence type, pathogenicity scores, even phenotype terms. Store these in pgvector—PostgreSQL's vector extension.

**[Slide 45: Applications]**

Now you can ask: show me patients with variant profiles **similar** to this undiagnosed case. Surface diagnoses from cases that looked like this one. Find research candidates with matching patterns.

It's a different way of thinking about genomic data.

**[Slide 46: Thank You]**

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
