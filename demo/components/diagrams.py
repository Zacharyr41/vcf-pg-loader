from rich.panel import Panel
from rich.text import Text

DNA_SEQUENCING_FLOW = """
        ┌─────────────────┐
   [bold cyan]①[/bold cyan]  │   DNA Sample    │    [dim]Patient blood or tissue sample[/dim]
        │   (Patient)     │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
   [bold cyan]②[/bold cyan]  │   Sequencing    │    [dim]Machines read DNA fragments[/dim]
        │   (Illumina,    │
        │    PacBio, etc) │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
   [bold cyan]③[/bold cyan]  │   FASTQ Files   │    [dim]Billions of short reads[/dim]
        │   (Raw reads)   │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
   [bold cyan]④[/bold cyan]  │   Alignment     │    [dim]Map reads to reference genome[/dim]
        │   (BWA, etc)    │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
   [bold cyan]⑤[/bold cyan]  │   BAM/CRAM      │    [dim]Sorted, indexed alignments[/dim]
        │   (Aligned)     │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
   [bold cyan]⑥[/bold cyan]  │  Variant Call   │    [dim]Find differences from reference[/dim]
        │  (GATK, etc)    │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
   [bold green]⑦[/bold green]  │    VCF File     │    [dim]Final output: all variants[/dim]
        │   (Variants)    │
        └─────────────────┘
"""

REFERENCE_VS_SAMPLE = """
Reference Genome (GRCh38):
    ... A T C [bold cyan]G[/bold cyan] A T T C G A ...
                ↑
                Position 12345

Patient's DNA:
    ... A T C [bold red]A[/bold red] A T T C G A ...
                ↑
                [bold yellow]Variant: G → A[/bold yellow]

[dim]This difference is recorded in the VCF file[/dim]
"""

VCF_PG_LOADER_ARCHITECTURE = """
┌─────────────────────────────────────────────────────────────────────┐
│                        vcf-pg-loader                                │
└─────────────────────────────────────────────────────────────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  VCF Parser     │   │   Normalizer    │   │  Schema Manager │
│  ─────────────  │   │  ─────────────  │   │  ─────────────  │
│  • cyvcf2       │   │  • Left-align   │   │  • DDL gen      │
│  • Streaming    │   │  • Trim         │   │  • Partitions   │
│  • Batch buffer │   │  • Decompose    │   │  • Indexes      │
└────────┬────────┘   └────────┬────────┘   └────────┬────────┘
         │                     │                     │
         └─────────────────────┼─────────────────────┘
                               │
                               ▼
                 ┌─────────────────────────┐
                 │     Binary COPY         │
                 │     (asyncpg)           │
                 │  ───────────────────    │
                 │  • Parallel workers     │
                 │  • Batch inserts        │
                 │  • 100K+ variants/sec   │
                 └───────────┬─────────────┘
                             │
                             ▼
                 ┌─────────────────────────┐
                 │      PostgreSQL         │
                 │  ───────────────────    │
                 │  • Partitioned tables   │
                 │  • Full SQL queries     │
                 │  • Concurrent access    │
                 │  • Audit trail          │
                 └─────────────────────────┘
"""

DATA_FLOW_SIMPLE = """
  VCF File ──▶ Parser ──▶ Normalizer ──▶ COPY ──▶ PostgreSQL
     │           │            │           │           │
     │           │            │           │           │
   Input     Streaming    Left-align   Binary      Query
   file       batches      & trim      protocol    ready!
"""

RARE_DISEASE_PIPELINE = """
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Proband    │    │    Mother    │    │    Father    │
│   VCF        │    │    VCF       │    │    VCF       │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │    vcf-pg-loader       │
              │    ────────────────    │
              │    Load all samples    │
              │    to PostgreSQL       │
              └───────────┬────────────┘
                          │
                          ▼
              ┌────────────────────────┐
              │    PostgreSQL DB       │
              │    ────────────────    │
              │    Unified variant     │
              │    database            │
              └───────────┬────────────┘
                          │
       ┌──────────────────┼──────────────────┐
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  De novo    │   │  Recessive  │   │  Compound   │
│  Query      │   │  Query      │   │  Het Query  │
└─────────────┘   └─────────────┘   └─────────────┘
"""

ITERATIVE_RESEARCH = """
Traditional Pipeline:                    vcf-pg-loader Pipeline:
─────────────────────                    ──────────────────────

  New Filter Idea                          New Filter Idea
        │                                        │
        ▼                                        ▼
  Re-run entire                            Write SQL query
  analysis pipeline                              │
        │                                        ▼
        ▼                                   Execute
  Wait 2-4 hours                            (seconds)
        │                                        │
        ▼                                        ▼
  Review results                            Review results
        │                                        │
        │                                        │
        ▼                                        ▼
  [bold red]Repeat for each[/bold red]                       [bold green]Iterate rapidly[/bold green]
  [bold red]filter change[/bold red]                         [bold green]with SQL[/bold green]
"""

VECTOR_EMBEDDING_FUTURE = """
                    ┌─────────────────────────┐
                    │   Filtered Variants     │
                    │   (from SQL query)      │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   Vector Embedding      │
                    │   ────────────────      │
                    │   • Variant features    │
                    │   • Gene context        │
                    │   • Phenotype terms     │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   pgvector (PostgreSQL) │
                    │   ────────────────────  │
                    │   Similarity search     │
                    └───────────┬─────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            │                   │                   │
            ▼                   ▼                   ▼
   ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
   │ Similar Cases   │ │ Known Diagnoses │ │ Research        │
   │ from cohort     │ │ matching pattern│ │ Candidates      │
   └─────────────────┘ └─────────────────┘ └─────────────────┘
"""


def sequencing_flow_panel() -> Panel:
    text = Text.from_markup(DNA_SEQUENCING_FLOW)
    return Panel(
        text,
        title="[bold]DNA to VCF Pipeline[/bold]",
        border_style="cyan",
    )


def reference_comparison_panel() -> Panel:
    text = Text.from_markup(REFERENCE_VS_SAMPLE)
    return Panel(
        text,
        title="[bold]Reference vs Sample[/bold]",
        border_style="yellow",
    )


def architecture_panel() -> Panel:
    return Panel(
        VCF_PG_LOADER_ARCHITECTURE,
        title="[bold]vcf-pg-loader Architecture[/bold]",
        border_style="green",
    )


def data_flow_panel() -> Panel:
    return Panel(
        DATA_FLOW_SIMPLE,
        title="[bold]Data Flow[/bold]",
        border_style="blue",
    )


def rare_disease_pipeline_panel() -> Panel:
    return Panel(
        RARE_DISEASE_PIPELINE,
        title="[bold]Rare Disease Research Pipeline[/bold]",
        border_style="magenta",
    )


def iterative_research_panel() -> Panel:
    text = Text.from_markup(ITERATIVE_RESEARCH)
    return Panel(
        text,
        title="[bold]Iterative Research Comparison[/bold]",
        border_style="cyan",
    )


def vector_embedding_panel() -> Panel:
    return Panel(
        VECTOR_EMBEDDING_FUTURE,
        title="[bold]Future: Vector Similarity Search[/bold]",
        border_style="magenta",
    )
