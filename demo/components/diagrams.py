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


ARCH_OVERVIEW = """
[bold cyan]┌─────────────────────────────────────────────────────────────────────┐[/bold cyan]
[bold cyan]│                        vcf-pg-loader                                │[/bold cyan]
[bold cyan]└─────────────────────────────────────────────────────────────────────┘[/bold cyan]
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
          ▼                       ▼                       ▼
[dim]┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
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
                 └─────────────────────────┘[/dim]
"""


ARCH_PARSER = """
[dim]┌─────────────────────────────────────────────────────────────────────┐
│                        vcf-pg-loader                                │
└─────────────────────────────────────────────────────────────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
          ▼                       ▼                       ▼[/dim]
[bold green]┌─────────────────┐[/bold green]   [dim]┌─────────────────┐   ┌─────────────────┐[/dim]
[bold green]│  VCF Parser     │[/bold green]   [dim]│   Normalizer    │   │  Schema Manager │[/dim]
[bold green]│  ─────────────  │[/bold green]   [dim]│  ─────────────  │   │  ─────────────  │[/dim]
[bold green]│  • cyvcf2       │[/bold green]   [dim]│  • Left-align   │   │  • DDL gen      │[/dim]
[bold green]│  • Streaming    │[/bold green]   [dim]│  • Trim         │   │  • Partitions   │[/dim]
[bold green]│  • Batch buffer │[/bold green]   [dim]│  • Decompose    │   │  • Indexes      │[/dim]
[bold green]└────────┬────────┘[/bold green]   [dim]└────────┬────────┘   └────────┬────────┘
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
                 └─────────────────────────┘[/dim]
"""


ARCH_NORMALIZER = """
[dim]┌─────────────────────────────────────────────────────────────────────┐
│                        vcf-pg-loader                                │
└─────────────────────────────────────────────────────────────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐[/dim]   [bold yellow]┌─────────────────┐[/bold yellow]   [dim]┌─────────────────┐
│  VCF Parser     │[/dim]   [bold yellow]│   Normalizer    │[/bold yellow]   [dim]│  Schema Manager │
│  ─────────────  │[/dim]   [bold yellow]│  ─────────────  │[/bold yellow]   [dim]│  ─────────────  │
│  • cyvcf2       │[/dim]   [bold yellow]│  • Left-align   │[/bold yellow]   [dim]│  • DDL gen      │
│  • Streaming    │[/dim]   [bold yellow]│  • Trim         │[/bold yellow]   [dim]│  • Partitions   │
│  • Batch buffer │[/dim]   [bold yellow]│  • Decompose    │[/bold yellow]   [dim]│  • Indexes      │
└────────┬────────┘[/dim]   [bold yellow]└────────┬────────┘[/bold yellow]   [dim]└────────┬────────┘
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
                 └─────────────────────────┘[/dim]
"""


ARCH_SCHEMA = """
[dim]┌─────────────────────────────────────────────────────────────────────┐
│                        vcf-pg-loader                                │
└─────────────────────────────────────────────────────────────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐   ┌─────────────────┐[/dim]   [bold magenta]┌─────────────────┐[/bold magenta]
[dim]│  VCF Parser     │   │   Normalizer    │[/dim]   [bold magenta]│  Schema Manager │[/bold magenta]
[dim]│  ─────────────  │   │  ─────────────  │[/dim]   [bold magenta]│  ─────────────  │[/bold magenta]
[dim]│  • cyvcf2       │   │  • Left-align   │[/dim]   [bold magenta]│  • DDL gen      │[/bold magenta]
[dim]│  • Streaming    │   │  • Trim         │[/dim]   [bold magenta]│  • Partitions   │[/bold magenta]
[dim]│  • Batch buffer │   │  • Decompose    │[/dim]   [bold magenta]│  • Indexes      │[/bold magenta]
[dim]└────────┬────────┘   └────────┬────────┘[/dim]   [bold magenta]└────────┬────────┘[/bold magenta]
[dim]         │                     │                     │
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
                 └─────────────────────────┘[/dim]
"""


ARCH_COPY = """
[dim]┌─────────────────────────────────────────────────────────────────────┐
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
                               ▼[/dim]
                 [bold blue]┌─────────────────────────┐[/bold blue]
                 [bold blue]│     Binary COPY         │[/bold blue]
                 [bold blue]│     (asyncpg)           │[/bold blue]
                 [bold blue]│  ───────────────────    │[/bold blue]
                 [bold blue]│  • Parallel workers     │[/bold blue]
                 [bold blue]│  • Batch inserts        │[/bold blue]
                 [bold blue]│  • 100K+ variants/sec   │[/bold blue]
                 [bold blue]└───────────┬─────────────┘[/bold blue]
[dim]                             │
                             ▼
                 ┌─────────────────────────┐
                 │      PostgreSQL         │
                 │  ───────────────────    │
                 │  • Partitioned tables   │
                 │  • Full SQL queries     │
                 │  • Concurrent access    │
                 │  • Audit trail          │
                 └─────────────────────────┘[/dim]
"""


ARCH_POSTGRES = """
[dim]┌─────────────────────────────────────────────────────────────────────┐
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
                             ▼[/dim]
                 [bold green]┌─────────────────────────┐[/bold green]
                 [bold green]│      PostgreSQL         │[/bold green]
                 [bold green]│  ───────────────────    │[/bold green]
                 [bold green]│  • Partitioned tables   │[/bold green]
                 [bold green]│  • Full SQL queries     │[/bold green]
                 [bold green]│  • Concurrent access    │[/bold green]
                 [bold green]│  • Audit trail          │[/bold green]
                 [bold green]└─────────────────────────┘[/bold green]
"""


def architecture_overview_panel() -> Panel:
    text = Text.from_markup(ARCH_OVERVIEW)
    return Panel(text, title="[bold]vcf-pg-loader Architecture[/bold]", border_style="cyan")


def architecture_parser_panel() -> Panel:
    text = Text.from_markup(ARCH_PARSER)
    return Panel(text, title="[bold]VCF Parser (cyvcf2)[/bold]", border_style="green")


def architecture_normalizer_panel() -> Panel:
    text = Text.from_markup(ARCH_NORMALIZER)
    return Panel(text, title="[bold]Normalizer[/bold]", border_style="yellow")


def architecture_schema_panel() -> Panel:
    text = Text.from_markup(ARCH_SCHEMA)
    return Panel(text, title="[bold]Schema Manager[/bold]", border_style="magenta")


def architecture_copy_panel() -> Panel:
    text = Text.from_markup(ARCH_COPY)
    return Panel(text, title="[bold]Binary COPY (asyncpg)[/bold]", border_style="blue")


def architecture_postgres_panel() -> Panel:
    text = Text.from_markup(ARCH_POSTGRES)
    return Panel(text, title="[bold]PostgreSQL[/bold]", border_style="green")


def data_flow_panel() -> Panel:
    return Panel(
        DATA_FLOW_SIMPLE,
        title="[bold]Data Flow[/bold]",
        border_style="blue",
    )


DATAFLOW_INPUT = """
  [bold cyan]VCF File[/bold cyan] ──▶ [dim]Parser ──▶ Normalizer ──▶ COPY ──▶ PostgreSQL[/dim]
     │           [dim]│            │           │           │[/dim]
     │           [dim]│            │           │           │[/dim]
   [bold cyan]Input[/bold cyan]     [dim]Streaming    Left-align   Binary      Query[/dim]
   [bold cyan]file[/bold cyan]       [dim]batches      & trim      protocol    ready![/dim]
"""


DATAFLOW_PARSER = """
  [dim]VCF File ──▶[/dim] [bold green]Parser[/bold green] [dim]──▶ Normalizer ──▶ COPY ──▶ PostgreSQL[/dim]
     [dim]│[/dim]           │            [dim]│           │           │[/dim]
     [dim]│[/dim]           │            [dim]│           │           │[/dim]
   [dim]Input[/dim]     [bold green]Streaming[/bold green]    [dim]Left-align   Binary      Query[/dim]
   [dim]file[/dim]       [bold green]batches[/bold green]      [dim]& trim      protocol    ready![/dim]
"""


DATAFLOW_NORMALIZER = """
  [dim]VCF File ──▶ Parser ──▶[/dim] [bold yellow]Normalizer[/bold yellow] [dim]──▶ COPY ──▶ PostgreSQL[/dim]
     [dim]│           │[/dim]            │           [dim]│           │[/dim]
     [dim]│           │[/dim]            │           [dim]│           │[/dim]
   [dim]Input     Streaming[/dim]    [bold yellow]Left-align[/bold yellow]   [dim]Binary      Query[/dim]
   [dim]file       batches[/dim]      [bold yellow]& trim[/bold yellow]      [dim]protocol    ready![/dim]
"""


DATAFLOW_COPY = """
  [dim]VCF File ──▶ Parser ──▶ Normalizer ──▶[/dim] [bold blue]COPY[/bold blue] [dim]──▶ PostgreSQL[/dim]
     [dim]│           │            │[/dim]           │           [dim]│[/dim]
     [dim]│           │            │[/dim]           │           [dim]│[/dim]
   [dim]Input     Streaming    Left-align[/dim]   [bold blue]Binary[/bold blue]      [dim]Query[/dim]
   [dim]file       batches      & trim[/dim]      [bold blue]protocol[/bold blue]    [dim]ready![/dim]
"""


DATAFLOW_POSTGRES = """
  [dim]VCF File ──▶ Parser ──▶ Normalizer ──▶ COPY ──▶[/dim] [bold green]PostgreSQL[/bold green]
     [dim]│           │            │           │[/dim]           │
     [dim]│           │            │           │[/dim]           │
   [dim]Input     Streaming    Left-align   Binary[/dim]      [bold green]Query[/bold green]
   [dim]file       batches      & trim      protocol[/dim]    [bold green]ready![/bold green]
"""


def dataflow_input_panel() -> Panel:
    text = Text.from_markup(DATAFLOW_INPUT)
    return Panel(text, title="[bold]Step 1: VCF File Input[/bold]", border_style="cyan")


def dataflow_parser_panel() -> Panel:
    text = Text.from_markup(DATAFLOW_PARSER)
    return Panel(text, title="[bold]Step 2: Streaming Parser[/bold]", border_style="green")


def dataflow_normalizer_panel() -> Panel:
    text = Text.from_markup(DATAFLOW_NORMALIZER)
    return Panel(text, title="[bold]Step 3: Variant Normalization[/bold]", border_style="yellow")


def dataflow_copy_panel() -> Panel:
    text = Text.from_markup(DATAFLOW_COPY)
    return Panel(text, title="[bold]Step 4: Binary COPY Protocol[/bold]", border_style="blue")


def dataflow_postgres_panel() -> Panel:
    text = Text.from_markup(DATAFLOW_POSTGRES)
    return Panel(text, title="[bold]Step 5: PostgreSQL Ready[/bold]", border_style="green")


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
