from rich.console import Group
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

from ..components.diagrams import (
    architecture_copy_panel,
    architecture_normalizer_panel,
    architecture_overview_panel,
    architecture_parser_panel,
    architecture_postgres_panel,
    architecture_schema_panel,
    dataflow_copy_panel,
    dataflow_input_panel,
    dataflow_normalizer_panel,
    dataflow_parser_panel,
    dataflow_postgres_panel,
    iterative_research_panel,
    pipeline_database_panel,
    pipeline_loader_panel,
    pipeline_queries_panel,
    pipeline_trio_panel,
    vector_embedding_panel,
)
from ..components.vcf_snippets import (
    SQL_COMPOUND_HET,
    SQL_RARE_DISEASE_QUERY,
    benchmark_table,
    sql_query_panel,
    tool_comparison_table,
)
from ..presenter import Presenter


def add_section_4(p: Presenter) -> None:
    p.section(4, "Previous Tools: GEMINI & slivar")

    p.slide(
        "GEMINI: The Original SQL VCF Database",
        lambda: Panel(
            Text.from_markup(
                "[bold cyan]GEMINI (2013)[/bold cyan]\n"
                "Quinlan Lab, University of Utah\n\n"
                "[bold green]Innovations:[/bold green]\n"
                "  • First to load VCF into a SQL database (SQLite)\n"
                "  • Pre-computed genotype queries\n"
                "  • Built-in annotation integration\n"
                "  • Pedigree-aware queries\n\n"
                "[bold red]Limitations:[/bold red]\n"
                "  • SQLite: single-user, no concurrent access\n"
                "  • Slow loading (~5K variants/sec)\n"
                "  • Limited to ~10M variants\n"
                "  • [bold]Archived/unmaintained since 2019[/bold]"
            ),
            title="[bold]GEMINI[/bold]",
            border_style="yellow",
        ),
    )

    p.slide(
        "slivar: Streaming Variant Filter",
        lambda: Panel(
            Text.from_markup(
                "[bold cyan]slivar (2021)[/bold cyan]\n"
                "Brent Pedersen, University of Colorado\n\n"
                "[bold green]Innovations:[/bold green]\n"
                "  • Blazing fast streaming filter\n"
                "  • Custom expression language\n"
                "  • No database needed\n"
                "  • Family-aware filtering\n\n"
                "[bold red]Limitations:[/bold red]\n"
                "  • No persistent storage\n"
                "  • Must re-filter for each query\n"
                "  • Custom syntax (not SQL)\n"
                "  • No multi-sample cohort analysis"
            ),
            title="[bold]slivar[/bold]",
            border_style="cyan",
        ),
    )

    p.slide(
        "The Gap: What's Missing?",
        lambda: Panel(
            Text.from_markup(
                "[bold yellow]Research Reality:[/bold yellow]\n"
                "  • Researchers iterate on filter criteria\n"
                "  • Need to add samples during study\n"
                "  • Want to query across cohorts\n"
                "  • Need audit trails for compliance\n\n"
                "[bold cyan]The Ideal Tool Would:[/bold cyan]\n"
                "  • Load fast (like slivar)\n"
                "  • Query flexibly (like GEMINI)\n"
                "  • Scale to large cohorts\n"
                "  • Support concurrent access\n"
                "  • Maintain clinical compliance"
            ),
            title="[bold]The Gap in Current Tools[/bold]",
            border_style="magenta",
        ),
    )


def add_section_5(p: Presenter) -> None:
    p.section(5, "vcf-pg-loader Architecture")

    p.slide(
        "Tool Comparison",
        tool_comparison_table,
    )

    p.slide(
        "Architecture Overview",
        architecture_overview_panel,
    )

    p.slide(
        "VCF Parser (cyvcf2)",
        architecture_parser_panel,
    )

    p.slide(
        "Normalizer",
        architecture_normalizer_panel,
    )

    p.slide(
        "Schema Manager",
        architecture_schema_panel,
    )

    p.slide(
        "Binary COPY (asyncpg)",
        architecture_copy_panel,
    )

    p.slide(
        "PostgreSQL",
        architecture_postgres_panel,
    )

    p.slide(
        "Data Flow: VCF Input",
        dataflow_input_panel,
    )

    p.slide(
        "Data Flow: Streaming Parser",
        dataflow_parser_panel,
    )

    p.slide(
        "Data Flow: Normalization",
        dataflow_normalizer_panel,
    )

    p.slide(
        "Data Flow: Binary COPY",
        dataflow_copy_panel,
    )

    p.slide(
        "Data Flow: Query Ready",
        dataflow_postgres_panel,
    )

    p.slide(
        "Zero-Config Mode",
        lambda: Group(
            Panel(
                Text.from_markup(
                    "[bold green]No PostgreSQL setup required![/bold green]\n\n"
                    "vcf-pg-loader can manage a local PostgreSQL database automatically:\n\n"
                    "[dim]1. Spins up PostgreSQL in Docker[/dim]\n"
                    "[dim]2. Initializes schema[/dim]\n"
                    "[dim]3. Loads your VCF[/dim]\n"
                    "[dim]4. Opens psql shell for queries[/dim]"
                ),
                title="[bold]Zero-Config Database[/bold]",
                border_style="cyan",
            ),
            Syntax(
                "# Just run this - no setup needed!\n"
                "vcf-pg-loader load patient.vcf.gz\n\n"
                "# Query your data\n"
                "vcf-pg-loader db shell",
                "bash",
                theme="monokai",
            ),
        ),
    )


def add_section_6(p: Presenter) -> None:
    p.section(6, "Research Pipeline Walkthrough")

    p.slide(
        "Pipeline: Trio VCF Files",
        pipeline_trio_panel,
    )

    p.slide(
        "Pipeline: vcf-pg-loader",
        pipeline_loader_panel,
    )

    p.slide(
        "Pipeline: Unified Database",
        pipeline_database_panel,
    )

    p.slide(
        "Pipeline: Inheritance Queries",
        pipeline_queries_panel,
    )

    p.slide(
        "Load Trio VCFs",
        lambda: Syntax(
            "# Load proband, mother, and father VCFs\n"
            "vcf-pg-loader load proband.vcf.gz --sample-id PROBAND_001\n"
            "vcf-pg-loader load mother.vcf.gz  --sample-id MOTHER_001\n"
            "vcf-pg-loader load father.vcf.gz  --sample-id FATHER_001\n\n"
            "# Or with your own PostgreSQL:\n"
            "vcf-pg-loader load proband.vcf.gz \\\n"
            "  --db postgresql://user:pass@localhost/variants \\\n"
            "  --sample-id PROBAND_001",
            "bash",
            theme="monokai",
        ),
    )

    p.slide(
        "Step 2: Query for Rare, Damaging Variants",
        sql_query_panel(SQL_RARE_DISEASE_QUERY, "Rare Disease Query"),
    )

    p.slide(
        "Step 3: Find Compound Heterozygotes",
        sql_query_panel(SQL_COMPOUND_HET, "Compound Heterozygote Query"),
    )

    p.slide(
        "Step 4: Add More Samples Mid-Study",
        lambda: Group(
            Panel(
                Text.from_markup(
                    "[bold green]No pipeline restart needed![/bold green]\n\n"
                    "When new samples arrive during your study:\n"
                    "  • Load new VCF (takes seconds)\n"
                    "  • Immediately available for queries\n"
                    "  • Previous analysis remains intact\n"
                    "  • No re-processing of existing data"
                ),
                title="[bold]Adding Samples[/bold]",
                border_style="cyan",
            ),
            Syntax(
                "# New sample arrives mid-study\n"
                "vcf-pg-loader load new_sibling.vcf.gz --sample-id SIBLING_001\n\n"
                "# Immediately query across all samples\n"
                "SELECT sample_id, COUNT(*) as variant_count\n"
                "FROM variants\n"
                "WHERE impact = 'HIGH'\n"
                "GROUP BY sample_id;",
                "sql",
                theme="monokai",
            ),
        ),
    )

    p.slide(
        "Iterative Research",
        iterative_research_panel,
    )


def add_section_7(p: Presenter) -> None:
    p.section(7, "Performance & Compliance")

    p.slide(
        "Performance Benchmarks",
        benchmark_table,
    )

    p.slide(
        "Why PostgreSQL?",
        lambda: Panel(
            Text.from_markup(
                "[bold cyan]Performance:[/bold cyan]\n"
                "  • Binary COPY protocol\n"
                "  • Parallel query execution\n"
                "  • Advanced query optimizer\n"
                "  • Partitioning by chromosome\n\n"
                "[bold cyan]Reliability:[/bold cyan]\n"
                "  • ACID compliance\n"
                "  • Point-in-time recovery\n"
                "  • Replication support\n"
                "  • Battle-tested at scale\n\n"
                "[bold cyan]Ecosystem:[/bold cyan]\n"
                "  • BI tool integration\n"
                "  • Cloud provider support\n"
                "  • Extensive documentation\n"
                "  • Active community"
            ),
            title="[bold]PostgreSQL Advantages[/bold]",
            border_style="blue",
        ),
    )

    p.slide(
        "Clinical Compliance Features",
        lambda: Panel(
            Text.from_markup(
                "[bold green]Audit Trail:[/bold green]\n"
                "  • Every load tracked with timestamp\n"
                "  • Source file MD5 checksums\n"
                "  • Load batch IDs for traceability\n"
                "  • Query logging available\n\n"
                "[bold green]Data Integrity:[/bold green]\n"
                "  • Constraint validation\n"
                "  • Variant normalization\n"
                "  • Duplicate detection\n"
                "  • Post-load validation command\n\n"
                "[bold green]Access Control:[/bold green]\n"
                "  • PostgreSQL role-based access\n"
                "  • Row-level security possible\n"
                "  • SSL/TLS encryption\n"
                "  • HIPAA-compatible infrastructure"
            ),
            title="[bold]Clinical-Grade Compliance[/bold]",
            border_style="green",
        ),
    )

    p.slide(
        "Validation & Reproducibility",
        lambda: Group(
            Panel(
                Text.from_markup(
                    "[bold]Every load is validated and traceable[/bold]\n\n"
                    "  • Automatic variant count verification\n"
                    "  • Duplicate detection\n"
                    "  • Re-loadable from source files\n"
                    "  • Batch IDs link variants to source"
                ),
                title="[bold]Data Validation[/bold]",
                border_style="yellow",
            ),
            Syntax(
                "# Validate a completed load\n"
                "vcf-pg-loader validate <load-batch-id>\n\n"
                "# Check load history\n"
                "SELECT load_batch_id, vcf_file_path, \n"
                "       variant_count, loaded_at\n"
                "FROM load_audit\n"
                "ORDER BY loaded_at DESC;",
                "sql",
                theme="monokai",
            ),
        ),
    )


def add_section_8(p: Presenter) -> None:
    p.section(8, "Future: Vector Embeddings")

    p.slide(
        "The Vision: Similarity-Based Matching",
        lambda: Panel(
            Text.from_markup(
                "[bold cyan]Current State:[/bold cyan]\n"
                "  • SQL queries find exact matches\n"
                "  • Limited to predefined criteria\n"
                "  • Manual review of candidates\n\n"
                "[bold yellow]Future Potential:[/bold yellow]\n"
                "  • Embed variant profiles as vectors\n"
                "  • Find [italic]similar[/italic] cases, not just exact matches\n"
                "  • Match phenotype patterns\n"
                "  • Learn from diagnosed cases"
            ),
            title="[bold]Beyond Exact Matching[/bold]",
            border_style="magenta",
        ),
    )

    p.slide(
        "Vector Embedding Architecture",
        vector_embedding_panel,
    )

    p.slide(
        "Potential Applications",
        lambda: Panel(
            Text.from_markup(
                "[bold green]Diagnosis Assistance:[/bold green]\n"
                "  • Find cases with similar variant profiles\n"
                "  • Surface diagnoses from similar resolved cases\n"
                "  • Prioritize variants seen in similar phenotypes\n\n"
                "[bold green]Research Discovery:[/bold green]\n"
                "  • Cluster patients by variant signature\n"
                "  • Identify novel gene-disease associations\n"
                "  • Cross-cohort similarity analysis\n\n"
                "[bold green]Technical Approach:[/bold green]\n"
                "  • PostgreSQL + pgvector extension\n"
                "  • Embed: gene, consequence, scores, phenotypes\n"
                "  • Nearest-neighbor search at query time"
            ),
            title="[bold]Vector Embedding Applications[/bold]",
            border_style="cyan",
        ),
    )

    p.slide(
        "Thank You",
        lambda: Panel(
            Text.from_markup(
                "[bold]vcf-pg-loader[/bold]\n"
                "[dim]High-performance VCF to PostgreSQL loader[/dim]\n\n"
                "[bold cyan]GitHub:[/bold cyan] github.com/Zacharyr41/vcf-pg-loader\n"
                "[bold cyan]PyPI:[/bold cyan] pip install vcf-pg-loader\n"
                "[bold cyan]Bioconda:[/bold cyan] conda install vcf-pg-loader\n\n"
                "[dim]Questions? Feedback?[/dim]\n"
                "[dim]Open an issue on GitHub![/dim]"
            ),
            title="[bold green]Get Started[/bold green]",
            border_style="green",
            padding=(1, 4),
        ),
    )


def build_part2(p: Presenter) -> None:
    p.part(2, "The Tool: vcf-pg-loader")
    add_section_4(p)
    add_section_5(p)
    add_section_6(p)
    add_section_7(p)
    add_section_8(p)
