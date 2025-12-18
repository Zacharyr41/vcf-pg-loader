from rich.console import Group
from rich.panel import Panel
from rich.text import Text

from ..components.diagrams import (
    reference_comparison_panel,
    sequencing_flow_panel,
)
from ..components.vcf_snippets import (
    genotype_table,
    impact_table,
    info_number_table,
    rare_disease_filters_table,
    variant_types_table,
    vcf_column_chrom_pos_panel,
    vcf_column_format_sample_panel,
    vcf_column_id_panel,
    vcf_column_info_panel,
    vcf_column_location_panel,
    vcf_column_qual_filter_panel,
    vcf_column_ref_alt_panel,
    vcf_header_panel,
    vcf_indel_panel,
    vcf_multiallelic_panel,
    vcf_variant_panel,
)
from ..presenter import Presenter


def add_section_1(p: Presenter) -> None:
    p.section(1, "VCF Files: From DNA to Data")

    p.slide(
        "What is a VCF file?",
        lambda: Panel(
            Text.from_markup(
                "[bold]VCF (Variant Call Format)[/bold] is a standard file format "
                "for storing genetic variations.\n\n"
                "Think of it as a [cyan]'track changes'[/cyan] document for DNA.\n\n"
                "Instead of storing your complete genome (3 billion letters), "
                "a VCF file only records the places where your DNA [yellow]differs[/yellow] "
                "from the reference human genome."
            ),
            border_style="cyan",
        ),
    )

    p.slide(
        "The Journey: DNA Sample to VCF",
        sequencing_flow_panel,
    )

    p.slide(
        "Why VCF?",
        lambda: Panel(
            Text.from_markup(
                "[bold yellow]200x Smaller[/bold yellow]\n"
                "  Full genome: [dim]~100 GB[/dim]  →  VCF file: [bold green]~100-500 MB[/bold green]\n\n"
                "[bold cyan]Storage[/bold cyan]\n"
                "  Thousands of VCFs on one hard drive.\n"
                "  Raw genomes? You need a data center.\n\n"
                "[bold cyan]Speed[/bold cyan]\n"
                "  Load a VCF in [green]seconds[/green]. Full genome? [red]Minutes to hours[/red].\n\n"
                "[bold cyan]Accessibility[/bold cyan]\n"
                "  Works on a laptop. No cluster. No cloud bill.\n\n"
                "[bold cyan]Transfer[/bold cyan]\n"
                "  Send to a collaborator in minutes, not hours.\n\n"
                "[bold yellow]Standardized by GA4GH[/bold yellow]\n"
                "  Global Alliance for Genomics and Health (600+ orgs)\n"
                "  [dim]Broad Institute ↔ UK Biobank ↔ Tokyo — same format[/dim]"
            ),
            title="[bold]Why Use VCF?[/bold]",
            border_style="green",
        ),
    )

    p.slide(
        "Reference vs Sample",
        reference_comparison_panel,
    )


def add_section_2(p: Presenter) -> None:
    p.section(2, "Anatomy of a VCF File")

    p.slide(
        "VCF Header",
        vcf_header_panel,
    )

    p.slide(
        "VCF Data Line",
        vcf_column_location_panel,
    )

    p.slide(
        "CHROM & POS: Location",
        vcf_column_chrom_pos_panel,
    )

    p.slide(
        "ID: Variant Identifier",
        vcf_column_id_panel,
    )

    p.slide(
        "REF & ALT: The Actual Change",
        vcf_column_ref_alt_panel,
    )

    p.slide(
        "QUAL & FILTER: Quality Control",
        vcf_column_qual_filter_panel,
    )

    p.slide(
        "INFO: Variant Annotations",
        vcf_column_info_panel,
    )

    p.slide(
        "FORMAT & SAMPLE: Per-Sample Data",
        vcf_column_format_sample_panel,
    )

    p.slide(
        "A Simple SNP",
        lambda: Group(
            vcf_variant_panel(),
            Text.from_markup(
                "\n[dim]This variant shows:[/dim]\n"
                "  • Position 12345 on chromosome 1\n"
                "  • Reference allele: [green]A[/green]\n"
                "  • Alternate allele: [red]G[/red]\n"
                "  • Quality: 99.5 (high confidence)\n"
                "  • Genotype 0/1 = heterozygous (one copy of each)"
            ),
        ),
    )

    p.slide(
        "Variant Types",
        variant_types_table,
    )

    p.slide(
        "Deletion Example",
        lambda: Group(
            vcf_indel_panel(),
            Text.from_markup(
                "\n[dim]This deletion shows:[/dim]\n"
                "  • Reference: [green]ACTG[/green] (4 bases)\n"
                "  • Alternate: [red]A[/red] (1 base)\n"
                "  • Result: [yellow]CTG deleted[/yellow]\n"
                "  • Position is the anchor base before deletion"
            ),
        ),
    )

    p.slide(
        "Multi-allelic Variants",
        lambda: Group(
            vcf_multiallelic_panel(),
            Text.from_markup(
                "\n[dim]At this position, we found [bold]two[/bold] different variants:[/dim]\n"
                "  • [green]G[/green] → [red]A[/red] (first alternate)\n"
                "  • [green]G[/green] → [red]T[/red] (second alternate)\n"
                "  • AC=1,1 means one of each in the sample\n"
                "  • These must be [cyan]decomposed[/cyan] for analysis"
            ),
        ),
    )

    p.slide(
        "INFO Field Numbers",
        info_number_table,
    )

    p.slide(
        "Genotype Notation",
        genotype_table,
    )


def add_section_3(p: Presenter) -> None:
    p.section(3, "VCF in Rare Disease Research")

    p.slide(
        "The Rare Disease Challenge",
        lambda: Panel(
            Text.from_markup(
                "[bold cyan]The Problem:[/bold cyan]\n"
                "  • Patient has ~4-6 million variants\n"
                "  • Only [bold yellow]1-3[/bold yellow] cause the disease\n"
                "  • Must find the needle in the haystack\n\n"
                "[bold cyan]The Goal:[/bold cyan]\n"
                "  • Filter millions → hundreds → [bold green]candidates[/bold green]\n"
                "  • Use population data to exclude common variants\n"
                "  • Use functional predictions to prioritize\n"
                "  • Match inheritance patterns"
            ),
            title="[bold]Rare Disease Diagnostics[/bold]",
            border_style="yellow",
        ),
    )

    p.slide(
        "Variant Impact Levels",
        impact_table,
    )

    p.slide(
        "Key Filtering Criteria",
        rare_disease_filters_table,
    )

    p.slide(
        "Inheritance Patterns",
        lambda: Panel(
            Text.from_markup(
                "[bold yellow]De Novo:[/bold yellow]\n"
                "  • Variant in child, absent in both parents\n"
                "  • Common in severe developmental disorders\n\n"
                "[bold yellow]Autosomal Recessive:[/bold yellow]\n"
                "  • Need two copies of damaging variant\n"
                "  • Homozygous or compound heterozygous\n\n"
                "[bold yellow]Autosomal Dominant:[/bold yellow]\n"
                "  • Single copy sufficient\n"
                "  • Often inherited from affected parent\n\n"
                "[bold yellow]X-Linked:[/bold yellow]\n"
                "  • Males more severely affected\n"
                "  • Females may be carriers"
            ),
            title="[bold]Mendelian Inheritance[/bold]",
            border_style="magenta",
        ),
    )

    p.slide(
        "A Typical Filtering Cascade",
        lambda: Panel(
            Text.from_markup(
                "[bold]Starting:[/bold] 5,000,000 variants\n"
                "    │\n"
                "    ▼ [dim]Remove common variants (AF > 1%)[/dim]\n"
                "[bold]Rare:[/bold] 50,000 variants\n"
                "    │\n"
                "    ▼ [dim]Keep HIGH/MODERATE impact[/dim]\n"
                "[bold]Functional:[/bold] 2,000 variants\n"
                "    │\n"
                "    ▼ [dim]Match inheritance pattern[/dim]\n"
                "[bold]Inherited:[/bold] 100 variants\n"
                "    │\n"
                "    ▼ [dim]Check ClinVar, gene lists[/dim]\n"
                "[bold green]Candidates:[/bold green] 5-20 variants\n"
                "    │\n"
                "    ▼ [dim]Manual review[/dim]\n"
                "[bold green]Diagnosis[/bold green]"
            ),
            title="[bold]Variant Filtering Cascade[/bold]",
            border_style="green",
        ),
    )

    p.slide(
        "Why SQL for Genomics?",
        lambda: Panel(
            Text.from_markup(
                "[bold cyan]Flexibility:[/bold cyan]\n"
                "  • Ad-hoc queries without re-running pipelines\n"
                "  • Complex joins across samples\n"
                "  • Aggregate statistics on the fly\n\n"
                "[bold cyan]Familiarity:[/bold cyan]\n"
                "  • SQL is widely known\n"
                "  • No custom query language to learn\n"
                "  • Integrates with BI tools\n\n"
                "[bold cyan]Performance:[/bold cyan]\n"
                "  • Indexes for fast lookups\n"
                "  • Query optimization\n"
                "  • Parallel execution"
            ),
            title="[bold]SQL-Based Variant Analysis[/bold]",
            border_style="blue",
        ),
    )


def build_part1(p: Presenter) -> None:
    p.part(1, "Background: Understanding VCF")
    add_section_1(p)
    add_section_2(p)
    add_section_3(p)
