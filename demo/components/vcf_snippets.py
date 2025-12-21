from rich.console import Group
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

VCF_HEADER_EXAMPLE = """##fileformat=VCFv4.2
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Read depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype quality">
#CHROM  POS     ID          REF  ALT  QUAL   FILTER  INFO             FORMAT     SAMPLE1"""

VCF_VARIANT_SIMPLE = """chr1    12345   rs123456    A    G    99.5   PASS    AC=1;AF=0.5;DP=30    GT:AD:GQ   0/1:15,15:99"""

VCF_MULTIALLELIC = """chr7    55249071  .  G    A,T   5000   PASS    AC=1,1;AF=0.25,0.25    GT:AD:GQ   0/1:20,10,10:99"""

VCF_INDEL_EXAMPLE = """chr17   41276045  rs80357906  ACTG   A    1500   PASS    AC=1;AF=0.5;DP=50    GT:AD:GQ   0/1:25,25:99"""

SQL_RARE_DISEASE_QUERY = """SELECT v.chrom, v.pos, v.ref, v.alt,
       v.gene_symbol, v.consequence, v.impact,
       v.af_gnomad, v.clinvar_significance
FROM variants v
WHERE v.sample_id = 'PROBAND_001'
  AND v.impact IN ('HIGH', 'MODERATE')
  AND (v.af_gnomad < 0.001 OR v.af_gnomad IS NULL)
  AND v.clinvar_significance NOT IN ('benign', 'likely_benign')
ORDER BY
  CASE v.impact
    WHEN 'HIGH' THEN 1
    WHEN 'MODERATE' THEN 2
  END,
  v.af_gnomad NULLS FIRST;"""

SQL_COMPOUND_HET = """WITH gene_variants AS (
  SELECT gene_symbol, variant_id,
         sample_id, genotype
  FROM variants
  WHERE sample_id = 'PROBAND_001'
    AND genotype = '0/1'
    AND impact IN ('HIGH', 'MODERATE')
)
SELECT g1.gene_symbol,
       COUNT(*) as het_count
FROM gene_variants g1
GROUP BY g1.gene_symbol
HAVING COUNT(*) >= 2;"""


def vcf_header_panel() -> Panel:
    syntax = Syntax(VCF_HEADER_EXAMPLE, "text", theme="monokai", line_numbers=False)
    return Panel(syntax, title="[bold]VCF Header Structure[/bold]", border_style="green")


def vcf_variant_panel() -> Panel:
    syntax = Syntax(VCF_VARIANT_SIMPLE, "text", theme="monokai", line_numbers=False)
    return Panel(syntax, title="[bold]Simple SNP Variant[/bold]", border_style="yellow")


def vcf_multiallelic_panel() -> Panel:
    syntax = Syntax(VCF_MULTIALLELIC, "text", theme="monokai", line_numbers=False)
    return Panel(syntax, title="[bold]Multi-allelic Variant[/bold]", border_style="magenta")


def vcf_indel_panel() -> Panel:
    syntax = Syntax(VCF_INDEL_EXAMPLE, "text", theme="monokai", line_numbers=False)
    return Panel(syntax, title="[bold]Deletion (Indel)[/bold]", border_style="cyan")


def sql_query_panel(query: str, title: str) -> Panel:
    syntax = Syntax(query, "sql", theme="monokai", line_numbers=False)
    return Panel(syntax, title=f"[bold]{title}[/bold]", border_style="blue")


def variant_types_table() -> Table:
    table = Table(title="Variant Types", border_style="cyan")
    table.add_column("Type", style="bold yellow")
    table.add_column("Reference", style="green")
    table.add_column("Alternate", style="red")
    table.add_column("Description")

    table.add_row("SNP", "A", "G", "Single nucleotide change")
    table.add_row("Deletion", "ATG", "A", "Bases removed from reference")
    table.add_row("Insertion", "A", "ATG", "Bases added to reference")
    table.add_row("MNP", "AT", "GC", "Multiple nucleotides changed")
    table.add_row("SV", "N", "<DEL>", "Large structural variant")
    return table


def vcf_columns_table() -> Table:
    table = Table(title="VCF Columns", border_style="green")
    table.add_column("#", style="dim")
    table.add_column("Column", style="bold cyan")
    table.add_column("Description")
    table.add_column("Example", style="yellow")

    table.add_row("1", "CHROM", "Chromosome", "chr1")
    table.add_row("2", "POS", "1-based position", "12345")
    table.add_row("3", "ID", "Variant identifier", "rs123456")
    table.add_row("4", "REF", "Reference allele", "A")
    table.add_row("5", "ALT", "Alternate allele(s)", "G,T")
    table.add_row("6", "QUAL", "Quality score", "99.5")
    table.add_row("7", "FILTER", "Filter status", "PASS")
    table.add_row("8", "INFO", "Variant annotations", "AC=1;AF=0.5")
    table.add_row("9", "FORMAT", "Sample field format", "GT:AD:GQ")
    table.add_row("10+", "SAMPLES", "Per-sample data", "0/1:15,15:99")
    return table


VCF_LINE_HEADER = (
    "#CHROM  POS      ID         REF  ALT  QUAL   FILTER  INFO              FORMAT     SAMPLE"
)
VCF_LINE_DATA = (
    "chr1    12345    rs123456   A    G    99.5   PASS    AC=1;AF=0.5;DP=30 GT:AD:GQ   0/1:15,15:99"
)


def vcf_column_location_panel() -> Panel:
    text = Text()
    text.append(VCF_LINE_HEADER + "\n", style="bold dim")
    text.append(VCF_LINE_DATA, style="white")
    return Panel(
        text,
        title="[bold]VCF Data Line[/bold]",
        subtitle="[dim]Each line = one variant[/dim]",
        border_style="green",
    )


def vcf_column_chrom_pos_panel() -> Group:
    text = Text()
    text.append("#CHROM  POS      ", style="bold yellow")
    text.append(
        "ID         REF  ALT  QUAL   FILTER  INFO              FORMAT     SAMPLE\n", style="dim"
    )
    text.append("chr1    12345    ", style="bold green")
    text.append(
        "rs123456   A    G    99.5   PASS    AC=1;AF=0.5;DP=30 GT:AD:GQ   0/1:15,15:99", style="dim"
    )

    explanation = Text.from_markup(
        "\n\n[bold yellow]CHROM[/bold yellow] — Which chromosome (chr1-22, X, Y, MT)\n"
        "[bold yellow]POS[/bold yellow] — 1-based position on the chromosome\n\n"
        "[dim]Together, CHROM:POS uniquely identifies the genomic location[/dim]"
    )

    return Group(
        Panel(text, title="[bold]Location Columns[/bold]", border_style="yellow"),
        explanation,
    )


def vcf_column_id_panel() -> Group:
    text = Text()
    text.append("#CHROM  POS      ", style="dim")
    text.append("ID         ", style="bold yellow")
    text.append("REF  ALT  QUAL   FILTER  INFO              FORMAT     SAMPLE\n", style="dim")
    text.append("chr1    12345    ", style="dim")
    text.append("rs123456   ", style="bold green")
    text.append("A    G    99.5   PASS    AC=1;AF=0.5;DP=30 GT:AD:GQ   0/1:15,15:99", style="dim")

    explanation = Text.from_markup(
        "\n\n[bold yellow]ID[/bold yellow] — Variant identifier (often from dbSNP)\n\n"
        "[bold green]rs123456[/bold green] — This is a known variant in dbSNP\n"
        "[dim]'.' means no known ID (novel variant)[/dim]"
    )

    return Group(
        Panel(text, title="[bold]ID Column[/bold]", border_style="yellow"),
        explanation,
    )


def vcf_column_ref_alt_panel() -> Group:
    text = Text()
    text.append("#CHROM  POS      ID         ", style="dim")
    text.append("REF  ALT  ", style="bold yellow")
    text.append("QUAL   FILTER  INFO              FORMAT     SAMPLE\n", style="dim")
    text.append("chr1    12345    rs123456   ", style="dim")
    text.append("A    G    ", style="bold")
    text.append("99.5   PASS    AC=1;AF=0.5;DP=30 GT:AD:GQ   0/1:15,15:99", style="dim")

    explanation = Text.from_markup(
        "\n\n[bold yellow]REF[/bold yellow] — Reference allele (what the genome normally has)\n"
        "[bold yellow]ALT[/bold yellow] — Alternate allele (what this sample has instead)\n\n"
        "[green]A[/green] → [red]G[/red] : This is a SNP (single nucleotide change)\n"
        "[dim]ALT can have multiple values: G,T means two alternate alleles[/dim]"
    )

    return Group(
        Panel(text, title="[bold]REF/ALT Columns[/bold]", border_style="yellow"),
        explanation,
    )


def vcf_column_qual_filter_panel() -> Group:
    text = Text()
    text.append("#CHROM  POS      ID         REF  ALT  ", style="dim")
    text.append("QUAL   FILTER  ", style="bold yellow")
    text.append("INFO              FORMAT     SAMPLE\n", style="dim")
    text.append("chr1    12345    rs123456   A    G    ", style="dim")
    text.append("99.5   PASS    ", style="bold green")
    text.append("AC=1;AF=0.5;DP=30 GT:AD:GQ   0/1:15,15:99", style="dim")

    explanation = Text.from_markup(
        "\n\n[bold yellow]QUAL[/bold yellow] — Phred-scaled quality score\n"
        "  [green]99.5[/green] = very high confidence (higher is better)\n\n"
        "[bold yellow]FILTER[/bold yellow] — Did this variant pass quality filters?\n"
        "  [green]PASS[/green] = yes, good quality\n"
        "  [dim]Other values (LowQual, etc.) = failed some filter[/dim]"
    )

    return Group(
        Panel(text, title="[bold]Quality Columns[/bold]", border_style="yellow"),
        explanation,
    )


def vcf_column_info_panel() -> Group:
    text = Text()
    text.append("#CHROM  POS      ID         REF  ALT  QUAL   FILTER  ", style="dim")
    text.append("INFO              ", style="bold yellow")
    text.append("FORMAT     SAMPLE\n", style="dim")
    text.append("chr1    12345    rs123456   A    G    99.5   PASS    ", style="dim")
    text.append("AC=1;AF=0.5;DP=30 ", style="bold green")
    text.append("GT:AD:GQ   0/1:15,15:99", style="dim")

    explanation = Text.from_markup(
        "\n\n[bold yellow]INFO[/bold yellow] — Variant-level annotations (key=value pairs)\n\n"
        "[cyan]AC=1[/cyan]    Allele Count: 1 chromosome carries this variant\n"
        "[cyan]AF=0.5[/cyan]  Allele Frequency: 50% of chromosomes in this sample\n"
        "[cyan]DP=30[/cyan]   Depth: 30 reads covered this position\n\n"
        "[dim]INFO fields are defined in the header and vary by caller[/dim]"
    )

    return Group(
        Panel(text, title="[bold]INFO Column[/bold]", border_style="yellow"),
        explanation,
    )


def vcf_column_format_sample_panel() -> Group:
    text = Text()
    text.append(
        "#CHROM  POS      ID         REF  ALT  QUAL   FILTER  INFO              ", style="dim"
    )
    text.append("FORMAT     SAMPLE\n", style="bold yellow")
    text.append(
        "chr1    12345    rs123456   A    G    99.5   PASS    AC=1;AF=0.5;DP=30 ", style="dim"
    )
    text.append("GT:AD:GQ   0/1:15,15:99", style="bold green")

    explanation = Text.from_markup(
        "\n\n[bold yellow]FORMAT[/bold yellow] — Defines the order of sample fields\n"
        "[bold yellow]SAMPLE[/bold yellow] — Per-sample data (one column per sample)\n\n"
        "[cyan]GT[/cyan] = [green]0/1[/green]      Genotype: heterozygous (0=ref, 1=alt)\n"
        "[cyan]AD[/cyan] = [green]15,15[/green]   Allelic Depth: 15 ref reads, 15 alt reads\n"
        "[cyan]GQ[/cyan] = [green]99[/green]      Genotype Quality: very confident\n\n"
        "[dim]Multi-sample VCFs have multiple sample columns[/dim]"
    )

    return Group(
        Panel(text, title="[bold]FORMAT & SAMPLE Columns[/bold]", border_style="yellow"),
        explanation,
    )


def info_number_table() -> Table:
    table = Table(title="INFO Field Number Specification", border_style="magenta")
    table.add_column("Number", style="bold yellow")
    table.add_column("Meaning", style="white")
    table.add_column("Example (2 ALTs)")

    table.add_row("1", "Exactly one value", "DP=50")
    table.add_row("A", "One per ALT allele", "AC=10,5")
    table.add_row("R", "One per allele (REF+ALTs)", "AD=30,10,5")
    table.add_row("G", "One per genotype", "PL=0,30,60,45,75,90")
    table.add_row(".", "Variable/unknown", "Varies")
    return table


def info_number_panel() -> Group:
    header_example = Text()
    header_example.append("##INFO=<ID=", style="dim")
    header_example.append("DP", style="bold cyan")
    header_example.append(",", style="dim")
    header_example.append("Number=1", style="bold yellow")
    header_example.append(',Type=Integer,Description="Read depth">\n', style="dim")

    header_example.append("##INFO=<ID=", style="dim")
    header_example.append("AC", style="bold cyan")
    header_example.append(",", style="dim")
    header_example.append("Number=A", style="bold yellow")
    header_example.append(',Type=Integer,Description="Allele count">\n', style="dim")

    header_example.append("##INFO=<ID=", style="dim")
    header_example.append("AD", style="bold cyan")
    header_example.append(",", style="dim")
    header_example.append("Number=R", style="bold yellow")
    header_example.append(',Type=Integer,Description="Allelic depths">\n', style="dim")

    header_example.append("##FORMAT=<ID=", style="dim")
    header_example.append("PL", style="bold cyan")
    header_example.append(",", style="dim")
    header_example.append("Number=G", style="bold yellow")
    header_example.append(',Type=Integer,Description="Phred-scaled likelihoods">', style="dim")

    data_example = Text()
    data_example.append("#CHROM  POS    ID  REF  ALT    ...  INFO\n", style="dim")
    data_example.append("chr7    12345  .   A    ", style="dim")
    data_example.append("G,T", style="bold magenta")
    data_example.append("    ...  ", style="dim")
    data_example.append("DP=50", style="bold green")
    data_example.append(";", style="dim")
    data_example.append("AC=10,5", style="bold green")
    data_example.append(";", style="dim")
    data_example.append("AD=30,10,5", style="bold green")

    table = Table(title="Number Specification Guide", border_style="magenta", show_header=True)
    table.add_column("Number", style="bold yellow", justify="center")
    table.add_column("Meaning", style="white")
    table.add_column("Values for G,T site", style="green")

    table.add_row("1", "Exactly one value", "DP=50 (one depth)")
    table.add_row("A", "One per ALT allele", "AC=10,5 (G count, T count)")
    table.add_row("R", "One per allele (REF+ALTs)", "AD=30,10,5 (A, G, T depths)")
    table.add_row("G", "One per genotype", "PL=... (0/0, 0/1, 1/1, 0/2, 1/2, 2/2)")

    return Group(
        Panel(
            header_example, title="[bold]VCF Header — Field Definitions[/bold]", border_style="cyan"
        ),
        Text(""),
        Panel(
            data_example,
            title="[bold]Data Line — Multi-allelic Site (2 ALTs: G and T)[/bold]",
            border_style="green",
        ),
        Text(""),
        table,
    )


def genotype_panel() -> Group:
    vcf_example = Text()
    vcf_example.append("#CHROM  POS    ID  REF  ALT  QUAL  FILTER  INFO  ", style="dim")
    vcf_example.append("FORMAT", style="bold cyan")
    vcf_example.append("     ", style="dim")
    vcf_example.append("SAMPLE\n", style="bold yellow")
    vcf_example.append("chr1    12345  .   A    G    99    PASS    ...   ", style="dim")
    vcf_example.append("GT", style="bold cyan")
    vcf_example.append(":AD:GQ   ", style="dim")
    vcf_example.append("0/1", style="bold green")
    vcf_example.append(":15,15:99", style="dim")

    table = Table(title="Genotype Notation", border_style="yellow", show_header=True)
    table.add_column("Genotype", style="bold cyan", justify="center")
    table.add_column("Meaning")
    table.add_column("Zygosity", style="yellow")

    table.add_row("0/0", "Two copies of reference", "Homozygous ref")
    table.add_row("0/1", "One ref, one alternate", "Heterozygous")
    table.add_row("1/1", "Two copies of alternate", "Homozygous alt")
    table.add_row("1/2", "Two different alternates", "Compound het")
    table.add_row("./.", "Unknown/missing", "No call")

    return Group(
        Panel(
            vcf_example,
            title="[bold]GT (Genotype) in FORMAT/SAMPLE[/bold]",
            border_style="cyan",
        ),
        Text(""),
        table,
    )


def genotype_table() -> Table:
    table = Table(title="Genotype Notation", border_style="yellow")
    table.add_column("Genotype", style="bold cyan")
    table.add_column("Meaning")
    table.add_column("Zygosity", style="yellow")

    table.add_row("0/0", "Two copies of reference", "Homozygous ref")
    table.add_row("0/1", "One ref, one alternate", "Heterozygous")
    table.add_row("1/1", "Two copies of alternate", "Homozygous alt")
    table.add_row("1/2", "Two different alternates", "Compound het")
    table.add_row("./.", "Unknown/missing", "No call")
    return table


def impact_table() -> Table:
    table = Table(title="Variant Impact Levels", border_style="red")
    table.add_column("Impact", style="bold")
    table.add_column("Description")
    table.add_column("Examples", style="dim")

    table.add_row(
        "[bold red]HIGH[/bold red]",
        "Likely disrupts gene function",
        "Stop gain, frameshift, splice donor/acceptor",
    )
    table.add_row(
        "[bold yellow]MODERATE[/bold yellow]",
        "May affect protein function",
        "Missense, in-frame indel",
    )
    table.add_row(
        "[bold green]LOW[/bold green]",
        "Unlikely to affect function",
        "Synonymous, splice region",
    )
    table.add_row(
        "[bold blue]MODIFIER[/bold blue]",
        "Non-coding or unknown",
        "Intergenic, intronic, UTR",
    )
    return table


def rare_disease_filters_table() -> Table:
    table = Table(title="Rare Disease Filtering Criteria", border_style="cyan")
    table.add_column("Filter", style="bold yellow")
    table.add_column("Typical Threshold")
    table.add_column("Rationale")

    table.add_row(
        "Allele Frequency",
        "< 0.1% (gnomAD)",
        "Disease-causing variants are rare in population",
    )
    table.add_row(
        "Impact",
        "HIGH or MODERATE",
        "Focus on protein-altering variants",
    )
    table.add_row(
        "ClinVar",
        "Not benign",
        "Exclude known benign variants",
    )
    table.add_row(
        "Inheritance",
        "Match pedigree",
        "De novo, recessive, dominant patterns",
    )
    table.add_row(
        "CADD Score",
        "> 20",
        "Computationally predicted deleteriousness",
    )
    return table


def tool_comparison_table() -> Table:
    table = Table(title="Tool Comparison", border_style="blue")
    table.add_column("Feature", style="bold")
    table.add_column("GEMINI", style="yellow")
    table.add_column("slivar", style="cyan")
    table.add_column("vcf-pg-loader", style="green")

    table.add_row("Database", "SQLite", "None (streaming)", "[bold]PostgreSQL[/bold]")
    table.add_row("Load Speed", "~5K var/sec", "N/A", "[bold]100K+ var/sec[/bold]")
    table.add_row("Query Language", "SQL", "Custom expressions", "[bold]Full SQL[/bold]")
    table.add_row("Multi-sample", "Limited", "Yes", "[bold]Unlimited[/bold]")
    table.add_row("Concurrent Access", "No", "N/A", "[bold]Yes[/bold]")
    table.add_row("Audit Trail", "No", "No", "[bold]Yes[/bold]")
    table.add_row("Maintained", "No (archived)", "Yes", "[bold]Yes[/bold]")
    return table


def benchmark_table() -> Table:
    table = Table(title="Performance Benchmarks", border_style="green")
    table.add_column("Variants", style="bold")
    table.add_column("Parse Time", style="cyan")
    table.add_column("Load Time", style="yellow")
    table.add_column("Rate", style="green")

    table.add_row("10,000", "0.09s", "0.15s", "~67K/sec")
    table.add_row("100,000", "0.94s", "1.2s", "~83K/sec")
    table.add_row("1,000,000", "9.1s", "11s", "~91K/sec")
    table.add_row("5,000,000", "45s", "52s", "~96K/sec")
    return table
