"""Synthetic VCF generator for unit tests."""

import tempfile
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SyntheticVariant:
    """Represents a synthetic variant for testing."""

    chrom: str
    pos: int
    ref: str
    alt: list[str]
    qual: float | None = 30.0
    filter: str = "PASS"
    info: dict = field(default_factory=dict)
    format_fields: dict = field(default_factory=dict)
    rs_id: str = "."


class VCFGenerator:
    """Generate minimal VCFs for targeted unit tests."""

    HEADER_TEMPLATE = """##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele Count">
##INFO=<ID=AN,Number=1,Type=Integer,Description="Total Alleles">
##INFO=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">
##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature">
##INFO=<ID=ANN,Number=.,Type=String,Description="Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'">
##INFO=<ID=LOF,Number=.,Type=String,Description="Predicted loss of function effects">
##INFO=<ID=NMD,Number=.,Type=String,Description="Predicted nonsense mediated decay effects">
##INFO=<ID=SYMBOL,Number=1,Type=String,Description="Gene symbol">
##INFO=<ID=gnomAD_AF,Number=A,Type=Float,Description="gnomAD allele frequency">
##INFO=<ID=GeneticModels,Number=.,Type=String,Description="Inheritance models from GENMOD">
##INFO=<ID=Compounds,Number=.,Type=String,Description="Compound pairs from GENMOD">
##INFO=<ID=RankScore,Number=.,Type=String,Description="Rank score from GENMOD">
##INFO=<ID=QD,Number=1,Type=Float,Description="Quality by Depth">
##INFO=<ID=FS,Number=1,Type=Float,Description="Fisher Strand bias">
##INFO=<ID=MQ,Number=1,Type=Float,Description="Mapping Quality">
##INFO=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality (site-level)">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">
##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Phred-scaled Likelihoods">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chr3,length=198295559>
##contig=<ID=chr13,length=114364328>
##contig=<ID=chr17,length=83257441>
##contig=<ID=chrX,length=156040895>
##contig=<ID=chrY,length=57227415>
##contig=<ID=chrM,length=16569>
"""

    @classmethod
    def generate(
        cls, variants: list[SyntheticVariant], samples: list[str] | None = None
    ) -> str:
        """Generate a minimal VCF string."""
        samples = samples or ["SAMPLE1"]
        lines = [cls.HEADER_TEMPLATE.strip()]
        lines.append(
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t"
            + "\t".join(samples)
        )

        for v in variants:
            info_str = cls._format_info(v.info) if v.info else "."
            alt_str = ",".join(v.alt)
            qual_str = str(v.qual) if v.qual is not None else "."

            format_keys = ["GT"]
            if v.format_fields:
                first_sample = list(v.format_fields.values())[0]
                format_keys = list(first_sample.keys())

            sample_cols = []
            for sample in samples:
                if v.format_fields and sample in v.format_fields:
                    vals = [str(v.format_fields[sample].get(k, ".")) for k in format_keys]
                    sample_cols.append(":".join(vals))
                else:
                    sample_cols.append("./.")

            line = (
                f"{v.chrom}\t{v.pos}\t{v.rs_id}\t{v.ref}\t{alt_str}\t{qual_str}\t"
                f"{v.filter}\t{info_str}\t{':'.join(format_keys)}\t"
                + "\t".join(sample_cols)
            )
            lines.append(line)

        return "\n".join(lines) + "\n"

    @classmethod
    def generate_file(
        cls, variants: list[SyntheticVariant], samples: list[str] | None = None
    ) -> Path:
        """Generate a VCF file and return the path."""
        content = cls.generate(variants, samples)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".vcf", delete=False
        ) as f:
            f.write(content)
            return Path(f.name)

    @staticmethod
    def _format_info(info: dict) -> str:
        parts = []
        for k, v in info.items():
            if v is True:
                parts.append(k)
            elif isinstance(v, list):
                parts.append(f"{k}={','.join(map(str, v))}")
            else:
                parts.append(f"{k}={v}")
        return ";".join(parts) if parts else "."


def make_multiallelic_vcf(n_alts: int = 3) -> str:
    """VCF with multi-allelic site for Number=A/R/G testing."""
    alts = ["G", "T", "C"][:n_alts]
    return VCFGenerator.generate([
        SyntheticVariant(
            chrom="chr1",
            pos=100,
            ref="A",
            alt=alts,
            info={
                "AF": [0.1] * n_alts,
                "AC": [10] * n_alts,
                "AD": [100] + [10] * n_alts,
                "AN": 200,
            },
        )
    ])


def make_multiallelic_vcf_file(n_alts: int = 3) -> Path:
    """VCF file with multi-allelic site for Number=A/R/G testing."""
    alts = ["G", "T", "C"][:n_alts]
    return VCFGenerator.generate_file([
        SyntheticVariant(
            chrom="chr1",
            pos=100,
            ref="A",
            alt=alts,
            info={
                "AF": [0.1] * n_alts,
                "AC": [10] * n_alts,
                "AD": [100] + [10] * n_alts,
                "AN": 200,
            },
        )
    ])


def make_unnormalized_vcf() -> str:
    """VCF with variants requiring normalization."""
    return VCFGenerator.generate([
        SyntheticVariant(chrom="chr1", pos=100, ref="ATG", alt=["AG"]),
        SyntheticVariant(chrom="chr1", pos=200, ref="GATC", alt=["GTTC"]),
        SyntheticVariant(chrom="chr1", pos=300, ref="A", alt=["G"]),
    ])


def make_unnormalized_vcf_file() -> Path:
    """VCF file with variants requiring normalization."""
    return VCFGenerator.generate_file([
        SyntheticVariant(chrom="chr1", pos=100, ref="ATG", alt=["AG"]),
        SyntheticVariant(chrom="chr1", pos=200, ref="GATC", alt=["GTTC"]),
        SyntheticVariant(chrom="chr1", pos=300, ref="A", alt=["G"]),
    ])


def make_vep_csq_vcf() -> str:
    """VCF with VEP CSQ annotations matching nf-core/sarek output."""
    csq = (
        "T|missense_variant|MODERATE|BRCA1|ENSG00000012048|Transcript|ENST00000357654"
    )
    return VCFGenerator.generate([
        SyntheticVariant(
            chrom="chr17",
            pos=43094464,
            ref="C",
            alt=["T"],
            info={"CSQ": csq},
        )
    ])


def make_vep_csq_vcf_file() -> Path:
    """VCF file with VEP CSQ annotations matching nf-core/sarek output."""
    csq = (
        "T|missense_variant|MODERATE|BRCA1|ENSG00000012048|Transcript|ENST00000357654"
    )
    return VCFGenerator.generate_file([
        SyntheticVariant(
            chrom="chr17",
            pos=43094464,
            ref="C",
            alt=["T"],
            info={"CSQ": csq},
        )
    ])


def make_trio_vcf() -> str:
    """Minimal trio VCF with inheritance patterns for testing."""
    samples = ["proband", "father", "mother"]
    return VCFGenerator.generate(
        [
            SyntheticVariant(
                chrom="chr1",
                pos=1000,
                ref="A",
                alt=["G"],
                format_fields={
                    "proband": {"GT": "0/1", "DP": 30, "GQ": 99},
                    "father": {"GT": "0/0", "DP": 25, "GQ": 99},
                    "mother": {"GT": "0/0", "DP": 28, "GQ": 99},
                },
            ),
            SyntheticVariant(
                chrom="chr2",
                pos=2000,
                ref="C",
                alt=["T"],
                format_fields={
                    "proband": {"GT": "1/1", "DP": 35, "GQ": 99},
                    "father": {"GT": "0/1", "DP": 30, "GQ": 99},
                    "mother": {"GT": "0/1", "DP": 32, "GQ": 99},
                },
            ),
            SyntheticVariant(
                chrom="chr3",
                pos=3000,
                ref="G",
                alt=["A"],
                info={"SYMBOL": "GENE1"},
                format_fields={
                    "proband": {"GT": "0/1", "DP": 28, "GQ": 99},
                    "father": {"GT": "0/1", "DP": 26, "GQ": 99},
                    "mother": {"GT": "0/0", "DP": 30, "GQ": 99},
                },
            ),
            SyntheticVariant(
                chrom="chr3",
                pos=3500,
                ref="T",
                alt=["C"],
                info={"SYMBOL": "GENE1"},
                format_fields={
                    "proband": {"GT": "0/1", "DP": 32, "GQ": 99},
                    "father": {"GT": "0/0", "DP": 29, "GQ": 99},
                    "mother": {"GT": "0/1", "DP": 31, "GQ": 99},
                },
            ),
        ],
        samples=samples,
    )


def make_trio_vcf_file() -> Path:
    """Minimal trio VCF file with inheritance patterns for testing."""
    samples = ["proband", "father", "mother"]
    return VCFGenerator.generate_file(
        [
            SyntheticVariant(
                chrom="chr1",
                pos=1000,
                ref="A",
                alt=["G"],
                format_fields={
                    "proband": {"GT": "0/1", "DP": 30, "GQ": 99},
                    "father": {"GT": "0/0", "DP": 25, "GQ": 99},
                    "mother": {"GT": "0/0", "DP": 28, "GQ": 99},
                },
            ),
            SyntheticVariant(
                chrom="chr2",
                pos=2000,
                ref="C",
                alt=["T"],
                format_fields={
                    "proband": {"GT": "1/1", "DP": 35, "GQ": 99},
                    "father": {"GT": "0/1", "DP": 30, "GQ": 99},
                    "mother": {"GT": "0/1", "DP": 32, "GQ": 99},
                },
            ),
            SyntheticVariant(
                chrom="chr3",
                pos=3000,
                ref="G",
                alt=["A"],
                info={"SYMBOL": "GENE1"},
                format_fields={
                    "proband": {"GT": "0/1", "DP": 28, "GQ": 99},
                    "father": {"GT": "0/1", "DP": 26, "GQ": 99},
                    "mother": {"GT": "0/0", "DP": 30, "GQ": 99},
                },
            ),
            SyntheticVariant(
                chrom="chr3",
                pos=3500,
                ref="T",
                alt=["C"],
                info={"SYMBOL": "GENE1"},
                format_fields={
                    "proband": {"GT": "0/1", "DP": 32, "GQ": 99},
                    "father": {"GT": "0/0", "DP": 29, "GQ": 99},
                    "mother": {"GT": "0/1", "DP": 31, "GQ": 99},
                },
            ),
        ],
        samples=samples,
    )


def make_genmod_vcf() -> str:
    """VCF with GENMOD annotations from nf-core/raredisease."""
    return VCFGenerator.generate([
        SyntheticVariant(
            chrom="chr1",
            pos=1000,
            ref="A",
            alt=["G"],
            info={
                "GeneticModels": "FAM001:AR_hom",
                "RankScore": "FAM001:15",
            },
        ),
        SyntheticVariant(
            chrom="chr2",
            pos=2000,
            ref="C",
            alt=["T"],
            info={
                "GeneticModels": "FAM001:AR_comp",
                "Compounds": "GENE1:chr2_2000_C_T>chr2_2500_G_A",
                "RankScore": "FAM001:12",
            },
        ),
    ])


def make_genmod_vcf_file() -> Path:
    """VCF file with GENMOD annotations from nf-core/raredisease."""
    return VCFGenerator.generate_file([
        SyntheticVariant(
            chrom="chr1",
            pos=1000,
            ref="A",
            alt=["G"],
            info={
                "GeneticModels": "FAM001:AR_hom",
                "RankScore": "FAM001:15",
            },
        ),
        SyntheticVariant(
            chrom="chr2",
            pos=2000,
            ref="C",
            alt=["T"],
            info={
                "GeneticModels": "FAM001:AR_comp",
                "Compounds": "GENE1:chr2_2000_C_T>chr2_2500_G_A",
                "RankScore": "FAM001:12",
            },
        ),
    ])


def make_snpeff_ann(
    allele: str,
    annotation: str,
    impact: str,
    gene_name: str,
    gene_id: str,
    transcript: str,
    biotype: str = "protein_coding",
    hgvs_c: str = "",
    hgvs_p: str = "",
    rank: str = "",
    cdna_pos: str = "",
    cds_pos: str = "",
    aa_pos: str = "",
    distance: str = "",
    warning: str = "",
) -> str:
    """Build a SnpEff ANN field string."""
    return "|".join([
        allele,
        annotation,
        impact,
        gene_name,
        gene_id,
        "transcript",
        transcript,
        biotype,
        rank,
        hgvs_c,
        hgvs_p,
        cdna_pos,
        cds_pos,
        aa_pos,
        distance,
        warning,
    ])


def make_snpeff_ann_vcf_file() -> Path:
    """VCF file with SnpEff ANN annotations for database testing."""
    variants = [
        SyntheticVariant(
            chrom="chr17",
            pos=7578406,
            ref="C",
            alt=["G"],
            info={
                "ANN": make_snpeff_ann(
                    allele="G",
                    annotation="missense_variant",
                    impact="MODERATE",
                    gene_name="TP53",
                    gene_id="ENSG00000141510",
                    transcript="ENST00000269305",
                    hgvs_c="c.817C>G",
                    hgvs_p="p.Pro273Arg",
                    rank="10/11",
                ),
                "gnomAD_AF": 0.00001,
            },
        ),
        SyntheticVariant(
            chrom="chr17",
            pos=7578500,
            ref="A",
            alt=["T"],
            info={
                "ANN": make_snpeff_ann(
                    allele="T",
                    annotation="stop_gained",
                    impact="HIGH",
                    gene_name="TP53",
                    gene_id="ENSG00000141510",
                    transcript="ENST00000269305",
                    hgvs_c="c.723T>A",
                    hgvs_p="p.Tyr241*",
                    rank="9/11",
                ),
                "LOF": "(TP53|ENSG00000141510|1|1.00)",
                "gnomAD_AF": 0.000001,
            },
        ),
        SyntheticVariant(
            chrom="chr17",
            pos=7578550,
            ref="G",
            alt=["A"],
            info={
                "ANN": ",".join([
                    make_snpeff_ann(
                        allele="A",
                        annotation="splice_acceptor_variant",
                        impact="HIGH",
                        gene_name="TP53",
                        gene_id="ENSG00000141510",
                        transcript="ENST00000269305",
                        hgvs_c="c.673-2G>A",
                    ),
                    make_snpeff_ann(
                        allele="A",
                        annotation="downstream_gene_variant",
                        impact="MODIFIER",
                        gene_name="WRAP53",
                        gene_id="ENSG00000141499",
                        transcript="ENST00000357449",
                        distance="4500",
                    ),
                ]),
                "gnomAD_AF": 0.0001,
            },
        ),
        SyntheticVariant(
            chrom="chr13",
            pos=32936732,
            ref="C",
            alt=["A"],
            info={
                "ANN": make_snpeff_ann(
                    allele="A",
                    annotation="frameshift_variant",
                    impact="HIGH",
                    gene_name="BRCA2",
                    gene_id="ENSG00000139618",
                    transcript="ENST00000380152",
                    hgvs_c="c.5946delT",
                    hgvs_p="p.Ser1982ArgfsTer22",
                    rank="11/27",
                ),
                "LOF": "(BRCA2|ENSG00000139618|1|1.00)",
                "gnomAD_AF": 0.00005,
            },
        ),
        SyntheticVariant(
            chrom="chr17",
            pos=7579000,
            ref="T",
            alt=["C"],
            info={
                "ANN": make_snpeff_ann(
                    allele="C",
                    annotation="synonymous_variant",
                    impact="LOW",
                    gene_name="TP53",
                    gene_id="ENSG00000141510",
                    transcript="ENST00000269305",
                    hgvs_c="c.600A>G",
                    hgvs_p="p.Leu200Leu",
                ),
                "gnomAD_AF": 0.05,
            },
        ),
        SyntheticVariant(
            chrom="chr17",
            pos=7580000,
            ref="A",
            alt=["G"],
            info={
                "ANN": make_snpeff_ann(
                    allele="G",
                    annotation="intron_variant",
                    impact="MODIFIER",
                    gene_name="TP53",
                    gene_id="ENSG00000141510",
                    transcript="ENST00000269305",
                    hgvs_c="c.200+50A>G",
                ),
                "gnomAD_AF": 0.15,
            },
        ),
    ]
    return VCFGenerator.generate_file(variants)
