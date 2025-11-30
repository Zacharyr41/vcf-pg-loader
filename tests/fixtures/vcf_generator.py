"""Synthetic VCF generator for unit tests."""

from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
import tempfile


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
##INFO=<ID=SYMBOL,Number=1,Type=String,Description="Gene symbol">
##INFO=<ID=GeneticModels,Number=.,Type=String,Description="Inheritance models from GENMOD">
##INFO=<ID=Compounds,Number=.,Type=String,Description="Compound pairs from GENMOD">
##INFO=<ID=RankScore,Number=.,Type=String,Description="Rank score from GENMOD">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">
##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Phred-scaled Likelihoods">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chr3,length=198295559>
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
