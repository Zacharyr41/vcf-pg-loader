"""Population genetics QC metrics for data integrity validation.

These metrics detect systematic biases that trio analysis might miss.
Errors affecting all samples uniformly won't create Mendelian violations
but will distort population statistics.

Sources:
- compass_artifact guidance doc lines 114-193
- Wang J et al. "Genome measures used for quality control are dependent on gene function and ancestry."
  Bioinformatics 31(3):318-323 (2015). DOI: 10.1093/bioinformatics/btu668
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.nf_core_datasets import GIABDataManager
from fixtures.vcf_generator import SyntheticVariant, VCFGenerator


def calculate_ti_tv_ratio(records: list) -> float | None:
    """Calculate transition/transversion ratio.

    Transitions: A<->G, C<->T (purines to purines, pyrimidines to pyrimidines)
    Transversions: All other SNP changes

    Expected values:
    - WGS: 2.0-2.2
    - WES: 2.8-3.3
    - Random errors: ~0.5
    """
    transitions = 0
    transversions = 0

    transition_pairs = {("A", "G"), ("G", "A"), ("C", "T"), ("T", "C")}

    for record in records:
        if len(record.ref) == 1 and len(record.alt) == 1:
            pair = (record.ref.upper(), record.alt.upper())
            if pair in transition_pairs:
                transitions += 1
            elif pair[0] in "ACGT" and pair[1] in "ACGT" and pair[0] != pair[1]:
                transversions += 1

    if transversions == 0:
        return None

    return transitions / transversions


def calculate_het_hom_ratio(samples_genotypes: list[str]) -> float | None:
    """Calculate heterozygous/homozygous alt ratio.

    Expected: ~2.0 for genome-wide SNPs under HWE
    Elevated (>2.5): May indicate sample contamination
    """
    het_count = 0
    hom_alt_count = 0

    for gt in samples_genotypes:
        if gt is None or gt in ("./.", ".|.", "."):
            continue

        alleles = gt.replace("|", "/").split("/")
        if len(alleles) == 2:
            a1, a2 = alleles
            if a1 == "." or a2 == ".":
                continue
            if a1 != a2 and a1 != "0" and a2 != "0":
                het_count += 1
            elif a1 == a2 and a1 != "0":
                hom_alt_count += 1
            elif a1 != a2:
                het_count += 1

    if hom_alt_count == 0:
        return None

    return het_count / hom_alt_count


@pytest.mark.validation
class TestTiTvRatio:
    """Test transition/transversion ratio calculations."""

    def test_synthetic_wgs_ti_tv_in_range(self):
        """Synthetic WGS-like data has Ti/Tv in expected range."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = []
        for i in range(800):
            pos = 10000 + i * 100
            if i % 3 == 0:
                variants.append(SyntheticVariant(chrom="chr1", pos=pos, ref="A", alt=["G"]))
            elif i % 3 == 1:
                variants.append(SyntheticVariant(chrom="chr1", pos=pos, ref="C", alt=["T"]))
            else:
                variants.append(SyntheticVariant(chrom="chr1", pos=pos, ref="A", alt=["C"]))

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            ti_tv = calculate_ti_tv_ratio(records)
            assert ti_tv is not None
            assert ti_tv > 1.5, f"Ti/Tv {ti_tv:.2f} too low for WGS-like data"
        finally:
            vcf_file.unlink()

    def test_random_mutations_low_ti_tv(self):
        """Random mutations should have Ti/Tv near 0.5."""
        import random

        from vcf_pg_loader.vcf_parser import VCFStreamingParser
        random.seed(42)

        bases = ["A", "C", "G", "T"]
        variants = []
        for i in range(1000):
            pos = 10000 + i * 100
            ref = random.choice(bases)
            alt = random.choice([b for b in bases if b != ref])
            variants.append(SyntheticVariant(chrom="chr1", pos=pos, ref=ref, alt=[alt]))

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            ti_tv = calculate_ti_tv_ratio(records)
            assert ti_tv is not None
            assert ti_tv < 1.0, f"Random Ti/Tv {ti_tv:.2f} should be near 0.5"
        finally:
            vcf_file.unlink()

    def test_indels_excluded_from_ti_tv(self):
        """Indels should not affect Ti/Tv calculation."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"]),
            SyntheticVariant(chrom="chr1", pos=200, ref="C", alt=["T"]),
            SyntheticVariant(chrom="chr1", pos=300, ref="ATG", alt=["A"]),
            SyntheticVariant(chrom="chr1", pos=400, ref="A", alt=["ATGC"]),
            SyntheticVariant(chrom="chr1", pos=500, ref="A", alt=["C"]),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            ti_tv = calculate_ti_tv_ratio(records)
            assert ti_tv is not None
            assert ti_tv == 2.0, f"Expected Ti/Tv of 2.0 (2 Ti, 1 Tv), got {ti_tv}"
        finally:
            vcf_file.unlink()


@pytest.mark.validation
class TestHetHomRatio:
    """Test heterozygous/homozygous ratio calculations."""

    def test_het_hom_ratio_calculation(self):
        """Het/Hom ratio is calculated correctly."""
        genotypes = ["0/1", "0/1", "1/1", "0/0", "./.", "0/1"]

        ratio = calculate_het_hom_ratio(genotypes)
        assert ratio is not None
        assert ratio == 3.0, f"Expected 3 het / 1 hom_alt = 3.0, got {ratio}"

    def test_het_hom_handles_phased(self):
        """Het/Hom calculation handles phased genotypes."""
        genotypes = ["0|1", "1|0", "1|1", "0|0"]

        ratio = calculate_het_hom_ratio(genotypes)
        assert ratio is not None
        assert ratio == 2.0, f"Expected 2 het / 1 hom_alt = 2.0, got {ratio}"

    def test_het_hom_handles_missing(self):
        """Het/Hom calculation handles missing genotypes."""
        genotypes = ["./.", "0/1", ".|.", "1/1", "."]

        ratio = calculate_het_hom_ratio(genotypes)
        assert ratio is not None
        assert ratio == 1.0, f"Expected 1 het / 1 hom_alt = 1.0, got {ratio}"


@pytest.mark.validation
@pytest.mark.giab
class TestRealDataTiTv:
    """Test Ti/Tv on real GIAB data."""

    @pytest.fixture
    def data_manager(self):
        return GIABDataManager()

    def test_giab_chr21_ti_tv_in_range(self, data_manager):
        """GIAB chr21 Ti/Tv should be in WGS expected range (2.0-2.2)."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_path = data_manager.get_giab_chr21("HG002")
        if vcf_path is None or not vcf_path.exists():
            pytest.skip("GIAB HG002 chr21 data not available")

        parser = VCFStreamingParser(vcf_path, human_genome=True)
        records = []
        for batch in parser.iter_batches():
            records.extend(batch)
        parser.close()

        ti_tv = calculate_ti_tv_ratio(records)
        assert ti_tv is not None

        assert 1.8 <= ti_tv <= 2.5, (
            f"GIAB chr21 Ti/Tv {ti_tv:.3f} outside expected WGS range (1.8-2.5)"
        )

        print(f"\nGIAB HG002 chr21 Ti/Tv ratio: {ti_tv:.3f}")


@pytest.mark.validation
class TestGnomADHardFilters:
    """Test gnomAD hard filter thresholds as validation criteria.

    Source: compass_artifact guidance doc lines 127-138
    """

    def test_qd_threshold(self):
        """QD (Quality by Depth) >= 2 is acceptable."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"], info={"QD": 2.5}),
            SyntheticVariant(chrom="chr1", pos=200, ref="C", alt=["T"], info={"QD": 1.5}),
            SyntheticVariant(chrom="chr1", pos=300, ref="G", alt=["A"], info={"QD": 10.0}),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            failed_qd = [r for r in records if r.info.get("QD", 99) < 2]
            assert len(failed_qd) == 1
            assert failed_qd[0].pos == 200
        finally:
            vcf_file.unlink()

    def test_fs_threshold(self):
        """FS (Fisher Strand) <= 60 is acceptable."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"], info={"FS": 5.0}),
            SyntheticVariant(chrom="chr1", pos=200, ref="C", alt=["T"], info={"FS": 65.0}),
            SyntheticVariant(chrom="chr1", pos=300, ref="G", alt=["A"], info={"FS": 30.0}),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            failed_fs = [r for r in records if r.info.get("FS", 0) > 60]
            assert len(failed_fs) == 1
            assert failed_fs[0].pos == 200
        finally:
            vcf_file.unlink()

    def test_mq_threshold(self):
        """MQ (Mapping Quality) >= 30 is acceptable."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"], info={"MQ": 60.0}),
            SyntheticVariant(chrom="chr1", pos=200, ref="C", alt=["T"], info={"MQ": 25.0}),
            SyntheticVariant(chrom="chr1", pos=300, ref="G", alt=["A"], info={"MQ": 40.0}),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            failed_mq = [r for r in records if r.info.get("MQ", 99) < 30]
            assert len(failed_mq) == 1
            assert failed_mq[0].pos == 200
        finally:
            vcf_file.unlink()


@pytest.mark.validation
class TestADJGenotypeQuality:
    """Test gnomAD ADJ genotype quality criteria.

    ADJ criteria for high-quality genotypes:
    - GQ >= 20
    - DP >= 10
    - AB >= 0.2 for heterozygotes

    Source: compass_artifact guidance doc lines 136-139
    """

    def test_gq_threshold(self):
        """GQ >= 20 is high quality."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"], info={"GQ": 99}),
            SyntheticVariant(chrom="chr1", pos=200, ref="C", alt=["T"], info={"GQ": 15}),
            SyntheticVariant(chrom="chr1", pos=300, ref="G", alt=["A"], info={"GQ": 25}),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            low_gq = [r for r in records if r.info.get("GQ", 99) < 20]
            assert len(low_gq) == 1
            assert low_gq[0].pos == 200
        finally:
            vcf_file.unlink()

    def test_dp_threshold(self):
        """DP >= 10 is high quality."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"], info={"DP": 30}),
            SyntheticVariant(chrom="chr1", pos=200, ref="C", alt=["T"], info={"DP": 5}),
            SyntheticVariant(chrom="chr1", pos=300, ref="G", alt=["A"], info={"DP": 15}),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            low_dp = [r for r in records if r.info.get("DP", 99) < 10]
            assert len(low_dp) == 1
            assert low_dp[0].pos == 200
        finally:
            vcf_file.unlink()


@pytest.mark.validation
class TestVariantTypeCounts:
    """Test variant type distribution validation."""

    def test_snp_indel_ratio(self):
        """Calculate SNP/indel ratio."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"]),
            SyntheticVariant(chrom="chr1", pos=200, ref="C", alt=["T"]),
            SyntheticVariant(chrom="chr1", pos=300, ref="G", alt=["A"]),
            SyntheticVariant(chrom="chr1", pos=400, ref="ATG", alt=["A"]),
            SyntheticVariant(chrom="chr1", pos=500, ref="A", alt=["ATGC"]),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            snps = sum(1 for r in records if len(r.ref) == 1 and len(r.alt) == 1)
            indels = sum(1 for r in records if len(r.ref) != len(r.alt))

            assert snps == 3, f"Expected 3 SNPs, got {snps}"
            assert indels == 2, f"Expected 2 indels, got {indels}"
        finally:
            vcf_file.unlink()

    def test_insertion_deletion_counts(self):
        """Distinguish insertions from deletions."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100, ref="ATG", alt=["A"]),
            SyntheticVariant(chrom="chr1", pos=200, ref="ATGC", alt=["A"]),
            SyntheticVariant(chrom="chr1", pos=300, ref="A", alt=["ATG"]),
            SyntheticVariant(chrom="chr1", pos=400, ref="A", alt=["ATGCATGC"]),
        ]

        vcf_file = VCFGenerator.generate_file(variants)
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)
            parser.close()

            deletions = sum(1 for r in records if len(r.ref) > len(r.alt))
            insertions = sum(1 for r in records if len(r.ref) < len(r.alt))

            assert deletions == 2, f"Expected 2 deletions, got {deletions}"
            assert insertions == 2, f"Expected 2 insertions, got {insertions}"
        finally:
            vcf_file.unlink()
