"""Round-trip validation tests for data integrity."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator


@pytest.mark.validation
class TestRoundTripIntegrity:
    """Test that variants survive the full load/query cycle unchanged."""

    def test_basic_variant_roundtrip(self):
        """Basic variant fields survive round-trip."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        original = SyntheticVariant(
            chrom="chr1",
            pos=12345,
            ref="ATCG",
            alt=["A"],
            rs_id="rs12345",
            qual=99.5,
            filter="PASS",
        )
        vcf_file = VCFGenerator.generate_file([original])

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            record = batches[0][0]

            assert record.chrom == "chr1"
            assert record.pos == 12345
            assert record.ref == "ATCG"
            assert record.alt == "A"
            assert record.rs_id == "rs12345"
            assert record.qual == 99.5
            assert record.filter == []
        finally:
            vcf_file.unlink()

    def test_info_field_roundtrip(self):
        """INFO fields survive round-trip with correct types."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        original = SyntheticVariant(
            chrom="chr1",
            pos=1000,
            ref="A",
            alt=["G"],
            info={
                "DP": 150,
                "AF": [0.45],
                "AC": [25],
                "AN": 50,
            },
        )
        vcf_file = VCFGenerator.generate_file([original])

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            record = batches[0][0]
            info = record.info

            assert info.get("DP") == 150
            af = info.get("AF")
            af_val = af[0] if isinstance(af, list) else af
            assert abs(af_val - 0.45) < 0.01
        finally:
            vcf_file.unlink()

    def test_multiallelic_roundtrip(self):
        """Multi-allelic sites are correctly decomposed and preserved."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        original = SyntheticVariant(
            chrom="chr1",
            pos=1000,
            ref="A",
            alt=["G", "T", "C"],
            info={
                "AF": [0.1, 0.2, 0.3],
                "AC": [10, 20, 30],
            },
        )
        vcf_file = VCFGenerator.generate_file([original])

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            records = batches[0]
            assert len(records) == 3

            alts = sorted([r.alt for r in records])
            assert alts == ["C", "G", "T"]

            for record in records:
                af = record.info.get("AF")
                af_val = af[0] if isinstance(af, list) else af
                if record.alt == "G":
                    assert abs(af_val - 0.1) < 0.01
                elif record.alt == "T":
                    assert abs(af_val - 0.2) < 0.01
                elif record.alt == "C":
                    assert abs(af_val - 0.3) < 0.01
        finally:
            vcf_file.unlink()

    def test_annotation_roundtrip(self):
        """VEP annotations survive round-trip."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t1000\t.\tA\tG\t.\t.\tCSQ=G|missense_variant|MODERATE|BRCA1|ENSG00000012048
"""
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".vcf", delete=False
        ) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            record = batches[0][0]

            assert record.gene == "BRCA1"
            assert record.consequence == "missense_variant"
            assert record.impact == "MODERATE"
        finally:
            vcf_file.unlink()


@pytest.mark.validation
class TestNormalizationRoundtrip:
    """Test that normalized variants preserve biological meaning."""

    def test_deletion_normalization_preserves_position(self):
        """Deletion normalization produces correct position."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        original = SyntheticVariant(
            chrom="chr1",
            pos=100,
            ref="ATG",
            alt=["A"],
        )
        vcf_file = VCFGenerator.generate_file([original])

        try:
            parser = VCFStreamingParser(
                vcf_file, human_genome=True, normalize=True
            )
            batches = list(parser.iter_batches())
            parser.close()

            record = batches[0][0]
            assert record.ref == "ATG"
            assert record.alt == "A"
        finally:
            vcf_file.unlink()

    def test_insertion_normalization_preserves_sequence(self):
        """Insertion normalization produces correct sequence."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        original = SyntheticVariant(
            chrom="chr1",
            pos=100,
            ref="A",
            alt=["ATG"],
        )
        vcf_file = VCFGenerator.generate_file([original])

        try:
            parser = VCFStreamingParser(
                vcf_file, human_genome=True, normalize=True
            )
            batches = list(parser.iter_batches())
            parser.close()

            record = batches[0][0]
            assert record.ref == "A"
            assert record.alt == "ATG"
        finally:
            vcf_file.unlink()


@pytest.mark.validation
class TestF1ScoreValidation:
    """Test F1 score calculation for loader accuracy."""

    def test_perfect_f1_score(self):
        """Loading and retrieval should achieve F1 > 0.99."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i * 10,
                ref="A",
                alt=["G"],
                rs_id=f"rs{100 + i}",
            )
            for i in range(1000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            loaded_variants = []
            for batch in parser.iter_batches():
                loaded_variants.extend(batch)
            parser.close()

            expected_positions = {100 + i * 10 for i in range(1000)}
            actual_positions = {v.pos for v in loaded_variants}

            true_positives = len(expected_positions & actual_positions)
            false_positives = len(actual_positions - expected_positions)
            false_negatives = len(expected_positions - actual_positions)

            precision = true_positives / (true_positives + false_positives)
            recall = true_positives / (true_positives + false_negatives)
            f1 = 2 * (precision * recall) / (precision + recall)

            assert f1 > 0.99, f"F1 score {f1:.4f} below 0.99 threshold"
        finally:
            vcf_file.unlink()

    def test_multiallelic_f1_score(self):
        """Multi-allelic decomposition maintains high F1 score."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=100 + i * 10,
                ref="A",
                alt=["G", "T", "C"],
            )
            for i in range(100)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            loaded_variants = []
            for batch in parser.iter_batches():
                loaded_variants.extend(batch)
            parser.close()

            expected_count = 100 * 3
            actual_count = len(loaded_variants)

            precision = min(actual_count, expected_count) / actual_count
            recall = min(actual_count, expected_count) / expected_count
            f1 = 2 * (precision * recall) / (precision + recall)

            assert f1 > 0.99, f"Multi-allelic F1 {f1:.4f} below 0.99"
        finally:
            vcf_file.unlink()


@pytest.mark.validation
@pytest.mark.integration
class TestDatabaseRoundTrip:
    """Test full database round-trip integrity."""

    async def test_database_roundtrip_integrity(self, test_db):
        """Variants loaded to DB can be retrieved with full fidelity."""
        from vcf_pg_loader.db_loader import load_variants
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        original_variants = [
            SyntheticVariant(
                chrom="chr1",
                pos=1000 + i,
                ref="A",
                alt=["G"],
                rs_id=f"rs{1000 + i}" if i % 2 == 0 else ".",
                qual=99.5 if i % 3 == 0 else None,
                info={"DP": 50 + i, "AF": [0.5]},
            )
            for i in range(100)
        ]
        vcf_file = VCFGenerator.generate_file(original_variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            for batch in parser.iter_batches():
                await load_variants(test_db, batch)
            parser.close()

            result = await test_db.fetchval("SELECT COUNT(*) FROM variants")
            assert result == 100

            row = await test_db.fetchrow(
                "SELECT * FROM variants WHERE pos = 1050"
            )
            assert row["chrom"] == "chr1"
            assert row["ref"] == "A"
            assert row["alt"] == "G"
            assert row["rs_id"] == "rs1050"
        finally:
            vcf_file.unlink()
