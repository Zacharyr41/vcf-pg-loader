"""Tests for input source flexibility (files, streams, compressed)."""

import gzip
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fixtures.vcf_generator import SyntheticVariant, VCFGenerator


class TestFileInputSources:
    """Test various file-based input sources."""

    def test_plain_vcf_file(self):
        """Parse plain .vcf file."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"]),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            assert len(batches[0]) == 1
        finally:
            vcf_file.unlink()

    def test_gzipped_vcf_file(self):
        """Parse gzipped .vcf.gz file."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = VCFGenerator.generate([
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"]),
        ])

        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as f:
            with gzip.open(f.name, "wt") as gz:
                gz.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
            assert len(batches[0]) == 1
        finally:
            vcf_file.unlink()

    def test_bgzipped_vcf_file(self):
        """Parse bgzipped .vcf.gz file (standard for VCF)."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"]),
        ])
        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()

    def test_path_as_string(self):
        """Accept path as string instead of Path object."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_file = VCFGenerator.generate_file([
            SyntheticVariant(chrom="chr1", pos=100, ref="A", alt=["G"]),
        ])
        try:
            parser = VCFStreamingParser(str(vcf_file), human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()

    def test_nonexistent_file_raises_error(self):
        """Non-existent file raises appropriate error."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        with pytest.raises((FileNotFoundError, OSError)):
            VCFStreamingParser("/nonexistent/path/file.vcf", human_genome=True)


class TestLargeFileHandling:
    """Test handling of large files."""

    def test_streaming_doesnt_load_entire_file(self):
        """Streaming parser doesn't load entire file into memory."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100 + i, ref="A", alt=["G"])
            for i in range(10000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True, batch_size=100)

            batch_count = 0
            for batch in parser.iter_batches():
                batch_count += 1
                assert len(batch) <= 100

            parser.close()
            assert batch_count == 100
        finally:
            vcf_file.unlink()

    def test_configurable_batch_size(self):
        """Batch size is configurable."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        variants = [
            SyntheticVariant(chrom="chr1", pos=100 + i, ref="A", alt=["G"])
            for i in range(1000)
        ]
        vcf_file = VCFGenerator.generate_file(variants)

        try:
            for batch_size in [10, 100, 500, 1000]:
                parser = VCFStreamingParser(
                    vcf_file, human_genome=True, batch_size=batch_size
                )
                batches = list(parser.iter_batches())
                parser.close()

                expected_batches = (1000 + batch_size - 1) // batch_size
                assert len(batches) == expected_batches
        finally:
            vcf_file.unlink()


class TestURLInputSources:
    """Test URL-based input sources (if supported)."""

    @pytest.mark.skip(reason="URL input not yet implemented")
    def test_http_url_input(self):
        """Parse VCF from HTTP URL."""
        pass

    @pytest.mark.skip(reason="URL input not yet implemented")
    def test_s3_url_input(self):
        """Parse VCF from S3 URL."""
        pass


class TestMultipleFileInput:
    """Test processing multiple VCF files."""

    def test_process_multiple_files_sequentially(self):
        """Process multiple VCF files in sequence."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        files = []
        for i in range(3):
            variants = [
                SyntheticVariant(
                    chrom=f"chr{i+1}", pos=100 + j, ref="A", alt=["G"]
                )
                for j in range(100)
            ]
            files.append(VCFGenerator.generate_file(variants))

        try:
            total_variants = 0
            chromosomes = set()

            for vcf_file in files:
                parser = VCFStreamingParser(vcf_file, human_genome=True)
                for batch in parser.iter_batches():
                    for record in batch:
                        total_variants += 1
                        chromosomes.add(record.chrom)
                parser.close()

            assert total_variants == 300
            assert chromosomes == {"chr1", "chr2", "chr3"}
        finally:
            for f in files:
                f.unlink()

    def test_sample_id_per_file(self):
        """Different sample IDs can be assigned per file."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        files = []
        for i in range(3):
            variants = [
                SyntheticVariant(chrom="chr1", pos=100 + i * 100, ref="A", alt=["G"])
            ]
            files.append(VCFGenerator.generate_file(variants))

        try:
            sample_variants = {}
            for idx, vcf_file in enumerate(files):
                sample_id = f"sample_{idx}"
                parser = VCFStreamingParser(vcf_file, human_genome=True)
                for batch in parser.iter_batches():
                    sample_variants[sample_id] = len(batch)
                parser.close()

            assert len(sample_variants) == 3
            for sample_id in ["sample_0", "sample_1", "sample_2"]:
                assert sample_id in sample_variants
        finally:
            for f in files:
                f.unlink()


class TestVCFVersionCompatibility:
    """Test compatibility with different VCF versions."""

    def test_vcf_v4_0(self):
        """Parse VCF v4.0 format."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.0
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t.\t.\t.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()

    def test_vcf_v4_1(self):
        """Parse VCF v4.1 format."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.1
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t.\t.\t.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()

    def test_vcf_v4_2(self):
        """Parse VCF v4.2 format."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t.\t.\t.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()

    def test_vcf_v4_3(self):
        """Parse VCF v4.3 format."""
        from vcf_pg_loader.vcf_parser import VCFStreamingParser

        vcf_content = """##fileformat=VCFv4.3
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t.\t.\t.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            vcf_file = Path(f.name)

        try:
            parser = VCFStreamingParser(vcf_file, human_genome=True)
            batches = list(parser.iter_batches())
            parser.close()

            assert len(batches) == 1
        finally:
            vcf_file.unlink()
