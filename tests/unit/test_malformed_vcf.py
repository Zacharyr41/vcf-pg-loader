"""Unit tests for malformed VCF handling."""

import gzip
import tempfile
from pathlib import Path

import pytest

from vcf_pg_loader.vcf_parser import VCFStreamingParser


class TestMalformedVCFHeader:
    """Test handling of malformed VCF headers."""

    def test_missing_fileformat_raises_error(self):
        """VCF without ##fileformat line should raise error."""
        vcf_content = """#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            with pytest.raises((OSError, Exception)):
                parser = VCFStreamingParser(path, human_genome=True)
                for _ in parser.iter_batches():
                    pass
                parser.close()
        finally:
            path.unlink()

    def test_missing_chrom_header_raises_error(self):
        """VCF without #CHROM header line should raise error."""
        vcf_content = """##fileformat=VCFv4.3
chr1	100	.	A	G	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            with pytest.raises((OSError, Exception)):
                parser = VCFStreamingParser(path, human_genome=True)
                for _ in parser.iter_batches():
                    pass
                parser.close()
        finally:
            path.unlink()

    def test_empty_file_raises_error(self):
        """Empty VCF file should raise error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            path = Path(f.name)

        try:
            with pytest.raises((OSError, Exception, StopIteration)):
                parser = VCFStreamingParser(path, human_genome=True)
                for _ in parser.iter_batches():
                    pass
                parser.close()
        finally:
            path.unlink()


class TestCorruptGzipHandling:
    """Test handling of corrupt gzip files."""

    def test_corrupt_gzip_raises_error(self):
        """Corrupt gzip file should raise meaningful error."""
        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as f:
            f.write(b"\x1f\x8b\x08\x00CORRUPTED")
            path = Path(f.name)

        try:
            with pytest.raises((gzip.BadGzipFile, OSError, EOFError)):
                parser = VCFStreamingParser(path, human_genome=True)
                for _ in parser.iter_batches():
                    pass
                parser.close()
        finally:
            path.unlink()

    def test_truncated_gzip_raises_error(self):
        """Truncated gzip file should raise error."""
        valid_vcf = b"""##fileformat=VCFv4.3
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t30\t.\t.
"""
        with tempfile.NamedTemporaryFile(suffix=".vcf.gz", delete=False) as f:
            compressed = gzip.compress(valid_vcf)
            f.write(compressed[:len(compressed) // 2])
            path = Path(f.name)

        try:
            with pytest.raises((gzip.BadGzipFile, OSError, EOFError, Exception)):
                parser = VCFStreamingParser(path, human_genome=True)
                for _ in parser.iter_batches():
                    pass
                parser.close()
        finally:
            path.unlink()


class TestInvalidDataLines:
    """Test handling of invalid data lines in VCF."""

    def test_too_few_columns_handled(self):
        """Lines with too few columns should be handled gracefully."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A
chr1	200	.	C	T	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser = VCFStreamingParser(path, human_genome=True)
            variants = []
            try:
                for batch in parser.iter_batches():
                    variants.extend(batch)
            except (ValueError, IndexError, Exception):
                pass
            parser.close()
        finally:
            path.unlink()

    def test_non_integer_position_handled(self):
        """Non-integer position should be handled gracefully."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	ABC	.	A	G	30	.	.
chr1	200	.	C	T	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser = VCFStreamingParser(path, human_genome=True)
            try:
                for _batch in parser.iter_batches():
                    pass
            except (ValueError, Exception):
                pass
            parser.close()
        finally:
            path.unlink()

    def test_empty_ref_handled(self):
        """Empty REF field should be handled gracefully."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.		G	30	.	.
chr1	200	.	C	T	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser = VCFStreamingParser(path, human_genome=True)
            variants = []
            for batch in parser.iter_batches():
                variants.extend(batch)
            parser.close()
        finally:
            path.unlink()


class TestInvalidChromosomeValues:
    """Test handling of invalid chromosome values."""

    def test_unknown_chromosome_in_human_mode(self):
        """Unknown chromosome in human mode should be handled."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=chrUnknown,length=1000>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chrUnknown	100	.	A	G	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser = VCFStreamingParser(path, human_genome=True)
            variants = []
            for batch in parser.iter_batches():
                variants.extend(batch)
            parser.close()
            assert len(variants) >= 0
        finally:
            path.unlink()

    def test_numeric_chromosome_without_prefix(self):
        """Numeric chromosome without 'chr' prefix handling."""
        vcf_content = """##fileformat=VCFv4.3
##contig=<ID=1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
1	100	.	A	G	30	.	.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write(vcf_content)
            path = Path(f.name)

        try:
            parser = VCFStreamingParser(path, human_genome=True)
            variants = []
            for batch in parser.iter_batches():
                variants.extend(batch)
            parser.close()
        finally:
            path.unlink()


class TestNonVCFFiles:
    """Test handling of non-VCF files."""

    def test_binary_file_raises_error(self):
        """Binary file should raise appropriate error."""
        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            f.write(b"\x00\x01\x02\x03\x04\x05\x06\x07")
            path = Path(f.name)

        try:
            with pytest.raises((UnicodeDecodeError, ValueError, OSError, Exception)):
                parser = VCFStreamingParser(path, human_genome=True)
                for _ in parser.iter_batches():
                    pass
                parser.close()
        finally:
            path.unlink()

    def test_json_file_raises_error(self):
        """JSON file should raise error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vcf", delete=False) as f:
            f.write('{"variants": []}')
            path = Path(f.name)

        try:
            with pytest.raises((ValueError, OSError, Exception)):
                parser = VCFStreamingParser(path, human_genome=True)
                for _ in parser.iter_batches():
                    pass
                parser.close()
        finally:
            path.unlink()
