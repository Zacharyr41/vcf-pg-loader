"""Tests for VCF parsing functionality."""

from vcf_pg_loader.vcf_parser import VCFHeaderParser, get_array_size


class TestVCFHeaderParser:
    def test_parse_info_field_definitions(self):
        """Test parsing of INFO field definitions from header."""
        header_lines = [
            '##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">',
            '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">',
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total depth">',
            '##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">',
        ]

        parser = VCFHeaderParser()
        info_fields = parser.parse_info_fields(header_lines)

        assert len(info_fields) == 4
        assert info_fields['AC'] == {'Number': 'A', 'Type': 'Integer', 'Description': 'Allele count'}
        assert info_fields['AF'] == {'Number': 'A', 'Type': 'Float', 'Description': 'Allele frequency'}
        assert info_fields['DP'] == {'Number': '1', 'Type': 'Integer', 'Description': 'Total depth'}
        assert info_fields['DB'] == {'Number': '0', 'Type': 'Flag', 'Description': 'dbSNP membership'}

    def test_parse_format_field_definitions(self):
        """Test parsing of FORMAT field definitions from header."""
        header_lines = [
            '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
            '##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">',
            '##FORMAT=<ID=PL,Number=G,Type=Integer,Description="Phred-scaled genotype likelihoods">',
        ]

        parser = VCFHeaderParser()
        format_fields = parser.parse_format_fields(header_lines)

        assert len(format_fields) == 3
        assert format_fields['GT'] == {'Number': '1', 'Type': 'String', 'Description': 'Genotype'}
        assert format_fields['AD'] == {'Number': 'R', 'Type': 'Integer', 'Description': 'Allelic depths'}
        assert format_fields['PL'] == {'Number': 'G', 'Type': 'Integer', 'Description': 'Phred-scaled genotype likelihoods'}

    def test_parse_vep_csq_header(self):
        """Test parsing of VEP CSQ field structure."""
        header_line = '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp">'

        parser = VCFHeaderParser()
        csq_fields = parser.parse_csq_header([header_line])

        expected_fields = [
            'Allele', 'Consequence', 'IMPACT', 'SYMBOL', 'Gene', 'Feature_type',
            'Feature', 'BIOTYPE', 'EXON', 'INTRON', 'HGVSc', 'HGVSp'
        ]
        assert csq_fields == expected_fields

    def test_empty_header_parsing(self):
        """Test handling of empty or malformed headers."""
        parser = VCFHeaderParser()

        assert parser.parse_info_fields([]) == {}
        assert parser.parse_format_fields([]) == {}
        assert parser.parse_csq_header([]) == []


class TestArraySizing:
    def test_number_a_sizing(self):
        """Test Number=A field sizing (per ALT allele)."""
        assert get_array_size('A', n_alts=1) == 1
        assert get_array_size('A', n_alts=2) == 2
        assert get_array_size('A', n_alts=3) == 3

    def test_number_r_sizing(self):
        """Test Number=R field sizing (per allele including REF)."""
        assert get_array_size('R', n_alts=1) == 2  # REF + 1 ALT
        assert get_array_size('R', n_alts=2) == 3  # REF + 2 ALT
        assert get_array_size('R', n_alts=3) == 4  # REF + 3 ALT

    def test_number_g_sizing(self):
        """Test Number=G field sizing (per genotype)."""
        # Diploid genotypes: C(n_alleles + ploidy - 1, ploidy)
        assert get_array_size('G', n_alts=1, ploidy=2) == 3  # 0/0, 0/1, 1/1
        assert get_array_size('G', n_alts=2, ploidy=2) == 6  # 0/0, 0/1, 0/2, 1/1, 1/2, 2/2
        assert get_array_size('G', n_alts=3, ploidy=2) == 10

    def test_fixed_number_sizing(self):
        """Test fixed number field sizing."""
        assert get_array_size('1', n_alts=2) == 1
        assert get_array_size('2', n_alts=3) == 2
        assert get_array_size('10', n_alts=1) == 10

    def test_variable_number_sizing(self):
        """Test variable length field sizing."""
        assert get_array_size('.', n_alts=2) == -1

    def test_invalid_number_sizing(self):
        """Test invalid Number specification handling."""
        assert get_array_size('X', n_alts=2) == 1
        assert get_array_size('', n_alts=2) == 1
