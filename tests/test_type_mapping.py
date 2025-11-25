"""Tests for VCF to PostgreSQL type mapping."""

from vcf_pg_loader.type_mapping import (
    get_pg_type,
    get_worst_impact,
    infer_column_definition,
    normalize_clinvar_significance,
)


class TestGetPgType:
    def test_integer_single_value(self):
        """Test Integer with Number=1 maps to INTEGER."""
        assert get_pg_type('Integer', '1') == 'INTEGER'

    def test_integer_per_alt(self):
        """Test Integer with Number=A maps to INTEGER[]."""
        assert get_pg_type('Integer', 'A') == 'INTEGER[]'

    def test_integer_per_allele(self):
        """Test Integer with Number=R maps to INTEGER[]."""
        assert get_pg_type('Integer', 'R') == 'INTEGER[]'

    def test_integer_per_genotype(self):
        """Test Integer with Number=G maps to INTEGER[]."""
        assert get_pg_type('Integer', 'G') == 'INTEGER[]'

    def test_integer_variable(self):
        """Test Integer with Number=. maps to INTEGER[]."""
        assert get_pg_type('Integer', '.') == 'INTEGER[]'

    def test_float_single_value(self):
        """Test Float with Number=1 maps to REAL."""
        assert get_pg_type('Float', '1') == 'REAL'

    def test_float_per_alt(self):
        """Test Float with Number=A maps to REAL[]."""
        assert get_pg_type('Float', 'A') == 'REAL[]'

    def test_float_per_allele(self):
        """Test Float with Number=R maps to REAL[]."""
        assert get_pg_type('Float', 'R') == 'REAL[]'

    def test_string_single_value(self):
        """Test String with Number=1 maps to TEXT."""
        assert get_pg_type('String', '1') == 'TEXT'

    def test_string_variable(self):
        """Test String with Number=. maps to TEXT[]."""
        assert get_pg_type('String', '.') == 'TEXT[]'

    def test_flag_type(self):
        """Test Flag with Number=0 maps to BOOLEAN."""
        assert get_pg_type('Flag', '0') == 'BOOLEAN'

    def test_character_single(self):
        """Test Character with Number=1 maps to CHAR(1)."""
        assert get_pg_type('Character', '1') == 'CHAR(1)'

    def test_fixed_number_greater_than_one(self):
        """Test fixed Number > 1 maps to array types."""
        assert get_pg_type('Integer', '2') == 'INTEGER[]'
        assert get_pg_type('Float', '3') == 'REAL[]'
        assert get_pg_type('String', '4') == 'TEXT[]'

    def test_unknown_number_spec(self):
        """Test handling of unknown Number specification."""
        assert get_pg_type('Integer', 'X') == 'INTEGER[]'
        assert get_pg_type('Float', 'unknown') == 'REAL[]'
        assert get_pg_type('String', 'Z') == 'TEXT'


class TestInferColumnDefinition:
    def test_simple_integer_column(self):
        """Test column definition for simple integer field."""
        result = infer_column_definition('DP', 'Integer', '1')
        assert result == 'dp INTEGER'

    def test_array_integer_column(self):
        """Test column definition for array integer field."""
        result = infer_column_definition('AC', 'Integer', 'A')
        assert result == 'ac INTEGER[]'

    def test_float_column(self):
        """Test column definition for float field."""
        result = infer_column_definition('AF', 'Float', 'A')
        assert result == 'af REAL[]'

    def test_string_column(self):
        """Test column definition for string field."""
        result = infer_column_definition('CSQ', 'String', '.')
        assert result == 'csq TEXT[]'

    def test_flag_column(self):
        """Test column definition for flag field."""
        result = infer_column_definition('DB', 'Flag', '0')
        assert result == 'db BOOLEAN'

    def test_lowercase_conversion(self):
        """Test that field IDs are lowercased in column names."""
        result = infer_column_definition('MIXED_Case', 'String', '1')
        assert result == 'mixed_case TEXT'


class TestNormalizeClinvarSignificance:
    def test_pathogenic(self):
        """Test pathogenic normalization."""
        assert normalize_clinvar_significance('Pathogenic') == 'pathogenic'

    def test_likely_pathogenic(self):
        """Test likely pathogenic normalization."""
        assert normalize_clinvar_significance('Likely_pathogenic') == 'likely_pathogenic'

    def test_benign(self):
        """Test benign normalization."""
        assert normalize_clinvar_significance('Benign') == 'benign'

    def test_likely_benign(self):
        """Test likely benign normalization."""
        assert normalize_clinvar_significance('Likely_benign') == 'likely_benign'

    def test_vus(self):
        """Test VUS (uncertain significance) normalization."""
        assert normalize_clinvar_significance('Uncertain_significance') == 'vus'

    def test_conflicting(self):
        """Test conflicting interpretations normalization."""
        assert normalize_clinvar_significance('Conflicting_interpretations_of_pathogenicity') == 'conflicting'

    def test_multiple_values_pathogenic_wins(self):
        """Test that pathogenic takes precedence in multiple values."""
        assert normalize_clinvar_significance('Benign,Pathogenic') == 'pathogenic'
        assert normalize_clinvar_significance('Uncertain_significance/Pathogenic') == 'pathogenic'

    def test_multiple_values_likely_pathogenic(self):
        """Test likely pathogenic precedence in multiple values."""
        assert normalize_clinvar_significance('Benign,Likely_pathogenic') == 'likely_pathogenic'

    def test_empty_string(self):
        """Test handling of empty string."""
        assert normalize_clinvar_significance('') == 'not_provided'

    def test_none_handling(self):
        """Test handling of None value."""
        assert normalize_clinvar_significance(None) == 'not_provided'

    def test_drug_response(self):
        """Test drug response normalization."""
        assert normalize_clinvar_significance('drug_response') == 'drug_response'


class TestGetWorstImpact:
    def test_single_high_impact(self):
        """Test single HIGH impact returns HIGH."""
        assert get_worst_impact(['HIGH']) == 'HIGH'

    def test_single_moderate_impact(self):
        """Test single MODERATE impact returns MODERATE."""
        assert get_worst_impact(['MODERATE']) == 'MODERATE'

    def test_single_low_impact(self):
        """Test single LOW impact returns LOW."""
        assert get_worst_impact(['LOW']) == 'LOW'

    def test_single_modifier_impact(self):
        """Test single MODIFIER impact returns MODIFIER."""
        assert get_worst_impact(['MODIFIER']) == 'MODIFIER'

    def test_high_takes_precedence(self):
        """Test HIGH takes precedence over other impacts."""
        assert get_worst_impact(['LOW', 'HIGH', 'MODERATE']) == 'HIGH'
        assert get_worst_impact(['MODIFIER', 'HIGH']) == 'HIGH'

    def test_moderate_takes_precedence_over_low(self):
        """Test MODERATE takes precedence over LOW and MODIFIER."""
        assert get_worst_impact(['LOW', 'MODERATE', 'MODIFIER']) == 'MODERATE'

    def test_low_takes_precedence_over_modifier(self):
        """Test LOW takes precedence over MODIFIER."""
        assert get_worst_impact(['MODIFIER', 'LOW']) == 'LOW'

    def test_empty_list(self):
        """Test empty list returns MODIFIER."""
        assert get_worst_impact([]) == 'MODIFIER'

    def test_unknown_impact_falls_through(self):
        """Test unknown impact value returns first element."""
        assert get_worst_impact(['UNKNOWN']) == 'UNKNOWN'
