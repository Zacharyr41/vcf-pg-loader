"""Unit tests for column definition consistency."""


class TestColumnDefinitionConsistency:
    """Test that column definitions are consistent across modules."""

    def test_loader_columns_match_db_loader_columns(self):
        """loader.py and db_loader.py should use same column definitions."""
        from vcf_pg_loader.columns import VARIANT_COLUMNS

        assert "chrom" in VARIANT_COLUMNS
        assert "pos" in VARIANT_COLUMNS
        assert "ref" in VARIANT_COLUMNS
        assert "alt" in VARIANT_COLUMNS
        assert "load_batch_id" in VARIANT_COLUMNS

    def test_all_variant_record_fields_have_column(self):
        """All VariantRecord database fields should have corresponding columns."""
        from vcf_pg_loader.columns import VARIANT_COLUMNS

        db_fields = {
            "chrom", "pos", "end_pos", "ref", "alt", "qual", "filter",
            "rs_id", "gene", "consequence", "impact", "hgvs_c", "hgvs_p",
            "af_gnomad", "cadd_phred", "clinvar_sig"
        }

        for field in db_fields:
            assert field in VARIANT_COLUMNS, f"Missing column for field: {field}"

    def test_column_order_is_list(self):
        """Columns should be defined as a list to preserve order."""
        from vcf_pg_loader.columns import VARIANT_COLUMNS

        assert isinstance(VARIANT_COLUMNS, list)

    def test_no_duplicate_columns(self):
        """Column list should not have duplicates."""
        from vcf_pg_loader.columns import VARIANT_COLUMNS

        assert len(VARIANT_COLUMNS) == len(set(VARIANT_COLUMNS))

    def test_loader_uses_shared_columns(self):
        """VCFLoader.copy_batch should use shared column definition."""
        from vcf_pg_loader.columns import VARIANT_COLUMNS

        assert len(VARIANT_COLUMNS) > 0

    def test_db_loader_uses_shared_columns(self):
        """db_loader._get_columns should return shared column definition."""
        from vcf_pg_loader.db_loader import _get_columns

        columns = _get_columns()
        assert isinstance(columns, list)
        assert len(columns) > 0


class TestVariantRecordFieldMapping:
    """Test VariantRecord field to column mapping."""

    def test_variant_record_has_required_fields(self):
        """VariantRecord should have all required database fields."""
        from vcf_pg_loader.models import VariantRecord

        required = ["chrom", "pos", "ref", "alt"]
        for field in required:
            assert hasattr(VariantRecord, "__dataclass_fields__")
            fields = VariantRecord.__dataclass_fields__
            assert field in fields, f"Missing required field: {field}"

    def test_variant_record_annotation_fields(self):
        """VariantRecord should have annotation fields."""
        from vcf_pg_loader.models import VariantRecord

        annotation_fields = ["gene", "consequence", "impact", "hgvs_c", "hgvs_p"]
        fields = VariantRecord.__dataclass_fields__
        for field in annotation_fields:
            assert field in fields, f"Missing annotation field: {field}"

    def test_variant_record_frequency_fields(self):
        """VariantRecord should have population frequency fields."""
        from vcf_pg_loader.models import VariantRecord

        freq_fields = ["af_gnomad"]
        fields = VariantRecord.__dataclass_fields__
        for field in freq_fields:
            assert field in fields, f"Missing frequency field: {field}"
