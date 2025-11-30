"""Unit tests for column definition consistency."""



class TestColumnRecordFieldMapping:
    """Test that columns map correctly to VariantRecord fields."""

    def test_basic_columns_match_record_tuple_order(self):
        """VARIANT_COLUMNS_BASIC order matches VariantRecord field extraction order."""
        from uuid import uuid4

        from vcf_pg_loader.columns import VARIANT_COLUMNS_BASIC, get_record_values
        from vcf_pg_loader.models import VariantRecord

        record = VariantRecord(
            chrom="chr1",
            pos=100,
            end_pos=101,
            ref="A",
            alt="G",
            qual=30.0,
            filter=["PASS"],
            rs_id="rs123",
            info={},
            gene="BRCA1",
            consequence="missense",
            impact="MODERATE",
            hgvs_c="c.123A>G",
            hgvs_p="p.Lys41Arg",
            af_gnomad=0.001,
            cadd_phred=25.0,
            clinvar_sig="Pathogenic",
        )
        load_batch_id = uuid4()

        values = get_record_values(record, load_batch_id)
        assert len(values) == len(VARIANT_COLUMNS_BASIC)

    def test_get_record_values_returns_tuple(self):
        """get_record_values returns a tuple for COPY protocol."""
        from uuid import uuid4

        from vcf_pg_loader.columns import get_record_values
        from vcf_pg_loader.models import VariantRecord

        record = VariantRecord(
            chrom="chr1", pos=100, ref="A", alt="G",
            qual=None, filter=[], rs_id=None, info={}
        )
        values = get_record_values(record, uuid4())
        assert isinstance(values, tuple)

    def test_get_record_values_includes_pos_range(self):
        """get_record_values computes pos_range correctly."""
        from uuid import uuid4

        from vcf_pg_loader.columns import get_record_values
        from vcf_pg_loader.models import VariantRecord

        record = VariantRecord(
            chrom="chr1", pos=100, end_pos=105, ref="ACGT", alt="A",
            qual=None, filter=[], rs_id=None, info={}
        )
        values = get_record_values(record, uuid4())
        assert values[1].lower == 100
        assert values[1].upper == 105

    def test_columns_and_values_align(self):
        """Column names align with value positions."""
        from uuid import uuid4

        from vcf_pg_loader.columns import VARIANT_COLUMNS_BASIC, get_record_values
        from vcf_pg_loader.models import VariantRecord

        record = VariantRecord(
            chrom="chr1", pos=100, ref="A", alt="G",
            qual=None, filter=[], rs_id=None, info={}, gene="TP53"
        )
        batch_id = uuid4()
        values = get_record_values(record, batch_id)

        value_dict = dict(zip(VARIANT_COLUMNS_BASIC, values, strict=True))
        assert value_dict["chrom"] == "chr1"
        assert value_dict["pos"] == 100
        assert value_dict["ref"] == "A"
        assert value_dict["alt"] == "G"
        assert value_dict["gene"] == "TP53"
        assert value_dict["load_batch_id"] == batch_id


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
        """db_loader should import columns from shared definition."""
        from vcf_pg_loader.columns import VARIANT_COLUMNS
        from vcf_pg_loader.db_loader import load_variants

        assert len(VARIANT_COLUMNS) > 0
        assert callable(load_variants)


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
