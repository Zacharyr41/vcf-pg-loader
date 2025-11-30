"""Unit tests for loader return types and type safety."""

from typing import get_type_hints


class TestLoadResultTypes:
    """Test LoadResult TypedDict structure."""

    def test_load_result_has_variants_loaded_field(self):
        """LoadResult must have variants_loaded field."""
        from vcf_pg_loader.loader import LoadResult

        hints = get_type_hints(LoadResult)
        assert "variants_loaded" in hints
        assert hints["variants_loaded"] is int

    def test_load_result_has_load_batch_id_field(self):
        """LoadResult must have load_batch_id field."""
        from vcf_pg_loader.loader import LoadResult

        hints = get_type_hints(LoadResult)
        assert "load_batch_id" in hints
        assert hints["load_batch_id"] is str

    def test_load_result_has_file_hash_field(self):
        """LoadResult must have file_hash field."""
        from vcf_pg_loader.loader import LoadResult

        hints = get_type_hints(LoadResult)
        assert "file_hash" in hints
        assert hints["file_hash"] is str

    def test_load_result_optional_fields(self):
        """LoadResult optional fields are properly typed."""
        from vcf_pg_loader.loader import LoadResult

        hints = get_type_hints(LoadResult)
        assert "parallel" in hints
        assert "is_reload" in hints
        assert "previous_load_id" in hints


class TestSkippedResultTypes:
    """Test SkippedResult TypedDict structure."""

    def test_skipped_result_has_skipped_field(self):
        """SkippedResult must have skipped field set to True."""
        from vcf_pg_loader.loader import SkippedResult

        hints = get_type_hints(SkippedResult)
        assert "skipped" in hints

    def test_skipped_result_has_reason_field(self):
        """SkippedResult must have reason field."""
        from vcf_pg_loader.loader import SkippedResult

        hints = get_type_hints(SkippedResult)
        assert "reason" in hints
        assert hints["reason"] is str

    def test_skipped_result_has_previous_load_id_field(self):
        """SkippedResult must have previous_load_id field."""
        from vcf_pg_loader.loader import SkippedResult

        hints = get_type_hints(SkippedResult)
        assert "previous_load_id" in hints
        assert hints["previous_load_id"] is str

    def test_skipped_result_has_file_hash_field(self):
        """SkippedResult must have file_hash field."""
        from vcf_pg_loader.loader import SkippedResult

        hints = get_type_hints(SkippedResult)
        assert "file_hash" in hints
        assert hints["file_hash"] is str


class TestCheckExistingResultTypes:
    """Test CheckExistingResult TypedDict structure."""

    def test_check_existing_result_has_required_fields(self):
        """CheckExistingResult must have all required fields."""
        from vcf_pg_loader.loader import CheckExistingResult

        hints = get_type_hints(CheckExistingResult)
        assert "load_batch_id" in hints
        assert "status" in hints
        assert "variants_loaded" in hints
        assert "load_completed_at" in hints


class TestTypeConsistency:
    """Test type consistency across the loader module."""

    def test_load_vcf_return_type_annotated(self):
        """load_vcf method should have proper return type annotation."""
        from vcf_pg_loader.loader import VCFLoader

        hints = get_type_hints(VCFLoader.load_vcf)
        assert "return" in hints

    def test_check_existing_return_type_annotated(self):
        """check_existing method should have proper return type annotation."""
        from vcf_pg_loader.loader import VCFLoader

        hints = get_type_hints(VCFLoader.check_existing)
        assert "return" in hints
