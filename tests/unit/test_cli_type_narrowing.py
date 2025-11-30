"""Unit tests for CLI result type narrowing."""

from typing import get_type_hints


class TestResultTypeGuards:
    """Test TypeGuard functions for result type narrowing."""

    def test_is_skipped_result_returns_true_for_skipped(self):
        """is_skipped_result returns True for SkippedResult."""
        from vcf_pg_loader.loader import is_skipped_result

        result = {
            "skipped": True,
            "reason": "already_loaded",
            "previous_load_id": "abc-123",
            "file_hash": "deadbeef" * 8,
        }
        assert is_skipped_result(result) is True

    def test_is_skipped_result_returns_false_for_load_result(self):
        """is_skipped_result returns False for LoadResult."""
        from vcf_pg_loader.loader import is_skipped_result

        result = {
            "variants_loaded": 1000,
            "load_batch_id": "abc-123",
            "file_hash": "deadbeef" * 8,
        }
        assert is_skipped_result(result) is False

    def test_is_load_result_returns_true_for_load_result(self):
        """is_load_result returns True for LoadResult."""
        from vcf_pg_loader.loader import is_load_result

        result = {
            "variants_loaded": 1000,
            "load_batch_id": "abc-123",
            "file_hash": "deadbeef" * 8,
        }
        assert is_load_result(result) is True

    def test_is_load_result_returns_false_for_skipped(self):
        """is_load_result returns False for SkippedResult."""
        from vcf_pg_loader.loader import is_load_result

        result = {
            "skipped": True,
            "reason": "already_loaded",
            "previous_load_id": "abc-123",
            "file_hash": "deadbeef" * 8,
        }
        assert is_load_result(result) is False

    def test_type_guards_are_mutually_exclusive(self):
        """is_skipped_result and is_load_result are mutually exclusive."""
        from vcf_pg_loader.loader import is_load_result, is_skipped_result

        skipped = {
            "skipped": True,
            "reason": "already_loaded",
            "previous_load_id": "abc-123",
            "file_hash": "deadbeef" * 8,
        }
        loaded = {
            "variants_loaded": 1000,
            "load_batch_id": "abc-123",
            "file_hash": "deadbeef" * 8,
        }

        assert is_skipped_result(skipped) != is_load_result(skipped)
        assert is_skipped_result(loaded) != is_load_result(loaded)


class TestTypeGuardSignatures:
    """Test that TypeGuard functions have correct signatures."""

    def test_is_skipped_result_has_typeguard_return(self):
        """is_skipped_result should return TypeGuard[SkippedResult]."""
        from vcf_pg_loader.loader import is_skipped_result

        hints = get_type_hints(is_skipped_result)
        assert "return" in hints
        return_type = str(hints["return"])
        assert "TypeGuard" in return_type or "bool" in return_type

    def test_is_load_result_has_typeguard_return(self):
        """is_load_result should return TypeGuard[LoadResult]."""
        from vcf_pg_loader.loader import is_load_result

        hints = get_type_hints(is_load_result)
        assert "return" in hints
        return_type = str(hints["return"])
        assert "TypeGuard" in return_type or "bool" in return_type
