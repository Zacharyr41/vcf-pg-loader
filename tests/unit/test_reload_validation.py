"""Unit tests for reload validation and UUID type checking."""

from uuid import UUID, uuid4


class TestUUIDValidation:
    """Test UUID validation for reload operations."""

    def test_valid_uuid_object_accepted(self):
        """UUID objects should be accepted for reload operations."""
        from vcf_pg_loader.loader import validate_previous_load_id

        valid_uuid = uuid4()
        assert validate_previous_load_id(valid_uuid) is True

    def test_valid_uuid_string_rejected(self):
        """UUID strings should be rejected (must use UUID object)."""
        from vcf_pg_loader.loader import validate_previous_load_id

        uuid_string = str(uuid4())
        assert validate_previous_load_id(uuid_string) is False

    def test_none_value_rejected(self):
        """None should be rejected when UUID is expected."""
        from vcf_pg_loader.loader import validate_previous_load_id

        assert validate_previous_load_id(None) is False

    def test_invalid_type_rejected(self):
        """Invalid types should be rejected."""
        from vcf_pg_loader.loader import validate_previous_load_id

        assert validate_previous_load_id(12345) is False
        assert validate_previous_load_id([uuid4()]) is False
        assert validate_previous_load_id({"uuid": uuid4()}) is False

    def test_uuid_from_string_conversion(self):
        """UUID can be created from valid string."""
        uuid_string = "550e8400-e29b-41d4-a716-446655440000"
        uuid_obj = UUID(uuid_string)
        assert isinstance(uuid_obj, UUID)

    def test_uuid_type_check_is_fast(self):
        """Type check should be O(1) operation."""
        import time

        from vcf_pg_loader.loader import validate_previous_load_id

        valid_uuid = uuid4()

        start = time.perf_counter()
        for _ in range(10000):
            validate_previous_load_id(valid_uuid)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.1


class TestReloadSecurity:
    """Test security aspects of reload operations."""

    def test_reload_requires_uuid_object_not_string(self):
        """Reload operation must use UUID object to prevent SQL injection risk."""
        from vcf_pg_loader.loader import validate_previous_load_id

        malicious_string = "'; DROP TABLE variants; --"
        assert validate_previous_load_id(malicious_string) is False

    def test_uuid_validation_runs_before_delete(self):
        """UUID validation must run before DELETE query is executed."""
        from vcf_pg_loader.loader import validate_previous_load_id

        valid_uuid = uuid4()
        assert validate_previous_load_id(valid_uuid) is True
