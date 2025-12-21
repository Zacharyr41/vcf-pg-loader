"""Tests for HIPAA-compliant audit logging."""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vcf_pg_loader.audit import (
    AuditContext,
    AuditEvent,
    AuditEventType,
    AuditLogger,
    audit_context,
    get_audit_context,
    set_audit_context,
)
from vcf_pg_loader.audit.models import PHI_PATTERNS


class TestAuditEventType:
    def test_all_event_types_defined(self):
        expected = {
            "AUTH_LOGIN",
            "AUTH_LOGOUT",
            "AUTH_FAILED",
            "DATA_READ",
            "DATA_WRITE",
            "DATA_DELETE",
            "DATA_EXPORT",
            "SCHEMA_CHANGE",
            "CONFIG_CHANGE",
            "PERMISSION_CHANGE",
            "PHI_ACCESS",
            "EMERGENCY_ACCESS",
        }
        actual = {e.value for e in AuditEventType}
        assert actual == expected


class TestAuditEvent:
    def test_basic_event_creation(self):
        event = AuditEvent(
            event_type=AuditEventType.DATA_WRITE,
            action="test_action",
            success=True,
        )
        assert event.event_type == AuditEventType.DATA_WRITE
        assert event.action == "test_action"
        assert event.success is True
        assert event.user_name == "system"
        assert event.details == {}

    def test_event_with_all_fields(self):
        session_id = uuid4()
        event = AuditEvent(
            event_type=AuditEventType.PHI_ACCESS,
            action="view_patient_data",
            success=True,
            user_id=42,
            user_name="dr_smith",
            session_id=session_id,
            resource_type="variant",
            resource_id="chr1:12345",
            client_ip="192.168.1.100",
            client_hostname="workstation-1",
            application_name="clinical-portal",
            details={"region": "chr1:10000-20000"},
        )
        assert event.user_id == 42
        assert event.session_id == session_id
        assert event.client_ip == "192.168.1.100"

    def test_sanitize_details_removes_phi(self):
        event = AuditEvent(
            event_type=AuditEventType.DATA_READ,
            action="query",
            success=True,
            details={
                "patient_name": "John Doe",
                "patient_dob": "1990-01-01",
                "patient_ssn": "123-45-6789",
                "variant_count": 100,
                "region": "chr1:100-200",
            },
        )
        sanitized = event.sanitize_details()
        assert sanitized["patient_name"] == "[REDACTED]"
        assert sanitized["patient_dob"] == "[REDACTED]"
        assert sanitized["patient_ssn"] == "[REDACTED]"
        assert sanitized["variant_count"] == 100
        assert sanitized["region"] == "chr1:100-200"

    def test_sanitize_details_nested(self):
        event = AuditEvent(
            event_type=AuditEventType.DATA_READ,
            action="query",
            success=True,
            details={
                "metadata": {
                    "sample_id": "secret-sample-123",
                    "chrom": "chr1",
                }
            },
        )
        sanitized = event.sanitize_details()
        assert sanitized["metadata"]["sample_id"] == "[REDACTED]"
        assert sanitized["metadata"]["chrom"] == "chr1"

    def test_sanitize_empty_details(self):
        event = AuditEvent(
            event_type=AuditEventType.DATA_READ,
            action="query",
            success=True,
        )
        assert event.sanitize_details() == {}

    def test_to_db_row(self):
        session_id = uuid4()
        event = AuditEvent(
            event_type=AuditEventType.DATA_WRITE,
            action="load_vcf",
            success=True,
            user_id=1,
            user_name="loader",
            session_id=session_id,
            resource_type="vcf_file",
            resource_id="batch-123",
            details={"variants": 1000},
        )
        row = event.to_db_row()
        assert row["event_type"] == "DATA_WRITE"
        assert row["action"] == "load_vcf"
        assert row["success"] is True
        assert row["user_id"] == 1
        assert row["session_id"] == session_id
        assert row["details"] == {"variants": 1000}


class TestAuditContext:
    def test_default_context(self):
        ctx = get_audit_context()
        assert ctx.user_name == "system"
        assert ctx.user_id is None
        assert ctx.session_id is None

    def test_set_and_get_context(self):
        session_id = uuid4()
        ctx = AuditContext(
            user_id=123,
            user_name="test_user",
            session_id=session_id,
        )
        set_audit_context(ctx)
        retrieved = get_audit_context()
        assert retrieved.user_id == 123
        assert retrieved.user_name == "test_user"
        assert retrieved.session_id == session_id

    def test_audit_context_manager(self):
        with audit_context(user_id=456, user_name="context_user") as ctx:
            assert ctx.user_id == 456
            assert ctx.user_name == "context_user"
            assert ctx.session_id is not None

            inner = get_audit_context()
            assert inner.user_id == 456

    def test_audit_context_restores_previous(self):
        original = AuditContext(user_id=1, user_name="original")
        set_audit_context(original)

        with audit_context(user_id=2, user_name="temporary"):
            pass

        after = get_audit_context()
        assert after.user_id == 1
        assert after.user_name == "original"


class TestAuditLogger:
    @pytest.fixture
    def temp_fallback_path(self):
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = Path(f.name)
        yield path
        if path.exists():
            path.unlink()

    @pytest.mark.asyncio
    async def test_log_event_adds_to_buffer(self, temp_fallback_path):
        logger = AuditLogger(fallback_path=temp_fallback_path)
        event = AuditEvent(
            event_type=AuditEventType.DATA_WRITE,
            action="test",
            success=True,
        )
        await logger.log_event(event)
        assert len(logger._buffer) == 1

    @pytest.mark.asyncio
    async def test_flush_writes_to_fallback_when_no_db(self, temp_fallback_path):
        logger = AuditLogger(fallback_path=temp_fallback_path)
        event = AuditEvent(
            event_type=AuditEventType.DATA_WRITE,
            action="test_fallback",
            success=True,
            details={"test": "data"},
        )
        await logger.log_event(event)
        await logger.flush()

        assert logger._buffer == []
        assert temp_fallback_path.exists()

        with open(temp_fallback_path) as f:
            lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["action"] == "test_fallback"
        assert data["event_type"] == "DATA_WRITE"

    @pytest.mark.asyncio
    async def test_batch_flush_at_threshold(self, temp_fallback_path):
        logger = AuditLogger(batch_size=3, fallback_path=temp_fallback_path)

        for i in range(3):
            await logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_READ,
                    action=f"action_{i}",
                    success=True,
                )
            )

        assert logger._buffer == []
        with open(temp_fallback_path) as f:
            lines = f.readlines()
        assert len(lines) == 3

    @pytest.mark.asyncio
    async def test_audit_operation_context_manager(self, temp_fallback_path):
        logger = AuditLogger(fallback_path=temp_fallback_path)

        async with logger.audit_operation(
            event_type=AuditEventType.DATA_WRITE,
            action="load_file",
            resource_type="vcf",
            resource_id="test.vcf",
        ):
            pass

        await logger.flush()

        with open(temp_fallback_path) as f:
            lines = f.readlines()

        assert len(lines) == 2
        start_event = json.loads(lines[0])
        end_event = json.loads(lines[1])

        assert start_event["details"]["phase"] == "started"
        assert end_event["details"]["phase"] == "completed"
        assert "duration_ms" in end_event["details"]

    @pytest.mark.asyncio
    async def test_audit_operation_logs_failure(self, temp_fallback_path):
        logger = AuditLogger(fallback_path=temp_fallback_path)

        with pytest.raises(ValueError):
            async with logger.audit_operation(
                event_type=AuditEventType.DATA_WRITE,
                action="failing_operation",
            ):
                raise ValueError("test error")

        await logger.flush()

        with open(temp_fallback_path) as f:
            lines = f.readlines()

        assert len(lines) == 2
        error_event = json.loads(lines[1])
        assert error_event["success"] is False
        assert error_event["details"]["phase"] == "failed"
        assert "test error" in error_event["error_message"]

    @pytest.mark.asyncio
    async def test_context_population(self, temp_fallback_path):
        logger = AuditLogger(fallback_path=temp_fallback_path)

        with audit_context(user_id=999, user_name="context_user"):
            await logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_READ,
                    action="context_test",
                    success=True,
                )
            )

        await logger.flush()

        with open(temp_fallback_path) as f:
            data = json.loads(f.readline())

        assert data["user_id"] == 999
        assert data["user_name"] == "context_user"

    @pytest.mark.asyncio
    async def test_write_to_db(self):
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        logger = AuditLogger(pool=mock_pool, batch_size=1)

        await logger.log_event(
            AuditEvent(
                event_type=AuditEventType.DATA_WRITE,
                action="db_test",
                success=True,
            )
        )

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "INSERT INTO hipaa_audit_log" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_db_failure_falls_back_to_file(self, temp_fallback_path):
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("DB error")
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        logger = AuditLogger(pool=mock_pool, batch_size=1, fallback_path=temp_fallback_path)

        await logger.log_event(
            AuditEvent(
                event_type=AuditEventType.DATA_WRITE,
                action="fallback_test",
                success=True,
            )
        )

        assert temp_fallback_path.exists()
        with open(temp_fallback_path) as f:
            data = json.loads(f.readline())
        assert data["action"] == "fallback_test"

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, temp_fallback_path):
        logger = AuditLogger(fallback_path=temp_fallback_path, flush_interval=0.1)

        await logger.start()
        assert logger._running is True
        assert logger._flush_task is not None

        await logger.stop()
        assert logger._running is False


class TestPHISanitization:
    def test_all_phi_patterns_redacted(self):
        details = {f"test_{pattern}_field": "sensitive" for pattern in PHI_PATTERNS}
        event = AuditEvent(
            event_type=AuditEventType.DATA_READ,
            action="test",
            success=True,
            details=details,
        )
        sanitized = event.sanitize_details()
        for key in sanitized:
            assert sanitized[key] == "[REDACTED]"

    def test_non_phi_fields_preserved(self):
        event = AuditEvent(
            event_type=AuditEventType.DATA_READ,
            action="test",
            success=True,
            details={
                "chromosome": "chr1",
                "position": 12345,
                "variant_count": 100,
                "load_batch_id": "abc-123",
            },
        )
        sanitized = event.sanitize_details()
        assert sanitized["chromosome"] == "chr1"
        assert sanitized["position"] == 12345
        assert sanitized["variant_count"] == 100
        assert sanitized["load_batch_id"] == "abc-123"
