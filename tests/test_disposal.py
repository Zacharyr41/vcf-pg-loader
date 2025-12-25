"""Tests for secure data disposal module.

HIPAA Reference: 164.530(j) - Retention and Disposal
"""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vcf_pg_loader.data import (
    DataDisposal,
    DisposalCertificate,
    DisposalResult,
    DisposalStatus,
    DisposalType,
    ExpiredData,
    RetentionPolicy,
    RetentionReport,
    VerificationResult,
    VerificationStatus,
)


class TestDisposalModels:
    def test_disposal_type_enum(self):
        assert DisposalType.BATCH.value == "batch"
        assert DisposalType.SAMPLE.value == "sample"
        assert DisposalType.DATE_RANGE.value == "date_range"

    def test_disposal_status_enum(self):
        assert DisposalStatus.PENDING.value == "pending"
        assert DisposalStatus.AUTHORIZED.value == "authorized"
        assert DisposalStatus.COMPLETED.value == "completed"
        assert DisposalStatus.FAILED.value == "failed"

    def test_verification_status_enum(self):
        assert VerificationStatus.PENDING.value == "pending"
        assert VerificationStatus.PASSED.value == "passed"
        assert VerificationStatus.FAILED.value == "failed"

    def test_disposal_result(self):
        disposal_id = uuid4()
        result = DisposalResult(
            disposal_id=disposal_id,
            disposal_type=DisposalType.BATCH,
            target_identifier="batch-123",
            variants_disposed=1000,
            genotypes_disposed=500,
            mappings_disposed=10,
            status=DisposalStatus.COMPLETED,
        )

        assert result.disposal_id == disposal_id
        assert result.variants_disposed == 1000
        assert result.status == DisposalStatus.COMPLETED

    def test_verification_result(self):
        result = VerificationResult(
            disposal_id=uuid4(),
            passed=True,
            remaining_variants=0,
            expected_deleted=1000,
            verified_at=datetime.now(UTC),
            verified_by=1,
        )

        assert result.passed is True
        assert result.remaining_variants == 0

    def test_expired_data(self):
        expired = ExpiredData(
            load_batch_id=uuid4(),
            vcf_file_path="/data/sample.vcf",
            loaded_at=datetime.now(UTC) - timedelta(days=2200),
            expires_at=datetime.now(UTC) - timedelta(days=10),
            policy_name="hipaa_minimum",
            variant_count=50000,
        )

        assert expired.variant_count == 50000
        assert expired.policy_name == "hipaa_minimum"


class TestDisposalCertificate:
    def test_certificate_creation(self):
        now = datetime.now(UTC)
        cert = DisposalCertificate(
            disposal_id=uuid4(),
            certificate_hash="abc123",
            generated_at=now,
            disposal_type="batch",
            target_identifier="batch-123",
            variants_disposed=1000,
            mappings_disposed=10,
            reason="Data retention expired",
            authorized_by=1,
            authorized_at=now - timedelta(hours=1),
            executed_by=2,
            executed_at=now - timedelta(minutes=30),
            verified_by=3,
            verified_at=now - timedelta(minutes=5),
            verification_result={"passed": True},
        )

        assert cert.certificate_hash == "abc123"
        assert cert.variants_disposed == 1000

    def test_certificate_to_dict(self):
        now = datetime.now(UTC)
        cert = DisposalCertificate(
            disposal_id=uuid4(),
            certificate_hash="abc123",
            generated_at=now,
            disposal_type="batch",
            target_identifier="batch-123",
            variants_disposed=1000,
            mappings_disposed=10,
            reason="Test",
            authorized_by=1,
            authorized_at=now,
            executed_by=2,
            executed_at=now,
            verified_by=3,
            verified_at=now,
            verification_result={"passed": True},
        )

        d = cert.to_dict()

        assert "certificate_of_destruction" in d
        assert d["certificate_of_destruction"]["certificate_hash"] == "abc123"
        assert d["disposal_details"]["type"] == "batch"
        assert d["data_destroyed"]["variants"] == 1000
        assert d["authorization"]["authorized_by_user_id"] == 1
        assert d["verification"]["result"]["passed"] is True

    def test_certificate_to_json(self):
        now = datetime.now(UTC)
        cert = DisposalCertificate(
            disposal_id=uuid4(),
            certificate_hash="abc123",
            generated_at=now,
            disposal_type="batch",
            target_identifier="batch-123",
            variants_disposed=1000,
            mappings_disposed=10,
            reason="Test",
            authorized_by=1,
            authorized_at=now,
            executed_by=2,
            executed_at=now,
            verified_by=3,
            verified_at=now,
            verification_result={},
        )

        json_str = cert.to_json()
        assert "certificate_of_destruction" in json_str
        assert "abc123" in json_str


class TestDataDisposal:
    @pytest.fixture
    def mock_pool(self):
        conn = AsyncMock()
        pool = MagicMock()

        @asynccontextmanager
        async def mock_acquire():
            yield conn

        pool.acquire = mock_acquire
        pool._conn = conn
        return pool

    @pytest.mark.asyncio
    async def test_request_disposal_two_person(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchval.return_value = disposal_id

        disposal = DataDisposal(mock_pool, require_two_person_auth=True)

        result = await disposal.request_disposal(
            disposal_type=DisposalType.BATCH,
            target_identifier=str(uuid4()),
            reason="Test disposal",
            authorized_by=1,
        )

        assert result == disposal_id
        mock_pool._conn.fetchval.assert_called_once()
        call_args = mock_pool._conn.fetchval.call_args[0]
        assert "pending" in str(call_args)

    @pytest.mark.asyncio
    async def test_request_disposal_single_person(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchval.return_value = disposal_id

        disposal = DataDisposal(mock_pool, require_two_person_auth=False)

        result = await disposal.request_disposal(
            disposal_type=DisposalType.SAMPLE,
            target_identifier=str(uuid4()),
            reason="Test disposal",
            authorized_by=1,
        )

        assert result == disposal_id
        call_args = mock_pool._conn.fetchval.call_args[0]
        assert "authorized" in str(call_args)

    @pytest.mark.asyncio
    async def test_authorize_disposal(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = {
            "authorized_by": 1,
            "second_authorizer": None,
            "execution_status": "pending",
        }

        disposal = DataDisposal(mock_pool)

        result = await disposal.authorize_disposal(disposal_id, authorizer_id=2)

        assert result is True
        mock_pool._conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_authorize_same_user_fails(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = {
            "authorized_by": 1,
            "second_authorizer": None,
            "execution_status": "pending",
        }

        disposal = DataDisposal(mock_pool)

        with pytest.raises(ValueError, match="must be different"):
            await disposal.authorize_disposal(disposal_id, authorizer_id=1)

    @pytest.mark.asyncio
    async def test_authorize_not_pending_fails(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = {
            "authorized_by": 1,
            "second_authorizer": None,
            "execution_status": "completed",
        }

        disposal = DataDisposal(mock_pool)

        with pytest.raises(ValueError, match="not pending"):
            await disposal.authorize_disposal(disposal_id, authorizer_id=2)

    @pytest.mark.asyncio
    async def test_authorize_not_found_fails(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = None

        disposal = DataDisposal(mock_pool)

        with pytest.raises(ValueError, match="not found"):
            await disposal.authorize_disposal(disposal_id, authorizer_id=2)

    @pytest.mark.asyncio
    async def test_dispose_batch_two_person(self, mock_pool):
        disposal_id = uuid4()
        batch_id = uuid4()
        mock_pool._conn.fetchval.return_value = disposal_id
        mock_pool._conn.fetchrow.return_value = {
            "disposal_id": disposal_id,
            "disposal_type": "batch",
            "target_identifier": str(batch_id),
            "variants_disposed": 0,
            "genotypes_disposed": 0,
            "mappings_disposed": 0,
            "execution_status": "pending",
        }

        disposal = DataDisposal(mock_pool, require_two_person_auth=True)

        result = await disposal.dispose_batch(
            batch_id=batch_id,
            reason="Test",
            authorized_by=1,
        )

        assert result.disposal_id == disposal_id
        assert result.status == DisposalStatus.PENDING
        assert result.disposal_type == DisposalType.BATCH

    @pytest.mark.asyncio
    async def test_dispose_sample_single_person(self, mock_pool):
        disposal_id = uuid4()
        sample_id = uuid4()

        mock_pool._conn.fetchval.side_effect = [
            disposal_id,
            {
                "variants_deleted": 100,
                "sample_id": str(sample_id),
                "completed_at": datetime.now(UTC),
            },
        ]
        mock_pool._conn.fetchrow.side_effect = [
            {
                "disposal_id": disposal_id,
                "disposal_type": "sample",
                "target_identifier": str(sample_id),
                "variants_disposed": 100,
                "genotypes_disposed": 0,
                "mappings_disposed": 0,
                "execution_status": "authorized",
            },
            {
                "disposal_id": disposal_id,
                "disposal_type": "sample",
                "target_identifier": str(sample_id),
                "variants_disposed": 100,
                "genotypes_disposed": 0,
                "mappings_disposed": 5,
                "execution_status": "completed",
                "executed_at": datetime.now(UTC),
            },
        ]

        disposal = DataDisposal(mock_pool, require_two_person_auth=False)

        result = await disposal.dispose_sample(
            sample_anonymous_id=sample_id,
            reason="Patient withdrawal",
            authorized_by=1,
        )

        assert result.disposal_id == disposal_id
        assert result.disposal_type == DisposalType.SAMPLE

    @pytest.mark.asyncio
    async def test_execute_disposal_not_authorized_fails(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = {
            "disposal_id": disposal_id,
            "disposal_type": "batch",
            "target_identifier": str(uuid4()),
            "execution_status": "pending",
        }

        disposal = DataDisposal(mock_pool)

        with pytest.raises(ValueError, match="not authorized"):
            await disposal.execute_disposal(disposal_id, executor_id=1)

    @pytest.mark.asyncio
    async def test_execute_disposal_not_found_fails(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = None

        disposal = DataDisposal(mock_pool)

        with pytest.raises(ValueError, match="not found"):
            await disposal.execute_disposal(disposal_id, executor_id=1)

    @pytest.mark.asyncio
    async def test_verify_disposal(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchval.return_value = {
            "verification_passed": True,
            "remaining_variants": 0,
            "expected_deleted": 1000,
            "verified_at": datetime.now(UTC),
        }
        mock_pool._conn.fetchrow.return_value = {
            "verified_at": datetime.now(UTC),
        }

        disposal = DataDisposal(mock_pool)

        result = await disposal.verify_disposal(disposal_id, verifier_id=1)

        assert result.passed is True
        assert result.remaining_variants == 0

    @pytest.mark.asyncio
    async def test_generate_certificate_not_verified_fails(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = {
            "disposal_id": disposal_id,
            "verification_status": "pending",
        }

        disposal = DataDisposal(mock_pool)

        with pytest.raises(ValueError, match="verification status"):
            await disposal.generate_disposal_certificate(disposal_id)

    @pytest.mark.asyncio
    async def test_list_disposals(self, mock_pool):
        records = [
            {
                "disposal_id": uuid4(),
                "disposal_type": "batch",
                "target_identifier": "target-1",
                "execution_status": "completed",
                "reason": "Test 1",
            },
            {
                "disposal_id": uuid4(),
                "disposal_type": "sample",
                "target_identifier": "target-2",
                "execution_status": "pending",
                "reason": "Test 2",
            },
        ]
        mock_pool._conn.fetch.return_value = records

        disposal = DataDisposal(mock_pool)

        result = await disposal.list_disposals(limit=10)

        assert len(result) == 2
        assert result[0]["disposal_type"] == "batch"
        assert result[1]["execution_status"] == "pending"

    @pytest.mark.asyncio
    async def test_list_disposals_with_filters(self, mock_pool):
        mock_pool._conn.fetch.return_value = []

        disposal = DataDisposal(mock_pool)

        start = datetime.now(UTC) - timedelta(days=30)
        end = datetime.now(UTC)

        await disposal.list_disposals(
            start_date=start,
            end_date=end,
            status=DisposalStatus.COMPLETED,
        )

        call_args = mock_pool._conn.fetch.call_args[0]
        query = call_args[0]
        assert "created_at >=" in query
        assert "created_at <=" in query
        assert "execution_status =" in query

    @pytest.mark.asyncio
    async def test_cancel_disposal(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.execute.return_value = "UPDATE 1"

        disposal = DataDisposal(mock_pool)

        result = await disposal.cancel_disposal(
            disposal_id=disposal_id,
            cancelled_by=1,
            reason="Changed mind",
        )

        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_disposal_already_executed(self, mock_pool):
        disposal_id = uuid4()
        mock_pool._conn.execute.return_value = "UPDATE 0"

        disposal = DataDisposal(mock_pool)

        result = await disposal.cancel_disposal(
            disposal_id=disposal_id,
            cancelled_by=1,
            reason="Changed mind",
        )

        assert result is False


class TestRetentionPolicy:
    @pytest.fixture
    def mock_pool(self):
        conn = AsyncMock()
        pool = MagicMock()

        @asynccontextmanager
        async def mock_acquire():
            yield conn

        pool.acquire = mock_acquire
        pool._conn = conn
        return pool

    @pytest.mark.asyncio
    async def test_check_expired_data(self, mock_pool):
        expired_batch = uuid4()
        mock_pool._conn.fetch.return_value = [
            {
                "load_batch_id": expired_batch,
                "vcf_file_path": "/data/old.vcf",
                "load_completed_at": datetime.now(UTC) - timedelta(days=2200),
                "policy_name": "hipaa_minimum",
                "expires_at": datetime.now(UTC) - timedelta(days=10),
                "variant_count": 50000,
            }
        ]

        policy = RetentionPolicy(mock_pool)

        expired = await policy.check_expired_data()

        assert len(expired) == 1
        assert expired[0].load_batch_id == expired_batch
        assert expired[0].variant_count == 50000

    @pytest.mark.asyncio
    async def test_check_expiring_soon(self, mock_pool):
        expiring_batch = uuid4()
        mock_pool._conn.fetch.return_value = [
            {
                "load_batch_id": expiring_batch,
                "vcf_file_path": "/data/expiring.vcf",
                "loaded_at": datetime.now(UTC) - timedelta(days=2100),
                "policy_name": "hipaa_minimum",
                "expires_at": datetime.now(UTC) + timedelta(days=30),
                "variant_count": 25000,
            }
        ]

        policy = RetentionPolicy(mock_pool)

        expiring = await policy.check_expiring_soon(days_ahead=90)

        assert len(expiring) == 1
        assert expiring[0].variant_count == 25000

    @pytest.mark.asyncio
    async def test_generate_expiration_report(self, mock_pool):
        expired_batch = uuid4()
        expiring_batch = uuid4()

        mock_pool._conn.fetch.side_effect = [
            [
                {
                    "load_batch_id": expired_batch,
                    "vcf_file_path": "/data/expired.vcf",
                    "load_completed_at": datetime.now(UTC) - timedelta(days=2200),
                    "policy_name": "hipaa_minimum",
                    "expires_at": datetime.now(UTC) - timedelta(days=10),
                    "variant_count": 10000,
                }
            ],
            [
                {
                    "load_batch_id": expiring_batch,
                    "vcf_file_path": "/data/expiring.vcf",
                    "loaded_at": datetime.now(UTC) - timedelta(days=2100),
                    "policy_name": "hipaa_minimum",
                    "expires_at": datetime.now(UTC) + timedelta(days=30),
                    "variant_count": 20000,
                }
            ],
        ]

        policy = RetentionPolicy(mock_pool)

        report = await policy.generate_expiration_report()

        assert isinstance(report, RetentionReport)
        assert len(report.expired_batches) == 1
        assert len(report.expiring_soon) == 1
        assert report.total_expired_variants == 10000
        assert report.total_expiring_variants == 20000

    @pytest.mark.asyncio
    async def test_get_policies(self, mock_pool):
        mock_pool._conn.fetch.return_value = [
            {
                "policy_id": 1,
                "policy_name": "hipaa_minimum",
                "retention_days": 2190,
                "data_type": "all",
                "auto_dispose": False,
            },
            {
                "policy_id": 2,
                "policy_name": "audit_logs",
                "retention_days": 2555,
                "data_type": "audit_logs",
                "auto_dispose": False,
            },
        ]

        policy = RetentionPolicy(mock_pool)

        policies = await policy.get_policies()

        assert len(policies) == 2
        assert policies[0]["policy_name"] == "hipaa_minimum"
        assert policies[0]["retention_days"] == 2190

    @pytest.mark.asyncio
    async def test_create_policy(self, mock_pool):
        mock_pool._conn.fetchval.return_value = 3

        policy = RetentionPolicy(mock_pool)

        policy_id = await policy.create_policy(
            policy_name="custom_policy",
            retention_days=365,
            data_type="variants",
            description="Custom 1-year policy",
            created_by=1,
        )

        assert policy_id == 3
        mock_pool._conn.fetchval.assert_called_once()


class TestDataDisposalWithAuditLogger:
    @pytest.fixture
    def mock_pool(self):
        conn = AsyncMock()
        pool = MagicMock()

        @asynccontextmanager
        async def mock_acquire():
            yield conn

        pool.acquire = mock_acquire
        pool._conn = conn
        return pool

    @pytest.fixture
    def mock_audit_logger(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_request_disposal_logs_audit(self, mock_pool, mock_audit_logger):
        disposal_id = uuid4()
        mock_pool._conn.fetchval.return_value = disposal_id

        disposal = DataDisposal(
            mock_pool,
            audit_logger=mock_audit_logger,
            require_two_person_auth=False,
        )

        await disposal.request_disposal(
            disposal_type=DisposalType.BATCH,
            target_identifier=str(uuid4()),
            reason="Test",
            authorized_by=1,
        )

        mock_audit_logger.log_event.assert_called()
        event = mock_audit_logger.log_event.call_args[0][0]
        assert event.action == "disposal_requested"

    @pytest.mark.asyncio
    async def test_authorize_disposal_logs_audit(self, mock_pool, mock_audit_logger):
        disposal_id = uuid4()
        mock_pool._conn.fetchrow.return_value = {
            "authorized_by": 1,
            "second_authorizer": None,
            "execution_status": "pending",
        }

        disposal = DataDisposal(mock_pool, audit_logger=mock_audit_logger)

        await disposal.authorize_disposal(disposal_id, authorizer_id=2)

        mock_audit_logger.log_event.assert_called()
        event = mock_audit_logger.log_event.call_args[0][0]
        assert event.action == "disposal_authorized"

    @pytest.mark.asyncio
    async def test_cancel_disposal_logs_audit(self, mock_pool, mock_audit_logger):
        disposal_id = uuid4()
        mock_pool._conn.execute.return_value = "UPDATE 1"

        disposal = DataDisposal(mock_pool, audit_logger=mock_audit_logger)

        await disposal.cancel_disposal(
            disposal_id=disposal_id,
            cancelled_by=1,
            reason="Test cancellation",
        )

        mock_audit_logger.log_event.assert_called()
        event = mock_audit_logger.log_event.call_args[0][0]
        assert event.action == "disposal_cancelled"
