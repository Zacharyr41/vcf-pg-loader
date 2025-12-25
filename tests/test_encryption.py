"""Encryption at Rest tests.

HIPAA Citation: 45 CFR 164.312(a)(2)(iv) - Encryption and Decryption
"Implement a mechanism to encrypt and decrypt electronic protected
health information."

NIST SP 800-111 Requirements:
- Algorithm: AES-256 (minimum AES-128)
- Mode: AES-GCM or other FIPS-approved modes
- Key storage: Separate from encrypted data

HHS Breach Safe Harbor (45 CFR 164.402):
Properly encrypted PHI is "unusable, unreadable, or indecipherable"
and exempt from breach notification requirements.
"""

import base64
import os
import secrets
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from cryptography.exceptions import InvalidTag

from vcf_pg_loader.security.encryption import (
    AES_KEY_SIZE_BYTES,
    EncryptionKey,
    EncryptionManager,
    KeyPurpose,
)


class TestKeyPurpose:
    """45 CFR 164.312(a)(2)(iv): Key purposes for different data types."""

    def test_all_purposes_defined(self):
        expected = {
            "data_encryption",
            "phi_encryption",
            "backup_encryption",
            "transport_encryption",
        }
        actual = {p.value for p in KeyPurpose}
        assert actual == expected


class TestEncryptionKey:
    """45 CFR 164.312(a)(2)(iv): Encryption key metadata."""

    def test_key_from_db_row(self):
        now = datetime.now(UTC)
        row = {
            "key_id": uuid4(),
            "key_name": "phi_key_v1",
            "key_version": 1,
            "algorithm": "AES-256-GCM",
            "purpose": "phi_encryption",
            "is_active": True,
            "created_at": now,
            "expires_at": now + timedelta(days=365),
            "retired_at": None,
            "use_count": 100,
        }

        key = EncryptionKey.from_db_row(row)

        assert key.key_name == "phi_key_v1"
        assert key.algorithm == "AES-256-GCM"
        assert key.purpose == KeyPurpose.PHI_ENCRYPTION

    def test_key_is_valid_active(self):
        key = EncryptionKey(
            key_id=uuid4(),
            key_name="test",
            key_version=1,
            algorithm="AES-256-GCM",
            purpose=KeyPurpose.PHI_ENCRYPTION,
            is_active=True,
            created_at=datetime.now(UTC),
        )

        assert key.is_valid() is True

    def test_key_is_valid_false_when_retired(self):
        key = EncryptionKey(
            key_id=uuid4(),
            key_name="test",
            key_version=1,
            algorithm="AES-256-GCM",
            purpose=KeyPurpose.PHI_ENCRYPTION,
            is_active=True,
            created_at=datetime.now(UTC),
            retired_at=datetime.now(UTC),
        )

        assert key.is_valid() is False

    def test_key_is_valid_false_when_expired(self):
        key = EncryptionKey(
            key_id=uuid4(),
            key_name="test",
            key_version=1,
            algorithm="AES-256-GCM",
            purpose=KeyPurpose.PHI_ENCRYPTION,
            is_active=True,
            created_at=datetime.now(UTC) - timedelta(days=400),
            expires_at=datetime.now(UTC) - timedelta(days=30),
        )

        assert key.is_valid() is False

    def test_key_is_valid_false_when_inactive(self):
        key = EncryptionKey(
            key_id=uuid4(),
            key_name="test",
            key_version=1,
            algorithm="AES-256-GCM",
            purpose=KeyPurpose.PHI_ENCRYPTION,
            is_active=False,
            created_at=datetime.now(UTC),
        )

        assert key.is_valid() is False


class TestEncryptionManager:
    """45 CFR 164.312(a)(2)(iv): Encryption mechanism implementation."""

    @pytest.fixture
    def master_key(self):
        return secrets.token_bytes(AES_KEY_SIZE_BYTES)

    @pytest.fixture
    def manager(self, master_key):
        return EncryptionManager(master_key=master_key)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    def test_requires_master_key(self):
        """NIST SP 800-111: Master key required for key encryption."""
        with pytest.raises(ValueError, match="Master key required"):
            EncryptionManager(master_key=None)

    def test_requires_correct_key_length(self):
        """NIST SP 800-111: AES-256 requires 32-byte key."""
        with pytest.raises(ValueError, match="32 bytes"):
            EncryptionManager(master_key=b"too_short")

    def test_master_key_from_environment(self, master_key):
        """Master key can be provided via environment variable."""
        key_b64 = base64.b64encode(master_key).decode("ascii")
        os.environ["VCF_PG_LOADER_MASTER_KEY"] = key_b64

        try:
            manager = EncryptionManager()
            assert manager is not None
        finally:
            del os.environ["VCF_PG_LOADER_MASTER_KEY"]

    def test_encrypt_decrypt_roundtrip(self, manager, master_key):
        """45 CFR 164.312(a)(2)(iv): Encrypt and decrypt ePHI."""
        key = secrets.token_bytes(AES_KEY_SIZE_BYTES)
        plaintext = b"Patient SSN: 123-45-6789"

        ciphertext = manager.encrypt(key, plaintext)
        decrypted = manager.decrypt(key, ciphertext)

        assert decrypted == plaintext
        assert ciphertext != plaintext

    def test_encrypt_produces_different_output(self, manager, master_key):
        """NIST SP 800-111: Unique nonce for each encryption."""
        key = secrets.token_bytes(AES_KEY_SIZE_BYTES)
        plaintext = b"Test data"

        ciphertext1 = manager.encrypt(key, plaintext)
        ciphertext2 = manager.encrypt(key, plaintext)

        assert ciphertext1 != ciphertext2

    def test_decrypt_detects_tampering(self, manager, master_key):
        """NIST SP 800-111: GCM provides integrity verification."""
        key = secrets.token_bytes(AES_KEY_SIZE_BYTES)
        plaintext = b"Protected data"

        ciphertext = manager.encrypt(key, plaintext)
        tampered = ciphertext[:20] + b"\x00" + ciphertext[21:]

        with pytest.raises(InvalidTag):
            manager.decrypt(key, tampered)

    def test_encrypt_string(self, manager, master_key):
        """45 CFR 164.312(a)(2)(iv): Encrypt string data."""
        key = secrets.token_bytes(AES_KEY_SIZE_BYTES)
        plaintext = "Patient Name: John Doe"

        encrypted = manager.encrypt_string(key, plaintext)
        decrypted = manager.decrypt_string(key, encrypted)

        assert decrypted == plaintext
        assert encrypted != plaintext

    async def test_create_key(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(iv): Create encryption key for PHI protection."""
        key_id = uuid4()
        now = datetime.now(UTC)

        mock_conn.fetchval.return_value = key_id
        mock_conn.fetchrow.return_value = {
            "key_id": key_id,
            "key_name": "phi_key",
            "key_version": 1,
            "algorithm": "AES-256-GCM",
            "purpose": "phi_encryption",
            "is_active": True,
            "created_at": now,
            "expires_at": now + timedelta(days=365),
            "retired_at": None,
            "use_count": 0,
        }

        key = await manager.create_key(
            mock_conn,
            key_name="phi_key",
            purpose=KeyPurpose.PHI_ENCRYPTION,
            expires_days=365,
        )

        assert key.key_name == "phi_key"
        assert key.purpose == KeyPurpose.PHI_ENCRYPTION
        mock_conn.fetchval.assert_called_once()

    async def test_get_key(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(iv): Retrieve encryption key for decryption."""
        key_id = uuid4()
        now = datetime.now(UTC)

        raw_key = secrets.token_bytes(AES_KEY_SIZE_BYTES)
        encrypted_key, nonce, tag = manager._encrypt_key(raw_key)

        mock_conn.fetchrow.side_effect = [
            {
                "key_id": key_id,
                "key_name": "phi_key",
                "key_version": 1,
                "encrypted_key_material": encrypted_key,
                "key_nonce": nonce,
                "key_tag": tag,
                "algorithm": "AES-256-GCM",
            },
            {
                "key_id": key_id,
                "key_name": "phi_key",
                "key_version": 1,
                "algorithm": "AES-256-GCM",
                "purpose": "phi_encryption",
                "is_active": True,
                "created_at": now,
                "expires_at": None,
                "retired_at": None,
                "use_count": 1,
            },
        ]

        result = await manager.get_key(mock_conn, KeyPurpose.PHI_ENCRYPTION)

        assert result is not None
        decrypted_key, key_meta = result
        assert decrypted_key == raw_key
        assert key_meta.key_name == "phi_key"

    async def test_rotate_key(self, manager, mock_conn):
        """NIST SP 800-57: Key rotation capability."""
        new_key_id = uuid4()
        now = datetime.now(UTC)

        mock_conn.fetchval.return_value = new_key_id
        mock_conn.fetchrow.return_value = {
            "key_id": new_key_id,
            "key_name": "phi_key",
            "key_version": 2,
            "algorithm": "AES-256-GCM",
            "purpose": "phi_encryption",
            "is_active": True,
            "created_at": now,
            "expires_at": None,
            "retired_at": None,
            "use_count": 0,
        }

        new_key = await manager.rotate_key(
            mock_conn,
            key_name="phi_key",
            rotated_by=1,
            reason="Scheduled rotation",
        )

        assert new_key.key_version == 2

    async def test_list_keys(self, manager, mock_conn):
        """45 CFR 164.312(a)(2)(iv): List encryption keys."""
        now = datetime.now(UTC)
        mock_conn.fetch.return_value = [
            {
                "key_id": uuid4(),
                "key_name": "key1",
                "key_version": 1,
                "algorithm": "AES-256-GCM",
                "purpose": "phi_encryption",
                "is_active": True,
                "created_at": now,
                "expires_at": None,
                "retired_at": None,
                "use_count": 10,
            },
            {
                "key_id": uuid4(),
                "key_name": "key2",
                "key_version": 1,
                "algorithm": "AES-256-GCM",
                "purpose": "data_encryption",
                "is_active": True,
                "created_at": now,
                "expires_at": None,
                "retired_at": None,
                "use_count": 5,
            },
        ]

        keys = await manager.list_keys(mock_conn)

        assert len(keys) == 2


class TestEncryptionWithAudit:
    """45 CFR 164.312(b): Audit controls for encryption operations."""

    @pytest.fixture
    def master_key(self):
        return secrets.token_bytes(AES_KEY_SIZE_BYTES)

    @pytest.fixture
    def audit_logger(self):
        return AsyncMock()

    @pytest.fixture
    def manager(self, master_key, audit_logger):
        return EncryptionManager(master_key=master_key, audit_logger=audit_logger)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    async def test_create_key_logs_audit(self, manager, mock_conn, audit_logger):
        """45 CFR 164.312(b): Key creation must be audited."""
        key_id = uuid4()
        now = datetime.now(UTC)

        mock_conn.fetchval.return_value = key_id
        mock_conn.fetchrow.return_value = {
            "key_id": key_id,
            "key_name": "phi_key",
            "key_version": 1,
            "algorithm": "AES-256-GCM",
            "purpose": "phi_encryption",
            "is_active": True,
            "created_at": now,
            "expires_at": None,
            "retired_at": None,
            "use_count": 0,
        }

        await manager.create_key(
            mock_conn,
            key_name="phi_key",
            purpose=KeyPurpose.PHI_ENCRYPTION,
            created_by=1,
        )

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "encryption_key_created"
