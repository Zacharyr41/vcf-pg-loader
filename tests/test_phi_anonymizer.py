"""Tests for PHI anonymization module.

HIPAA Reference: 164.514(b) - De-identification Standard
"""

import base64
import secrets
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vcf_pg_loader.phi.anonymizer import (
    RE_IDENTIFICATION_WARNING,
    SampleAnonymizer,
    log_re_identification_warning,
)
from vcf_pg_loader.phi.encryption import PHIEncryptionError, PHIEncryptor


class TestPHIEncryptor:
    def test_generate_key(self):
        key = PHIEncryptor.generate_key()
        assert len(key) == 32
        assert isinstance(key, bytes)

    def test_key_to_base64(self):
        key = PHIEncryptor.generate_key()
        encoded = PHIEncryptor.key_to_base64(key)
        assert isinstance(encoded, str)
        decoded = base64.b64decode(encoded)
        assert decoded == key

    def test_key_from_base64(self):
        key = secrets.token_bytes(32)
        encoded = base64.b64encode(key).decode("ascii")
        decoded = PHIEncryptor.key_from_base64(encoded)
        assert decoded == key

    def test_encryptor_without_key(self):
        encryptor = PHIEncryptor()
        assert not encryptor.is_available

    def test_encryptor_with_invalid_key_length(self):
        with pytest.raises(PHIEncryptionError, match="must be 32 bytes"):
            PHIEncryptor(key=b"short")

    def test_encryptor_from_env(self, monkeypatch):
        key = PHIEncryptor.generate_key()
        encoded = PHIEncryptor.key_to_base64(key)
        monkeypatch.setenv("VCF_PG_LOADER_PHI_KEY", encoded)

        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401

            encryptor = PHIEncryptor()
            assert encryptor.is_available
        except ImportError:
            encryptor = PHIEncryptor()
            assert not encryptor.is_available

    def test_encrypt_without_cipher(self):
        key = PHIEncryptor.generate_key()
        encryptor = PHIEncryptor(key=key)
        encryptor._aesgcm = None

        with pytest.raises(PHIEncryptionError, match="not available"):
            encryptor.encrypt("test")

    def test_decrypt_without_cipher(self):
        key = PHIEncryptor.generate_key()
        encryptor = PHIEncryptor(key=key)
        encryptor._aesgcm = None

        with pytest.raises(PHIEncryptionError, match="not available"):
            encryptor.decrypt(b"ciphertext", b"iv")


class TestPHIEncryptorWithCrypto:
    @pytest.fixture
    def encryptor(self):
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401

            key = PHIEncryptor.generate_key()
            return PHIEncryptor(key=key)
        except ImportError:
            pytest.skip("cryptography library not installed")

    def test_encrypt_decrypt_roundtrip(self, encryptor):
        plaintext = "JohnDoe_2024"
        ciphertext, iv = encryptor.encrypt(plaintext)

        assert isinstance(ciphertext, bytes)
        assert isinstance(iv, bytes)
        assert len(iv) == 12

        decrypted = encryptor.decrypt(ciphertext, iv)
        assert decrypted == plaintext

    def test_encrypt_different_each_time(self, encryptor):
        plaintext = "sample123"
        ct1, iv1 = encryptor.encrypt(plaintext)
        ct2, iv2 = encryptor.encrypt(plaintext)

        assert iv1 != iv2
        assert ct1 != ct2

    def test_decrypt_with_wrong_iv(self, encryptor):
        plaintext = "test"
        ciphertext, _ = encryptor.encrypt(plaintext)
        wrong_iv = secrets.token_bytes(12)

        with pytest.raises(PHIEncryptionError, match="Decryption failed"):
            encryptor.decrypt(ciphertext, wrong_iv)


class TestSampleAnonymizer:
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
    async def test_anonymize_sample_id(self, mock_pool):
        expected_uuid = uuid4()
        conn = mock_pool._conn
        conn.fetchval.return_value = expected_uuid

        anonymizer = SampleAnonymizer(pool=mock_pool)
        result = await anonymizer.anonymize_sample_id(
            original_id="JohnDoe_2024",
            source_file="/path/to/test.vcf",
            load_batch_id=uuid4(),
        )

        assert result == expected_uuid
        conn.fetchval.assert_called_once()

    @pytest.mark.asyncio
    async def test_anonymize_uses_cache(self, mock_pool):
        expected_uuid = uuid4()
        conn = mock_pool._conn
        conn.fetchval.return_value = expected_uuid

        anonymizer = SampleAnonymizer(pool=mock_pool)
        batch_id = uuid4()

        result1 = await anonymizer.anonymize_sample_id("sample1", "/test.vcf", batch_id)
        result2 = await anonymizer.anonymize_sample_id("sample1", "/test.vcf", batch_id)

        assert result1 == result2
        assert conn.fetchval.call_count == 1

    @pytest.mark.asyncio
    async def test_bulk_anonymize(self, mock_pool):
        uuid1, uuid2 = uuid4(), uuid4()
        conn = mock_pool._conn
        conn.fetch.return_value = []
        conn.fetchval.side_effect = [uuid1, uuid2]

        anonymizer = SampleAnonymizer(pool=mock_pool)
        result = await anonymizer.bulk_anonymize(
            sample_ids=["sample1", "sample2"],
            source_file="/test.vcf",
            load_batch_id=uuid4(),
        )

        assert len(result) == 2
        assert result["sample1"] == uuid1
        assert result["sample2"] == uuid2

    @pytest.mark.asyncio
    async def test_bulk_anonymize_existing(self, mock_pool):
        existing_uuid = uuid4()
        conn = mock_pool._conn
        conn.fetch.return_value = [{"original_id": "sample1", "anonymous_id": existing_uuid}]

        anonymizer = SampleAnonymizer(pool=mock_pool)
        result = await anonymizer.bulk_anonymize(
            sample_ids=["sample1"],
            source_file="/test.vcf",
            load_batch_id=uuid4(),
        )

        assert result["sample1"] == existing_uuid
        assert conn.fetchval.call_count == 0

    @pytest.mark.asyncio
    async def test_reverse_lookup(self, mock_pool):
        conn = mock_pool._conn
        conn.fetchval.return_value = "JohnDoe_2024"

        anonymizer = SampleAnonymizer(pool=mock_pool)
        result = await anonymizer.reverse_lookup(
            anonymous_id=uuid4(),
            requester_id=1,
            reason="testing",
        )

        assert result == "JohnDoe_2024"
        conn.fetchval.assert_called_once()

    @pytest.mark.asyncio
    async def test_reverse_lookup_not_found(self, mock_pool):
        conn = mock_pool._conn
        conn.fetchval.return_value = None

        anonymizer = SampleAnonymizer(pool=mock_pool)
        result = await anonymizer.reverse_lookup(
            anonymous_id=uuid4(),
            requester_id=1,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_get_anonymous_id(self, mock_pool):
        expected_uuid = uuid4()
        conn = mock_pool._conn
        conn.fetchval.return_value = expected_uuid

        anonymizer = SampleAnonymizer(pool=mock_pool)
        result = await anonymizer.get_anonymous_id("sample1", "/test.vcf")

        assert result == expected_uuid

    def test_clear_cache(self, mock_pool):
        anonymizer = SampleAnonymizer(pool=mock_pool)
        anonymizer._cache[("sample1", "/test.vcf")] = uuid4()

        assert len(anonymizer._cache) == 1
        anonymizer.clear_cache()
        assert len(anonymizer._cache) == 0


class TestReIdentificationWarning:
    def test_warning_content(self):
        assert "HIPAA" in RE_IDENTIFICATION_WARNING
        assert "Expert Determination" in RE_IDENTIFICATION_WARNING
        assert "Data Use Agreements" in RE_IDENTIFICATION_WARNING

    def test_log_warning(self, caplog):
        with caplog.at_level("WARNING"):
            log_re_identification_warning()
        assert "re-identifiable" in caplog.text
