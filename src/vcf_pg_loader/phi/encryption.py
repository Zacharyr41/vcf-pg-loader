"""PHI encryption utilities using AES-256-GCM.

HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption
"""

import base64
import logging
import os
import secrets
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

ENV_KEY_NAME = "VCF_PG_LOADER_PHI_KEY"
ENV_KEY_FILE = "VCF_PG_LOADER_PHI_KEY_FILE"

logger = logging.getLogger(__name__)


class PHIEncryptionError(Exception):
    """Raised when PHI encryption/decryption fails."""


class KeySource(Enum):
    """Source of encryption key."""

    ENVIRONMENT = "environment"
    FILE = "file"
    AWS_KMS = "aws_kms"
    GCP_KMS = "gcp_kms"
    AZURE_KEYVAULT = "azure_keyvault"


@dataclass
class EncryptionStatus:
    """Status of encryption configuration."""

    enabled: bool
    key_source: KeySource | None
    key_id: str | None
    algorithm: str = "AES-256-GCM"
    library_version: str | None = None


class KeyManager:
    """Manages encryption keys from various sources.

    Supports:
    - Environment variables (VCF_PG_LOADER_PHI_KEY)
    - Key files (VCF_PG_LOADER_PHI_KEY_FILE)
    - AWS KMS (placeholder for enterprise)
    - GCP KMS (placeholder for enterprise)
    - Azure Key Vault (placeholder for enterprise)
    """

    def __init__(
        self,
        key_source: KeySource = KeySource.ENVIRONMENT,
        key_id: str | None = None,
    ):
        self._key_source = key_source
        self._key_id = key_id
        self._cached_key: bytes | None = None

    @property
    def key_source(self) -> KeySource:
        return self._key_source

    @property
    def key_id(self) -> str | None:
        return self._key_id

    def get_key(self) -> bytes:
        """Get encryption key from configured source.

        Returns:
            32-byte encryption key

        Raises:
            PHIEncryptionError: If key cannot be retrieved
        """
        if self._cached_key is not None:
            return self._cached_key

        if self._key_source == KeySource.ENVIRONMENT:
            key = self._get_key_from_env()
        elif self._key_source == KeySource.FILE:
            key = self._get_key_from_file()
        elif self._key_source == KeySource.AWS_KMS:
            key = self._get_key_from_aws_kms()
        elif self._key_source == KeySource.GCP_KMS:
            key = self._get_key_from_gcp_kms()
        elif self._key_source == KeySource.AZURE_KEYVAULT:
            key = self._get_key_from_azure_keyvault()
        else:
            raise PHIEncryptionError(f"Unknown key source: {self._key_source}")

        if len(key) != 32:
            raise PHIEncryptionError(f"Encryption key must be 32 bytes, got {len(key)}")

        self._cached_key = key
        return key

    def _get_key_from_env(self) -> bytes:
        """Get key from environment variable."""
        env_key = os.environ.get(ENV_KEY_NAME)
        if not env_key:
            raise PHIEncryptionError(
                f"Encryption key not found. Set {ENV_KEY_NAME} environment variable "
                "with base64-encoded 256-bit key."
            )
        try:
            return base64.b64decode(env_key)
        except Exception as e:
            raise PHIEncryptionError(f"Invalid base64 key in {ENV_KEY_NAME}: {e}") from e

    def _get_key_from_file(self) -> bytes:
        """Get key from file specified by environment variable."""
        key_file = self._key_id or os.environ.get(ENV_KEY_FILE)
        if not key_file:
            raise PHIEncryptionError(
                f"Key file path not specified. Set {ENV_KEY_FILE} environment variable "
                "or provide key_id parameter."
            )

        path = Path(key_file)
        if not path.exists():
            raise PHIEncryptionError(f"Key file not found: {key_file}")

        stat = path.stat()
        if stat.st_mode & 0o077:
            logger.warning(
                "Key file %s has insecure permissions. Should be 0600 or 0400.", key_file
            )

        content = path.read_text().strip()
        try:
            return base64.b64decode(content)
        except Exception as e:
            raise PHIEncryptionError(f"Invalid base64 key in {key_file}: {e}") from e

    def _get_key_from_aws_kms(self) -> bytes:
        """Get key from AWS KMS (placeholder)."""
        raise PHIEncryptionError(
            "AWS KMS integration requires enterprise configuration. "
            "See docs/deployment/aws-rds-encryption.md for setup instructions."
        )

    def _get_key_from_gcp_kms(self) -> bytes:
        """Get key from GCP KMS (placeholder)."""
        raise PHIEncryptionError(
            "GCP KMS integration requires enterprise configuration. "
            "See docs/deployment/gcp-cloudsql-encryption.md for setup instructions."
        )

    def _get_key_from_azure_keyvault(self) -> bytes:
        """Get key from Azure Key Vault (placeholder)."""
        raise PHIEncryptionError(
            "Azure Key Vault integration requires enterprise configuration. "
            "See docs/deployment/azure-encryption.md for setup instructions."
        )

    def clear_cache(self) -> None:
        """Clear cached key from memory."""
        self._cached_key = None

    @staticmethod
    def generate_key() -> bytes:
        """Generate a new 256-bit encryption key."""
        return secrets.token_bytes(32)

    @staticmethod
    def key_to_base64(key: bytes) -> str:
        """Encode key as base64 for storage."""
        return base64.b64encode(key).decode("ascii")

    @staticmethod
    def key_from_base64(encoded: str) -> bytes:
        """Decode key from base64."""
        return base64.b64decode(encoded)


class PHIEncryptor:
    """Handles AES-256-GCM encryption of PHI data.

    Uses cryptography library for FIPS-compliant encryption.
    Key should be 32 bytes (256 bits) and stored securely.
    """

    IV_SIZE = 12
    TAG_SIZE = 16

    def __init__(
        self,
        key: bytes | None = None,
        key_manager: KeyManager | None = None,
    ):
        """Initialize encryptor with key or key manager.

        Args:
            key: 32-byte encryption key (legacy, direct key)
            key_manager: KeyManager instance for key retrieval
        """
        self._key: bytes | None = None
        self._key_manager = key_manager
        self._aesgcm: AESGCM | None = None

        if key is not None:
            if len(key) != 32:
                raise PHIEncryptionError(f"Encryption key must be 32 bytes, got {len(key)}")
            self._key = key
            self._init_cipher()
        elif key_manager is not None:
            self._key = key_manager.get_key()
            self._init_cipher()
        else:
            env_key = os.environ.get(ENV_KEY_NAME)
            if env_key:
                try:
                    self._key = base64.b64decode(env_key)
                    if len(self._key) != 32:
                        raise PHIEncryptionError(
                            f"Encryption key must be 32 bytes, got {len(self._key)}"
                        )
                    self._init_cipher()
                except Exception as e:
                    if isinstance(e, PHIEncryptionError):
                        raise
                    raise PHIEncryptionError(f"Invalid base64 key in {ENV_KEY_NAME}: {e}") from e

    def _init_cipher(self) -> None:
        """Initialize AES-GCM cipher."""
        if self._key is not None:
            self._aesgcm = AESGCM(self._key)

    @property
    def is_available(self) -> bool:
        """Check if encryption is available (key set and cipher initialized)."""
        return self._aesgcm is not None

    def get_status(self) -> EncryptionStatus:
        """Get current encryption status."""
        from cryptography import __version__ as crypto_version

        key_source = None
        key_id = None

        if self._key_manager is not None:
            key_source = self._key_manager.key_source
            key_id = self._key_manager.key_id
        elif self._key is not None:
            key_source = KeySource.ENVIRONMENT

        return EncryptionStatus(
            enabled=self.is_available,
            key_source=key_source,
            key_id=key_id,
            library_version=crypto_version,
        )

    def encrypt(self, plaintext: str) -> tuple[bytes, bytes]:
        """Encrypt plaintext using AES-256-GCM.

        Args:
            plaintext: String to encrypt

        Returns:
            Tuple of (ciphertext, iv)

        Raises:
            PHIEncryptionError: If encryption fails or not available
        """
        if not self.is_available:
            raise PHIEncryptionError(
                "Encryption not available. Set VCF_PG_LOADER_PHI_KEY or " "provide key/key_manager."
            )

        iv = secrets.token_bytes(self.IV_SIZE)
        ciphertext = self._aesgcm.encrypt(iv, plaintext.encode("utf-8"), None)
        return ciphertext, iv

    def decrypt(self, ciphertext: bytes, iv: bytes) -> str:
        """Decrypt ciphertext using AES-256-GCM.

        Args:
            ciphertext: Encrypted bytes
            iv: Initialization vector used for encryption

        Returns:
            Decrypted plaintext string

        Raises:
            PHIEncryptionError: If decryption fails or not available
        """
        if not self.is_available:
            raise PHIEncryptionError(
                "Encryption not available. Set VCF_PG_LOADER_PHI_KEY or " "provide key/key_manager."
            )

        try:
            plaintext = self._aesgcm.decrypt(iv, ciphertext, None)
            return plaintext.decode("utf-8")
        except Exception as e:
            raise PHIEncryptionError(f"Decryption failed: {e}") from e

    @staticmethod
    def generate_key() -> bytes:
        """Generate a new 256-bit encryption key.

        Returns:
            32-byte random key suitable for AES-256
        """
        return secrets.token_bytes(32)

    @staticmethod
    def key_to_base64(key: bytes) -> str:
        """Encode key as base64 for storage in environment variable.

        Args:
            key: 32-byte encryption key

        Returns:
            Base64-encoded string
        """
        return base64.b64encode(key).decode("ascii")

    @staticmethod
    def key_from_base64(encoded: str) -> bytes:
        """Decode key from base64.

        Args:
            encoded: Base64-encoded key string

        Returns:
            32-byte encryption key
        """
        return base64.b64decode(encoded)


class KeyRotator:
    """Handles key rotation for encrypted PHI data.

    Key rotation re-encrypts all encrypted data with a new key
    while maintaining availability (no downtime).
    """

    def __init__(
        self,
        old_encryptor: PHIEncryptor,
        new_encryptor: PHIEncryptor,
    ):
        """Initialize key rotator.

        Args:
            old_encryptor: Encryptor with current key
            new_encryptor: Encryptor with new key
        """
        if not old_encryptor.is_available:
            raise PHIEncryptionError("Old encryptor must have a valid key")
        if not new_encryptor.is_available:
            raise PHIEncryptionError("New encryptor must have a valid key")

        self._old = old_encryptor
        self._new = new_encryptor

    def rotate_value(
        self,
        old_ciphertext: bytes,
        old_iv: bytes,
    ) -> tuple[bytes, bytes]:
        """Re-encrypt a single value with the new key.

        Args:
            old_ciphertext: Data encrypted with old key
            old_iv: IV used for old encryption

        Returns:
            Tuple of (new_ciphertext, new_iv)
        """
        plaintext = self._old.decrypt(old_ciphertext, old_iv)
        return self._new.encrypt(plaintext)

    async def rotate_table(
        self,
        conn,
        table: str = "phi_vault.sample_id_mapping",
        batch_size: int = 1000,
        progress_callback=None,
    ) -> int:
        """Rotate encryption for all rows in a table.

        Args:
            conn: asyncpg connection
            table: Table name (default: phi_vault.sample_id_mapping)
            batch_size: Number of rows to process per batch
            progress_callback: Optional callback(processed, total) for progress

        Returns:
            Number of rows rotated
        """
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM {table} WHERE original_id_encrypted IS NOT NULL"
        )

        if total == 0:
            logger.info("No encrypted rows to rotate")
            return 0

        logger.info("Starting key rotation for %d rows in %s", total, table)
        processed = 0

        while processed < total:
            rows = await conn.fetch(
                f"""
                SELECT mapping_id, original_id_encrypted, encryption_iv
                FROM {table}
                WHERE original_id_encrypted IS NOT NULL
                ORDER BY mapping_id
                LIMIT $1 OFFSET $2
                """,
                batch_size,
                processed,
            )

            if not rows:
                break

            async with conn.transaction():
                for row in rows:
                    new_ciphertext, new_iv = self.rotate_value(
                        row["original_id_encrypted"],
                        row["encryption_iv"],
                    )

                    await conn.execute(
                        f"""
                        UPDATE {table}
                        SET original_id_encrypted = $1,
                            encryption_iv = $2
                        WHERE mapping_id = $3
                        """,
                        new_ciphertext,
                        new_iv,
                        row["mapping_id"],
                    )

                processed += len(rows)

                if progress_callback:
                    progress_callback(processed, total)

        logger.info("Key rotation complete: %d rows rotated", processed)
        return processed


def check_encryption_status() -> EncryptionStatus:
    """Check current PHI encryption status.

    Returns:
        EncryptionStatus indicating configuration state
    """
    from cryptography import __version__ as crypto_version

    env_key = os.environ.get(ENV_KEY_NAME)
    key_file = os.environ.get(ENV_KEY_FILE)

    if env_key:
        try:
            key = base64.b64decode(env_key)
            if len(key) == 32:
                return EncryptionStatus(
                    enabled=True,
                    key_source=KeySource.ENVIRONMENT,
                    key_id=None,
                    library_version=crypto_version,
                )
        except Exception:
            pass

    if key_file:
        path = Path(key_file)
        if path.exists():
            try:
                content = path.read_text().strip()
                key = base64.b64decode(content)
                if len(key) == 32:
                    return EncryptionStatus(
                        enabled=True,
                        key_source=KeySource.FILE,
                        key_id=key_file,
                        library_version=crypto_version,
                    )
            except Exception:
                pass

    return EncryptionStatus(
        enabled=False,
        key_source=None,
        key_id=None,
        library_version=crypto_version,
    )
