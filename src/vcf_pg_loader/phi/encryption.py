"""PHI encryption utilities using AES-256-GCM.

HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption
"""

import base64
import os
import secrets

ENV_KEY_NAME = "VCF_PG_LOADER_PHI_KEY"


class PHIEncryptionError(Exception):
    """Raised when PHI encryption/decryption fails."""


class PHIEncryptor:
    """Handles AES-256-GCM encryption of PHI data.

    Uses cryptography library for FIPS-compliant encryption.
    Key should be 32 bytes (256 bits) and stored securely.
    """

    IV_SIZE = 12
    TAG_SIZE = 16

    def __init__(self, key: bytes | None = None):
        """Initialize encryptor with optional key.

        Args:
            key: 32-byte encryption key. If None, attempts to load from
                 VCF_PG_LOADER_PHI_KEY environment variable.
        """
        self._key = key
        self._cipher_available = False

        if self._key is None:
            env_key = os.environ.get(ENV_KEY_NAME)
            if env_key:
                try:
                    self._key = base64.b64decode(env_key)
                except Exception as e:
                    raise PHIEncryptionError(f"Invalid base64 key in {ENV_KEY_NAME}: {e}") from e

        if self._key is not None:
            if len(self._key) != 32:
                raise PHIEncryptionError(f"Encryption key must be 32 bytes, got {len(self._key)}")
            self._init_cipher()

    def _init_cipher(self) -> None:
        """Initialize cryptography library."""
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM

            self._aesgcm = AESGCM(self._key)
            self._cipher_available = True
        except ImportError:
            self._cipher_available = False

    @property
    def is_available(self) -> bool:
        """Check if encryption is available (key set and library present)."""
        return self._key is not None and self._cipher_available

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
                "Encryption not available. Set VCF_PG_LOADER_PHI_KEY or "
                "install cryptography package."
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
                "Encryption not available. Set VCF_PG_LOADER_PHI_KEY or "
                "install cryptography package."
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
