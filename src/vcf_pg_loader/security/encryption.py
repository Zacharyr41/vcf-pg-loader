"""HIPAA-compliant Encryption at Rest using AES-256-GCM.

HIPAA Citation: 45 CFR 164.312(a)(2)(iv) - Encryption and Decryption
"Implement a mechanism to encrypt and decrypt electronic protected
health information."

NIST SP 800-111 Requirements:
- Algorithm: AES-256 (minimum AES-128)
- Mode: AES-GCM or other FIPS-approved modes
- Key storage: Separate from encrypted data

HHS Breach Safe Harbor (45 CFR 164.402):
Properly encrypted PHI is "unusable, unreadable, or indecipherable"
and exempt from breach notification requirements IF:
1. Encryption meets NIST standards
2. Encryption key was not compromised with the data
"""

import base64
import logging
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from importlib.resources import files
from uuid import UUID

import asyncpg
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..audit.logger import AuditLogger
from ..audit.models import AuditEvent, AuditEventType

logger = logging.getLogger(__name__)

AES_KEY_SIZE_BYTES = 32
AES_NONCE_SIZE_BYTES = 12
AES_TAG_SIZE_BYTES = 16


class KeyPurpose(Enum):
    DATA_ENCRYPTION = "data_encryption"
    PHI_ENCRYPTION = "phi_encryption"
    BACKUP_ENCRYPTION = "backup_encryption"
    TRANSPORT_ENCRYPTION = "transport_encryption"


@dataclass
class EncryptionKey:
    key_id: UUID
    key_name: str
    key_version: int
    algorithm: str
    purpose: KeyPurpose
    is_active: bool
    created_at: datetime
    expires_at: datetime | None = None
    retired_at: datetime | None = None
    use_count: int = 0

    @classmethod
    def from_db_row(cls, row: dict) -> "EncryptionKey":
        return cls(
            key_id=row["key_id"],
            key_name=row["key_name"],
            key_version=row["key_version"],
            algorithm=row["algorithm"],
            purpose=KeyPurpose(row["purpose"]),
            is_active=row["is_active"],
            created_at=row["created_at"],
            expires_at=row.get("expires_at"),
            retired_at=row.get("retired_at"),
            use_count=row.get("use_count", 0),
        )

    def is_valid(self) -> bool:
        if not self.is_active:
            return False
        if self.retired_at is not None:
            return False
        if self.expires_at and datetime.now(UTC) > self.expires_at:
            return False
        return True


class EncryptionManager:
    """Manages encryption at rest for HIPAA compliance.

    45 CFR 164.312(a)(2)(iv): Implement encryption/decryption mechanism.

    Uses AES-256-GCM per NIST SP 800-111 recommendations:
    - AES-256: FIPS 197 approved
    - GCM mode: Provides both confidentiality and integrity
    - 96-bit nonce: Per NIST SP 800-38D

    Key encryption key (KEK) is provided externally (environment variable
    or key management service) - keys stored in database are encrypted.
    """

    def __init__(
        self,
        master_key: bytes | None = None,
        audit_logger: AuditLogger | None = None,
    ):
        """Initialize encryption manager.

        Args:
            master_key: 32-byte master key for encrypting data keys.
                        If not provided, reads from VCF_PG_LOADER_MASTER_KEY env var.
            audit_logger: Optional audit logger for key operations.

        Raises:
            ValueError: If no master key is provided or available.
        """
        if master_key is None:
            key_b64 = os.environ.get("VCF_PG_LOADER_MASTER_KEY")
            if key_b64:
                master_key = base64.b64decode(key_b64)

        if master_key is None:
            raise ValueError(
                "Master key required. Set VCF_PG_LOADER_MASTER_KEY environment variable "
                "or provide master_key parameter."
            )

        if len(master_key) != AES_KEY_SIZE_BYTES:
            raise ValueError(f"Master key must be {AES_KEY_SIZE_BYTES} bytes (256 bits)")

        self._master_cipher = AESGCM(master_key)
        self._audit_logger = audit_logger
        self._key_cache: dict[str, tuple[bytes, datetime]] = {}
        self._cache_ttl = timedelta(minutes=5)

    async def create_schema(self, conn: asyncpg.Connection) -> None:
        """Create encryption schema.

        45 CFR 164.312(a)(2)(iv): Establish encryption infrastructure.
        """
        sql_path = files("vcf_pg_loader.db.schema").joinpath("encryption_tables.sql")
        sql = sql_path.read_text()
        await conn.execute(sql)
        logger.info("Encryption schema created/updated")

    async def schema_exists(self, conn: asyncpg.Connection) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'encryption_keys'
            )
            """
        )

    def _generate_data_key(self) -> bytes:
        """Generate a new 256-bit data encryption key."""
        return secrets.token_bytes(AES_KEY_SIZE_BYTES)

    def _encrypt_key(self, key: bytes) -> tuple[bytes, bytes, bytes]:
        """Encrypt a data key with the master key.

        Returns:
            Tuple of (encrypted_key, nonce, tag)
        """
        nonce = secrets.token_bytes(AES_NONCE_SIZE_BYTES)
        ciphertext = self._master_cipher.encrypt(nonce, key, None)
        encrypted_key = ciphertext[:-AES_TAG_SIZE_BYTES]
        tag = ciphertext[-AES_TAG_SIZE_BYTES:]
        return encrypted_key, nonce, tag

    def _decrypt_key(
        self,
        encrypted_key: bytes,
        nonce: bytes,
        tag: bytes,
    ) -> bytes:
        """Decrypt a data key with the master key."""
        ciphertext = encrypted_key + tag
        return self._master_cipher.decrypt(nonce, ciphertext, None)

    async def create_key(
        self,
        conn: asyncpg.Connection,
        key_name: str,
        purpose: KeyPurpose,
        expires_days: int | None = None,
        created_by: int | None = None,
    ) -> EncryptionKey:
        """Create a new encryption key.

        45 CFR 164.312(a)(2)(iv): Create encryption keys for PHI protection.

        Args:
            conn: Database connection
            key_name: Unique name for the key
            purpose: What the key will be used for
            expires_days: Optional expiration in days
            created_by: User ID creating the key

        Returns:
            EncryptionKey metadata (key material is stored encrypted)
        """
        data_key = self._generate_data_key()
        encrypted_key, nonce, tag = self._encrypt_key(data_key)

        expires_at = None
        if expires_days:
            expires_at = datetime.now(UTC) + timedelta(days=expires_days)

        key_id = await conn.fetchval(
            """
            INSERT INTO encryption_keys (
                key_name, encrypted_key_material, key_nonce, key_tag,
                purpose, expires_at, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING key_id
            """,
            key_name,
            encrypted_key,
            nonce,
            tag,
            purpose.value,
            expires_at,
            created_by,
        )

        row = await conn.fetchrow(
            "SELECT * FROM encryption_keys WHERE key_id = $1",
            key_id,
        )

        key = EncryptionKey.from_db_row(dict(row))

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.CONFIG_CHANGE,
                    action="encryption_key_created",
                    user_id=created_by,
                    success=True,
                    resource_type="encryption_key",
                    resource_id=str(key_id),
                    details={
                        "key_name": key_name,
                        "purpose": purpose.value,
                        "expires_days": expires_days,
                    },
                )
            )

        logger.info(
            "Encryption key created: name=%s, purpose=%s, key_id=%s",
            key_name,
            purpose.value,
            key_id,
        )

        return key

    async def get_key(
        self,
        conn: asyncpg.Connection,
        purpose: KeyPurpose,
    ) -> tuple[bytes, EncryptionKey] | None:
        """Get active encryption key for a purpose.

        Args:
            conn: Database connection
            purpose: Key purpose to retrieve

        Returns:
            Tuple of (decrypted_key_material, key_metadata) or None
        """
        cache_key = purpose.value
        if cache_key in self._key_cache:
            cached_key, cached_at = self._key_cache[cache_key]
            if datetime.now(UTC) - cached_at < self._cache_ttl:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM encryption_keys
                    WHERE purpose = $1 AND is_active = true
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    purpose.value,
                )
                if row:
                    return cached_key, EncryptionKey.from_db_row(dict(row))

        row = await conn.fetchrow(
            "SELECT * FROM get_active_encryption_key($1)",
            purpose.value,
        )

        if not row or row["key_id"] is None:
            return None

        decrypted_key = self._decrypt_key(
            bytes(row["encrypted_key_material"]),
            bytes(row["key_nonce"]),
            bytes(row["key_tag"]),
        )

        self._key_cache[cache_key] = (decrypted_key, datetime.now(UTC))

        key_row = await conn.fetchrow(
            "SELECT * FROM encryption_keys WHERE key_id = $1",
            row["key_id"],
        )
        key = EncryptionKey.from_db_row(dict(key_row))

        return decrypted_key, key

    async def rotate_key(
        self,
        conn: asyncpg.Connection,
        key_name: str,
        rotated_by: int,
        reason: str,
    ) -> EncryptionKey:
        """Rotate an encryption key.

        NIST SP 800-57 recommends periodic key rotation.

        Args:
            conn: Database connection
            key_name: Key to rotate
            rotated_by: User performing rotation
            reason: Reason for rotation

        Returns:
            New EncryptionKey metadata
        """
        data_key = self._generate_data_key()
        encrypted_key, nonce, tag = self._encrypt_key(data_key)

        new_key_id = await conn.fetchval(
            "SELECT rotate_encryption_key($1, $2, $3, $4, $5, $6)",
            key_name,
            encrypted_key,
            nonce,
            tag,
            rotated_by,
            reason,
        )

        row = await conn.fetchrow(
            "SELECT * FROM encryption_keys WHERE key_id = $1",
            new_key_id,
        )

        key = EncryptionKey.from_db_row(dict(row))

        self._key_cache.pop(key.purpose.value, None)

        if self._audit_logger:
            await self._audit_logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.CONFIG_CHANGE,
                    action="encryption_key_rotated",
                    user_id=rotated_by,
                    success=True,
                    resource_type="encryption_key",
                    resource_id=str(new_key_id),
                    details={
                        "key_name": key_name,
                        "reason": reason,
                        "new_version": key.key_version,
                    },
                )
            )

        logger.info(
            "Encryption key rotated: name=%s, new_version=%d, key_id=%s",
            key_name,
            key.key_version,
            new_key_id,
        )

        return key

    def encrypt(self, key: bytes, plaintext: bytes) -> bytes:
        """Encrypt data using AES-256-GCM.

        45 CFR 164.312(a)(2)(iv): Encrypt ePHI.

        Args:
            key: 256-bit encryption key
            plaintext: Data to encrypt

        Returns:
            nonce + ciphertext + tag (concatenated)
        """
        cipher = AESGCM(key)
        nonce = secrets.token_bytes(AES_NONCE_SIZE_BYTES)
        ciphertext = cipher.encrypt(nonce, plaintext, None)
        return nonce + ciphertext

    def decrypt(self, key: bytes, ciphertext: bytes) -> bytes:
        """Decrypt data using AES-256-GCM.

        45 CFR 164.312(a)(2)(iv): Decrypt ePHI.

        Args:
            key: 256-bit encryption key
            ciphertext: nonce + ciphertext + tag (concatenated)

        Returns:
            Decrypted plaintext

        Raises:
            cryptography.exceptions.InvalidTag: If authentication fails
        """
        cipher = AESGCM(key)
        nonce = ciphertext[:AES_NONCE_SIZE_BYTES]
        data = ciphertext[AES_NONCE_SIZE_BYTES:]
        return cipher.decrypt(nonce, data, None)

    def encrypt_string(self, key: bytes, plaintext: str) -> str:
        """Encrypt a string and return base64-encoded ciphertext."""
        encrypted = self.encrypt(key, plaintext.encode("utf-8"))
        return base64.b64encode(encrypted).decode("ascii")

    def decrypt_string(self, key: bytes, ciphertext_b64: str) -> str:
        """Decrypt a base64-encoded ciphertext to string."""
        ciphertext = base64.b64decode(ciphertext_b64)
        plaintext = self.decrypt(key, ciphertext)
        return plaintext.decode("utf-8")

    async def list_keys(
        self,
        conn: asyncpg.Connection,
        purpose: KeyPurpose | None = None,
        include_retired: bool = False,
    ) -> list[EncryptionKey]:
        """List encryption keys.

        Args:
            conn: Database connection
            purpose: Optional filter by purpose
            include_retired: Include retired keys

        Returns:
            List of EncryptionKey metadata
        """
        query = "SELECT * FROM encryption_keys WHERE 1=1"
        params: list = []

        if purpose:
            params.append(purpose.value)
            query += f" AND purpose = ${len(params)}"

        if not include_retired:
            query += " AND retired_at IS NULL"

        query += " ORDER BY key_name, key_version DESC"

        rows = await conn.fetch(query, *params)
        return [EncryptionKey.from_db_row(dict(row)) for row in rows]

    async def register_encrypted_column(
        self,
        conn: asyncpg.Connection,
        key_id: UUID,
        table_name: str,
        column_name: str,
    ) -> None:
        """Register a column as encrypted with a specific key.

        This helps track data re-encryption needs during key rotation.
        """
        await conn.execute(
            """
            INSERT INTO encrypted_data_registry (key_id, table_name, column_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (table_name, column_name) DO UPDATE
            SET key_id = EXCLUDED.key_id
            """,
            key_id,
            table_name,
            column_name,
        )
