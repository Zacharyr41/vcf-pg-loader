"""Sample ID anonymization for HIPAA compliance.

HIPAA Reference: 164.514(b) - De-identification Standard

VCF sample IDs may contain identifiable information (e.g., "JohnDoe_2024",
hospital MRNs). This module replaces them with anonymous UUIDs while
maintaining secure linkage capability.
"""

import logging
from uuid import UUID

import asyncpg

from .encryption import PHIEncryptor

logger = logging.getLogger(__name__)


class SampleAnonymizer:
    """Anonymizes VCF sample IDs with optional encryption.

    Provides deterministic mapping: same (original_id, source_file) always
    maps to the same anonymous UUID. This enables consistent identification
    across multiple loads of the same sample.

    All reverse lookups are logged to phi_vault.reverse_lookup_audit for
    HIPAA compliance.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        encryptor: PHIEncryptor | None = None,
        created_by: int | None = None,
    ):
        """Initialize anonymizer.

        Args:
            pool: Database connection pool
            encryptor: Optional PHI encryptor for encrypting original IDs
            created_by: User ID to record as creator of mappings
        """
        self._pool = pool
        self._encryptor = encryptor
        self._created_by = created_by
        self._cache: dict[tuple[str, str], UUID] = {}

    async def anonymize_sample_id(
        self,
        original_id: str,
        source_file: str,
        load_batch_id: UUID,
    ) -> UUID:
        """Get or create anonymous ID for a sample.

        If the sample was seen before in this source file, returns the
        existing anonymous ID. Otherwise, creates a new mapping.

        Args:
            original_id: Original sample ID from VCF
            source_file: Path to source VCF file
            load_batch_id: UUID of current load batch

        Returns:
            Anonymous UUID for this sample
        """
        cache_key = (original_id, source_file)
        if cache_key in self._cache:
            return self._cache[cache_key]

        encrypted_id = None
        encryption_iv = None

        if self._encryptor and self._encryptor.is_available:
            encrypted_id, encryption_iv = self._encryptor.encrypt(original_id)

        async with self._pool.acquire() as conn:
            anonymous_id = await conn.fetchval(
                """
                SELECT phi_vault.get_or_create_anonymous_id($1, $2, $3, $4, $5, $6)
                """,
                original_id,
                source_file,
                load_batch_id,
                self._created_by,
                encrypted_id,
                encryption_iv,
            )

        self._cache[cache_key] = anonymous_id
        return anonymous_id

    async def bulk_anonymize(
        self,
        sample_ids: list[str],
        source_file: str,
        load_batch_id: UUID,
    ) -> dict[str, UUID]:
        """Anonymize multiple sample IDs efficiently.

        Args:
            sample_ids: List of original sample IDs
            source_file: Path to source VCF file
            load_batch_id: UUID of current load batch

        Returns:
            Dict mapping original IDs to anonymous UUIDs
        """
        result: dict[str, UUID] = {}
        to_create: list[str] = []

        for sample_id in sample_ids:
            cache_key = (sample_id, source_file)
            if cache_key in self._cache:
                result[sample_id] = self._cache[cache_key]
            else:
                to_create.append(sample_id)

        if not to_create:
            return result

        async with self._pool.acquire() as conn:
            existing = await conn.fetch(
                """
                SELECT original_id, anonymous_id
                FROM phi_vault.sample_id_mapping
                WHERE original_id = ANY($1) AND source_file = $2
                """,
                to_create,
                source_file,
            )

            for row in existing:
                orig = row["original_id"]
                anon = row["anonymous_id"]
                result[orig] = anon
                self._cache[(orig, source_file)] = anon
                to_create.remove(orig)

            for sample_id in to_create:
                encrypted_id = None
                encryption_iv = None
                if self._encryptor and self._encryptor.is_available:
                    encrypted_id, encryption_iv = self._encryptor.encrypt(sample_id)

                anonymous_id = await conn.fetchval(
                    """
                    INSERT INTO phi_vault.sample_id_mapping (
                        original_id, source_file, load_batch_id, created_by,
                        original_id_encrypted, encryption_iv
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (original_id, source_file) DO UPDATE
                        SET original_id = EXCLUDED.original_id
                    RETURNING anonymous_id
                    """,
                    sample_id,
                    source_file,
                    load_batch_id,
                    self._created_by,
                    encrypted_id,
                    encryption_iv,
                )
                result[sample_id] = anonymous_id
                self._cache[(sample_id, source_file)] = anonymous_id

        return result

    async def reverse_lookup(
        self,
        anonymous_id: UUID,
        requester_id: int,
        client_ip: str | None = None,
        reason: str | None = None,
    ) -> str | None:
        """Reverse lookup - get original ID from anonymous ID.

        This operation is audited per HIPAA requirements. All lookups
        are logged to phi_vault.reverse_lookup_audit.

        Args:
            anonymous_id: Anonymous UUID to look up
            requester_id: User ID of person requesting lookup
            client_ip: Optional client IP address
            reason: Optional reason for lookup (recommended)

        Returns:
            Original sample ID if found, None otherwise
        """
        logger.info(
            "PHI reverse lookup requested for %s by user %d (reason: %s)",
            anonymous_id,
            requester_id,
            reason or "not provided",
        )

        async with self._pool.acquire() as conn:
            original_id = await conn.fetchval(
                """
                SELECT phi_vault.reverse_lookup($1, $2, $3, $4)
                """,
                anonymous_id,
                requester_id,
                client_ip,
                reason,
            )

        if original_id:
            logger.info("PHI reverse lookup successful for %s", anonymous_id)
        else:
            logger.warning("PHI reverse lookup failed - no mapping for %s", anonymous_id)

        return original_id

    async def get_anonymous_id(
        self,
        original_id: str,
        source_file: str,
    ) -> UUID | None:
        """Get existing anonymous ID without creating new mapping.

        Args:
            original_id: Original sample ID
            source_file: Source file path

        Returns:
            Anonymous UUID if exists, None otherwise
        """
        cache_key = (original_id, source_file)
        if cache_key in self._cache:
            return self._cache[cache_key]

        async with self._pool.acquire() as conn:
            anonymous_id = await conn.fetchval(
                """
                SELECT anonymous_id FROM phi_vault.sample_id_mapping
                WHERE original_id = $1 AND source_file = $2
                """,
                original_id,
                source_file,
            )

        if anonymous_id:
            self._cache[cache_key] = anonymous_id

        return anonymous_id

    def clear_cache(self) -> None:
        """Clear the in-memory mapping cache."""
        self._cache.clear()


RE_IDENTIFICATION_WARNING = """
WARNING: Genomic data may still be re-identifiable even after sample ID anonymization.

HIPAA Note: De-identification of genomic data may require Expert Determination
(164.514(b)(1)) rather than Safe Harbor (164.514(b)(2)) due to the inherently
identifiable nature of DNA sequences.

Recommendations:
1. Maintain Data Use Agreements (DUAs) for all data sharing
2. Consider additional technical safeguards (data aggregation, access controls)
3. Consult your organization's Privacy Officer for guidance
4. Document your de-identification methodology for compliance audits

Reference: NIH Genomic Data Sharing Policy, HIPAA Privacy Rule 164.514
"""


def log_re_identification_warning() -> None:
    """Log warning about re-identification risk of genomic data."""
    logger.warning(RE_IDENTIFICATION_WARNING)
