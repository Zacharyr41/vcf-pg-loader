"""VCF to PostgreSQL loader with binary COPY support."""

import asyncio
import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, NotRequired, TypedDict, TypeGuard
from uuid import UUID, uuid4

import asyncpg

from .models import VariantRecord
from .schema import SchemaManager
from .vcf_parser import VCFStreamingParser

logger = logging.getLogger(__name__)

HASH_CHUNK_SIZE = 65536

ProgressCallback = Callable[[int, int, int], None]


class LoadResult(TypedDict):
    """Result of a successful VCF load operation."""

    variants_loaded: int
    load_batch_id: str
    file_hash: str
    parallel: NotRequired[bool]
    is_reload: NotRequired[bool]
    previous_load_id: NotRequired[str]


class SkippedResult(TypedDict):
    """Result when a VCF load is skipped (already loaded)."""

    skipped: Literal[True]
    reason: str
    previous_load_id: str
    file_hash: str


def is_skipped_result(result: LoadResult | SkippedResult) -> TypeGuard[SkippedResult]:
    """Type guard to narrow result to SkippedResult."""
    return result.get("skipped") is True


def is_load_result(result: LoadResult | SkippedResult) -> TypeGuard[LoadResult]:
    """Type guard to narrow result to LoadResult."""
    return "variants_loaded" in result


class CheckExistingResult(TypedDict):
    """Result of checking for existing load."""

    load_batch_id: UUID
    status: str
    variants_loaded: int
    load_completed_at: datetime | None


def compute_file_hash(path: Path) -> str:
    """Compute SHA256 hash of a file using streaming reads.

    Uses chunked reading to avoid loading large files entirely into memory.
    Returns a 64-character hexadecimal string.
    """
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def validate_previous_load_id(value: object) -> bool:
    """Validate that previous_load_id is a proper UUID object.

    This validation is important for security - we must ensure that
    the value passed to DELETE queries is a proper UUID object, not
    a string that could be manipulated.
    """
    return isinstance(value, UUID)


@dataclass
class LoadConfig:
    """Configuration for VCF loading."""

    batch_size: int = 50_000
    workers: int = 8
    drop_indexes: bool = True
    normalize: bool = True
    human_genome: bool = True
    log_level: str = "INFO"
    progress_callback: ProgressCallback | None = None


class VCFLoader:
    """High-performance VCF to PostgreSQL loader using binary COPY."""

    def __init__(
        self,
        db_url: str,
        config: LoadConfig | None = None,
        logger: logging.Logger | None = None
    ):
        self.db_url = db_url
        self.config = config or LoadConfig()
        self.pool: asyncpg.Pool | None = None
        self.load_batch_id: UUID = uuid4()
        self._schema_manager = SchemaManager(human_genome=self.config.human_genome)
        self.logger = logger or logging.getLogger(__name__)

    async def connect(self) -> None:
        """Establish database connection pool."""
        self.logger.debug("Establishing database connection pool")
        self.pool = await asyncpg.create_pool(
            self.db_url,
            min_size=4,
            max_size=self.config.workers * 2,
            command_timeout=300
        )

    async def close(self) -> None:
        """Close database connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def __aenter__(self) -> "VCFLoader":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def check_existing(self, vcf_path: Path | str) -> CheckExistingResult | None:
        """Check if a file was previously loaded."""
        vcf_path = Path(vcf_path)

        if self.pool is None:
            await self.connect()

        file_hash = compute_file_hash(vcf_path)

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT load_batch_id, status, variants_loaded, load_completed_at
                FROM variant_load_audit
                WHERE vcf_file_hash = $1 AND status = 'completed'
                ORDER BY load_completed_at DESC
                LIMIT 1
                """,
                file_hash
            )

        if row:
            return {
                "load_batch_id": row["load_batch_id"],
                "status": row["status"],
                "variants_loaded": row["variants_loaded"],
                "load_completed_at": row["load_completed_at"]
            }
        return None

    async def load_vcf(
        self, vcf_path: Path | str, force_reload: bool = False, parallel: bool = False
    ) -> LoadResult | SkippedResult:
        """Load a VCF file into the database."""
        vcf_path = Path(vcf_path)
        self.logger.info("Starting load of %s", vcf_path.name)

        if self.pool is None:
            try:
                await self.connect()
            except Exception as e:
                self.logger.error("Failed to connect to database: %s", e)
                raise

        file_hash = compute_file_hash(vcf_path)

        existing = await self.check_existing(vcf_path)
        if existing and not force_reload:
            return {
                "skipped": True,
                "reason": "already_loaded",
                "previous_load_id": str(existing["load_batch_id"]),
                "file_hash": file_hash
            }

        is_reload = existing is not None
        previous_load_id = existing["load_batch_id"] if existing else None

        if is_reload:
            if not validate_previous_load_id(previous_load_id):
                raise ValueError(
                    f"Invalid previous_load_id type: expected UUID, got {type(previous_load_id).__name__}"
                )
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM variants WHERE load_batch_id = $1",
                    previous_load_id
                )

        self.load_batch_id = uuid4()

        streaming_parser = VCFStreamingParser(
            vcf_path,
            batch_size=self.config.batch_size,
            normalize=self.config.normalize,
            human_genome=self.config.human_genome
        )

        try:
            if self.config.drop_indexes:
                async with self.pool.acquire() as conn:
                    await self._schema_manager.drop_indexes(conn)

            await self._start_audit(
                vcf_path, file_hash, len(streaming_parser.samples),
                is_reload=is_reload, previous_load_id=previous_load_id
            )

            total_loaded = 0
            if parallel and self.config.workers > 1:
                total_loaded = await self._load_parallel(streaming_parser)
            else:
                batch_num = 0
                for batch in streaming_parser.iter_batches():
                    await self.copy_batch(batch)
                    total_loaded += len(batch)
                    batch_num += 1
                    self.logger.debug(
                        "Batch %d: loaded %d variants (total: %d)",
                        batch_num, len(batch), total_loaded
                    )
                    if self.config.progress_callback is not None:
                        self.config.progress_callback(batch_num, len(batch), total_loaded)

            if self.config.drop_indexes:
                async with self.pool.acquire() as conn:
                    await self._schema_manager.create_indexes(conn)

            await self._complete_audit(total_loaded)
            self.logger.info(
                "Completed load: %d variants loaded (batch_id=%s)",
                total_loaded, self.load_batch_id
            )

            result = {
                "variants_loaded": total_loaded,
                "load_batch_id": str(self.load_batch_id),
                "file_hash": file_hash
            }
            if parallel:
                result["parallel"] = True
            if is_reload:
                result["is_reload"] = True
                result["previous_load_id"] = str(previous_load_id)

            return result

        finally:
            streaming_parser.close()

    async def copy_batch(self, batch: list[VariantRecord]) -> None:
        """Copy a batch of records using binary COPY protocol."""
        if not batch:
            return

        from .columns import VARIANT_COLUMNS_BASIC, get_record_values

        records = [get_record_values(r, self.load_batch_id) for r in batch]

        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "variants",
                records=records,
                columns=VARIANT_COLUMNS_BASIC
            )

    async def _start_audit(
        self, vcf_path: Path, file_hash: str, samples_count: int,
        is_reload: bool = False, previous_load_id: UUID | None = None
    ) -> None:
        """Create audit record for this load."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO variant_load_audit (
                    load_batch_id, vcf_file_path, vcf_file_hash,
                    vcf_file_size, reference_genome, samples_count, status,
                    is_reload, previous_load_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                self.load_batch_id,
                str(vcf_path),
                file_hash,
                vcf_path.stat().st_size,
                "GRCh38",
                samples_count,
                "started",
                is_reload,
                previous_load_id
            )

    async def _complete_audit(self, variants_loaded: int) -> None:
        """Update audit record with completion status."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE variant_load_audit
                SET status = 'completed',
                    variants_loaded = $2,
                    load_completed_at = NOW()
                WHERE load_batch_id = $1
                """,
                self.load_batch_id,
                variants_loaded
            )

    async def _fail_audit(self, error_message: str) -> None:
        """Update audit record with failed status."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE variant_load_audit
                SET status = 'failed',
                    error_message = $2,
                    load_completed_at = NOW()
                WHERE load_batch_id = $1
                """,
                self.load_batch_id,
                error_message
            )

    async def _rollback_variants(self) -> None:
        """Rollback any variants loaded for current batch."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM variants WHERE load_batch_id = $1",
                self.load_batch_id
            )

    async def _load_parallel(self, streaming_parser: VCFStreamingParser) -> int:
        """Load variants in parallel by chromosome.

        Handles worker failures by rolling back partial loads and marking
        the audit record as failed.

        WARNING: Memory Requirements
            Parallel mode loads the entire VCF into memory before processing,
            grouping variants by chromosome. Expect RAM usage of approximately
            2x the uncompressed VCF file size. For large files (>1GB), consider
            using sequential mode (parallel=False) or ensure sufficient RAM.

            Example: A 500MB compressed VCF (~2GB uncompressed) may require
            4GB+ of RAM in parallel mode.
        """
        chrom_batches: dict[str, list[VariantRecord]] = {}
        for batch in streaming_parser.iter_batches():
            for record in batch:
                if record.chrom not in chrom_batches:
                    chrom_batches[record.chrom] = []
                chrom_batches[record.chrom].append(record)

        async def load_chromosome(chrom: str, records: list[VariantRecord]) -> int:
            batch_size = self.config.batch_size
            for i in range(0, len(records), batch_size):
                await self.copy_batch(records[i:i + batch_size])
            return len(records)

        tasks = [
            load_chromosome(chrom, records)
            for chrom, records in chrom_batches.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            await self._rollback_variants()
            error_msg = f"Parallel loading failed: {len(errors)} worker(s) failed. First error: {errors[0]}"
            await self._fail_audit(error_msg)
            raise RuntimeError(error_msg) from errors[0]

        return sum(results)
