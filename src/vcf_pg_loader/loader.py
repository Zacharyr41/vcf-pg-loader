"""VCF to PostgreSQL loader with binary COPY support."""

import hashlib
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
from asyncpg import Range

from .models import VariantRecord
from .schema import SchemaManager
from .vcf_parser import VCFStreamingParser


@dataclass
class LoadConfig:
    """Configuration for VCF loading."""

    batch_size: int = 50_000
    workers: int = 8
    drop_indexes: bool = True
    normalize: bool = True
    human_genome: bool = True


class VCFLoader:
    """High-performance VCF to PostgreSQL loader using binary COPY."""

    def __init__(self, db_url: str, config: LoadConfig | None = None):
        self.db_url = db_url
        self.config = config or LoadConfig()
        self.pool: asyncpg.Pool | None = None
        self.load_batch_id: UUID = uuid4()
        self._schema_manager = SchemaManager(human_genome=self.config.human_genome)

    async def connect(self) -> None:
        """Establish database connection pool."""
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

    async def load_vcf(self, vcf_path: Path | str) -> dict:
        """Load a VCF file into the database."""
        vcf_path = Path(vcf_path)

        if self.pool is None:
            await self.connect()

        file_md5 = hashlib.md5(vcf_path.read_bytes()).hexdigest()

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

            await self._start_audit(vcf_path, file_md5, len(streaming_parser.samples))

            total_loaded = 0
            for batch in streaming_parser.iter_batches():
                await self.copy_batch(batch)
                total_loaded += len(batch)

            if self.config.drop_indexes:
                async with self.pool.acquire() as conn:
                    await self._schema_manager.create_indexes(conn)

            await self._complete_audit(total_loaded)

            return {
                "variants_loaded": total_loaded,
                "load_batch_id": str(self.load_batch_id),
                "file_md5": file_md5
            }

        finally:
            streaming_parser.close()

    async def copy_batch(self, batch: list[VariantRecord]) -> None:
        """Copy a batch of records using binary COPY protocol."""
        if not batch:
            return

        records = [
            (
                r.chrom,
                Range(r.pos, r.end_pos or r.pos + len(r.ref)),
                r.pos,
                r.end_pos,
                r.ref,
                r.alt,
                r.qual,
                r.filter if r.filter else None,
                r.rs_id,
                r.gene,
                r.consequence,
                r.impact,
                r.hgvs_c,
                r.hgvs_p,
                r.af_gnomad,
                r.cadd_phred,
                r.clinvar_sig,
                self.load_batch_id
            )
            for r in batch
        ]

        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "variants",
                records=records,
                columns=[
                    "chrom", "pos_range", "pos", "end_pos", "ref", "alt",
                    "qual", "filter", "rs_id", "gene", "consequence",
                    "impact_severity", "hgvs_c", "hgvs_p", "af_gnomad",
                    "cadd_phred", "clinvar_sig", "load_batch_id"
                ]
            )

    async def _start_audit(
        self, vcf_path: Path, file_md5: str, samples_count: int
    ) -> None:
        """Create audit record for this load."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO variant_load_audit (
                    load_batch_id, vcf_file_path, vcf_file_md5,
                    vcf_file_size, reference_genome, samples_count, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                self.load_batch_id,
                str(vcf_path),
                file_md5,
                vcf_path.stat().st_size,
                "GRCh38",
                samples_count,
                "started"
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
