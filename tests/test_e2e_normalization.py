"""End-to-end CLI tests for normalization.

These tests verify that normalization works through the CLI.
They should FAIL until normalization is wired into the pipeline.
"""

import re
import subprocess
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return re.sub(r'\x1b\[[0-9;]*m', '', text)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def postgres_container():
    """Provide a PostgreSQL test container for the module."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def db_url(postgres_container):
    """Provide database URL for CLI commands."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope="module")
def initialized_db(db_url):
    """Initialize database schema before tests."""
    result = subprocess.run(
        ["uv", "run", "vcf-pg-loader", "init-db", "--db", db_url],
        capture_output=True,
        text=True,
        timeout=60
    )
    assert result.returncode == 0, f"init-db failed: {result.stderr}"
    return db_url


@pytest.mark.integration
class TestE2ENormalization:
    """E2E tests for CLI normalization."""

    def test_cli_normalize_flag_default(self, initialized_db):
        """CLI should have --normalize flag enabled by default."""
        result = subprocess.run(
            ["uv", "run", "vcf-pg-loader", "load", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        assert result.returncode == 0
        clean_output = strip_ansi(result.stdout)
        assert "--normalize" in clean_output or "--no-normalize" in clean_output

    def test_cli_load_with_normalize(self, initialized_db):
        """CLI should accept --normalize flag."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tATG\tAG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            result = subprocess.run(
                [
                    "uv", "run", "vcf-pg-loader", "load",
                    str(vcf_path),
                    "--db", initialized_db,
                    "--normalize"
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            assert result.returncode == 0, f"load failed: {result.stderr}"
            assert "Loaded" in result.stdout
        finally:
            vcf_path.unlink()

    def test_cli_load_with_no_normalize(self, initialized_db):
        """CLI should accept --no-normalize flag."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t200\t.\tATG\tAG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            result = subprocess.run(
                [
                    "uv", "run", "vcf-pg-loader", "load",
                    str(vcf_path),
                    "--db", initialized_db,
                    "--no-normalize"
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            assert result.returncode == 0, f"load failed: {result.stderr}"
            assert "Loaded" in result.stdout
        finally:
            vcf_path.unlink()

    def test_cli_normalize_actually_normalizes(self, initialized_db):
        """CLI with --normalize should produce normalized variants in database."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t300\t.\tATG\tAG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            result = subprocess.run(
                [
                    "uv", "run", "vcf-pg-loader", "load",
                    str(vcf_path),
                    "--db", initialized_db,
                    "--normalize"
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            assert result.returncode == 0

            load_batch_id = None
            for line in result.stdout.split("\n"):
                if "Batch ID:" in line:
                    load_batch_id = line.split("Batch ID:")[1].strip()
                    break
            assert load_batch_id is not None

            import asyncio
            async def verify():
                conn = await asyncpg.connect(initialized_db)
                try:
                    row = await conn.fetchrow(
                        "SELECT ref, alt FROM variants WHERE load_batch_id = $1::uuid",
                        load_batch_id
                    )
                    assert row["ref"] == "AT", \
                        f"REF should be normalized to 'AT', got '{row['ref']}'"
                    assert row["alt"] == "A", \
                        f"ALT should be normalized to 'A', got '{row['alt']}'"
                finally:
                    await conn.close()

            asyncio.run(verify())
        finally:
            vcf_path.unlink()

    def test_cli_no_normalize_preserves_original(self, initialized_db):
        """CLI with --no-normalize should preserve original alleles."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t400\t.\tATG\tAG\t30\tPASS\t.
'''
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf', delete=False) as f:
            f.write(vcf_content)
            f.flush()
            vcf_path = Path(f.name)

        try:
            result = subprocess.run(
                [
                    "uv", "run", "vcf-pg-loader", "load",
                    str(vcf_path),
                    "--db", initialized_db,
                    "--no-normalize"
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            assert result.returncode == 0

            load_batch_id = None
            for line in result.stdout.split("\n"):
                if "Batch ID:" in line:
                    load_batch_id = line.split("Batch ID:")[1].strip()
                    break
            assert load_batch_id is not None

            import asyncio
            async def verify():
                conn = await asyncpg.connect(initialized_db)
                try:
                    row = await conn.fetchrow(
                        "SELECT ref, alt FROM variants WHERE load_batch_id = $1::uuid",
                        load_batch_id
                    )
                    assert row["ref"] == "ATG", \
                        f"REF should be preserved as 'ATG', got '{row['ref']}'"
                    assert row["alt"] == "AG", \
                        f"ALT should be preserved as 'AG', got '{row['alt']}'"
                finally:
                    await conn.close()

            asyncio.run(verify())
        finally:
            vcf_path.unlink()

    def test_cli_normalize_mills_indels(self, initialized_db):
        """CLI should normalize Mills indels correctly."""
        vcf_path = FIXTURES_DIR / "mills_indels.vcf.gz"
        if not vcf_path.exists():
            pytest.skip("mills_indels.vcf.gz fixture not found")

        result = subprocess.run(
            [
                "uv", "run", "vcf-pg-loader", "load",
                str(vcf_path),
                "--db", initialized_db,
                "--normalize",
                "--batch", "500"
            ],
            capture_output=True,
            text=True,
            timeout=120
        )
        assert result.returncode == 0

        load_batch_id = None
        for line in result.stdout.split("\n"):
            if "Batch ID:" in line:
                load_batch_id = line.split("Batch ID:")[1].strip()
                break

        import asyncio
        async def verify():
            conn = await asyncpg.connect(initialized_db)
            try:
                unnormalized = await conn.fetchval("""
                    SELECT COUNT(*) FROM variants
                    WHERE load_batch_id = $1::uuid
                    AND LENGTH(ref) > 1 AND LENGTH(alt) > 1
                    AND RIGHT(ref, 1) = RIGHT(alt, 1)
                """, load_batch_id)

                assert unnormalized == 0, \
                    f"Found {unnormalized} un-normalized variants (same trailing base)"
            finally:
                await conn.close()

        asyncio.run(verify())
