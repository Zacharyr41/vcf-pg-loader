"""End-to-end CLI tests for human vs non-human genome support.

These tests verify that the CLI correctly handles human and non-human
genome configurations. They should FAIL until the feature is implemented.
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
def postgres_container_human():
    """Provide a PostgreSQL test container for human genome tests."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def postgres_container_non_human():
    """Provide a PostgreSQL test container for non-human genome tests."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def db_url_human(postgres_container_human):
    """Provide database URL for human genome CLI commands."""
    host = postgres_container_human.get_container_host_ip()
    port = postgres_container_human.get_exposed_port(5432)
    user = postgres_container_human.username
    password = postgres_container_human.password
    database = postgres_container_human.dbname
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope="module")
def db_url_non_human(postgres_container_non_human):
    """Provide database URL for non-human genome CLI commands."""
    host = postgres_container_non_human.get_container_host_ip()
    port = postgres_container_non_human.get_exposed_port(5432)
    user = postgres_container_non_human.username
    password = postgres_container_non_human.password
    database = postgres_container_non_human.dbname
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope="module")
def initialized_db_human(db_url_human):
    """Initialize human genome database schema."""
    result = subprocess.run(
        ["uv", "run", "vcf-pg-loader", "init-db", "--db", db_url_human, "--human-genome"],
        capture_output=True,
        text=True,
        timeout=60
    )
    assert result.returncode == 0, f"init-db failed: {result.stderr}"
    return db_url_human


@pytest.fixture(scope="module")
def initialized_db_non_human(db_url_non_human):
    """Initialize non-human genome database schema."""
    result = subprocess.run(
        ["uv", "run", "vcf-pg-loader", "init-db", "--db", db_url_non_human, "--no-human-genome"],
        capture_output=True,
        text=True,
        timeout=60
    )
    assert result.returncode == 0, f"init-db failed: {result.stderr}"
    return db_url_non_human


@pytest.mark.integration
class TestE2EHumanGenomeCLI:
    """E2E tests for CLI human genome support."""

    def test_cli_init_db_has_human_genome_flag(self):
        """init-db command should have --human-genome/--no-human-genome flags."""
        result = subprocess.run(
            ["uv", "run", "vcf-pg-loader", "init-db", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        assert result.returncode == 0
        clean_output = strip_ansi(result.stdout)
        assert "--human-genome" in clean_output or "--no-human-genome" in clean_output

    def test_cli_load_has_human_genome_flag(self):
        """load command should have --human-genome/--no-human-genome flags."""
        result = subprocess.run(
            ["uv", "run", "vcf-pg-loader", "load", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        assert result.returncode == 0
        clean_output = strip_ansi(result.stdout)
        assert "--human-genome" in clean_output or "--no-human-genome" in clean_output

    def test_cli_init_db_creates_chromosome_enum(self, initialized_db_human):
        """init-db with --human-genome should create chromosome_type enum."""
        import asyncio

        async def verify():
            conn = await asyncpg.connect(initialized_db_human)
            try:
                types = await conn.fetch("""
                    SELECT typname FROM pg_type
                    WHERE typname = 'chromosome_type'
                """)
                assert len(types) == 1, "chromosome_type enum should exist"
            finally:
                await conn.close()

        asyncio.run(verify())

    def test_cli_load_human_vcf(self, initialized_db_human):
        """Should load human VCF with chromosome validation."""
        vcf_path = FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"
        if not vcf_path.exists():
            pytest.skip("strelka_snvs_chr22.vcf.gz fixture not found")

        result = subprocess.run(
            [
                "uv", "run", "vcf-pg-loader", "load",
                str(vcf_path),
                "--db", initialized_db_human,
                "--human-genome",
                "--batch", "500"
            ],
            capture_output=True,
            text=True,
            timeout=120
        )
        assert result.returncode == 0, f"load failed: {result.stderr}"
        assert "Loaded" in result.stdout
        assert "2,627" in result.stdout

    def test_cli_human_rejects_non_human_chromosome(self, initialized_db_human):
        """Human genome mode should reject non-human chromosome names."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
MN908947.3\t100\t.\tA\tG\t30\tPASS\t.
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
                    "--db", initialized_db_human,
                    "--human-genome"
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            assert result.returncode == 1, "Should fail with non-human chromosome"
            assert "error" in result.stdout.lower() or "error" in result.stderr.lower()
        finally:
            vcf_path.unlink()


@pytest.mark.integration
class TestE2ENonHumanGenomeCLI:
    """E2E tests for CLI non-human genome support."""

    def test_cli_init_db_no_enum(self, initialized_db_non_human):
        """init-db with --no-human-genome should NOT create chromosome_type enum."""
        import asyncio

        async def verify():
            conn = await asyncpg.connect(initialized_db_non_human)
            try:
                types = await conn.fetch("""
                    SELECT typname FROM pg_type
                    WHERE typname = 'chromosome_type'
                """)
                assert len(types) == 0, "chromosome_type enum should NOT exist for non-human"
            finally:
                await conn.close()

        asyncio.run(verify())

    def test_cli_load_sarscov2(self, initialized_db_non_human):
        """Should load SARS-CoV-2 VCF with arbitrary chromosome names."""
        vcf_path = FIXTURES_DIR / "sarscov2.vcf.gz"
        if not vcf_path.exists():
            pytest.skip("sarscov2.vcf.gz fixture not found")

        result = subprocess.run(
            [
                "uv", "run", "vcf-pg-loader", "load",
                str(vcf_path),
                "--db", initialized_db_non_human,
                "--no-human-genome"
            ],
            capture_output=True,
            text=True,
            timeout=60
        )
        assert result.returncode == 0, f"load failed: {result.stderr}"
        assert "Loaded" in result.stdout

        load_batch_id = None
        for line in result.stdout.split("\n"):
            if "Batch ID:" in line:
                load_batch_id = line.split("Batch ID:")[1].strip()
                break

        import asyncio
        async def verify():
            conn = await asyncpg.connect(initialized_db_non_human)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid",
                    load_batch_id
                )
                assert count == 9, f"Expected 9 SARS-CoV-2 variants, got {count}"
            finally:
                await conn.close()

        asyncio.run(verify())

    def test_cli_load_arbitrary_chromosomes(self, initialized_db_non_human):
        """Should load VCF with any arbitrary chromosome names."""
        vcf_content = '''##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
NC_045512.2\t100\t.\tA\tG\t30\tPASS\t.
scaffold_xyz_123\t200\t.\tC\tT\t30\tPASS\t.
my_custom_contig\t300\t.\tG\tA\t30\tPASS\t.
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
                    "--db", initialized_db_non_human,
                    "--no-human-genome"
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            assert result.returncode == 0, f"load failed: {result.stderr}"
            assert "Loaded 3 variants" in result.stdout

            load_batch_id = None
            for line in result.stdout.split("\n"):
                if "Batch ID:" in line:
                    load_batch_id = line.split("Batch ID:")[1].strip()
                    break

            import asyncio
            async def verify():
                conn = await asyncpg.connect(initialized_db_non_human)
                try:
                    chroms = await conn.fetch(
                        "SELECT DISTINCT chrom FROM variants WHERE load_batch_id = $1::uuid",
                        load_batch_id
                    )
                    chrom_list = [r['chrom'] for r in chroms]
                    assert 'NC_045512.2' in chrom_list
                    assert 'scaffold_xyz_123' in chrom_list
                    assert 'my_custom_contig' in chrom_list
                finally:
                    await conn.close()

            asyncio.run(verify())
        finally:
            vcf_path.unlink()


@pytest.mark.integration
class TestE2EGenomeTypeDefaults:
    """E2E tests for genome type default behavior."""

    def test_cli_init_db_default_is_human(self, postgres_container_human):
        """init-db without flag should default to human genome (enum)."""
        host = postgres_container_human.get_container_host_ip()
        port = postgres_container_human.get_exposed_port(5432)
        user = postgres_container_human.username
        password = postgres_container_human.password
        database = "test_default"

        import asyncio
        async def create_db():
            conn = await asyncpg.connect(
                host=host, port=port, user=user, password=password, database="postgres"
            )
            try:
                await conn.execute(f"CREATE DATABASE {database}")
            finally:
                await conn.close()

        asyncio.run(create_db())

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        result = subprocess.run(
            ["uv", "run", "vcf-pg-loader", "init-db", "--db", db_url],
            capture_output=True,
            text=True,
            timeout=60
        )
        assert result.returncode == 0

        async def verify():
            conn = await asyncpg.connect(db_url)
            try:
                types = await conn.fetch("""
                    SELECT typname FROM pg_type
                    WHERE typname = 'chromosome_type'
                """)
                assert len(types) == 1, \
                    "Default init-db should create chromosome_type enum (human genome)"
            finally:
                await conn.close()

        asyncio.run(verify())
