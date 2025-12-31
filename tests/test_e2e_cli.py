"""End-to-end CLI tests using real VCF files and PostgreSQL.

These tests verify the complete pipeline:
1. Initialize database schema via CLI
2. Load VCF files via CLI
3. Validate data via CLI
4. Assert data integrity in PostgreSQL
"""

import os
import subprocess
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from conftest import parse_db_credentials

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def postgres_container():
    """Provide a PostgreSQL test container for the module."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def db_credentials(postgres_container):
    """Provide database credentials for CLI commands.

    Returns tuple of (cli_url, password, full_url):
    - cli_url: URL without password for CLI --db argument
    - password: Password for VCF_PG_LOADER_DB_PASSWORD env var
    - full_url: Complete URL for asyncpg connections
    """
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname
    full_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    cli_url, pwd = parse_db_credentials(full_url)
    return cli_url, pwd, full_url


@pytest.fixture(scope="module")
def initialized_db(db_credentials):
    """Initialize database schema before tests."""
    cli_url, password, full_url = db_credentials
    env = {
        **os.environ,
        "VCF_PG_LOADER_DB_PASSWORD": password,
        "VCF_PG_LOADER_REQUIRE_TLS": "false",
    }
    result = subprocess.run(
        ["uv", "run", "vcf-pg-loader", "init-db", "--db", cli_url, "--no-require-tls"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    assert result.returncode == 0, f"init-db failed: {result.stdout}"
    return cli_url, password, full_url


@pytest.fixture(scope="class")
def postgres_container_non_human():
    """Provide a separate PostgreSQL container for non-human genome tests."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="class")
def initialized_db_non_human(postgres_container_non_human):
    """Initialize non-human genome database schema."""
    host = postgres_container_non_human.get_container_host_ip()
    port = postgres_container_non_human.get_exposed_port(5432)
    user = postgres_container_non_human.username
    password = postgres_container_non_human.password
    database = postgres_container_non_human.dbname
    full_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    cli_url, pwd = parse_db_credentials(full_url)

    env = {**os.environ, "VCF_PG_LOADER_DB_PASSWORD": pwd, "VCF_PG_LOADER_REQUIRE_TLS": "false"}
    result = subprocess.run(
        [
            "uv",
            "run",
            "vcf-pg-loader",
            "init-db",
            "--db",
            cli_url,
            "--no-human-genome",
            "--no-require-tls",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    assert result.returncode == 0, f"init-db failed: {result.stderr}"
    return cli_url, pwd, full_url


@pytest.mark.integration
class TestE2ECLI:
    """End-to-end CLI tests."""

    def test_cli_load_strelka_snvs(self, initialized_db):
        """Should load Strelka SNVs VCF via CLI and verify in database."""
        vcf_path = FIXTURES_DIR / "strelka_snvs_chr22.vcf.gz"
        cli_url, password, full_url = initialized_db
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }

        result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "load",
                str(vcf_path),
                "--db",
                cli_url,
                "--batch",
                "500",
                "--workers",
                "2",
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )

        assert result.returncode == 0, f"load failed: {result.stdout}"
        assert "Loaded" in result.stdout
        assert "variants" in result.stdout

        load_batch_id = None
        for line in result.stdout.split("\n"):
            if "Batch ID:" in line:
                load_batch_id = line.split("Batch ID:")[1].strip()
                break

        assert load_batch_id is not None, "Batch ID not found in output"

        async def verify_data():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", load_batch_id
                )
                assert count == 2627, f"Expected 2627 variants, got {count}"

                chr22_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE chrom = 'chr22'"
                )
                assert chr22_count >= 2627

                audit = await conn.fetchrow(
                    "SELECT * FROM variant_load_audit WHERE load_batch_id = $1::uuid", load_batch_id
                )
                assert audit is not None
                assert audit["status"] == "completed"
                assert audit["variants_loaded"] == 2627
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify_data())

    def test_cli_validate_command(self, initialized_db):
        """Should validate a loaded batch via CLI."""
        vcf_path = FIXTURES_DIR / "mutect2_chr22.vcf.gz"
        cli_url, password, full_url = initialized_db
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }

        load_result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "load",
                str(vcf_path),
                "--db",
                cli_url,
                "--batch",
                "100",
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert load_result.returncode == 0

        load_batch_id = None
        for line in load_result.stdout.split("\n"):
            if "Batch ID:" in line:
                load_batch_id = line.split("Batch ID:")[1].strip()
                break

        assert load_batch_id is not None

        validate_result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "validate",
                load_batch_id,
                "--db",
                cli_url,
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        assert validate_result.returncode == 0, f"validate failed: {validate_result.stderr}"
        assert "Validation passed" in validate_result.stdout

    def test_cli_load_annotated_vcf(self, initialized_db):
        """Should load annotated VCF and preserve annotations."""
        vcf_path = FIXTURES_DIR / "annotated_ranked.vcf.gz"
        cli_url, password, full_url = initialized_db
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }

        result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "load",
                str(vcf_path),
                "--db",
                cli_url,
                "--batch",
                "50",
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )

        assert result.returncode == 0, f"load failed: {result.stderr}"

        load_batch_id = None
        for line in result.stdout.split("\n"):
            if "Batch ID:" in line:
                load_batch_id = line.split("Batch ID:")[1].strip()
                break

        async def verify_annotations():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", load_batch_id
                )
                assert count >= 113

                chromosomes = await conn.fetch(
                    "SELECT DISTINCT chrom FROM variants WHERE load_batch_id = $1::uuid",
                    load_batch_id,
                )
                chroms = {r["chrom"] for r in chromosomes}
                assert "chr16" in chroms
                assert "chrX" in chroms

                sample = await conn.fetchrow(
                    "SELECT * FROM variants WHERE load_batch_id = $1::uuid LIMIT 1", load_batch_id
                )
                assert sample is not None
                assert sample["chrom"] is not None
                assert sample["pos"] > 0
                assert sample["ref"] is not None
                assert sample["alt"] is not None
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify_annotations())

    def test_cli_error_on_missing_file(self, initialized_db):
        """Should exit with error for missing VCF file."""
        cli_url, password, full_url = initialized_db
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }
        result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "load",
                "/nonexistent/file.vcf",
                "--db",
                cli_url,
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        assert result.returncode == 1
        assert "not found" in result.stdout.lower() or "error" in result.stdout.lower()

    def test_cli_error_on_invalid_batch_id(self, initialized_db):
        """Should exit with error for invalid batch ID in validate."""
        cli_url, password, full_url = initialized_db
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }
        result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "validate",
                "not-a-valid-uuid",
                "--db",
                cli_url,
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        assert result.returncode == 1
        assert "invalid" in result.stdout.lower() or "error" in result.stdout.lower()


@pytest.mark.integration
class TestE2EDataIntegrity:
    """Tests for data integrity after CLI operations."""

    def test_no_duplicate_variants(self, initialized_db):
        """Should not create duplicate variants from the same load."""
        vcf_path = FIXTURES_DIR / "mills_indels.vcf.gz"
        cli_url, password, full_url = initialized_db
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }

        result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "load",
                str(vcf_path),
                "--db",
                cli_url,
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert result.returncode == 0

        load_batch_id = None
        for line in result.stdout.split("\n"):
            if "Batch ID:" in line:
                load_batch_id = line.split("Batch ID:")[1].strip()
                break

        async def check_duplicates():
            conn = await asyncpg.connect(full_url)
            try:
                duplicates = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT chrom, pos, ref, alt, COUNT(*)
                        FROM variants WHERE load_batch_id = $1::uuid
                        GROUP BY chrom, pos, ref, alt
                        HAVING COUNT(*) > 1
                    ) dupes
                """,
                    load_batch_id,
                )
                assert duplicates == 0, f"Found {duplicates} duplicate variants"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(check_duplicates())

    def test_multiallelic_decomposition(self, initialized_db):
        """Should correctly decompose multi-allelic variants."""
        vcf_path = FIXTURES_DIR / "dbsnp_subset.vcf.gz"
        cli_url, password, full_url = initialized_db
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }

        result = subprocess.run(
            [
                "uv",
                "run",
                "vcf-pg-loader",
                "load",
                str(vcf_path),
                "--db",
                cli_url,
                "--batch",
                "500",
                "--no-require-tls",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert result.returncode == 0

        load_batch_id = None
        for line in result.stdout.split("\n"):
            if "Batch ID:" in line:
                load_batch_id = line.split("Batch ID:")[1].strip()
                break

        async def verify_decomposition():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", load_batch_id
                )
                assert count == 2216, f"Expected 2216 decomposed variants, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify_decomposition())


@pytest.mark.integration
class TestE2EAllFixtures:
    """E2E tests for all VCF fixture files."""

    def _load_vcf_and_get_batch_id(
        self,
        vcf_path: Path,
        cli_url: str,
        password: str,
        extra_args: list[str] | None = None,
    ) -> tuple[str, str]:
        """Helper to load a VCF and return (batch_id, stdout)."""
        cmd = [
            "uv",
            "run",
            "vcf-pg-loader",
            "load",
            str(vcf_path),
            "--db",
            cli_url,
            "--batch",
            "100",
            "--no-require-tls",
        ]
        if extra_args:
            cmd.extend(extra_args)
        env = {
            **os.environ,
            "VCF_PG_LOADER_DB_PASSWORD": password,
            "VCF_PG_LOADER_REQUIRE_TLS": "false",
        }
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        return result, self._extract_batch_id(result.stdout)

    def _extract_batch_id(self, stdout: str) -> str | None:
        """Extract batch ID from CLI output."""
        for line in stdout.split("\n"):
            if "Batch ID:" in line:
                return line.split("Batch ID:")[1].strip()
        return None

    def test_load_empty_vcf(self, initialized_db):
        """Should handle empty VCF (header only, no variants)."""
        vcf_path = FIXTURES_DIR / "empty.vcf.gz"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"
        assert "Loaded 0 variants" in result.stdout

    def test_load_genmod_sv(self, initialized_db):
        """Should load structural variants with VEP annotations."""
        vcf_path = FIXTURES_DIR / "genmod_sv.vcf.gz"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count >= 57, f"Expected >= 57 SV variants, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_gnomad_subset(self, initialized_db):
        """Should load gnomAD population frequency data."""
        vcf_path = FIXTURES_DIR / "gnomad_subset.vcf.gz"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count == 3500, f"Expected 3500 gnomAD variants, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_gridss_sv(self, initialized_db):
        """Should load GRIDSS structural variants with BND format."""
        vcf_path = FIXTURES_DIR / "gridss_sv.vcf"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count == 192, f"Expected 192 GRIDSS BND variants, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_gvcf_sample(self, initialized_db):
        """Should load gVCF with NON_REF alleles."""
        vcf_path = FIXTURES_DIR / "gvcf_sample.vcf.gz"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert (
                    count == 136
                ), f"Expected 136 gVCF records (130 lines + 6 from multi-allelic decomposition), got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_multiallelic(self, initialized_db):
        """Should decompose multi-allelic variants."""
        vcf_path = FIXTURES_DIR / "multiallelic.vcf"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count == 8, f"Expected 8 decomposed variants, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_pacbio_repeats(self, initialized_db_non_human):
        """Should load PacBio PBSV repeat annotations (non-standard chromosome)."""
        vcf_path = FIXTURES_DIR / "pacbio_repeats.vcf.gz"
        cli_url, password, full_url = initialized_db_non_human

        result, batch_id = self._load_vcf_and_get_batch_id(
            vcf_path, cli_url, password, ["--no-human-genome"]
        )

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count == 1, f"Expected 1 PacBio variant, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_sarscov2(self, initialized_db_non_human):
        """Should load non-human (SARS-CoV-2) VCF."""
        vcf_path = FIXTURES_DIR / "sarscov2.vcf.gz"
        cli_url, password, full_url = initialized_db_non_human

        result, batch_id = self._load_vcf_and_get_batch_id(
            vcf_path, cli_url, password, ["--no-human-genome"]
        )

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count == 9, f"Expected 9 SARS-CoV-2 variants, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_strelka_indels(self, initialized_db):
        """Should load Strelka2 somatic indels."""
        vcf_path = FIXTURES_DIR / "strelka_indels_chr22.vcf.gz"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count == 140, f"Expected 140 Strelka indels, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())

    def test_load_with_annotations(self, initialized_db):
        """Should load VCF with BCSQ annotations."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        cli_url, password, full_url = initialized_db

        result, batch_id = self._load_vcf_and_get_batch_id(vcf_path, cli_url, password)

        assert result.returncode == 0, f"load failed: {result.stdout}"

        async def verify():
            conn = await asyncpg.connect(full_url)
            try:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM variants WHERE load_batch_id = $1::uuid", batch_id
                )
                assert count == 4, f"Expected 4 annotated variants, got {count}"
            finally:
                await conn.close()

        import asyncio

        asyncio.run(verify())
