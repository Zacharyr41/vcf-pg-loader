"""Pytest configuration and fixtures for VCF-PG-Loader tests."""

import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pytest


def parse_db_credentials(url: str) -> tuple[str, str]:
    """Parse database URL into password-less URL and password.

    For HIPAA compliance, CLI tests must pass password via environment variable.
    This helper extracts the password and returns a URL without it.

    Args:
        url: Full database connection URL with embedded password.

    Returns:
        Tuple of (password_less_url, password).
    """
    if url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://")

    parsed = urlparse(url)
    password = parsed.password or ""

    if parsed.username:
        netloc = f"{parsed.username}@{parsed.hostname}"
    else:
        netloc = parsed.hostname or ""

    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"

    password_less_url = urlunparse(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )

    return password_less_url, password


sys.path.insert(0, str(Path(__file__).parent))

from fixtures.gwas_generator import (  # noqa: E402
    GWASStudyMetadata,
    GWASSummaryStatistic,
    make_gwas_ssf_file_basic,
    make_gwas_ssf_file_for_variant_matching,
    make_gwas_ssf_file_minimal,
    make_gwas_ssf_file_strand_ambiguous,
)
from fixtures.nf_core_datasets import GIABDataManager  # noqa: E402
from fixtures.vcf_generator import (  # noqa: E402
    SyntheticVariant,
    VCFGenerator,
    make_genmod_vcf_file,
    make_multiallelic_vcf_file,
    make_trio_vcf_file,
    make_unnormalized_vcf_file,
    make_vep_csq_vcf_file,
)

try:
    from testcontainers.postgres import PostgresContainer

    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return path to test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def test_data_manager() -> GIABDataManager:
    """Manage test data downloads with caching."""
    return GIABDataManager()


@pytest.fixture(scope="session")
def postgres_container():
    """Create a PostgreSQL container for integration tests."""
    if not HAS_TESTCONTAINERS:
        pytest.skip("testcontainers not installed")

    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture
async def test_db(postgres_container):
    """Create isolated test database connection."""
    import asyncpg

    from vcf_pg_loader.schema import SchemaManager

    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://")

    conn = await asyncpg.connect(url)

    schema_manager = SchemaManager(human_genome=True)
    await schema_manager.create_schema(conn)

    yield conn

    await conn.close()


@pytest.fixture
async def test_db_non_human(postgres_container):
    """Create isolated test database for non-human genome."""
    import asyncpg

    from vcf_pg_loader.schema import SchemaManager

    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://")

    conn = await asyncpg.connect(url)

    schema_manager = SchemaManager(human_genome=False)
    await schema_manager.create_schema(conn)

    yield conn

    await conn.close()


@pytest.fixture
def vcf_generator():
    """Provide VCFGenerator class for tests."""
    return VCFGenerator


@pytest.fixture
def synthetic_variant_factory():
    """Factory for creating SyntheticVariant instances."""

    def _factory(**kwargs):
        defaults = {
            "chrom": "chr1",
            "pos": 100,
            "ref": "A",
            "alt": ["G"],
        }
        defaults.update(kwargs)
        return SyntheticVariant(**defaults)

    return _factory


@pytest.fixture
def multiallelic_vcf_file():
    """Generate a VCF file with multi-allelic variants."""
    path = make_multiallelic_vcf_file(n_alts=3)
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def unnormalized_vcf_file():
    """Generate a VCF file with unnormalized variants."""
    path = make_unnormalized_vcf_file()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def vep_csq_vcf_file():
    """Generate a VCF file with VEP CSQ annotations."""
    path = make_vep_csq_vcf_file()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def trio_vcf_file():
    """Generate a VCF file with trio inheritance patterns."""
    path = make_trio_vcf_file()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def genmod_vcf_file():
    """Generate a VCF file with GENMOD annotations."""
    path = make_genmod_vcf_file()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture(scope="session")
def giab_chr21_vcf(test_data_manager):
    """GIAB HG002 chr21 subset for fast testing (if available)."""
    path = test_data_manager.get_giab_chr21("HG002")
    if path is None or not path.exists():
        pytest.skip("GIAB HG002 chr21 data not available")
    return path


@pytest.fixture(scope="session")
def giab_trio_vcfs(test_data_manager):
    """Full GIAB Ashkenazi trio VCFs (if available)."""
    try:
        return {
            "proband": test_data_manager.get_vcf("HG002_benchmark"),
            "father": test_data_manager.get_vcf("HG003_benchmark"),
            "mother": test_data_manager.get_vcf("HG004_benchmark"),
        }
    except Exception:
        pytest.skip("GIAB trio data not available")


@pytest.fixture
def sarek_test_vcf(test_data_manager):
    """Get sarek test output VCF."""
    vcf_path = test_data_manager.get_nf_core_output("sarek", "annotation")
    if vcf_path is None:
        pytest.skip("Sarek test output not available")
    return vcf_path


@pytest.fixture
def raredisease_vcf(test_data_manager):
    """Get raredisease test output VCF."""
    vcf_path = test_data_manager.get_nf_core_output("raredisease", "variants")
    if vcf_path is None:
        pytest.skip("Raredisease test output not available")
    return vcf_path


def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests requiring database")
    config.addinivalue_line("markers", "acceptance: marks acceptance tests")
    config.addinivalue_line("markers", "nf_core: marks tests requiring nf-core test data")
    config.addinivalue_line("markers", "performance: marks performance benchmark tests")
    config.addinivalue_line("markers", "validation: marks validation tests")
    config.addinivalue_line("markers", "giab: marks tests requiring GIAB benchmark data")
    config.addinivalue_line(
        "markers", "giab_full: marks tests requiring full GIAB files (~4M variants each)"
    )
    config.addinivalue_line(
        "markers", "benchmark: marks benchmark tests for performance measurement"
    )
    config.addinivalue_line("markers", "gwas: marks GWAS import tests")


@pytest.fixture
def gwas_ssf_file_basic():
    """Generate a basic GWAS-SSF file."""
    path = make_gwas_ssf_file_basic()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def gwas_ssf_file_strand_ambiguous():
    """Generate a GWAS-SSF file with strand-ambiguous variants."""
    path = make_gwas_ssf_file_strand_ambiguous()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def gwas_ssf_file_for_variant_matching():
    """Generate a GWAS-SSF file for variant matching tests."""
    path = make_gwas_ssf_file_for_variant_matching()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def gwas_ssf_file_minimal():
    """Generate a minimal GWAS-SSF file."""
    path = make_gwas_ssf_file_minimal()
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture
def gwas_summary_stat_factory():
    """Factory for creating GWASSummaryStatistic instances."""

    def _factory(**kwargs):
        defaults = {
            "chromosome": "1",
            "base_pair_location": 100,
            "effect_allele": "A",
            "other_allele": "G",
            "p_value": 1e-8,
        }
        defaults.update(kwargs)
        return GWASSummaryStatistic(**defaults)

    return _factory


@pytest.fixture
def gwas_study_metadata_factory():
    """Factory for creating GWASStudyMetadata instances."""

    def _factory(**kwargs):
        defaults = {
            "study_accession": "GCST90002357",
            "trait_name": "Height",
            "genome_build": "GRCh38",
        }
        defaults.update(kwargs)
        return GWASStudyMetadata(**defaults)

    return _factory


@pytest.fixture
def test_tls_config():
    """TLS configuration for tests - disables TLS for testcontainers."""
    from vcf_pg_loader.tls import TLSConfig

    return TLSConfig(require_tls=False)


@pytest.fixture(autouse=True)
def disable_tls_for_tests(monkeypatch):
    """Disable TLS requirement for all tests using testcontainers.

    This is needed because testcontainers PostgreSQL doesn't support TLS,
    and the loader defaults to requiring TLS for HIPAA compliance.
    """
    monkeypatch.setenv("VCF_PG_LOADER_REQUIRE_TLS", "false")
