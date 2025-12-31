"""TLS enforcement tests for HIPAA compliance."""

import ssl

import pytest


@pytest.mark.integration
class TestTLSConfiguration:
    """Test TLS configuration and SSL context creation."""

    def test_create_ssl_context_default(self):
        """Test default SSL context creation."""
        from vcf_pg_loader.tls import TLSConfig, create_ssl_context

        config = TLSConfig(require_tls=True, verify_server=False)
        ctx = create_ssl_context(config)

        assert ctx is not None
        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2

    def test_create_ssl_context_disabled(self):
        """Test SSL context when TLS disabled."""
        from vcf_pg_loader.tls import TLSConfig, create_ssl_context

        config = TLSConfig(require_tls=False)
        ctx = create_ssl_context(config)

        assert ctx is None

    def test_ssl_context_minimum_version(self):
        """Test minimum TLS version is 1.2."""
        from vcf_pg_loader.tls import MIN_TLS_VERSION, TLSConfig, create_ssl_context

        config = TLSConfig(require_tls=True, verify_server=False)
        ctx = create_ssl_context(config)

        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
        assert MIN_TLS_VERSION == ssl.TLSVersion.TLSv1_2

    def test_tls_config_from_env(self, monkeypatch):
        """Test TLS config loading from environment."""
        from vcf_pg_loader.tls import TLSConfig

        monkeypatch.setenv("VCF_PG_LOADER_REQUIRE_TLS", "true")
        monkeypatch.setenv("VCF_PG_LOADER_TLS_VERIFY", "false")

        config = TLSConfig.from_env()

        assert config.require_tls is True
        assert config.verify_server is False

    def test_tls_config_from_env_disabled(self, monkeypatch):
        """Test TLS disabled via environment."""
        from vcf_pg_loader.tls import TLSConfig

        monkeypatch.setenv("VCF_PG_LOADER_REQUIRE_TLS", "false")

        config = TLSConfig.from_env()

        assert config.require_tls is False

    def test_get_ssl_param_require_mode(self):
        """Test SSL parameter for asyncpg with require mode."""
        from vcf_pg_loader.tls import TLSConfig, get_ssl_param_for_asyncpg

        config = TLSConfig(require_tls=True, verify_server=False)
        ssl_param = get_ssl_param_for_asyncpg(config)

        assert ssl_param == "require"

    def test_get_ssl_param_verify_full_mode(self):
        """Test SSL parameter for asyncpg with verify-full mode."""
        from vcf_pg_loader.tls import TLSConfig, get_ssl_param_for_asyncpg

        config = TLSConfig(require_tls=True, verify_server=True)
        ssl_param = get_ssl_param_for_asyncpg(config)

        assert ssl_param == "verify-full"

    def test_get_ssl_param_disabled(self):
        """Test SSL parameter when TLS disabled."""
        from vcf_pg_loader.tls import TLSConfig, get_ssl_param_for_asyncpg

        config = TLSConfig(require_tls=False)
        ssl_param = get_ssl_param_for_asyncpg(config)

        assert ssl_param is False


@pytest.mark.integration
class TestTLSCertificateValidation:
    """Test TLS certificate path validation."""

    def test_missing_ca_cert_raises_error(self, tmp_path):
        """Test that missing CA cert raises TLSError."""
        from pathlib import Path

        from vcf_pg_loader.tls import TLSConfig, TLSError, create_ssl_context

        config = TLSConfig(
            require_tls=True,
            verify_server=True,
            ca_cert_path=Path("/nonexistent/ca.crt"),
        )

        with pytest.raises(TLSError) as exc_info:
            create_ssl_context(config)

        assert "CA certificate not found" in str(exc_info.value)

    def test_missing_client_cert_raises_error(self, tmp_path):
        """Test that missing client cert raises TLSError."""
        from pathlib import Path

        from vcf_pg_loader.tls import TLSConfig, TLSError, create_ssl_context

        config = TLSConfig(
            require_tls=True,
            verify_server=False,
            client_cert_path=Path("/nonexistent/client.crt"),
            client_key_path=tmp_path / "client.key",
        )

        (tmp_path / "client.key").touch()

        with pytest.raises(TLSError) as exc_info:
            create_ssl_context(config)

        assert "Client certificate not found" in str(exc_info.value)


@pytest.mark.integration
class TestLoaderTLSIntegration:
    """Test TLS integration with VCF loader."""

    def test_load_config_accepts_tls_config(self):
        """Test LoadConfig accepts TLS configuration."""
        from vcf_pg_loader.loader import LoadConfig
        from vcf_pg_loader.tls import TLSConfig

        tls_config = TLSConfig(require_tls=True)
        config = LoadConfig(tls_config=tls_config)

        assert config.tls_config is tls_config
        assert config.tls_config.require_tls is True

    def test_load_config_default_tls_none(self):
        """Test LoadConfig has no TLS config by default."""
        from vcf_pg_loader.loader import LoadConfig

        config = LoadConfig()

        assert config.tls_config is None


@pytest.mark.integration
class TestDoctorTLSChecks:
    """Test doctor command TLS checks."""

    def test_check_tls_support(self):
        """Test TLS support check passes on normal systems."""
        from vcf_pg_loader.doctor import DependencyChecker

        checker = DependencyChecker()
        result = checker.check_tls_support()

        assert result.passed is True
        assert "TLS 1.2" in result.version

    def test_check_tls_certificates_default(self, monkeypatch):
        """Test TLS certificate check with no custom certs configured."""
        from vcf_pg_loader.doctor import DependencyChecker

        monkeypatch.delenv("VCF_PG_LOADER_TLS_CA_CERT", raising=False)
        monkeypatch.delenv("VCF_PG_LOADER_TLS_CLIENT_CERT", raising=False)

        checker = DependencyChecker()
        result = checker.check_tls_certificates()

        assert result.passed is True
        assert "system defaults" in result.version

    def test_check_tls_certificates_missing_ca(self, monkeypatch):
        """Test TLS certificate check detects missing CA cert."""
        from vcf_pg_loader.doctor import DependencyChecker

        monkeypatch.setenv("VCF_PG_LOADER_TLS_CA_CERT", "/nonexistent/ca.crt")

        checker = DependencyChecker()
        result = checker.check_tls_certificates()

        assert result.passed is False
        assert "CA cert not found" in result.message

    def test_check_all_includes_tls_checks(self):
        """Test check_all includes TLS checks."""
        from vcf_pg_loader.doctor import DependencyChecker

        checker = DependencyChecker()
        results = checker.check_all()

        names = [r.name for r in results]
        assert "TLS Support" in names
        assert "TLS Certificates" in names
