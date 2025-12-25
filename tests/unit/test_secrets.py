"""Tests for secrets management module."""

import os
import tempfile
from pathlib import Path

import pytest

from vcf_pg_loader.secrets import (
    CredentialValidationError,
    EnvSecretProvider,
    MaskedSecret,
    SecretProvider,
    get_database_password,
    get_default_provider,
    mask_password_in_url,
    validate_no_password_in_url,
)


class TestMaskedSecret:
    """Tests for MaskedSecret wrapper class."""

    def test_str_returns_masked(self):
        secret = MaskedSecret("super-secret-password")
        assert str(secret) == "***MASKED***"

    def test_repr_returns_masked(self):
        secret = MaskedSecret("super-secret-password")
        assert repr(secret) == "MaskedSecret(***)"

    def test_get_value_returns_actual_value(self):
        secret = MaskedSecret("super-secret-password")
        assert secret.get_value() == "super-secret-password"

    def test_equality_with_same_value(self):
        secret1 = MaskedSecret("password123")
        secret2 = MaskedSecret("password123")
        assert secret1 == secret2

    def test_inequality_with_different_value(self):
        secret1 = MaskedSecret("password123")
        secret2 = MaskedSecret("different")
        assert secret1 != secret2

    def test_inequality_with_non_masked_secret(self):
        secret = MaskedSecret("password123")
        assert secret != "password123"

    def test_hash_consistency(self):
        secret1 = MaskedSecret("password123")
        secret2 = MaskedSecret("password123")
        assert hash(secret1) == hash(secret2)

    def test_fstring_interpolation_is_masked(self):
        secret = MaskedSecret("super-secret-password")
        message = f"The password is: {secret}"
        assert "super-secret-password" not in message
        assert "***MASKED***" in message


class TestEnvSecretProvider:
    """Tests for EnvSecretProvider."""

    def test_get_secret_returns_env_value(self, monkeypatch):
        monkeypatch.setenv("TEST_SECRET_KEY", "test-secret-value")
        provider = EnvSecretProvider()
        assert provider.get_secret("TEST_SECRET_KEY") == "test-secret-value"

    def test_get_secret_returns_none_for_missing(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_KEY", raising=False)
        provider = EnvSecretProvider()
        assert provider.get_secret("NONEXISTENT_KEY") is None

    def test_get_secret_with_prefix(self, monkeypatch):
        monkeypatch.setenv("MY_APP_DB_PASSWORD", "secret123")
        provider = EnvSecretProvider(prefix="MY_APP_")
        assert provider.get_secret("DB_PASSWORD") == "secret123"

    def test_get_secret_masked_returns_masked_secret(self, monkeypatch):
        monkeypatch.setenv("TEST_PASSWORD", "secret123")
        provider = EnvSecretProvider()
        masked = provider.get_secret_masked("TEST_PASSWORD")
        assert isinstance(masked, MaskedSecret)
        assert masked.get_value() == "secret123"
        assert str(masked) == "***MASKED***"

    def test_get_secret_masked_returns_none_for_missing(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_KEY", raising=False)
        provider = EnvSecretProvider()
        assert provider.get_secret_masked("NONEXISTENT_KEY") is None


class TestValidateNoPasswordInUrl:
    """Tests for validate_no_password_in_url function."""

    def test_url_without_password_passes(self):
        validate_no_password_in_url("postgresql://user@localhost:5432/db")

    def test_url_with_empty_password_passes(self):
        validate_no_password_in_url("postgresql://user@localhost:5432/db")

    def test_url_with_password_raises_error(self):
        with pytest.raises(CredentialValidationError) as exc_info:
            validate_no_password_in_url("postgresql://user:password123@localhost:5432/db")
        assert "password detected" in str(exc_info.value).lower()
        assert "HIPAA" in str(exc_info.value)

    def test_url_with_special_chars_password_raises_error(self):
        with pytest.raises(CredentialValidationError):
            validate_no_password_in_url("postgresql://user:p%40ssword@localhost:5432/db")

    def test_simple_host_url_passes(self):
        validate_no_password_in_url("postgresql://localhost/db")

    def test_no_user_url_passes(self):
        validate_no_password_in_url("postgresql://localhost:5432/db")


class TestMaskPasswordInUrl:
    """Tests for mask_password_in_url function."""

    def test_masks_password_in_url(self):
        url = "postgresql://user:secret123@localhost:5432/db"
        masked = mask_password_in_url(url)
        assert "secret123" not in masked
        assert "***MASKED***" in masked
        assert "user:" in masked
        assert "@localhost" in masked

    def test_preserves_url_without_password(self):
        url = "postgresql://user@localhost:5432/db"
        assert mask_password_in_url(url) == url

    def test_preserves_url_structure(self):
        url = "postgresql://admin:mysecret@db.example.com:5432/mydb"
        masked = mask_password_in_url(url)
        assert masked.startswith("postgresql://admin:")
        assert masked.endswith("@db.example.com:5432/mydb")


class TestGetDatabasePassword:
    """Tests for get_database_password function."""

    def test_returns_password_from_custom_env_var(self, monkeypatch):
        monkeypatch.setenv("VCF_PG_LOADER_DB_PASSWORD", "custom-password")
        monkeypatch.delenv("PGPASSWORD", raising=False)
        password = get_database_password()
        assert password == "custom-password"

    def test_falls_back_to_pgpassword(self, monkeypatch):
        monkeypatch.delenv("VCF_PG_LOADER_DB_PASSWORD", raising=False)
        monkeypatch.setenv("PGPASSWORD", "pg-password")
        password = get_database_password()
        assert password == "pg-password"

    def test_custom_env_var_takes_priority(self, monkeypatch):
        monkeypatch.setenv("VCF_PG_LOADER_DB_PASSWORD", "custom-password")
        monkeypatch.setenv("PGPASSWORD", "pg-password")
        password = get_database_password()
        assert password == "custom-password"

    def test_returns_none_when_not_set(self, monkeypatch):
        monkeypatch.delenv("VCF_PG_LOADER_DB_PASSWORD", raising=False)
        monkeypatch.delenv("PGPASSWORD", raising=False)
        password = get_database_password()
        assert password is None

    def test_uses_custom_env_var_name(self, monkeypatch):
        monkeypatch.setenv("MY_CUSTOM_PASSWORD", "my-secret")
        monkeypatch.delenv("PGPASSWORD", raising=False)
        password = get_database_password(password_env_var="MY_CUSTOM_PASSWORD")
        assert password == "my-secret"


class TestGetDefaultProvider:
    """Tests for get_default_provider function."""

    def test_returns_env_secret_provider(self):
        provider = get_default_provider()
        assert isinstance(provider, EnvSecretProvider)

    def test_provider_can_read_env_vars(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", "test_value")
        provider = get_default_provider()
        assert provider.get_secret("TEST_KEY") == "test_value"


class TestSecretProviderAbstract:
    """Tests for SecretProvider abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        with pytest.raises(TypeError):
            SecretProvider()

    def test_subclass_must_implement_get_secret(self):
        class IncompleteProvider(SecretProvider):
            pass

        with pytest.raises(TypeError):
            IncompleteProvider()

    def test_subclass_with_implementation_works(self):
        class SimpleProvider(SecretProvider):
            def get_secret(self, key: str) -> str | None:
                return f"secret-{key}"

        provider = SimpleProvider()
        assert provider.get_secret("test") == "secret-test"


class TestConfigCredentialDetection:
    """Tests for credential detection in config files."""

    def test_detect_password_in_toml(self):
        from vcf_pg_loader.config import detect_credentials_in_config

        config = {"database": {"password": "secret123"}}
        detected = detect_credentials_in_config(config)
        assert "database.password" in detected

    def test_detect_db_password_key(self):
        from vcf_pg_loader.config import detect_credentials_in_config

        config = {"db_password": "secret123"}
        detected = detect_credentials_in_config(config)
        assert "db_password" in detected

    def test_no_detection_for_empty_password(self):
        from vcf_pg_loader.config import detect_credentials_in_config

        config = {"password": ""}
        detected = detect_credentials_in_config(config)
        assert len(detected) == 0

    def test_no_detection_for_normal_keys(self):
        from vcf_pg_loader.config import detect_credentials_in_config

        config = {"batch_size": 1000, "workers": 4}
        detected = detect_credentials_in_config(config)
        assert len(detected) == 0

    def test_raises_error_when_warn_only_false(self):
        from vcf_pg_loader.config import (
            CredentialInConfigError,
            detect_credentials_in_config,
        )

        config = {"password": "secret123"}
        with pytest.raises(CredentialInConfigError):
            detect_credentials_in_config(config, warn_only=False)

    def test_load_config_warns_on_credentials(self, caplog):
        from vcf_pg_loader.config import load_config

        toml_content = """
[vcf_pg_loader]
batch_size = 1000

[database]
password = "secret123"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()

            import logging

            with caplog.at_level(logging.WARNING):
                load_config(Path(f.name))

            assert any(
                "credentials detected" in record.message.lower() for record in caplog.records
            )

        os.unlink(f.name)
