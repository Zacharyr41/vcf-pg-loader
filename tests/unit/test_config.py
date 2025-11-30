"""Tests for TOML configuration file support."""

import tempfile
from pathlib import Path

import pytest

from vcf_pg_loader.config import ConfigValidationError, load_config


class TestLoadConfigFromToml:
    """Tests for loading configuration from TOML files."""

    def test_load_config_from_toml_file(self):
        """Should load configuration from a TOML file."""
        toml_content = """
[vcf_pg_loader]
batch_size = 10000
workers = 4
normalize = false
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            config = load_config(Path(f.name))

        assert config.batch_size == 10000
        assert config.workers == 4
        assert config.normalize is False

    def test_load_config_with_all_options(self):
        """Should load all configuration options from TOML."""
        toml_content = """
[vcf_pg_loader]
batch_size = 25000
workers = 16
drop_indexes = false
normalize = true
human_genome = false
log_level = "DEBUG"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            config = load_config(Path(f.name))

        assert config.batch_size == 25000
        assert config.workers == 16
        assert config.drop_indexes is False
        assert config.normalize is True
        assert config.human_genome is False
        assert config.log_level == "DEBUG"

    def test_load_config_uses_defaults_for_missing_keys(self):
        """Should use defaults for missing configuration keys."""
        toml_content = """
[vcf_pg_loader]
batch_size = 5000
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            config = load_config(Path(f.name))

        assert config.batch_size == 5000
        assert config.workers == 8
        assert config.drop_indexes is True
        assert config.normalize is True

    def test_load_config_returns_defaults_for_empty_file(self):
        """Should return defaults for empty TOML file."""
        toml_content = ""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            config = load_config(Path(f.name))

        assert config.batch_size == 50_000
        assert config.workers == 8

    def test_load_config_returns_defaults_for_missing_section(self):
        """Should return defaults if vcf_pg_loader section is missing."""
        toml_content = """
[other_section]
key = "value"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            config = load_config(Path(f.name))

        assert config.batch_size == 50_000

    def test_load_config_file_not_found(self):
        """Should raise FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.toml"))


class TestConfigValidation:
    """Tests for configuration validation."""

    def test_invalid_batch_size_raises_error(self):
        """Should raise error for invalid batch_size."""
        toml_content = """
[vcf_pg_loader]
batch_size = -1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            with pytest.raises(ConfigValidationError) as exc_info:
                load_config(Path(f.name))

        assert "batch_size" in str(exc_info.value)

    def test_invalid_workers_raises_error(self):
        """Should raise error for invalid workers."""
        toml_content = """
[vcf_pg_loader]
workers = 0
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            with pytest.raises(ConfigValidationError) as exc_info:
                load_config(Path(f.name))

        assert "workers" in str(exc_info.value)

    def test_invalid_log_level_raises_error(self):
        """Should raise error for invalid log_level."""
        toml_content = """
[vcf_pg_loader]
log_level = "INVALID"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            with pytest.raises(ConfigValidationError) as exc_info:
                load_config(Path(f.name))

        assert "log_level" in str(exc_info.value)

    def test_wrong_type_raises_error(self):
        """Should raise error for wrong type."""
        toml_content = """
[vcf_pg_loader]
batch_size = "not_a_number"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            with pytest.raises(ConfigValidationError):
                load_config(Path(f.name))


class TestConfigOverrides:
    """Tests for configuration overrides."""

    def test_override_with_dict(self):
        """Should allow overriding loaded config with dict."""
        toml_content = """
[vcf_pg_loader]
batch_size = 10000
workers = 4
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml_content)
            f.flush()
            config = load_config(Path(f.name), overrides={"workers": 8})

        assert config.batch_size == 10000
        assert config.workers == 8
