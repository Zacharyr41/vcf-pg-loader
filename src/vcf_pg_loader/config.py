"""Configuration file support for vcf-pg-loader."""

import tomllib
from pathlib import Path
from typing import Any

from .loader import LoadConfig

VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""

    pass


def validate_config(config_dict: dict[str, Any]) -> None:
    """Validate configuration values.

    Raises:
        ConfigValidationError: If any configuration value is invalid.
    """
    if "batch_size" in config_dict:
        batch_size = config_dict["batch_size"]
        if not isinstance(batch_size, int):
            raise ConfigValidationError(
                f"batch_size must be an integer, got {type(batch_size).__name__}"
            )
        if batch_size <= 0:
            raise ConfigValidationError(
                f"batch_size must be positive, got {batch_size}"
            )

    if "workers" in config_dict:
        workers = config_dict["workers"]
        if not isinstance(workers, int):
            raise ConfigValidationError(
                f"workers must be an integer, got {type(workers).__name__}"
            )
        if workers <= 0:
            raise ConfigValidationError(
                f"workers must be positive, got {workers}"
            )

    if "log_level" in config_dict:
        log_level = config_dict["log_level"]
        if not isinstance(log_level, str):
            raise ConfigValidationError(
                f"log_level must be a string, got {type(log_level).__name__}"
            )
        if log_level.upper() not in VALID_LOG_LEVELS:
            raise ConfigValidationError(
                f"log_level must be one of {VALID_LOG_LEVELS}, got '{log_level}'"
            )


def load_config(
    config_path: Path,
    overrides: dict[str, Any] | None = None
) -> LoadConfig:
    """Load configuration from a TOML file.

    Args:
        config_path: Path to the TOML configuration file.
        overrides: Optional dict of values to override loaded config.

    Returns:
        LoadConfig instance with loaded values.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        ConfigValidationError: If any configuration value is invalid.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "rb") as f:
        toml_data = tomllib.load(f)

    config_dict = toml_data.get("vcf_pg_loader", {})

    if overrides:
        config_dict.update(overrides)

    validate_config(config_dict)

    valid_fields = {
        "batch_size",
        "workers",
        "drop_indexes",
        "normalize",
        "human_genome",
        "log_level",
    }

    filtered_config = {k: v for k, v in config_dict.items() if k in valid_fields}

    return LoadConfig(**filtered_config)
