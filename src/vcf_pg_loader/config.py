"""Configuration file support for vcf-pg-loader."""

import logging
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .loader import LoadConfig

logger = logging.getLogger(__name__)


@dataclass
class PHIDetectionConfig:
    """Configuration for PHI detection during VCF loading."""

    enabled: bool = True
    sample_rate: float = 0.01
    scan_headers: bool = True
    scan_info_fields: bool = True
    scan_sample_ids: bool = True

    critical_action: str = "abort"
    high_action: str = "pause"
    medium_action: str = "warn"
    low_action: str = "log"

    slack_webhook: str | None = None
    email: str | None = None
    email_from: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587

    custom_patterns_path: Path | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PHIDetectionConfig":
        actions = data.get("actions", {})
        alerts = data.get("alerts", {})

        patterns_path = data.get("custom_patterns_path")
        if patterns_path:
            patterns_path = Path(patterns_path)

        return cls(
            enabled=data.get("enabled", True),
            sample_rate=data.get("sample_rate", 0.01),
            scan_headers=data.get("scan_headers", True),
            scan_info_fields=data.get("scan_info_fields", True),
            scan_sample_ids=data.get("scan_sample_ids", True),
            critical_action=actions.get("critical", "abort"),
            high_action=actions.get("high", "pause"),
            medium_action=actions.get("medium", "warn"),
            low_action=actions.get("low", "log"),
            slack_webhook=alerts.get("slack_webhook"),
            email=alerts.get("email"),
            email_from=alerts.get("email_from"),
            smtp_host=alerts.get("smtp_host"),
            smtp_port=alerts.get("smtp_port", 587),
            custom_patterns_path=patterns_path,
        )

    def to_alert_config_dict(self) -> dict[str, Any]:
        return {
            "actions": {
                "critical": self.critical_action,
                "high": self.high_action,
                "medium": self.medium_action,
                "low": self.low_action,
            },
            "alerts": {
                "slack_webhook": self.slack_webhook,
                "email": self.email,
                "email_from": self.email_from,
                "smtp_host": self.smtp_host,
                "smtp_port": self.smtp_port,
            },
        }


VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

CREDENTIAL_KEYS = {
    "password",
    "db_password",
    "database_password",
    "secret",
    "api_key",
    "token",
    "credentials",
    "auth",
}


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""

    pass


class CredentialInConfigError(Exception):
    """Raised when credentials are detected in configuration files."""

    pass


def detect_credentials_in_config(
    config_dict: dict[str, Any],
    path: str = "",
    warn_only: bool = True,
) -> list[str]:
    """Detect potential credentials in configuration dictionary.

    Args:
        config_dict: Configuration dictionary to check.
        path: Current path in nested config (for error messages).
        warn_only: If True, emit warning. If False, raise error.

    Returns:
        List of detected credential key paths.

    Raises:
        CredentialInConfigError: If credentials found and warn_only=False.
    """
    detected = []

    for key, value in config_dict.items():
        current_path = f"{path}.{key}" if path else key
        key_lower = key.lower()

        if key_lower in CREDENTIAL_KEYS or any(
            cred_key in key_lower for cred_key in CREDENTIAL_KEYS
        ):
            if value and value != "":
                detected.append(current_path)

        if isinstance(value, dict):
            detected.extend(detect_credentials_in_config(value, current_path, warn_only=True))

    if detected:
        msg = (
            f"Potential credentials detected in config file: {', '.join(detected)}. "
            "For HIPAA compliance, secrets must be provided via environment "
            "variables or secrets manager, not configuration files."
        )
        if warn_only:
            logger.warning(msg)
        else:
            raise CredentialInConfigError(msg)

    return detected


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
            raise ConfigValidationError(f"batch_size must be positive, got {batch_size}")

    if "workers" in config_dict:
        workers = config_dict["workers"]
        if not isinstance(workers, int):
            raise ConfigValidationError(f"workers must be an integer, got {type(workers).__name__}")
        if workers <= 0:
            raise ConfigValidationError(f"workers must be positive, got {workers}")

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


def load_config(config_path: Path, overrides: dict[str, Any] | None = None) -> LoadConfig:
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

    detect_credentials_in_config(toml_data, warn_only=True)

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


def load_phi_detection_config(config_path: Path) -> PHIDetectionConfig:
    """Load PHI detection configuration from a TOML file.

    Args:
        config_path: Path to the TOML configuration file.

    Returns:
        PHIDetectionConfig instance with loaded values.
    """
    if not config_path.exists():
        return PHIDetectionConfig()

    with open(config_path, "rb") as f:
        toml_data = tomllib.load(f)

    phi_data = toml_data.get("phi_detection", {})
    if not phi_data:
        return PHIDetectionConfig()

    return PHIDetectionConfig.from_dict(phi_data)
