"""Secure secrets management for HIPAA compliance.

Provides abstraction for retrieving secrets from various backends
without exposing values in logs or error messages.

HIPAA 164.312(d) - Person or Entity Authentication
"""

import logging
import os
import re
from abc import ABC, abstractmethod
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class MaskedSecret:
    """Wrapper that prevents accidental exposure of secret values."""

    def __init__(self, value: str):
        self._value = value

    def get_value(self) -> str:
        return self._value

    def __str__(self) -> str:
        return "***MASKED***"

    def __repr__(self) -> str:
        return "MaskedSecret(***)"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MaskedSecret):
            return self._value == other._value
        return False

    def __hash__(self) -> int:
        return hash(self._value)


class SecretProvider(ABC):
    """Abstract base class for secrets backends."""

    @abstractmethod
    def get_secret(self, key: str) -> str | None:
        """Retrieve a secret value by key.

        Args:
            key: The secret key/name to retrieve.

        Returns:
            The secret value, or None if not found.
        """
        pass

    def get_secret_masked(self, key: str) -> MaskedSecret | None:
        """Retrieve a secret wrapped in MaskedSecret.

        Args:
            key: The secret key/name to retrieve.

        Returns:
            MaskedSecret wrapping the value, or None if not found.
        """
        value = self.get_secret(key)
        if value is not None:
            return MaskedSecret(value)
        return None


class EnvSecretProvider(SecretProvider):
    """Retrieve secrets from environment variables."""

    def __init__(self, prefix: str = ""):
        self.prefix = prefix

    def get_secret(self, key: str) -> str | None:
        full_key = f"{self.prefix}{key}" if self.prefix else key
        value = os.environ.get(full_key)
        if value is not None:
            logger.debug("Secret loaded from environment variable: %s", full_key)
        return value


class AWSSecretsManagerProvider(SecretProvider):
    """Retrieve secrets from AWS Secrets Manager.

    Requires: pip install boto3
    """

    def __init__(
        self,
        region_name: str | None = None,
        secret_prefix: str = "",
    ):
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "AWS Secrets Manager support requires boto3. " "Install with: pip install boto3"
            ) from e

        self.client = boto3.client("secretsmanager", region_name=region_name)
        self.secret_prefix = secret_prefix

    def get_secret(self, key: str) -> str | None:
        import json

        from botocore.exceptions import ClientError

        secret_name = f"{self.secret_prefix}{key}" if self.secret_prefix else key

        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            secret_string = response.get("SecretString")
            if secret_string:
                try:
                    parsed = json.loads(secret_string)
                    if isinstance(parsed, dict):
                        return parsed.get("password") or parsed.get("value") or secret_string
                except json.JSONDecodeError:
                    return secret_string
            return None
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceNotFoundException":
                logger.debug("Secret not found in AWS Secrets Manager: %s", secret_name)
                return None
            raise


class VaultProvider(SecretProvider):
    """Retrieve secrets from HashiCorp Vault.

    Requires: pip install hvac
    """

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        mount_point: str = "secret",
        path_prefix: str = "",
    ):
        try:
            import hvac
        except ImportError as e:
            raise ImportError(
                "HashiCorp Vault support requires hvac. " "Install with: pip install hvac"
            ) from e

        self.url = url or os.environ.get("VAULT_ADDR", "http://localhost:8200")
        self.token = token or os.environ.get("VAULT_TOKEN")
        self.mount_point = mount_point
        self.path_prefix = path_prefix

        self.client = hvac.Client(url=self.url, token=self.token)
        if not self.client.is_authenticated():
            raise ValueError("Vault authentication failed. Check VAULT_TOKEN.")

    def get_secret(self, key: str) -> str | None:
        path = f"{self.path_prefix}{key}" if self.path_prefix else key

        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=self.mount_point,
            )
            data = response.get("data", {}).get("data", {})
            return data.get("password") or data.get("value")
        except Exception as e:
            if "permission denied" in str(e).lower():
                logger.error("Vault permission denied for path: %s", path)
            else:
                logger.debug("Secret not found in Vault: %s", path)
            return None


class SecretProviderError(Exception):
    """Raised when secret retrieval fails."""

    pass


class CredentialValidationError(Exception):
    """Raised when credentials are found in insecure locations."""

    pass


def validate_no_password_in_url(url: str) -> None:
    """Validate that a database URL does not contain a password.

    Args:
        url: The database connection URL to validate.

    Raises:
        CredentialValidationError: If password is detected in URL.
    """
    parsed = urlparse(url)

    if parsed.password:
        raise CredentialValidationError(
            "Database password detected in connection URL. "
            "For HIPAA compliance, passwords must be provided via environment "
            "variable (VCF_PG_LOADER_DB_PASSWORD or PGPASSWORD) or secrets manager."
        )

    user_info = parsed.netloc.split("@")[0] if "@" in parsed.netloc else ""
    if ":" in user_info:
        raise CredentialValidationError(
            "Database password detected in connection URL. "
            "For HIPAA compliance, passwords must be provided via environment "
            "variable (VCF_PG_LOADER_DB_PASSWORD or PGPASSWORD) or secrets manager."
        )


def mask_password_in_url(url: str) -> str:
    """Mask any password in a database URL for safe logging.

    Args:
        url: The database connection URL.

    Returns:
        URL with password replaced by ***MASKED***.
    """
    pattern = r"(://[^:]+:)([^@]+)(@)"
    return re.sub(pattern, r"\1***MASKED***\3", url)


def get_default_provider() -> SecretProvider:
    """Get the default secrets provider (environment variables).

    Returns:
        EnvSecretProvider instance.
    """
    return EnvSecretProvider()


def get_database_password(
    provider: SecretProvider | None = None,
    password_env_var: str = "VCF_PG_LOADER_DB_PASSWORD",
) -> str | None:
    """Get database password from secrets provider with fallback.

    Args:
        provider: SecretProvider to use, or None for default.
        password_env_var: Primary environment variable name for password.

    Returns:
        The password if found, None otherwise.
    """
    if provider is None:
        provider = get_default_provider()

    password = provider.get_secret(password_env_var)
    if password:
        logger.info("Database password loaded from %s", password_env_var)
        return password

    password = provider.get_secret("PGPASSWORD")
    if password:
        logger.info("Database password loaded from PGPASSWORD")
        return password

    return None
