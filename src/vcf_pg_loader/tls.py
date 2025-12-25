"""TLS/SSL configuration for secure PostgreSQL connections.

Implements HIPAA 164.312(e)(1) encryption in transit requirements.
"""

import logging
import os
import ssl
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

MIN_TLS_VERSION = ssl.TLSVersion.TLSv1_2


@dataclass
class TLSConfig:
    """TLS configuration for database connections."""

    require_tls: bool = True
    verify_server: bool = True
    ca_cert_path: Path | None = None
    client_cert_path: Path | None = None
    client_key_path: Path | None = None

    @classmethod
    def from_env(cls) -> "TLSConfig":
        """Create TLS config from environment variables.

        Environment variables:
            VCF_PG_LOADER_REQUIRE_TLS: Require TLS (default: true)
            VCF_PG_LOADER_TLS_VERIFY: Verify server certificate (default: true)
            VCF_PG_LOADER_TLS_CA_CERT: Path to CA certificate
            VCF_PG_LOADER_TLS_CLIENT_CERT: Path to client certificate
            VCF_PG_LOADER_TLS_CLIENT_KEY: Path to client key
        """
        require_tls = os.environ.get("VCF_PG_LOADER_REQUIRE_TLS", "true").lower() in (
            "true",
            "1",
            "yes",
        )
        verify_server = os.environ.get("VCF_PG_LOADER_TLS_VERIFY", "true").lower() in (
            "true",
            "1",
            "yes",
        )

        ca_cert = os.environ.get("VCF_PG_LOADER_TLS_CA_CERT")
        client_cert = os.environ.get("VCF_PG_LOADER_TLS_CLIENT_CERT")
        client_key = os.environ.get("VCF_PG_LOADER_TLS_CLIENT_KEY")

        return cls(
            require_tls=require_tls,
            verify_server=verify_server,
            ca_cert_path=Path(ca_cert) if ca_cert else None,
            client_cert_path=Path(client_cert) if client_cert else None,
            client_key_path=Path(client_key) if client_key else None,
        )


class TLSError(Exception):
    """Raised when TLS configuration or negotiation fails."""

    pass


def create_ssl_context(config: TLSConfig | None = None) -> ssl.SSLContext | None:
    """Create an SSL context for asyncpg connections.

    Args:
        config: TLS configuration. If None, loads from environment.

    Returns:
        SSLContext configured for TLS 1.2+ or None if TLS not required.

    Raises:
        TLSError: If certificate files are missing or invalid.
    """
    if config is None:
        config = TLSConfig.from_env()

    if not config.require_tls:
        logger.warning("TLS disabled - connections will not be encrypted")
        return None

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = MIN_TLS_VERSION
    ctx.maximum_version = ssl.TLSVersion.TLSv1_3

    ctx.set_ciphers("HIGH:MEDIUM:+3DES:!aNULL:!eNULL:!MD5")

    if config.verify_server:
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED

        if config.ca_cert_path:
            if not config.ca_cert_path.exists():
                raise TLSError(f"CA certificate not found: {config.ca_cert_path}")
            ctx.load_verify_locations(cafile=str(config.ca_cert_path))
            logger.debug("Loaded CA certificate from %s", config.ca_cert_path)
        else:
            ctx.load_default_certs()
            logger.debug("Using system default CA certificates")
    else:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        logger.warning("TLS server verification disabled - vulnerable to MITM attacks")

    if config.client_cert_path and config.client_key_path:
        if not config.client_cert_path.exists():
            raise TLSError(f"Client certificate not found: {config.client_cert_path}")
        if not config.client_key_path.exists():
            raise TLSError(f"Client key not found: {config.client_key_path}")

        ctx.load_cert_chain(
            certfile=str(config.client_cert_path),
            keyfile=str(config.client_key_path),
        )
        logger.debug(
            "Loaded client certificate from %s",
            config.client_cert_path,
        )

    logger.debug(
        "Created SSL context: min_version=%s, verify=%s",
        ctx.minimum_version.name,
        ctx.verify_mode.name,
    )

    return ctx


async def verify_tls_connection(conn) -> dict:
    """Verify that a connection is using TLS and log connection details.

    Args:
        conn: asyncpg connection object.

    Returns:
        Dict with TLS connection details.

    Raises:
        TLSError: If connection is not using TLS when required.
    """
    ssl_info = conn.get_settings().ssl
    is_encrypted = ssl_info is not None

    if not is_encrypted:
        raise TLSError("Connection is not encrypted - TLS negotiation failed")

    details = {
        "encrypted": True,
        "ssl_in_use": True,
    }

    logger.debug("TLS connection verified: %s", details)

    return details


def get_ssl_param_for_asyncpg(config: TLSConfig | None = None) -> ssl.SSLContext | str | bool:
    """Get the ssl parameter value for asyncpg.connect() or create_pool().

    asyncpg accepts several values for ssl:
    - SSLContext: Use this specific context
    - True: Use default SSL
    - 'require': Require SSL but don't verify
    - 'verify-ca': Verify server certificate
    - 'verify-full': Verify server certificate and hostname

    Args:
        config: TLS configuration. If None, loads from environment.

    Returns:
        Value to pass as ssl parameter to asyncpg.
    """
    if config is None:
        config = TLSConfig.from_env()

    if not config.require_tls:
        return False

    if config.client_cert_path or config.ca_cert_path:
        return create_ssl_context(config)

    if config.verify_server:
        return "verify-full"

    return "require"
