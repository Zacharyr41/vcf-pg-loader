"""Audit context management for async-safe user tracking."""

import os
import socket
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import UUID, uuid4


@dataclass
class AuditContext:
    user_id: int | None = None
    user_name: str = "system"
    session_id: UUID | None = None
    client_ip: str | None = None
    client_hostname: str | None = None
    application_name: str = "vcf-pg-loader"


_audit_context: ContextVar[AuditContext | None] = ContextVar("audit_context", default=None)


def get_audit_context() -> AuditContext:
    """Get current audit context, creating default if none exists."""
    ctx = _audit_context.get()
    if ctx is None:
        ctx = AuditContext()
    return ctx


def set_audit_context(ctx: AuditContext) -> None:
    """Set the current audit context."""
    _audit_context.set(ctx)


def clear_audit_context() -> None:
    """Clear the current audit context."""
    _audit_context.set(None)


@contextmanager
def audit_context(
    user_id: int | None = None,
    user_name: str | None = None,
    session_id: UUID | None = None,
    client_ip: str | None = None,
    application_name: str | None = None,
):
    """Context manager for scoped audit context.

    Automatically creates a session ID if not provided.
    Restores previous context on exit.
    """
    previous = _audit_context.get()

    if user_name is None:
        user_name = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))

    ctx = AuditContext(
        user_id=user_id,
        user_name=user_name,
        session_id=session_id or uuid4(),
        client_ip=client_ip,
        client_hostname=socket.gethostname(),
        application_name=application_name or "vcf-pg-loader",
    )
    _audit_context.set(ctx)

    try:
        yield ctx
    finally:
        _audit_context.set(previous)


def create_cli_context(
    user_name: str | None = None,
    session_id: UUID | None = None,
) -> AuditContext:
    """Create an audit context for CLI operations.

    Automatically detects hostname and uses environment for user.
    """
    if user_name is None:
        user_name = os.environ.get("USER", os.environ.get("USERNAME", "cli-user"))

    return AuditContext(
        user_id=None,
        user_name=user_name,
        session_id=session_id or uuid4(),
        client_ip=None,
        client_hostname=socket.gethostname(),
        application_name="vcf-pg-loader",
    )
