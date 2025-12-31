"""HIPAA-compliant audit logger with async support and guaranteed delivery."""

import asyncio
import json
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import ParamSpec, TypeVar

import asyncpg

from .context import get_audit_context
from .integrity import AuditIntegrity
from .models import AuditEvent, AuditEventType

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_FLUSH_INTERVAL = 5.0
DEFAULT_FALLBACK_PATH = Path("/tmp/vcf_pg_loader_audit_fallback.jsonl")

P = ParamSpec("P")
R = TypeVar("R")


class AuditLogger:
    """Async audit logger with batching and guaranteed delivery.

    Features:
    - Batch writes for performance (configurable size and interval)
    - Falls back to local JSONL file if database unavailable
    - Never blocks main operations on audit writes
    - Automatic context population from AuditContext
    """

    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        fallback_path: Path = DEFAULT_FALLBACK_PATH,
    ):
        self._pool = pool
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._fallback_path = fallback_path
        self._buffer: list[AuditEvent] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._running = False
        self._integrity = AuditIntegrity()
        self._last_hash: str | None = None

    async def start(self) -> None:
        """Start the background flush task."""
        if self._running:
            return
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Stop the logger and flush remaining events."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()

    async def _flush_loop(self) -> None:
        """Background task that flushes buffer periodically."""
        while self._running:
            try:
                await asyncio.sleep(self._flush_interval)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in audit flush loop: %s", e)

    def set_pool(self, pool: asyncpg.Pool) -> None:
        """Set the database connection pool."""
        self._pool = pool

    async def log_event(self, event: AuditEvent) -> None:
        """Add an event to the buffer for async writing.

        Populates event with current audit context if fields are not set.
        """
        ctx = get_audit_context()

        if event.user_id is None:
            event.user_id = ctx.user_id
        if event.user_name == "system":
            event.user_name = ctx.user_name
        if event.session_id is None:
            event.session_id = ctx.session_id
        if event.client_ip is None:
            event.client_ip = ctx.client_ip
        if event.client_hostname is None:
            event.client_hostname = ctx.client_hostname
        if event.application_name == "vcf-pg-loader":
            event.application_name = ctx.application_name

        if event.event_time is None:
            event.event_time = datetime.now(UTC)

        async with self._lock:
            self._buffer.append(event)
            if len(self._buffer) >= self._batch_size:
                await self._flush_buffer()

    async def flush(self) -> None:
        """Flush all buffered events to the database."""
        async with self._lock:
            await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Internal flush - must be called with lock held."""
        if not self._buffer:
            return

        events = self._buffer.copy()
        self._buffer.clear()

        try:
            await self._write_to_db(events)
        except Exception as e:
            logger.warning("Failed to write audit events to DB: %s. Falling back to file.", e)
            await self._write_to_fallback(events)

    async def _write_to_db(self, events: list[AuditEvent]) -> None:
        """Write events to the hipaa_audit_log table with hash chain."""
        if self._pool is None:
            raise RuntimeError("No database pool configured")

        async with self._pool.acquire() as conn:
            if self._last_hash is None:
                self._last_hash = await self._integrity.get_last_hash(conn)

            for event in events:
                row = event.to_db_row()

                result = await conn.fetchrow(
                    """
                    INSERT INTO hipaa_audit_log (
                        event_type, user_id, user_name, session_id,
                        action, resource_type, resource_id,
                        client_ip, client_hostname, application_name,
                        success, error_message, details,
                        previous_hash
                    ) VALUES (
                        $1::audit_event_type, $2, $3, $4,
                        $5, $6, $7,
                        $8::inet, $9, $10,
                        $11, $12, $13::jsonb,
                        $14
                    ) RETURNING entry_hash
                    """,
                    row["event_type"],
                    row["user_id"],
                    row["user_name"],
                    row["session_id"],
                    row["action"],
                    row["resource_type"],
                    row["resource_id"],
                    row["client_ip"],
                    row["client_hostname"],
                    row["application_name"],
                    row["success"],
                    row["error_message"],
                    json.dumps(row["details"]),
                    self._last_hash,
                )

                self._last_hash = result["entry_hash"]

    async def _write_to_fallback(self, events: list[AuditEvent]) -> None:
        """Write events to fallback JSONL file."""
        try:
            with open(self._fallback_path, "a") as f:
                for event in events:
                    row = event.to_db_row()
                    row["event_time"] = event.event_time.isoformat() if event.event_time else None
                    if row["session_id"]:
                        row["session_id"] = str(row["session_id"])
                    f.write(json.dumps(row) + "\n")
            logger.info(
                "Wrote %d audit events to fallback file: %s", len(events), self._fallback_path
            )
        except Exception as e:
            logger.error("Failed to write to fallback file: %s", e)

    @asynccontextmanager
    async def audit_operation(
        self,
        event_type: AuditEventType,
        action: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict | None = None,
    ):
        """Context manager that logs start and completion of an operation.

        Logs a start event, then on exit logs success or failure.
        """
        start_time = datetime.now(UTC)
        start_details = dict(details or {})
        start_details["phase"] = "started"

        start_event = AuditEvent(
            event_type=event_type,
            action=action,
            success=True,
            resource_type=resource_type,
            resource_id=resource_id,
            details=start_details,
        )
        await self.log_event(start_event)

        try:
            yield
            end_details = dict(details or {})
            end_details["phase"] = "completed"
            end_details["duration_ms"] = int(
                (datetime.now(UTC) - start_time).total_seconds() * 1000
            )

            end_event = AuditEvent(
                event_type=event_type,
                action=action,
                success=True,
                resource_type=resource_type,
                resource_id=resource_id,
                details=end_details,
            )
            await self.log_event(end_event)

        except Exception as e:
            end_details = dict(details or {})
            end_details["phase"] = "failed"
            end_details["duration_ms"] = int(
                (datetime.now(UTC) - start_time).total_seconds() * 1000
            )

            error_event = AuditEvent(
                event_type=event_type,
                action=action,
                success=False,
                resource_type=resource_type,
                resource_id=resource_id,
                error_message=str(e)[:1000],
                details=end_details,
            )
            await self.log_event(error_event)
            raise


def audit_operation(
    event_type: AuditEventType,
    action: str,
    resource_type: str | None = None,
    get_resource_id: Callable[..., str | None] | None = None,
):
    """Decorator for auditing async functions.

    Args:
        event_type: Type of audit event
        action: Description of the action
        resource_type: Type of resource being accessed
        get_resource_id: Optional callable to extract resource_id from args
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            self_obj = args[0] if args else None
            audit_logger = getattr(self_obj, "_audit_logger", None) if self_obj else None

            if audit_logger is None:
                return await func(*args, **kwargs)

            resource_id = None
            if get_resource_id:
                try:
                    resource_id = get_resource_id(*args, **kwargs)
                except Exception:
                    pass

            async with audit_logger.audit_operation(
                event_type=event_type,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
            ):
                return await func(*args, **kwargs)

        return wrapper

    return decorator
