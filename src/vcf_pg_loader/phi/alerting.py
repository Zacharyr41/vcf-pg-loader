"""PHI detection alerting system.

HIPAA Reference: 164.308(a)(6) - Security Incident Procedures

Handles PHI detection events with configurable actions and notifications.
Integrates with audit logging and supports webhook/email alerting.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..audit import AuditLogger

from .detector import PHIDetection, PHIScanReport

logger = logging.getLogger(__name__)


class AlertAction(Enum):
    """Action to take when PHI is detected."""

    LOG_ONLY = "log_only"
    WARN_AND_CONTINUE = "warn_and_continue"
    PAUSE_FOR_REVIEW = "pause_for_review"
    ABORT_LOAD = "abort_load"


@dataclass
class AlertConfig:
    """Configuration for PHI alerting."""

    critical_action: AlertAction = AlertAction.ABORT_LOAD
    high_action: AlertAction = AlertAction.PAUSE_FOR_REVIEW
    medium_action: AlertAction = AlertAction.WARN_AND_CONTINUE
    low_action: AlertAction = AlertAction.LOG_ONLY

    slack_webhook: str | None = None
    email_to: str | None = None
    email_from: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587

    @classmethod
    def from_dict(cls, data: dict) -> "AlertConfig":
        actions = data.get("actions", {})
        alerts = data.get("alerts", {})

        action_map = {
            "log": AlertAction.LOG_ONLY,
            "warn": AlertAction.WARN_AND_CONTINUE,
            "pause": AlertAction.PAUSE_FOR_REVIEW,
            "abort": AlertAction.ABORT_LOAD,
        }

        return cls(
            critical_action=action_map.get(
                actions.get("critical", "abort"), AlertAction.ABORT_LOAD
            ),
            high_action=action_map.get(actions.get("high", "pause"), AlertAction.PAUSE_FOR_REVIEW),
            medium_action=action_map.get(
                actions.get("medium", "warn"), AlertAction.WARN_AND_CONTINUE
            ),
            low_action=action_map.get(actions.get("low", "log"), AlertAction.LOG_ONLY),
            slack_webhook=alerts.get("slack_webhook"),
            email_to=alerts.get("email"),
            email_from=alerts.get("email_from"),
            smtp_host=alerts.get("smtp_host"),
            smtp_port=alerts.get("smtp_port", 587),
        )


@dataclass
class LoadContext:
    """Context for the current load operation."""

    vcf_path: str
    load_batch_id: str | None = None
    user_id: int | None = None
    user_name: str = "system"


@dataclass
class AlertEvent:
    """Record of an alert that was triggered."""

    detection: PHIDetection
    action_taken: AlertAction
    context: LoadContext
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    notification_sent: bool = False
    notification_error: str | None = None


class PHIAlertHandler:
    """Handles PHI detection events with configurable actions."""

    def __init__(
        self,
        config: AlertConfig | None = None,
        audit_logger: "AuditLogger | None" = None,
    ):
        self._config = config or AlertConfig()
        self._audit_logger = audit_logger
        self._events: list[AlertEvent] = []
        self._http_session = None

    @property
    def config(self) -> AlertConfig:
        return self._config

    @property
    def events(self) -> list[AlertEvent]:
        return self._events.copy()

    def get_action_for_severity(self, severity: str) -> AlertAction:
        action_map = {
            "critical": self._config.critical_action,
            "high": self._config.high_action,
            "medium": self._config.medium_action,
            "low": self._config.low_action,
        }
        return action_map.get(severity, AlertAction.WARN_AND_CONTINUE)

    async def handle_detection(
        self,
        detection: PHIDetection,
        context: LoadContext,
    ) -> AlertAction:
        action = self.get_action_for_severity(detection.severity)

        event = AlertEvent(
            detection=detection,
            action_taken=action,
            context=context,
        )
        self._events.append(event)

        if self._audit_logger:
            await self._log_to_audit(event)

        logger.warning(
            "PHI detected: pattern=%s severity=%s action=%s location=%s",
            detection.pattern_name,
            detection.severity,
            action.value,
            detection.location,
        )

        if action in (AlertAction.PAUSE_FOR_REVIEW, AlertAction.ABORT_LOAD):
            await self._send_notifications(event)

        return action

    async def handle_scan_report(
        self,
        report: PHIScanReport,
        context: LoadContext,
    ) -> AlertAction:
        if not report.has_phi:
            return AlertAction.LOG_ONLY

        highest_action = AlertAction.LOG_ONLY
        action_priority = {
            AlertAction.LOG_ONLY: 0,
            AlertAction.WARN_AND_CONTINUE: 1,
            AlertAction.PAUSE_FOR_REVIEW: 2,
            AlertAction.ABORT_LOAD: 3,
        }

        for detection in report.detections:
            action = await self.handle_detection(detection, context)
            if action_priority[action] > action_priority[highest_action]:
                highest_action = action

        return highest_action

    async def _log_to_audit(self, event: AlertEvent) -> None:
        if not self._audit_logger:
            return

        try:
            from ..audit import AuditEvent, AuditEventType

            audit_event = AuditEvent(
                event_type=AuditEventType.PHI_ACCESS,
                action="phi_detection_alert",
                success=True,
                user_id=event.context.user_id,
                user_name=event.context.user_name,
                resource_type="vcf_file",
                resource_id=event.context.vcf_path,
                details={
                    "pattern_name": event.detection.pattern_name,
                    "severity": event.detection.severity,
                    "action_taken": event.action_taken.value,
                    "location": event.detection.location,
                    "masked_value": event.detection.masked_value,
                    "load_batch_id": event.context.load_batch_id,
                },
            )
            await self._audit_logger.log_event(audit_event)
        except Exception as e:
            logger.error("Failed to log PHI alert to audit: %s", e)

    async def _send_notifications(self, event: AlertEvent) -> None:
        tasks = []
        if self._config.slack_webhook:
            tasks.append(self._send_slack_notification(event))
        if self._config.email_to and self._config.smtp_host:
            tasks.append(self._send_email_notification(event))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    event.notification_error = str(result)
                    logger.error("Notification failed: %s", result)

    async def _send_slack_notification(self, event: AlertEvent) -> None:
        if not self._config.slack_webhook:
            return

        try:
            import aiohttp

            severity_emoji = {
                "critical": ":rotating_light:",
                "high": ":warning:",
                "medium": ":large_yellow_circle:",
                "low": ":information_source:",
            }

            payload = {
                "text": f"{severity_emoji.get(event.detection.severity, ':warning:')} PHI Detection Alert",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"PHI Detected - {event.detection.severity.upper()}",
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Pattern:*\n{event.detection.pattern_name}",
                            },
                            {"type": "mrkdwn", "text": f"*Severity:*\n{event.detection.severity}"},
                            {"type": "mrkdwn", "text": f"*Location:*\n{event.detection.location}"},
                            {"type": "mrkdwn", "text": f"*Action:*\n{event.action_taken.value}"},
                        ],
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": f"File: `{event.context.vcf_path}`"},
                            {"type": "mrkdwn", "text": f"User: {event.context.user_name}"},
                        ],
                    },
                ],
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self._config.slack_webhook,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"Slack webhook failed: {resp.status} {text}")

            event.notification_sent = True
            logger.info("Slack notification sent for PHI detection")

        except ImportError:
            logger.warning("aiohttp not installed, cannot send Slack notification")
        except Exception as e:
            logger.error("Failed to send Slack notification: %s", e)
            raise

    async def _send_email_notification(self, event: AlertEvent) -> None:
        if not all([self._config.email_to, self._config.smtp_host]):
            return

        try:
            from email.mime.text import MIMEText

            subject = (
                f"[PHI ALERT] {event.detection.severity.upper()} - {event.detection.pattern_name}"
            )
            body = f"""
PHI Detection Alert
===================

Severity: {event.detection.severity}
Pattern: {event.detection.pattern_name}
Description: {event.detection.false_positive_hints}
Location: {event.detection.location}
Action Taken: {event.action_taken.value}

File: {event.context.vcf_path}
User: {event.context.user_name}
Load Batch: {event.context.load_batch_id}
Time: {event.timestamp.isoformat()}

Masked Value: {event.detection.masked_value}

This is an automated alert from vcf-pg-loader HIPAA compliance monitoring.
"""

            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = self._config.email_from or "vcf-pg-loader@localhost"
            msg["To"] = self._config.email_to

            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._send_smtp_email(msg),
            )

            event.notification_sent = True
            logger.info("Email notification sent for PHI detection")

        except Exception as e:
            logger.error("Failed to send email notification: %s", e)
            raise

    def _send_smtp_email(self, msg) -> None:
        import smtplib

        with smtplib.SMTP(self._config.smtp_host, self._config.smtp_port) as server:
            server.send_message(msg)
