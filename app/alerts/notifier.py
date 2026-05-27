"""Notification dispatcher — routes alert notifications to configured channels.

Supports:
  - Email (via SMTP)
  - Slack (via webhook)

Notification failures are logged and swallowed — they must NOT
break the metric processing pipeline.
"""

import asyncio
import uuid
from datetime import datetime, timezone

from app.alerts import metrics
from app.alerts.cache import CachedRule
from app.alerts.notifiers.email import send_email_alert
from app.alerts.notifiers.slack import send_slack_alert
from app.core.logging import logger

# Cap concurrent notification sends to avoid exhausting the event loop
# under heavy alert load (N4).
_notification_semaphore = asyncio.Semaphore(100)


async def dispatch_notifications(
    rule: CachedRule,
    node_id: uuid.UUID,
    value: float,
    alert_history_id: uuid.UUID | None = None,
    is_renotify: bool = False,
    is_resolve: bool = False,
    escalation_level: int = 0,
    tenant_id: str = "",
) -> list[str]:
    """Send alert notifications to all configured channels.

    Iterates through rule.channels and dispatches to each.
    Returns a list of channel names that were successfully notified.
    Failures are logged but never raised.

    Args:
        rule: The triggered rule
        node_id: The affected node
        value: The metric value that triggered the alert
        alert_history_id: DB alert record (for context)
        is_renotify: True if this is a re-notification
        is_resolve: True if this is a resolve notification
        escalation_level: 0=default, 1=escalated, 2=critical
        tenant_id: The tenant that owns this alert (F3)
    """
    notified = []
    channels = rule.channels or []
    timestamp = datetime.now(timezone.utc).isoformat()

    if is_resolve:
        title = "✅ Alert Resolved"
    elif is_renotify:
        title = "🔄 Re-Notification"
    else:
        title = "🚨 Alert Fired"

    subject = f"{title}: {rule.name}"

    body_lines = [
        f"Rule: {rule.name}",
        f"Metric: {rule.metric_field}",
        f"Condition: {rule.metric_field} {rule.operator} {rule.threshold}",
        f"Current value: {value}",
        f"Node: {node_id}",
        f"Time: {timestamp}",
        f"Description: {rule.metric_field} is {rule.operator} {rule.threshold} "
        f"(current: {value})",
    ]
    body = "\n".join(body_lines)

    for channel in channels:
        channel_type = channel.get("type", "")

        try:
            if channel_type == "email":
                recipient = channel.get("recipient", "")
                if recipient:
                    async with _notification_semaphore:
                        with metrics.alert_notification_duration.labels(channel="email").time():
                            ok = await send_email_alert(recipient, subject, body)
                    if ok:
                        notified.append(f"email:{recipient}")
                    metrics.alert_notifications.labels(channel="email", result="ok" if ok else "fail").inc()

            elif channel_type == "slack":
                webhook = channel.get("webhook_url", "")
                async with _notification_semaphore:
                    with metrics.alert_notification_duration.labels(channel="slack").time():
                        ok = await send_slack_alert(
                        webhook_url=webhook,
                        message=subject,
                        title=title,
                        rule_name=rule.name,
                        metric_field=rule.metric_field,
                        value=value,
                        threshold=rule.threshold,
                        operator=rule.operator,
                        timestamp=timestamp,
                    )
                if ok:
                    notified.append("slack")
                metrics.alert_notifications.labels(channel="slack", result="ok" if ok else "fail").inc()

            else:
                logger.warning("unknown_channel_type", channel_type=channel_type)

        except Exception as exc:
            logger.error(
                "notification_failed",
                channel=channel_type,
                rule_id=str(rule.id),
                tenant_id=tenant_id,
                error=str(exc),
            )

    if not notified:
        logger.debug("no_notifications_sent", rule_id=str(rule.id), tenant_id=tenant_id)

    return notified
