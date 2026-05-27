"""Slack alert notification via webhook.

Uses httpx for async HTTP POST to Slack Incoming Webhook URL.
"""

import httpx

from app.config import settings
from app.core.logging import logger


async def send_slack_alert(
    webhook_url: str | None = None,
    message: str = "",
    **blocks,
) -> bool:
    """Send an alert notification to Slack via Incoming Webhook.

    If no webhook_url is provided, falls back to the default
    configured in settings.SLACK_WEBHOOK_URL.

    Returns True on success, False on failure (logged).
    """
    url = webhook_url or settings.SLACK_WEBHOOK_URL
    if not url:
        logger.warning("slack_not_configured")
        return False

    payload = {"text": message}
    if blocks:
        payload["blocks"] = _build_blocks(**blocks)

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        return True
    except Exception as exc:
        logger.error("slack_webhook_failed", error=str(exc), webhook_url=url[:50] + "...")
        return False


def _build_blocks(
    title: str = "",
    rule_name: str = "",
    node_name: str = "",
    metric_field: str = "",
    value: float = 0.0,
    threshold: float = 0.0,
    operator: str = "",
    timestamp: str = "",
) -> list[dict]:
    """Build Slack Block Kit blocks for a rich alert notification."""
    blocks = []
    if title:
        blocks.append({
            "type": "header",
            "text": {"type": "plain_text", "text": title, "emoji": True},
        })
    fields = []
    if rule_name:
        fields.append({"type": "mrkdwn", "text": f"*Rule:* {rule_name}"})
    if node_name:
        fields.append({"type": "mrkdwn", "text": f"*Node:* {node_name}"})
    if metric_field:
        fields.append({"type": "mrkdwn", "text": f"*Metric:* {metric_field}"})
    if value and threshold:
        fields.append({"type": "mrkdwn", "text": f"*Value:* {value} ({operator} {threshold})"})
    if timestamp:
        fields.append({"type": "mrkdwn", "text": f"*Time:* {timestamp}"})
    if fields:
        blocks.append({"type": "section", "fields": fields})
    return blocks
