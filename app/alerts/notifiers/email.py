"""Email alert notification via SMTP.

Runs SMTP send in a thread pool to avoid blocking the event loop.
"""

import asyncio
import smtplib
from email.mime.text import MIMEText

from app.config import settings
from app.core.logging import logger


async def send_email_alert(
    recipient: str,
    subject: str,
    body: str,
) -> bool:
    """Send an alert notification via SMTP asynchronously.

    Uses asyncio.to_thread to avoid blocking the event loop
    on the SMTP handshake.

    Returns True on success, False on failure (logged).
    """
    if not settings.SMTP_HOST or not settings.SMTP_USER:
        logger.warning("email_not_configured", recipient=recipient)
        return False

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = settings.SMTP_USER
    msg["To"] = recipient

    def _send():
        try:
            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=10) as server:
                if settings.SMTP_PORT == 587:
                    server.starttls()
                if settings.SMTP_PASSWORD:
                    server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                server.send_message(msg)
            return True
        except Exception as exc:
            logger.error("email_send_failed", error=str(exc), recipient=recipient)
            return False

    return await asyncio.to_thread(_send)
