#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: discord_hook.py

Discord webhook notifications.
"""

from typing import Any

import httpx
from loguru import logger as log

from .config import settings


def send_discord_notification(message: str, webhook_url: str | None = None) -> bool:
    """
    Send notification to Discord webhook.

    :param message: Message to send
    :param webhook_url: Webhook URL (defaults to settings)
    :returns: True if successful, False otherwise
    """
    url = webhook_url or settings.discord_hook_url

    if not url:
        log.debug("Discord webhook not configured, skipping notification")
        return False

    try:
        # Build payload
        content = message
        if settings.discord_at_mention:
            content = f"{settings.discord_at_mention} {message}"

        payload = {"content": content}

        # Send notification
        response = httpx.post(url, json=payload, timeout=10)
        response.raise_for_status()

        log.debug("Discord notification sent successfully")
        return True

    except Exception as e:
        log.warning(f"Failed to send Discord notification: {e}")
        return False


def handle_return(message: str, webhook_url: str | None = None) -> dict[str, Any]:
    """
    Send notification and return response dict.

    :param message: Message to send and return
    :param webhook_url: Optional webhook URL
    :returns: Response dictionary with status and message
    """
    send_discord_notification(message, webhook_url)

    return {
        "status": "success",
        "message": message,
    }
