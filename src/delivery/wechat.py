"""
WeChat delivery via webhook (企业微信群机器人).

Sends the generated report as a markdown message to a WeChat group.
"""

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)

# WeChat webhook message size limit (roughly 4KB for markdown)
MAX_MESSAGE_LENGTH = 4000


def send_wechat_message(
    content: str,
    webhook_url: str | None = None,
    msg_type: str = "markdown",
) -> dict:
    """
    Send a message to WeChat group via webhook.

    Args:
        content: Message content (markdown or text)
        webhook_url: WeChat webhook URL. Falls back to WECHAT_WEBHOOK_URL env var.
        msg_type: "markdown" or "text"

    Returns:
        Dict with 'success', 'response', 'error'.
    """
    if webhook_url is None:
        webhook_url = os.environ.get("WECHAT_WEBHOOK_URL", "")

    if not webhook_url:
        logger.warning("No WeChat webhook URL configured, skipping delivery")
        return {
            "success": False,
            "response": None,
            "error": "No webhook URL configured",
        }

    # Truncate if too long
    if len(content) > MAX_MESSAGE_LENGTH:
        logger.warning(
            "Message too long (%d chars), truncating to %d",
            len(content), MAX_MESSAGE_LENGTH,
        )
        content = content[:MAX_MESSAGE_LENGTH - 50] + "\n\n...(报告已截断，完整版请查看文件)"

    payload = {
        "msgtype": msg_type,
        msg_type: {
            "content": content,
        },
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        result = resp.json()

        success = result.get("errcode", -1) == 0
        if success:
            logger.info("WeChat message sent successfully")
        else:
            logger.error("WeChat API error: %s", result)

        return {
            "success": success,
            "response": result,
            "error": None if success else result.get("errmsg", "Unknown error"),
        }

    except requests.RequestException as e:
        logger.error("WeChat delivery failed: %s", e)
        return {
            "success": False,
            "response": None,
            "error": str(e),
        }


def deliver_report(report_text: str, config: dict) -> dict:
    """
    Deliver the report via WeChat if enabled.

    Args:
        report_text: The generated report markdown
        config: Settings dict

    Returns:
        Dict with delivery result.
    """
    wechat_cfg = config.get("wechat", {})

    if not wechat_cfg.get("enabled", False):
        logger.info("WeChat delivery is disabled in config")
        return {
            "success": True,
            "skipped": True,
            "reason": "WeChat delivery disabled in config",
        }

    webhook_url = os.environ.get("WECHAT_WEBHOOK_URL", "")
    msg_type = wechat_cfg.get("msg_type", "markdown")

    return send_wechat_message(
        content=report_text,
        webhook_url=webhook_url,
        msg_type=msg_type,
    )
