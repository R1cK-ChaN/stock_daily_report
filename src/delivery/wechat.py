"""
WeChat delivery via webhook (企业微信群机器人).

Sends the generated report as a markdown message to a WeChat group.
"""

import logging
import os

from src.delivery.common import make_result, post_json_with_retry

logger = logging.getLogger(__name__)

# WeChat webhook message size limit (roughly 4KB for markdown)
MAX_MESSAGE_LENGTH = 4000
PROVIDER_NAME = "wechat"


def send_wechat_message(
    content: str,
    webhook_url: str | None = None,
    msg_type: str = "markdown",
    timeout_seconds: int = 10,
    retry_count: int = 0,
    event: str = "report_success",
) -> dict:
    """
    Send a message to WeChat group via webhook.

    Args:
        content: Message content (markdown or text)
        webhook_url: WeChat webhook URL. Falls back to WECHAT_WEBHOOK_URL env var.
        msg_type: "markdown" or "text"

    Returns:
        Normalized delivery result dict.
    """
    if webhook_url is None:
        webhook_url = os.environ.get("WECHAT_WEBHOOK_URL", "")

    if not webhook_url:
        logger.warning("No WeChat webhook URL configured, skipping delivery")
        return make_result(
            PROVIDER_NAME,
            success=False,
            response=None,
            error="No webhook URL configured",
            event=event,
        )

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

    result = post_json_with_retry(
        provider=PROVIDER_NAME,
        url=webhook_url,
        payload=payload,
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
        event=event,
        success_evaluator=lambda body, _: isinstance(body, dict) and body.get("errcode", -1) == 0,
        error_extractor=lambda body, response: (
            body.get("errmsg")
            if isinstance(body, dict)
            else f"HTTP {response.status_code}"
        ) or f"HTTP {response.status_code}",
    )

    if result["success"]:
        logger.info("WeChat message sent successfully")
    return result


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
        return make_result(
            PROVIDER_NAME,
            success=True,
            skipped=True,
            event="report_success",
            reason="WeChat delivery disabled in config",
        )

    webhook_url = os.environ.get("WECHAT_WEBHOOK_URL", "")
    msg_type = wechat_cfg.get("msg_type", "markdown")

    return send_wechat_message(
        content=report_text,
        webhook_url=webhook_url,
        msg_type=msg_type,
        event="report_success",
    )
