"""
Feishu custom bot delivery via webhook.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any

from src.delivery.common import (
    env_bool,
    env_int,
    make_result,
    post_json_with_retry,
    redact_webhook_url,
    truncate_utf8_bytes,
)

logger = logging.getLogger(__name__)

PROVIDER_NAME = "feishu"
MAX_REQUEST_BYTES = 20 * 1024
TRUNCATION_SUFFIX = "\n\n...(消息已截断，完整内容请查看本地报告)"
RETRYABLE_API_CODES = {11232}


def resolve_feishu_config() -> dict:
    """Load Feishu delivery configuration from environment variables."""
    return {
        "enabled": env_bool("FEISHU_ENABLED", False),
        "webhook_url": os.environ.get("FEISHU_WEBHOOK_URL", "").strip(),
        "secret": os.environ.get("FEISHU_SECRET", "").strip(),
        "timeout_seconds": env_int("FEISHU_TIMEOUT_SECONDS", 10),
        "retry_count": env_int("FEISHU_RETRY_COUNT", 2),
    }


def generate_signature(timestamp: str, secret: str) -> str:
    """Generate the Feishu custom bot signature."""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def _build_payload(content: str, secret: str | None, timestamp: str | None) -> dict:
    payload = {
        "msg_type": "text",
        "content": {
            "text": content,
        },
    }
    if secret and timestamp:
        payload["timestamp"] = timestamp
        payload["sign"] = generate_signature(timestamp, secret)
    return payload


def _build_fitted_payload(content: str, secret: str | None) -> dict:
    """
    Fit the full JSON body within Feishu's 20KB request limit.
    """
    timestamp = str(int(time.time())) if secret else None
    payload = _build_payload(content, secret, timestamp)
    payload_bytes = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    if payload_bytes <= MAX_REQUEST_BYTES:
        return payload

    content_bytes = len(content.encode("utf-8"))
    overhead_bytes = payload_bytes - content_bytes
    budget = max(MAX_REQUEST_BYTES - overhead_bytes, 0)
    trimmed_content = truncate_utf8_bytes(content, budget, suffix=TRUNCATION_SUFFIX)
    payload = _build_payload(trimmed_content, secret, timestamp)

    while len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) > MAX_REQUEST_BYTES:
        next_budget = len(trimmed_content.encode("utf-8")) - 256
        trimmed_content = truncate_utf8_bytes(
            trimmed_content,
            max(next_budget, 0),
            suffix=TRUNCATION_SUFFIX,
        )
        payload = _build_payload(trimmed_content, secret, timestamp)

    logger.warning(
        "Feishu message exceeded %d bytes and was truncated for delivery",
        MAX_REQUEST_BYTES,
    )
    return payload


def _extract_error(body: Any, response) -> str:
    if isinstance(body, dict):
        return body.get("msg") or body.get("StatusMessage") or f"HTTP {response.status_code}"
    return f"HTTP {response.status_code}"


def send_feishu_message(
    content: str,
    webhook_url: str | None = None,
    secret: str | None = None,
    timeout_seconds: int | None = None,
    retry_count: int | None = None,
    event: str = "report_success",
) -> dict:
    """
    Send a text message to a Feishu custom bot webhook.
    """
    config = resolve_feishu_config()
    webhook_url = webhook_url if webhook_url is not None else config["webhook_url"]
    secret = secret if secret is not None else config["secret"]
    timeout_seconds = timeout_seconds if timeout_seconds is not None else config["timeout_seconds"]
    retry_count = retry_count if retry_count is not None else config["retry_count"]

    if not webhook_url:
        logger.warning("No Feishu webhook URL configured, skipping send")
        return make_result(
            PROVIDER_NAME,
            success=False,
            response=None,
            error="No webhook URL configured",
            event=event,
        )

    payload = _build_fitted_payload(content, secret or None)
    result = post_json_with_retry(
        provider=PROVIDER_NAME,
        url=webhook_url,
        payload=payload,
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
        event=event,
        success_evaluator=lambda body, _: isinstance(body, dict) and body.get("code", -1) == 0,
        error_extractor=_extract_error,
        should_retry_result=lambda body, _: isinstance(body, dict) and body.get("code") in RETRYABLE_API_CODES,
    )

    if result["success"]:
        logger.info("Feishu notification sent successfully")
    elif result.get("error"):
        logger.error(
            "Feishu notification failed via %s: %s",
            redact_webhook_url(webhook_url),
            result["error"],
        )
    return result


def notify_feishu_event(event: str, content: str) -> dict:
    """Send a Feishu event notification if Feishu is enabled."""
    config = resolve_feishu_config()
    if not config["enabled"]:
        logger.info("Feishu notifications are disabled")
        return make_result(
            PROVIDER_NAME,
            success=True,
            skipped=True,
            event=event,
            reason="Feishu notifications disabled",
        )

    return send_feishu_message(
        content=content,
        webhook_url=config["webhook_url"],
        secret=config["secret"],
        timeout_seconds=config["timeout_seconds"],
        retry_count=config["retry_count"],
        event=event,
    )
