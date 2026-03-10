"""
Shared helpers for outbound notification providers.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Sequence
from urllib.parse import urlsplit

import requests

logger = logging.getLogger(__name__)

TRUTHY_VALUES = {"1", "true", "yes", "on"}
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}


def env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in TRUTHY_VALUES


def env_int(name: str, default: int) -> int:
    """Parse an integer environment variable with fallback logging."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s, using default=%s", name, default)
        return default


def redact_webhook_url(url: str | None) -> str:
    """Return a log-safe representation of a webhook URL."""
    if not url:
        return ""

    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        return "[REDACTED_WEBHOOK_URL]"

    return f"{parsed.scheme}://{parsed.netloc}/***"


def sanitize_error_text(text: str, secrets: Sequence[str] | None = None) -> str:
    """Strip known secret values from error strings before logging/returning."""
    sanitized = text
    for secret in secrets or ():
        if secret:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    return sanitized


def truncate_utf8_bytes(text: str, max_bytes: int, suffix: str = "") -> str:
    """Truncate a string by UTF-8 byte length while preserving valid characters."""
    if max_bytes <= 0:
        return ""

    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text

    suffix_bytes = suffix.encode("utf-8")
    if len(suffix_bytes) >= max_bytes:
        return suffix_bytes[:max_bytes].decode("utf-8", errors="ignore")

    trimmed = encoded[: max_bytes - len(suffix_bytes)].decode("utf-8", errors="ignore")
    return trimmed + suffix


def make_result(
    provider: str,
    *,
    success: bool,
    skipped: bool = False,
    response: Any = None,
    error: str | None = None,
    status_code: int | None = None,
    attempts: int = 1,
    event: str | None = None,
    reason: str | None = None,
) -> dict:
    """Build a normalized delivery result object."""
    return {
        "provider": provider,
        "success": success,
        "skipped": skipped,
        "event": event,
        "response": response,
        "error": error,
        "reason": reason,
        "status_code": status_code,
        "attempts": attempts,
    }


def _parse_response_body(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {"text": response.text}


def post_json_with_retry(
    *,
    provider: str,
    url: str,
    payload: dict,
    timeout_seconds: int,
    retry_count: int,
    event: str | None = None,
    success_evaluator: Callable[[Any, requests.Response], bool],
    error_extractor: Callable[[Any, requests.Response], str] | None = None,
    should_retry_result: Callable[[Any, requests.Response], bool] | None = None,
) -> dict:
    """
    POST JSON with short exponential backoff and a normalized result.

    retry_count is the number of retries after the first attempt.
    """
    max_attempts = max(int(retry_count), 0) + 1

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(url, json=payload, timeout=timeout_seconds)
            body = _parse_response_body(response)
        except requests.RequestException as exc:
            error_text = sanitize_error_text(str(exc), secrets=[url])
            if attempt < max_attempts:
                backoff_seconds = 2 ** (attempt - 1)
                logger.warning(
                    "%s notification request failed on attempt %d/%d: %s. Retrying in %ss",
                    provider,
                    attempt,
                    max_attempts,
                    error_text,
                    backoff_seconds,
                )
                time.sleep(backoff_seconds)
                continue

            logger.error("%s notification request failed: %s", provider, error_text)
            return make_result(
                provider,
                success=False,
                response=None,
                error=error_text,
                attempts=attempt,
                event=event,
            )

        status_code = response.status_code
        if status_code in RETRYABLE_HTTP_STATUS_CODES and attempt < max_attempts:
            error_text = (
                error_extractor(body, response)
                if error_extractor is not None
                else f"HTTP {status_code}"
            )
            backoff_seconds = 2 ** (attempt - 1)
            logger.warning(
                "%s notification returned retryable status %s on attempt %d/%d: %s. Retrying in %ss",
                provider,
                status_code,
                attempt,
                max_attempts,
                error_text,
                backoff_seconds,
            )
            time.sleep(backoff_seconds)
            continue

        if success_evaluator(body, response):
            return make_result(
                provider,
                success=True,
                response=body,
                status_code=status_code,
                attempts=attempt,
                event=event,
            )

        error_text = (
            error_extractor(body, response)
            if error_extractor is not None
            else f"HTTP {status_code}"
        )
        retryable_result = (
            should_retry_result(body, response)
            if should_retry_result is not None
            else False
        )

        if retryable_result and attempt < max_attempts:
            backoff_seconds = 2 ** (attempt - 1)
            logger.warning(
                "%s notification returned retryable API error on attempt %d/%d: %s. Retrying in %ss",
                provider,
                attempt,
                max_attempts,
                error_text,
                backoff_seconds,
            )
            time.sleep(backoff_seconds)
            continue

        logger.error("%s notification failed: %s", provider, error_text)
        return make_result(
            provider,
            success=False,
            response=body,
            error=error_text,
            status_code=status_code,
            attempts=attempt,
            event=event,
        )

    return make_result(
        provider,
        success=False,
        response=None,
        error="Unknown notification failure",
        attempts=max_attempts,
        event=event,
    )
