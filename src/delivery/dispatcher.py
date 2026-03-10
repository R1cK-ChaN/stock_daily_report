"""
Notification event routing for outbound delivery providers.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.delivery.feishu import notify_feishu_event
from src.delivery.wechat import deliver_report as deliver_wechat_report

logger = logging.getLogger(__name__)

EVENT_REPORT_SUCCESS = "report_success"
EVENT_DELIVERY_BLOCKED = "delivery_blocked"
EVENT_PIPELINE_FAILURE = "pipeline_failure"
EVENT_PIPELINE_EXCEPTION = "pipeline_exception"


def _format_path(report_path: str | Path | None) -> str:
    if report_path is None:
        return "N/A"
    return str(report_path)


def _format_review_flags(review_flags: list[str] | None) -> str:
    if not review_flags:
        return "None"
    return "\n".join(f"- {flag}" for flag in review_flags)


def _build_report_success_message(
    report_text: str,
    *,
    report_path: str | Path | None,
    fact_check: dict | None,
    generated_at: str | None,
) -> str:
    run_date = datetime.now().strftime("%Y-%m-%d")
    fact_status = "PASSED" if fact_check and fact_check.get("passed") else "NEEDS REVIEW"
    lines = [
        f"[Daily Report] {run_date} {fact_status}",
        f"Generated: {generated_at or datetime.now().isoformat(timespec='seconds')}",
        f"Path: {_format_path(report_path)}",
    ]

    review_flags = fact_check.get("review_flags", []) if fact_check else []
    if review_flags:
        lines.append("Review flags:")
        lines.extend(f"- {flag}" for flag in review_flags)

    lines.append("")
    lines.append(report_text)
    return "\n".join(lines)


def _build_delivery_blocked_message(
    *,
    report_path: str | Path | None,
    review_flags: list[str] | None,
    reason: str | None,
) -> str:
    lines = [
        "[Alert] Delivery blocked after fact-check retry",
        f"Time: {datetime.now().isoformat(timespec='seconds')}",
        f"Path: {_format_path(report_path)}",
    ]
    if reason:
        lines.append(f"Reason: {reason}")
    lines.append("Review flags:")
    lines.append(_format_review_flags(review_flags))
    return "\n".join(lines)


def _build_pipeline_failure_message(
    *,
    stage: str | None,
    error: str | None,
    issues: list[dict] | None = None,
) -> str:
    lines = [
        f"[Alert] Pipeline failed at {stage or 'unknown'}",
        f"Time: {datetime.now().isoformat(timespec='seconds')}",
        f"Error: {error or 'unknown'}",
    ]
    if issues:
        critical_issues = [
            issue.get("message", "Unknown issue")
            for issue in issues
            if issue.get("severity") == "critical"
        ]
        if critical_issues:
            lines.append("Critical issues:")
            lines.extend(f"- {message}" for message in critical_issues[:5])
    return "\n".join(lines)


def _build_pipeline_exception_message(exception: Exception) -> str:
    return "\n".join([
        "[Alert] Unhandled exception in stock_daily_report",
        f"Time: {datetime.now().isoformat(timespec='seconds')}",
        f"Type: {type(exception).__name__}",
        f"Error: {exception}",
    ])


def _aggregate_results(event: str, results: list[dict]) -> dict:
    attempted_results = [result for result in results if not result.get("skipped")]
    success = all(result.get("success") for result in attempted_results) if attempted_results else True
    return {
        "event": event,
        "success": success,
        "results": results,
        "providers": {
            result.get("provider", f"provider_{index}"): result
            for index, result in enumerate(results)
        },
        "attempted_count": len(attempted_results),
        "skipped_count": len(results) - len(attempted_results),
    }


def summarize_delivery_result(result: dict | None) -> str:
    """Render provider results into a concise log-friendly summary."""
    if not result:
        return "skipped"

    summaries = []
    for provider, provider_result in result.get("providers", {}).items():
        if provider_result.get("skipped"):
            detail = provider_result.get("reason") or "skipped"
        elif provider_result.get("success"):
            detail = "OK"
        else:
            detail = provider_result.get("error") or "error"
        summaries.append(f"{provider}={detail}")
    return ", ".join(summaries) if summaries else "skipped"


def deliver_report(
    report_text: str,
    config: dict,
    *,
    report_path: str | Path | None = None,
    fact_check: dict | None = None,
    generated_at: str | None = None,
) -> dict:
    """
    Deliver the successful report to all success-path providers.
    """
    wechat_result = deliver_wechat_report(report_text, config)
    feishu_result = notify_feishu_event(
        EVENT_REPORT_SUCCESS,
        _build_report_success_message(
            report_text,
            report_path=report_path,
            fact_check=fact_check,
            generated_at=generated_at,
        ),
    )
    return _aggregate_results(EVENT_REPORT_SUCCESS, [wechat_result, feishu_result])


def notify_event(event: str, *, config: dict | None = None, **context: Any) -> dict:
    """
    Notify alert-style events. Feishu is the only alert provider in v1.
    """
    if event == EVENT_REPORT_SUCCESS:
        return deliver_report(
            context["report_text"],
            config or {},
            report_path=context.get("report_path"),
            fact_check=context.get("fact_check"),
            generated_at=context.get("generated_at"),
        )

    if event == EVENT_DELIVERY_BLOCKED:
        content = _build_delivery_blocked_message(
            report_path=context.get("report_path"),
            review_flags=context.get("review_flags"),
            reason=context.get("reason"),
        )
    elif event == EVENT_PIPELINE_FAILURE:
        content = _build_pipeline_failure_message(
            stage=context.get("stage"),
            error=context.get("error"),
            issues=context.get("issues"),
        )
    elif event == EVENT_PIPELINE_EXCEPTION:
        content = _build_pipeline_exception_message(context["exception"])
    else:
        logger.warning("Unknown notification event: %s", event)
        return _aggregate_results(event, [])

    return _aggregate_results(event, [notify_feishu_event(event, content)])
