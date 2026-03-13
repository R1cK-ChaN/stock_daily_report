"""
Daily China A-Shares Market Report — Pipeline Entry Point

Orchestrates the full workflow:
1. Fetch market data, news, and PBOC data (parallel)
2. Validate data (pre-LLM checks)
3. Generate report via OpenRouter
4. Fact-check the generated report (post-LLM checks)
5. Save output and deliver via configured webhook notifiers
"""

import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env from project root
load_dotenv(PROJECT_ROOT / ".env")

from src.fetchers.market_data import fetch_all_market_data
from src.fetchers.telegram_news import fetch_telegram_news
from src.fetchers.news_ranker import rank_news
from src.fetchers.pboc import fetch_pboc_data
from src.generator.report_generator import generate_report
from src.checker.fact_check import run_pre_generation_checks, run_post_generation_checks
from src.delivery import (
    EVENT_DELIVERY_BLOCKED,
    EVENT_PIPELINE_EXCEPTION,
    EVENT_PIPELINE_FAILURE,
    deliver_report,
    notify_event,
    summarize_delivery_result,
)
from src.delivery.common import env_bool

logger = logging.getLogger(__name__)


def allow_needs_review_delivery() -> bool:
    """Allow NEEDS REVIEW reports to be delivered for manual testing."""
    return env_bool("ALLOW_NEEDS_REVIEW_DELIVERY", False)


def build_test_delivery_text(report_text: str, review_flags: list[str], reason: str | None) -> str:
    """Prepend a clear warning banner when forcing delivery of a flagged report."""
    lines = [
        "[TEST DELIVERY][NEEDS REVIEW]",
        "This report failed fact-check but was delivered because ALLOW_NEEDS_REVIEW_DELIVERY=true.",
    ]
    if reason:
        lines.append(f"Reason: {reason}")
    if review_flags:
        lines.append("Review flags:")
        lines.extend(f"- {flag}" for flag in review_flags)
    lines.extend(["", report_text])
    return "\n".join(lines)


def _today_output_dir() -> Path:
    """Return today's date-based output directory, e.g. output/2026-03-01/."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    d = PROJECT_ROOT / "output" / today_str
    d.mkdir(parents=True, exist_ok=True)
    return d


def setup_logging():
    """Configure logging for the pipeline."""
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                _today_output_dir() / f"pipeline_{datetime.now():%H%M%S}.log",
                encoding="utf-8",
            ),
        ],
    )


def load_config() -> dict:
    """Load configuration from settings.yaml."""
    config_path = PROJECT_ROOT / "config" / "settings.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


def load_delivery_retry_config(config: dict) -> dict:
    """Load delivery retry settings with sane defaults."""
    retry_cfg = config.get("delivery_retry", {})
    initial_backoff = max(int(retry_cfg.get("initial_backoff_seconds", 30)), 0)
    max_backoff = max(int(retry_cfg.get("max_backoff_seconds", 300)), initial_backoff)
    return {
        "enabled": bool(retry_cfg.get("enabled", True)),
        "max_attempts": max(int(retry_cfg.get("max_attempts", 10)), 1),
        "initial_backoff_seconds": initial_backoff,
        "backoff_multiplier": max(int(retry_cfg.get("backoff_multiplier", 2)), 1),
        "max_backoff_seconds": max_backoff,
        "notify_each_blocked": bool(retry_cfg.get("notify_each_blocked", True)),
    }


def delivery_attempt_mode(attempt_number: int, max_attempts: int) -> str:
    """Return the retry strategy label for a given attempt."""
    if attempt_number == 1:
        return "initial run"
    if max_attempts > 1 and attempt_number == max_attempts:
        return "full refresh retry"
    return "same-data retry"


def should_refresh_attempt_data(attempt_number: int, max_attempts: int) -> bool:
    """Refresh source data on the first and final attempts only."""
    return attempt_number == 1 or (max_attempts > 1 and attempt_number == max_attempts)


def delivery_retry_backoff_seconds(attempt_number: int, retry_config: dict) -> int:
    """Compute the sleep duration before the next retry attempt."""
    base = retry_config["initial_backoff_seconds"]
    multiplier = retry_config["backoff_multiplier"]
    max_backoff = retry_config["max_backoff_seconds"]
    backoff = base * (multiplier ** max(attempt_number - 1, 0))
    return min(backoff, max_backoff)


def build_regeneration_hints(post_checks: dict | None) -> list[str]:
    """Deduplicate review flags and critical claim findings into retry hints."""
    if not post_checks:
        return []

    hints = []
    seen = set()

    for flag in post_checks.get("review_flags", []):
        normalized = str(flag).strip()
        if normalized and normalized not in seen:
            hints.append(normalized)
            seen.add(normalized)

    for issue in post_checks.get("claim_check", {}).get("issues", []):
        if issue.get("severity") != "critical":
            continue
        claim = str(issue.get("claim", "")).strip()
        explanation = str(issue.get("explanation", "")).strip()
        normalized = f"{claim}: {explanation}" if claim and explanation else claim or explanation
        if normalized and normalized not in seen:
            hints.append(normalized)
            seen.add(normalized)

    return hints


def attempt_artifact_paths(run_id: str, attempt_number: int) -> tuple[Path, Path]:
    """Return the report/audit paths for a retry attempt."""
    attempt_dir = _today_output_dir() / "attempts" / run_id
    return (
        attempt_dir / f"attempt_{attempt_number:02d}_report.md",
        attempt_dir / f"attempt_{attempt_number:02d}_audit.json",
    )


def fetch_all_data(config: dict) -> tuple[dict, dict, dict]:
    """
    Fetch all data sources in parallel.

    Returns:
        Tuple of (market_data, news_data, pboc_data)
    """
    logger.info("=" * 60)
    logger.info("STEP 1: Fetching data from all sources...")
    logger.info("=" * 60)

    results = {}

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_all_market_data, config): "market",
            executor.submit(fetch_telegram_news, config): "news",
            executor.submit(fetch_pboc_data, config): "pboc",
        }

        for future in as_completed(futures):
            source = futures[future]
            try:
                results[source] = future.result()
                logger.info("✓ %s data fetched successfully", source)
            except Exception as e:
                logger.error("✗ %s data fetch FAILED: %s", source, e)
                results[source] = {}

    return results.get("market", {}), results.get("news", {}), results.get("pboc", {})


def save_report(
    report_text: str,
    check_results: dict,
    news_data: dict,
    config: dict,
    *,
    report_path: Path | None = None,
    audit_path: Path | None = None,
    generated_at: str | None = None,
) -> Path:
    """
    Save the generated report to the output directory.

    Returns:
        Path to the saved report file.
    """
    output_dir = _today_output_dir()
    report_path = report_path or (output_dir / "report.md")
    audit_path = audit_path or (output_dir / "audit.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    # Add metadata header and review flags
    header_lines = [
        f"<!-- Generated: {generated_at or datetime.now().isoformat()} -->",
        f"<!-- Fact-check: {'PASSED' if check_results.get('passed') else 'NEEDS REVIEW'} -->",
    ]

    review_flags = check_results.get("review_flags", [])
    if review_flags:
        header_lines.append("<!-- Review flags:")
        for flag in review_flags:
            header_lines.append(f"  - {flag}")
        header_lines.append("-->")

    header = "\n".join(header_lines) + "\n\n"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(header + report_text)

    logger.info("Report saved to: %s", report_path)

    # Also save raw data for audit trail
    audit_data = {
        "generated_at": generated_at or datetime.now().isoformat(),
        "check_results": {
            "passed": check_results.get("passed"),
            "review_flags": check_results.get("review_flags", []),
            "number_check": {
                "verified_count": check_results.get("number_check", {}).get("verified_count", 0),
                "unverified_count": check_results.get("number_check", {}).get("unverified_count", 0),
                "verification_rate": check_results.get("number_check", {}).get("verification_rate", 0),
            },
        },
        "news_ranking": news_data.get("ranking_details", {}),
    }
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit_data, f, ensure_ascii=False, indent=2)

    return report_path


def prepare_generation_inputs(config: dict) -> tuple[dict | None, dict | None]:
    """Fetch, rank, and validate the source data for one generation cycle."""
    market_data, news_data, pboc_data = fetch_all_data(config)

    logger.info("=" * 60)
    logger.info("STEP 1.5: Ranking news by market relevance...")
    logger.info("=" * 60)

    try:
        news_data = rank_news(news_data, config)
        logger.info(
            "News ranking complete: %d ranked items",
            len(news_data.get("ranked_news", [])),
        )
    except Exception as e:
        logger.warning("News ranking failed (%s), proceeding with unranked news", e)

    logger.info("=" * 60)
    logger.info("STEP 2: Running pre-generation data validation...")
    logger.info("=" * 60)

    pre_checks = run_pre_generation_checks(market_data, news_data, pboc_data, config)

    if not pre_checks["passed"]:
        logger.error(
            "Pre-generation checks FAILED with %d critical issues. Aborting.",
            pre_checks["critical_count"],
        )
        for issue in pre_checks["issues"]:
            if issue["severity"] == "critical":
                logger.error("  CRITICAL: %s", issue["message"])
        notification_result = notify_event(
            EVENT_PIPELINE_FAILURE,
            config=config,
            stage="pre_validation",
            error="Critical data validation failures",
            issues=pre_checks["issues"],
        )
        return None, {
            "success": False,
            "stage": "pre_validation",
            "error": "Critical data validation failures",
            "issues": pre_checks["issues"],
            "notification": notification_result,
        }

    if pre_checks["warning_count"] > 0:
        logger.warning("Pre-checks passed with %d warnings", pre_checks["warning_count"])

    return {
        "market_data": market_data,
        "news_data": news_data,
        "pboc_data": pboc_data,
    }, None


def log_report_snapshot(report_path: Path, fallback_text: str) -> None:
    """
    Append the final report text to the pipeline log for traceability.
    This preserves each run's output even though report.md is overwritten.
    """
    try:
        report_content = report_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.warning("Failed to read saved report for log snapshot (%s), using in-memory text", e)
        report_content = fallback_text

    logger.info("=" * 60)
    logger.info("FINAL REPORT SNAPSHOT BEGIN")
    for line in report_content.splitlines():
        logger.info("[REPORT] %s", line)
    logger.info("FINAL REPORT SNAPSHOT END")
    logger.info("=" * 60)


def is_trading_day() -> bool:
    """Check if today is an A-share trading day using Sina's calendar.
    Covers weekends and Chinese holidays (Spring Festival, National Day, etc.).
    Falls back to a simple weekday check if the API call fails.
    """
    import akshare as ak

    today = datetime.now().date()
    try:
        df = ak.tool_trade_date_hist_sina()
        trade_dates = set(df["trade_date"].astype(str))
        return today.strftime("%Y-%m-%d") in trade_dates
    except Exception as e:
        logger.warning("Trading calendar fetch failed (%s), falling back to weekday check", e)
        return today.weekday() < 5


def run_pipeline():
    """
    Run the full report generation pipeline.

    Returns:
        Dict with pipeline results.
    """
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Daily A-Shares Market Report Pipeline")
    logger.info("Started at: %s", start_time.isoformat())
    logger.info("=" * 60)

    # Check if today is a trading day
    if not is_trading_day():
        logger.info("Today is not a trading day (weekend/holiday). Skipping.")
        return {
            "success": True,
            "skipped": True,
            "reason": "Not a trading day",
        }

    # Load config
    config = load_config()
    retry_config = load_delivery_retry_config(config)
    delivery_override_enabled = allow_needs_review_delivery()
    effective_max_attempts = 1 if delivery_override_enabled else (
        retry_config["max_attempts"] if retry_config["enabled"] else 1
    )
    run_id = start_time.strftime("%H%M%S_%f")
    data_bundle = None
    last_post_checks = None

    for attempt_number in range(1, effective_max_attempts + 1):
        attempt_mode = delivery_attempt_mode(attempt_number, effective_max_attempts)
        refresh_data = should_refresh_attempt_data(attempt_number, effective_max_attempts)

        logger.info("=" * 60)
        logger.info("ATTEMPT %d/%d: %s", attempt_number, effective_max_attempts, attempt_mode)
        logger.info("=" * 60)

        if data_bundle is None or refresh_data:
            if attempt_number > 1:
                logger.info("Refreshing source data for %s", attempt_mode)
            data_bundle, failure_result = prepare_generation_inputs(config)
            if failure_result:
                return failure_result
        else:
            logger.info("Reusing source data snapshot from attempt 1")

        market_data = data_bundle["market_data"]
        news_data = data_bundle["news_data"]
        pboc_data = data_bundle["pboc_data"]
        regeneration_hints = (
            build_regeneration_hints(last_post_checks)
            if attempt_mode == "same-data retry"
            else None
        )
        temperature_override = 0.0 if attempt_mode == "same-data retry" else None

        logger.info("=" * 60)
        logger.info("STEP 3: Generating report via OpenRouter...")
        logger.info("=" * 60)

        try:
            generation_result = generate_report(
                market_data,
                news_data,
                pboc_data,
                config,
                regeneration_hints=regeneration_hints,
                temperature_override=temperature_override,
            )
            report_text = generation_result["report_text"]
            logger.info("Report generated successfully (%d characters)", len(report_text))
        except Exception as e:
            logger.error("Report generation FAILED: %s", e)
            notification_result = notify_event(
                EVENT_PIPELINE_FAILURE,
                config=config,
                stage="generation",
                error=str(e),
            )
            return {
                "success": False,
                "stage": "generation",
                "error": str(e),
                "notification": notification_result,
            }

        logger.info("=" * 60)
        logger.info("STEP 4: Running post-generation fact checks...")
        logger.info("=" * 60)

        post_checks = run_post_generation_checks(
            report_text, market_data, news_data, pboc_data, config,
        )
        last_post_checks = post_checks

        attempt_report_path, attempt_audit_path = attempt_artifact_paths(run_id, attempt_number)
        attempt_report_path = save_report(
            report_text,
            post_checks,
            news_data,
            config,
            report_path=attempt_report_path,
            audit_path=attempt_audit_path,
            generated_at=generation_result.get("generated_at"),
        )

        if post_checks["passed"]:
            logger.info(
                "Post-generation checks PASSED on attempt %d/%d",
                attempt_number,
                effective_max_attempts,
            )
            logger.info("=" * 60)
            logger.info("STEP 5: Saving report and delivering...")
            logger.info("=" * 60)

            report_path = save_report(
                report_text,
                post_checks,
                news_data,
                config,
                generated_at=generation_result.get("generated_at"),
            )
            delivery_result = deliver_report(
                report_text,
                config,
                report_path=report_path,
                fact_check=post_checks,
                generated_at=generation_result.get("generated_at"),
            )

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("=" * 60)
            logger.info("Pipeline completed in %.1f seconds", elapsed)
            logger.info("Report: %s", report_path)
            logger.info("Fact-check: PASSED")
            logger.info("Delivery: %s", summarize_delivery_result(delivery_result))
            logger.info("=" * 60)
            log_report_snapshot(report_path, report_text)

            return {
                "success": True,
                "report_path": str(report_path),
                "elapsed_seconds": elapsed,
                "generation": {
                    "model": generation_result.get("model"),
                    "usage": generation_result.get("usage"),
                },
                "fact_check": {
                    "passed": True,
                    "review_flags": post_checks.get("review_flags", []),
                    "delivery_blocked": False,
                    "delivery_forced": False,
                    "delivery_blocked_reason": None,
                },
                "delivery": delivery_result,
                "attempts_used": attempt_number,
                "max_attempts": effective_max_attempts,
                "retry_exhausted": False,
                "final_attempt_mode": attempt_mode,
            }

        delivery_blocked_reason = (
            f"Fact-check failed after {effective_max_attempts} attempts"
            if attempt_number == effective_max_attempts
            else f"Fact-check failed on attempt {attempt_number}/{effective_max_attempts}"
        )

        if delivery_override_enabled:
            logger.info("=" * 60)
            logger.info("STEP 5: Saving report and delivering...")
            logger.info("=" * 60)

            report_path = save_report(
                report_text,
                post_checks,
                news_data,
                config,
                generated_at=generation_result.get("generated_at"),
            )
            delivery_result = deliver_report(
                build_test_delivery_text(
                    report_text,
                    post_checks.get("review_flags", []),
                    delivery_blocked_reason,
                ),
                config,
                report_path=report_path,
                fact_check=post_checks,
                generated_at=generation_result.get("generated_at"),
            )

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.warning(
                "Delivery override enabled via ALLOW_NEEDS_REVIEW_DELIVERY — sending NEEDS REVIEW report"
            )
            logger.info("=" * 60)
            logger.info("Pipeline completed in %.1f seconds", elapsed)
            logger.info("Report: %s", report_path)
            logger.info("Fact-check: NEEDS REVIEW")
            logger.info(
                "Delivery: FORCED (NEEDS REVIEW); providers: %s",
                summarize_delivery_result(delivery_result),
            )
            logger.info("=" * 60)
            log_report_snapshot(report_path, report_text)

            return {
                "success": True,
                "report_path": str(report_path),
                "elapsed_seconds": elapsed,
                "generation": {
                    "model": generation_result.get("model"),
                    "usage": generation_result.get("usage"),
                },
                "fact_check": {
                    "passed": False,
                    "review_flags": post_checks.get("review_flags", []),
                    "delivery_blocked": False,
                    "delivery_forced": True,
                    "delivery_blocked_reason": delivery_blocked_reason,
                },
                "delivery": delivery_result,
                "attempts_used": attempt_number,
                "max_attempts": effective_max_attempts,
                "retry_exhausted": False,
                "final_attempt_mode": attempt_mode,
            }

        retry_exhausted = attempt_number == effective_max_attempts
        next_delay_seconds = (
            None if retry_exhausted else delivery_retry_backoff_seconds(attempt_number, retry_config)
        )
        delivery_result = None
        if retry_config["notify_each_blocked"] or retry_exhausted:
            delivery_result = notify_event(
                EVENT_DELIVERY_BLOCKED,
                config=config,
                report_path=attempt_report_path,
                review_flags=post_checks.get("review_flags", []),
                reason=delivery_blocked_reason,
                attempt=attempt_number,
                max_attempts=effective_max_attempts,
                attempt_mode=attempt_mode,
                next_delay_seconds=next_delay_seconds,
                is_final_attempt=retry_exhausted,
            )

        if retry_exhausted:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.error(
                "Post-generation checks STILL FAILED after %d attempts — delivery BLOCKED",
                effective_max_attempts,
            )
            logger.info("=" * 60)
            logger.info("Pipeline failed in %.1f seconds", elapsed)
            logger.info("Report: %s", attempt_report_path)
            logger.info("Fact-check: NEEDS REVIEW")
            logger.info(
                "Delivery: BLOCKED after %d attempts; alerts: %s",
                effective_max_attempts,
                summarize_delivery_result(delivery_result),
            )
            logger.info("=" * 60)
            log_report_snapshot(attempt_report_path, report_text)

            return {
                "success": False,
                "stage": "delivery_retry_exhausted",
                "error": delivery_blocked_reason,
                "report_path": str(attempt_report_path),
                "elapsed_seconds": elapsed,
                "generation": {
                    "model": generation_result.get("model"),
                    "usage": generation_result.get("usage"),
                },
                "fact_check": {
                    "passed": False,
                    "review_flags": post_checks.get("review_flags", []),
                    "delivery_blocked": True,
                    "delivery_forced": False,
                    "delivery_blocked_reason": delivery_blocked_reason,
                },
                "delivery": delivery_result,
                "attempts_used": attempt_number,
                "max_attempts": effective_max_attempts,
                "retry_exhausted": True,
                "final_attempt_mode": attempt_mode,
            }

        logger.warning(
            "Delivery BLOCKED on attempt %d/%d — retrying in %ss",
            attempt_number,
            effective_max_attempts,
            next_delay_seconds,
        )
        time.sleep(next_delay_seconds)


def main():
    """CLI entry point."""
    # Ensure output directory exists before setting up logging
    (PROJECT_ROOT / "output").mkdir(parents=True, exist_ok=True)

    setup_logging()

    try:
        result = run_pipeline()
        if result.get("skipped"):
            print(f"\nSkipped: {result.get('reason')}")
        elif result["success"]:
            print(f"\nReport generated successfully: {result['report_path']}")
        else:
            print(f"\nPipeline failed at stage: {result.get('stage', 'unknown')}")
            print(f"Error: {result.get('error', 'unknown')}")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception("Unexpected pipeline error: %s", e)
        try:
            notify_event(EVENT_PIPELINE_EXCEPTION, exception=e)
        except Exception as notify_exc:
            logger.error("Failed to send exception notification: %s", notify_exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
