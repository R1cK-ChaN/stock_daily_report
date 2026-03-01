"""
Daily China A-Shares Market Report — Pipeline Entry Point

Orchestrates the full workflow:
1. Fetch market data, news, and PBOC data (parallel)
2. Validate data (pre-LLM checks)
3. Generate report via Claude API
4. Fact-check the generated report (post-LLM checks)
5. Save output and deliver via WeChat
"""

import json
import logging
import os
import sys
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
from src.fetchers.news import fetch_all_news
from src.fetchers.pboc import fetch_pboc_data
from src.generator.report_generator import generate_report
from src.checker.fact_check import run_pre_generation_checks, run_post_generation_checks
from src.delivery.wechat import deliver_report

logger = logging.getLogger(__name__)


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
            executor.submit(fetch_all_news, config): "news",
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


def save_report(report_text: str, check_results: dict, config: dict) -> Path:
    """
    Save the generated report to the output directory.

    Returns:
        Path to the saved report file.
    """
    output_dir = _today_output_dir()
    today_str = datetime.now().strftime("%Y-%m-%d")

    report_path = output_dir / "report.md"

    # Add metadata header and review flags
    header_lines = [
        f"<!-- Generated: {datetime.now().isoformat()} -->",
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
    audit_path = output_dir / "audit.json"
    audit_data = {
        "generated_at": datetime.now().isoformat(),
        "check_results": {
            "passed": check_results.get("passed"),
            "review_flags": check_results.get("review_flags", []),
            "number_check": {
                "verified_count": check_results.get("number_check", {}).get("verified_count", 0),
                "unverified_count": check_results.get("number_check", {}).get("unverified_count", 0),
                "verification_rate": check_results.get("number_check", {}).get("verification_rate", 0),
            },
        },
    }
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit_data, f, ensure_ascii=False, indent=2)

    return report_path


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

    # Step 1: Fetch all data
    market_data, news_data, pboc_data = fetch_all_data(config)

    # Step 2: Pre-generation validation
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
        return {
            "success": False,
            "stage": "pre_validation",
            "error": "Critical data validation failures",
            "issues": pre_checks["issues"],
        }

    if pre_checks["warning_count"] > 0:
        logger.warning("Pre-checks passed with %d warnings", pre_checks["warning_count"])

    # Step 3: Generate report
    logger.info("=" * 60)
    logger.info("STEP 3: Generating report via Claude API...")
    logger.info("=" * 60)

    try:
        generation_result = generate_report(market_data, news_data, pboc_data, config)
        report_text = generation_result["report_text"]
        logger.info("Report generated successfully (%d characters)", len(report_text))
    except Exception as e:
        logger.error("Report generation FAILED: %s", e)
        return {
            "success": False,
            "stage": "generation",
            "error": str(e),
        }

    # Step 4: Post-generation fact-checking
    logger.info("=" * 60)
    logger.info("STEP 4: Running post-generation fact checks...")
    logger.info("=" * 60)

    post_checks = run_post_generation_checks(
        report_text, market_data, news_data, pboc_data, config,
    )

    if not post_checks["passed"]:
        logger.warning("Post-generation checks found issues — report marked for review")
    else:
        logger.info("Post-generation checks PASSED")

    # Step 5: Save report
    logger.info("=" * 60)
    logger.info("STEP 5: Saving report and delivering...")
    logger.info("=" * 60)

    report_path = save_report(report_text, post_checks, config)

    # Step 6: Deliver via WeChat
    delivery_result = deliver_report(report_text, config)

    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("Pipeline completed in %.1f seconds", elapsed)
    logger.info("Report: %s", report_path)
    logger.info("Fact-check: %s", "PASSED" if post_checks["passed"] else "NEEDS REVIEW")
    logger.info("Delivery: %s", "OK" if delivery_result.get("success") else delivery_result.get("error", "skipped"))
    logger.info("=" * 60)

    return {
        "success": True,
        "report_path": str(report_path),
        "elapsed_seconds": elapsed,
        "generation": {
            "model": generation_result.get("model"),
            "usage": generation_result.get("usage"),
        },
        "fact_check": {
            "passed": post_checks["passed"],
            "review_flags": post_checks.get("review_flags", []),
        },
        "delivery": delivery_result,
    }


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
        sys.exit(1)


if __name__ == "__main__":
    main()
