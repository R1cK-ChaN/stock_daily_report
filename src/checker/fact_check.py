"""
Fact-checking layer for generated market reports.

Three layers of verification:
1. Data validation (pre-LLM) — freshness, completeness, range checks
2. Number cross-check (post-LLM) — verify all numbers in report match source data
3. LLM claim verification — use a second LLM call to check for ungrounded claims
"""

import json
import logging
import os
import re
from datetime import date, datetime
from typing import Any

import openai

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────
# Layer 1: Pre-LLM Data Validation
# ────────────────────────────────────────────────────────────

def validate_data_freshness(market_data: dict, pboc_data: dict) -> list[dict]:
    """
    Check that fetched data is from today.

    Returns:
        List of issue dicts with 'severity', 'field', 'message'.
    """
    issues = []
    today_str = date.today().isoformat()

    # Check market data fetch time
    fetch_time = market_data.get("fetch_time", "")
    if fetch_time and not fetch_time.startswith(today_str):
        issues.append({
            "severity": "critical",
            "field": "market_data.fetch_time",
            "message": f"Market data not from today (fetched: {fetch_time})",
        })

    # Check PBOC data date
    pboc_date = pboc_data.get("date", "")
    if pboc_date and pboc_date != today_str:
        issues.append({
            "severity": "warning",
            "field": "pboc_data.date",
            "message": f"PBOC data date mismatch (got: {pboc_date}, expected: {today_str})",
        })

    return issues


def validate_data_completeness(market_data: dict, news_data: dict, pboc_data: dict) -> list[dict]:
    """
    Check that all required data fields are present.

    Returns:
        List of issue dicts.
    """
    issues = []

    # Check index data
    if not market_data.get("indices"):
        issues.append({
            "severity": "critical",
            "field": "market_data.indices",
            "message": "No index data available",
        })

    # Check breadth data
    if not market_data.get("breadth"):
        issues.append({
            "severity": "warning",
            "field": "market_data.breadth",
            "message": "Market breadth data missing",
        })

    # Check sector data
    if not market_data.get("sectors"):
        issues.append({
            "severity": "warning",
            "field": "market_data.sectors",
            "message": "Sector performance data missing",
        })

    # Check news
    total_news = (
        len(news_data.get("market_news", []))
        + len(news_data.get("cctv_news", []))
    )
    if total_news == 0:
        issues.append({
            "severity": "warning",
            "field": "news_data",
            "message": "No news data available from any source",
        })

    # Check PBOC data (may legitimately be empty on non-operation days)
    if not pboc_data.get("has_data"):
        issues.append({
            "severity": "info",
            "field": "pboc_data",
            "message": "No PBOC operations today (may be normal)",
        })

    return issues


def validate_data_ranges(market_data: dict, config: dict) -> list[dict]:
    """
    Check that values are within reasonable ranges.

    Returns:
        List of issue dicts.
    """
    issues = []
    validation_cfg = config.get("validation", {})
    max_change = validation_cfg.get("max_daily_change_pct", 15.0)

    for idx in market_data.get("indices", []):
        # Check for extreme moves
        if abs(idx.get("change_pct", 0)) > max_change:
            issues.append({
                "severity": "warning",
                "field": f"index.{idx['name']}",
                "message": (
                    f"{idx['name']} change of {idx['change_pct']:.2f}% "
                    f"exceeds threshold of {max_change}%"
                ),
            })

        # Check for zero/missing close price
        if idx.get("close", 0) <= 0:
            issues.append({
                "severity": "critical",
                "field": f"index.{idx['name']}.close",
                "message": f"{idx['name']} has invalid close price: {idx.get('close')}",
            })

    return issues


def run_pre_generation_checks(
    market_data: dict,
    news_data: dict,
    pboc_data: dict,
    config: dict,
) -> dict:
    """
    Run all pre-LLM validation checks.

    Returns:
        Dict with 'passed', 'issues', 'critical_count', 'warning_count'.
    """
    all_issues = []
    all_issues.extend(validate_data_freshness(market_data, pboc_data))
    all_issues.extend(validate_data_completeness(market_data, news_data, pboc_data))
    all_issues.extend(validate_data_ranges(market_data, config))

    critical = [i for i in all_issues if i["severity"] == "critical"]
    warnings = [i for i in all_issues if i["severity"] == "warning"]

    result = {
        "passed": len(critical) == 0,
        "issues": all_issues,
        "critical_count": len(critical),
        "warning_count": len(warnings),
    }

    if critical:
        logger.error("Pre-generation checks FAILED with %d critical issues", len(critical))
        for issue in critical:
            logger.error("  CRITICAL: [%s] %s", issue["field"], issue["message"])
    else:
        logger.info(
            "Pre-generation checks passed (%d warnings, %d info)",
            len(warnings),
            len(all_issues) - len(critical) - len(warnings),
        )

    return result


# ────────────────────────────────────────────────────────────
# Layer 2: Post-LLM Number Cross-Check
# ────────────────────────────────────────────────────────────

def extract_numbers_from_text(text: str) -> list[dict]:
    """
    Extract all numbers from the generated report text.

    Returns:
        List of dicts with 'value' (float), 'context' (surrounding text).
    """
    # Match numbers including decimals and percentages
    pattern = r'(?<!\d)(\d+\.?\d*)\s*(%|亿|万|元|点|家|天|期|个月|年|号)?'
    matches = []

    for match in re.finditer(pattern, text):
        start = max(0, match.start() - 20)
        end = min(len(text), match.end() + 20)
        context = text[start:end].strip()

        try:
            value = float(match.group(1))
            unit = match.group(2) or ""
            matches.append({
                "value": value,
                "unit": unit,
                "context": context,
                "position": match.start(),
            })
        except ValueError:
            continue

    return matches


def build_source_numbers(market_data: dict, pboc_data: dict, news_data: dict | None = None) -> set[float]:
    """
    Build a set of all numbers present in the source data.
    Used for cross-checking against the generated report.
    """
    numbers = set()

    # Index data
    for idx in market_data.get("indices", []):
        for key in ["close", "change", "change_pct", "volume", "amount", "open", "high", "low", "amplitude"]:
            val = idx.get(key)
            if val is not None:
                numbers.add(round(float(val), 2))
                # Also add common transformations
                if key == "amount":
                    numbers.add(round(float(val) / 1e8, 2))  # 亿元
                # Add absolute values for change fields (report may say "跌幅5.21%" for -5.21)
                if key in ("change", "change_pct") and float(val) < 0:
                    numbers.add(round(abs(float(val)), 2))

    # Breadth data
    breadth = market_data.get("breadth", {})
    for key in ["up_count", "down_count", "flat_count", "limit_up", "limit_down", "total_stocks", "total_amount"]:
        val = breadth.get(key)
        if val is not None:
            numbers.add(round(float(val), 2))
            if key == "total_amount":
                numbers.add(round(float(val) / 1e8, 2))

    # Sector data
    for direction in ["gainers", "losers"]:
        for s in market_data.get("sectors", {}).get(direction, []):
            for key in ["change_pct", "leader_change_pct", "num_up", "num_down"]:
                val = s.get(key)
                if val is not None:
                    numbers.add(round(float(val), 2))
                    if key in ("change_pct", "leader_change_pct") and float(val) < 0:
                        numbers.add(round(abs(float(val)), 2))

    # PBOC data — repo rates, shibor, lpr
    repo = pboc_data.get("repo_rates", {})
    for key in ["FR001", "FR007", "FR014"]:
        val = repo.get(key)
        if val is not None:
            numbers.add(round(float(val), 2))
            numbers.add(round(float(val), 3))
    for entry in repo.get("recent_trend", []):
        for key in ["FR001", "FR007", "FR014"]:
            val = entry.get(key)
            if val is not None:
                numbers.add(round(float(val), 2))

    shibor = pboc_data.get("shibor", {})
    for key in ["overnight", "1W", "2W", "1M", "3M"]:
        val = shibor.get(key)
        if val is not None:
            numbers.add(round(float(val), 2))
            numbers.add(round(float(val), 3))

    lpr = pboc_data.get("lpr", {})
    for key in ["LPR_1Y", "LPR_5Y"]:
        val = lpr.get(key)
        if val is not None:
            numbers.add(round(float(val), 2))

    # PBOC OMO data
    omo = pboc_data.get("omo", {})
    if omo.get("has_data"):
        total = omo.get("total_amount")
        if total is not None:
            numbers.add(round(float(total), 1))
            numbers.add(round(float(total), 2))
        for op in omo.get("operations", []):
            for key in ["rate", "bid_amount", "win_amount"]:
                val = op.get(key)
                if val is not None:
                    numbers.add(round(float(val), 1))
                    numbers.add(round(float(val), 2))

    # Economic data from news
    if news_data:
        for econ in news_data.get("economic_data", []):
            try:
                val = float(econ.get("value", ""))
                numbers.add(round(val, 2))
                numbers.add(round(val, 1))
            except (ValueError, TypeError):
                pass

    return numbers


def cross_check_numbers(
    report_text: str,
    market_data: dict,
    pboc_data: dict,
    tolerance: float = 0.5,
    news_data: dict | None = None,
) -> dict:
    """
    Extract numbers from the report and verify they exist in source data.

    Args:
        report_text: Generated report text
        market_data: Source market data
        pboc_data: Source PBOC data
        tolerance: Allowed difference for floating point comparison
        news_data: Source news data (for economic indicator numbers)

    Returns:
        Dict with 'verified', 'unverified', 'total_numbers'.
    """
    report_numbers = extract_numbers_from_text(report_text)
    source_numbers = build_source_numbers(market_data, pboc_data, news_data)

    # Build set of numbers embedded in index names (e.g. 50 from "科创50", 100 from "中小100")
    index_name_numbers: set[float] = set()
    for idx in market_data.get("indices", []):
        for m in re.finditer(r'\d+', idx.get("name", "")):
            index_name_numbers.add(float(m.group()))

    verified = []
    unverified = []

    for num_info in report_numbers:
        value = num_info["value"]
        # Skip trivially common numbers (1, 2, 3, etc. under 10 — likely section numbers)
        if value < 10 and value == int(value) and num_info["unit"] == "":
            continue

        # Skip year-like numbers (2020–2030 with no unit)
        if 2020 <= value <= 2030 and value == int(value) and num_info["unit"] == "":
            continue

        # Skip numbers embedded in index names (e.g. 50, 100, 300)
        if value in index_name_numbers:
            continue

        # Skip numbers with temporal/ordinal units (e.g. "3个月", "2026年", "1号")
        if num_info["unit"] in ("个月", "年", "号"):
            continue

        # Check if this number is close to any source number
        matched = any(
            abs(value - src) <= tolerance
            for src in source_numbers
        )

        if matched:
            verified.append(num_info)
        else:
            unverified.append(num_info)

    result = {
        "verified": verified,
        "unverified": unverified,
        "total_numbers": len(report_numbers),
        "verified_count": len(verified),
        "unverified_count": len(unverified),
        "verification_rate": len(verified) / max(len(verified) + len(unverified), 1),
    }

    if unverified:
        logger.warning(
            "Number cross-check: %d/%d numbers unverified",
            len(unverified), len(verified) + len(unverified),
        )
        for num in unverified[:5]:
            logger.warning("  Unverified: %s (context: %s)", num["value"], num["context"])

    return result


# ────────────────────────────────────────────────────────────
# Layer 3: LLM Claim Verification
# ────────────────────────────────────────────────────────────

def verify_claims_with_llm(
    report_text: str,
    market_data: dict,
    news_data: dict,
    pboc_data: dict,
    config: dict,
) -> dict:
    """
    Use a second LLM call to verify that every claim in the report
    is grounded in the provided data.

    Returns:
        Dict with 'verified', 'issues', 'verifier_response'.
    """
    llm_cfg = config.get("llm", config.get("claude", {}))
    model = llm_cfg.get("model", "anthropic/claude-sonnet-4-20250514")
    base_url = llm_cfg.get("base_url", "https://openrouter.ai/api/v1")
    api_key = os.environ.get("OPENROUTER_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))

    # Build a concise data summary for the verifier
    # Include news content (not just headlines) to match what the report generator sees
    news_items = []
    for n in news_data.get("ranked_news", news_data.get("market_news", []))[:10]:
        item = {"title": n.get("title", "")}
        content = n.get("content") or n.get("summary") or n.get("description") or ""
        if content:
            item["content"] = content[:300]
        source = n.get("source") or n.get("feed_name") or ""
        if source:
            item["source"] = source
        news_items.append(item)

    data_summary = {
        "indices": market_data.get("indices", []),
        "breadth": market_data.get("breadth", {}),
        "sectors_gainers": [s["name"] + f" {s['change_pct']}%" for s in market_data.get("sectors", {}).get("gainers", [])],
        "sectors_losers": [s["name"] + f" {s['change_pct']}%" for s in market_data.get("sectors", {}).get("losers", [])],
        "news_items": news_items,
        "pboc_repo_rates": pboc_data.get("repo_rates", {}),
        "pboc_shibor": pboc_data.get("shibor", {}),
        "pboc_lpr": pboc_data.get("lpr", {}),
        "pboc_omo": pboc_data.get("omo", {}),
    }

    verification_prompt = f"""你是一位事实核查员。请检查以下市场报告中的每一个事实声明是否都有数据支持。

===== 原始数据 =====
{json.dumps(data_summary, ensure_ascii=False, indent=2)}

===== 待核查报告 =====
{report_text}

===== 核查要求 =====
请逐一检查报告中的：
1. 数字准确性已由独立的数字校验层处理，你无需重复检查数字。不要基于你自己的先验假设质疑数据的量级或合理性（如成交额大小、涨跌幅范围等），范围校验已在第一层完成。
2. 新闻引用：报告中的新闻内容是否可以从提供的新闻条目（标题和内容）中合理推断或综合得出。允许对多条新闻进行合理的综合概括，但不允许凭空捏造完全没有新闻来源支持的事件。
3. 所有因果关系和分析判断是否有数据基础

请用以下JSON格式回复：
{{
    "overall_verified": true/false,
    "issues": [
        {{
            "claim": "报告中的具体声明",
            "issue_type": "unsupported_claim|fabricated_news",
            "severity": "critical|warning|info",
            "explanation": "具体说明问题"
        }}
    ],
    "summary": "总体评估"
}}"""

    logger.info("Running LLM claim verification...")

    try:
        client = openai.OpenAI(base_url=base_url, api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            max_tokens=2048,
            temperature=0,
            messages=[{"role": "user", "content": verification_prompt}],
        )

        response_text = response.choices[0].message.content

        # Try to parse JSON from response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if json_match:
            verification_result = json.loads(json_match.group())
        else:
            verification_result = {
                "overall_verified": None,
                "issues": [],
                "summary": response_text,
            }

        usage = response.usage
        result = {
            "verified": verification_result.get("overall_verified", None),
            "issues": verification_result.get("issues", []),
            "summary": verification_result.get("summary", ""),
            "verifier_response": response_text,
            "usage": {
                "input_tokens": usage.prompt_tokens if usage else 0,
                "output_tokens": usage.completion_tokens if usage else 0,
            },
        }

        critical_issues = [i for i in result["issues"] if i.get("severity") == "critical"]
        if critical_issues:
            logger.warning("LLM verification found %d critical issues", len(critical_issues))
        else:
            logger.info("LLM verification passed")

        return result

    except Exception as e:
        logger.error("LLM claim verification failed: %s", e)
        return {
            "verified": None,
            "issues": [],
            "summary": f"Verification failed: {e}",
            "verifier_response": "",
        }


# ────────────────────────────────────────────────────────────
# Combined Check Runner
# ────────────────────────────────────────────────────────────

def run_post_generation_checks(
    report_text: str,
    market_data: dict,
    news_data: dict,
    pboc_data: dict,
    config: dict,
) -> dict:
    """
    Run all post-LLM verification checks.

    Returns:
        Dict with results from number cross-check and LLM verification.
    """
    # Layer 2: Number cross-check
    number_check = cross_check_numbers(report_text, market_data, pboc_data, news_data=news_data)

    # Layer 3: LLM claim verification
    claim_check = verify_claims_with_llm(
        report_text, market_data, news_data, pboc_data, config,
    )

    # Determine overall pass/fail
    validation_cfg = config.get("validation", {})
    number_threshold = validation_cfg.get("number_verification_rate", 0.60)
    number_ok = number_check["verification_rate"] >= number_threshold
    claims_ok = claim_check.get("verified", True) is not False

    # Count critical issues from LLM verification
    # Exclude number_mismatch — Layer 2 already handles number verification
    critical_claim_issues = [
        i for i in claim_check.get("issues", [])
        if i.get("severity") == "critical" and i.get("issue_type") != "number_mismatch"
    ]

    overall_passed = number_ok and len(critical_claim_issues) == 0

    # Build needs-review flags
    review_flags = []
    if not number_ok:
        review_flags.append(
            f"[NEEDS REVIEW] Number verification rate: {number_check['verification_rate']:.0%}"
        )
    for issue in critical_claim_issues:
        review_flags.append(f"[NEEDS REVIEW] {issue.get('claim', 'Unknown claim')}: {issue.get('explanation', '')}")

    return {
        "passed": overall_passed,
        "number_check": number_check,
        "claim_check": claim_check,
        "review_flags": review_flags,
        "checked_at": datetime.now().isoformat(),
    }
