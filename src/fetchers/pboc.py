"""
Fetch PBOC (People's Bank of China) monetary policy data.

Uses AKShare functions:
- repo_rate_query(): daily repo rates (FR001/FR007/FR014)
- macro_china_lpr(): LPR rates
- macro_china_shibor_all(): SHIBOR interbank rates
"""

import logging
from datetime import datetime, date, timedelta
from typing import Any

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_repo_rates() -> dict:
    """
    Fetch latest repo rates (FR001, FR007, FR014).

    Returns:
        Dict with today's rates, recent trend, and metadata.
    """
    logger.info("Fetching repo rates...")
    try:
        df = ak.repo_rate_query()
    except Exception as e:
        logger.error("Failed to fetch repo rate data: %s", e)
        return {"has_data": False, "error": str(e)}

    if df.empty:
        return {"has_data": False, "error": "Empty dataframe"}

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date", ascending=False)

    latest = df.iloc[0]
    latest_date = latest["date"].strftime("%Y-%m-%d")

    # Recent trend (last 5 trading days)
    recent = []
    for _, row in df.head(5).iterrows():
        recent.append({
            "date": row["date"].strftime("%Y-%m-%d"),
            "FR001": float(row["FR001"]),
            "FR007": float(row["FR007"]),
            "FR014": float(row["FR014"]),
        })

    result = {
        "latest_date": latest_date,
        "FR001": float(latest["FR001"]),
        "FR007": float(latest["FR007"]),
        "FR014": float(latest["FR014"]),
        "recent_trend": recent,
        "has_data": True,
    }

    logger.info("Repo rates (%s): FR001=%.2f, FR007=%.2f, FR014=%.2f",
                latest_date, result["FR001"], result["FR007"], result["FR014"])
    return result


def fetch_shibor_rates() -> dict:
    """
    Fetch latest SHIBOR interbank rates.

    Returns:
        Dict with overnight, 1W, 2W, 1M, 3M rates.
    """
    logger.info("Fetching SHIBOR rates...")
    try:
        df = ak.macro_china_shibor_all()
    except Exception as e:
        logger.error("Failed to fetch SHIBOR data: %s", e)
        return {"has_data": False}

    if df.empty:
        return {"has_data": False}

    df["日期"] = pd.to_datetime(df["日期"])
    df = df.sort_values("日期", ascending=False)
    latest = df.iloc[0]
    latest_date = latest["日期"].strftime("%Y-%m-%d")

    result = {
        "latest_date": latest_date,
        "overnight": float(latest["O/N-定价"]),
        "1W": float(latest["1W-定价"]),
        "2W": float(latest["2W-定价"]),
        "1M": float(latest["1M-定价"]),
        "3M": float(latest["3M-定价"]),
        "has_data": True,
    }

    logger.info("SHIBOR (%s): O/N=%.3f, 1W=%.3f", latest_date, result["overnight"], result["1W"])
    return result


def fetch_lpr_rates() -> dict:
    """
    Fetch latest LPR (Loan Prime Rate).

    Returns:
        Dict with 1Y and 5Y LPR rates.
    """
    logger.info("Fetching LPR rates...")
    try:
        df = ak.macro_china_lpr()
    except Exception as e:
        logger.error("Failed to fetch LPR data: %s", e)
        return {"has_data": False}

    if df.empty:
        return {"has_data": False}

    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
    df = df.sort_values("TRADE_DATE", ascending=False)
    latest = df.iloc[0]

    result = {
        "latest_date": latest["TRADE_DATE"].strftime("%Y-%m-%d"),
        "LPR_1Y": float(latest["LPR1Y"]),
        "LPR_5Y": float(latest["LPR5Y"]),
        "has_data": True,
    }

    logger.info("LPR: 1Y=%.2f, 5Y=%.2f", result["LPR_1Y"], result["LPR_5Y"])
    return result


def fetch_pboc_data(config: dict) -> dict:
    """
    Fetch all PBOC-related monetary data.

    Returns:
        Dict with repo_rates, shibor, lpr, and metadata.
    """
    today_str = date.today().isoformat()

    repo_rates = fetch_repo_rates()
    shibor = fetch_shibor_rates()
    lpr = fetch_lpr_rates()

    has_data = repo_rates.get("has_data", False) or shibor.get("has_data", False)

    return {
        "date": today_str,
        "repo_rates": repo_rates,
        "shibor": shibor,
        "lpr": lpr,
        "has_data": has_data,
        "fetch_time": datetime.now().isoformat(),
    }
