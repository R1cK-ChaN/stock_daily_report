"""
Fetch PBOC (People's Bank of China) reverse repo operation data.

Primary source: AKShare macro_china_gksccz() for open market operations.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Any

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_repo_operations() -> dict:
    """
    Fetch PBOC open market operations (reverse repo, MLF, etc.).

    Uses AKShare macro_china_gksccz() which returns:
    - 操作日期: operation date
    - 期限(天): tenor in days
    - 交易量(亿元): transaction volume (100M RMB)
    - 中标利率(%): winning bid rate
    - 正/逆回购: repo type (forward/reverse)

    Returns:
        Dict with today's operations, maturing repos, net injection, and rate info.
    """
    logger.info("Fetching PBOC open market operations...")
    try:
        df = ak.macro_china_gksccz()
    except Exception as e:
        logger.error("Failed to fetch PBOC repo data: %s", e)
        raise

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    # Normalize date column
    date_col = None
    for col in df.columns:
        if "日期" in col or "date" in col.lower():
            date_col = col
            break

    if date_col is None:
        # Try first column as date
        date_col = df.columns[0]

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Today's operations
    today_ops = df[df[date_col].dt.date == today]

    # Calculate today's injection
    today_injection = 0.0
    today_details = []

    # Detect column names dynamically
    volume_col = next((c for c in df.columns if "交易量" in c or "量" in c), None)
    rate_col = next((c for c in df.columns if "利率" in c or "rate" in c.lower()), None)
    tenor_col = next((c for c in df.columns if "期限" in c or "tenor" in c.lower()), None)
    type_col = next((c for c in df.columns if "回购" in c or "type" in c.lower()), None)

    for _, row in today_ops.iterrows():
        volume = float(row[volume_col]) if volume_col and pd.notna(row.get(volume_col)) else 0
        rate = float(row[rate_col]) if rate_col and pd.notna(row.get(rate_col)) else 0
        tenor = int(row[tenor_col]) if tenor_col and pd.notna(row.get(tenor_col)) else 0
        op_type = str(row[type_col]) if type_col and pd.notna(row.get(type_col)) else ""

        is_reverse = "逆" in op_type  # 逆回购 = reverse repo (injection)
        if is_reverse:
            today_injection += volume
        else:
            today_injection -= volume

        today_details.append({
            "type": op_type,
            "tenor_days": tenor,
            "volume_billion": volume,
            "rate_pct": rate,
            "is_injection": is_reverse,
        })

    # Calculate maturing repos (operations from 7/14/28 days ago that mature today)
    maturing_volume = 0.0
    maturing_details = []
    for tenor_days in [7, 14, 28]:
        maturity_origin = today - timedelta(days=tenor_days)
        maturing_ops = df[
            (df[date_col].dt.date == maturity_origin)
        ]
        for _, row in maturing_ops.iterrows():
            tenor = int(row[tenor_col]) if tenor_col and pd.notna(row.get(tenor_col)) else 0
            if tenor == tenor_days:
                volume = float(row[volume_col]) if volume_col and pd.notna(row.get(volume_col)) else 0
                op_type = str(row[type_col]) if type_col and pd.notna(row.get(type_col)) else ""
                if "逆" in op_type:
                    maturing_volume += volume
                    maturing_details.append({
                        "origin_date": maturity_origin.isoformat(),
                        "tenor_days": tenor_days,
                        "volume_billion": volume,
                    })

    net_injection = today_injection - maturing_volume

    # Get recent rate trend (last 10 operations)
    recent_ops = df.sort_values(date_col, ascending=False).head(10)
    recent_rates = []
    for _, row in recent_ops.iterrows():
        if rate_col and pd.notna(row.get(rate_col)):
            recent_rates.append({
                "date": row[date_col].strftime("%Y-%m-%d") if pd.notna(row[date_col]) else "",
                "rate_pct": float(row[rate_col]),
            })

    result = {
        "date": today_str,
        "today_operations": today_details,
        "today_injection_billion": today_injection,
        "maturing_repos": maturing_details,
        "maturing_volume_billion": maturing_volume,
        "net_injection_billion": net_injection,
        "recent_rates": recent_rates,
        "has_data": len(today_details) > 0,
        "fetch_time": datetime.now().isoformat(),
    }

    if today_details:
        logger.info(
            "PBOC: injection=%.0f亿, maturing=%.0f亿, net=%.0f亿",
            today_injection, maturing_volume, net_injection,
        )
    else:
        logger.warning("No PBOC operations found for today (%s)", today_str)

    return result


def fetch_pboc_data(config: dict) -> dict:
    """
    Fetch all PBOC-related data.

    Args:
        config: Settings dict

    Returns:
        Dict with repo operation data.
    """
    return fetch_repo_operations()
