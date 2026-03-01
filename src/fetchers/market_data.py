"""
Fetch China A-shares market data from AKShare (东方财富).

Provides: index prices, sector performance, top gainers/losers, volume/turnover.
"""

import logging
from datetime import datetime, date
from typing import Any

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_index_quotes(indices: list[dict]) -> list[dict]:
    """
    Fetch real-time quotes for major A-share indices.

    Args:
        indices: List of dicts with 'code' and 'name' keys,
                 e.g. [{"code": "1.000001", "name": "上证指数"}]

    Returns:
        List of dicts with index name, close, change, change_pct, volume, amount.
    """
    logger.info("Fetching index quotes...")
    try:
        # stock_zh_index_spot_em returns all indices from 东方财富
        df = ak.stock_zh_index_spot_em()
    except Exception as e:
        logger.error("Failed to fetch index spot data: %s", e)
        raise

    index_codes = {idx["code"]: idx["name"] for idx in indices}
    results = []

    for _, row in df.iterrows():
        # Match by code — the df uses 代码 column
        code = row.get("代码", "")
        # Build the composite code like "1.000001" for matching
        # AKShare spot_em returns raw codes like "000001"
        # We need to match against our config codes
        for cfg_code, cfg_name in index_codes.items():
            raw_code = cfg_code.split(".")[-1] if "." in cfg_code else cfg_code
            if code == raw_code:
                results.append({
                    "name": cfg_name,
                    "code": code,
                    "close": float(row.get("最新价", 0)),
                    "change": float(row.get("涨跌额", 0)),
                    "change_pct": float(row.get("涨跌幅", 0)),
                    "volume": float(row.get("成交量", 0)),
                    "amount": float(row.get("成交额", 0)),
                    "open": float(row.get("今开", 0)),
                    "high": float(row.get("最高", 0)),
                    "low": float(row.get("最低", 0)),
                    "amplitude": float(row.get("振幅", 0)),
                })
                break

    logger.info("Fetched %d index quotes", len(results))
    return results


def fetch_sector_performance(top_n_gainers: int = 5, top_n_losers: int = 5) -> dict:
    """
    Fetch sector/industry performance rankings.

    Returns:
        Dict with 'gainers' and 'losers' lists, each containing
        sector name, change_pct, leader stock, etc.
    """
    logger.info("Fetching sector performance...")
    try:
        df = ak.stock_board_industry_name_em()
    except Exception as e:
        logger.error("Failed to fetch sector data: %s", e)
        raise

    # Sort by 涨跌幅 (change percentage)
    df = df.sort_values("涨跌幅", ascending=False)

    def row_to_dict(row):
        return {
            "name": row.get("板块名称", ""),
            "change_pct": float(row.get("涨跌幅", 0)),
            "turnover_rate": float(row.get("换手率", 0)) if pd.notna(row.get("换手率")) else 0,
            "amount": float(row.get("总成交额", 0)) if pd.notna(row.get("总成交额")) else 0,
            "leader_stock": row.get("领涨股票", ""),
            "leader_change_pct": float(row.get("领涨股票-涨跌幅", 0)) if pd.notna(row.get("领涨股票-涨跌幅")) else 0,
            "num_up": int(row.get("上涨家数", 0)) if pd.notna(row.get("上涨家数")) else 0,
            "num_down": int(row.get("下跌家数", 0)) if pd.notna(row.get("下跌家数")) else 0,
        }

    gainers = [row_to_dict(row) for _, row in df.head(top_n_gainers).iterrows()]
    losers = [row_to_dict(row) for _, row in df.tail(top_n_losers).iterrows()]
    # Reverse losers so worst is first
    losers.reverse()

    logger.info("Fetched sector performance: %d gainers, %d losers", len(gainers), len(losers))
    return {"gainers": gainers, "losers": losers}


def fetch_market_breadth() -> dict:
    """
    Fetch market breadth — number of stocks up/down/flat.

    Returns:
        Dict with up_count, down_count, flat_count, limit_up, limit_down.
    """
    logger.info("Fetching market breadth...")
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        logger.error("Failed to fetch A-share spot data: %s", e)
        raise

    change_pct = df["涨跌幅"].astype(float)
    close = df["最新价"]
    prev_close = close - df["涨跌额"].astype(float)

    up_count = int((change_pct > 0).sum())
    down_count = int((change_pct < 0).sum())
    flat_count = int((change_pct == 0).sum())

    # Limit up/down: roughly +/-10% for main board, +/-20% for ChiNext/STAR
    # Simplified: check if change_pct >= 9.9 or <= -9.9
    limit_up = int((change_pct >= 9.9).sum())
    limit_down = int((change_pct <= -9.9).sum())

    total_volume = float(df["成交量"].astype(float).sum())
    total_amount = float(df["成交额"].astype(float).sum())

    result = {
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "total_stocks": len(df),
        "total_volume": total_volume,
        "total_amount": total_amount,
    }
    logger.info("Market breadth: %d up, %d down, %d flat", up_count, down_count, flat_count)
    return result


def fetch_top_movers(top_n: int = 10) -> dict:
    """
    Fetch top gaining and losing individual stocks.

    Returns:
        Dict with 'gainers' and 'losers' lists.
    """
    logger.info("Fetching top movers...")
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        logger.error("Failed to fetch A-share spot data: %s", e)
        raise

    # Filter out stocks with 0 volume (suspended)
    df = df[df["成交量"].astype(float) > 0].copy()
    df["涨跌幅"] = df["涨跌幅"].astype(float)
    df = df.sort_values("涨跌幅", ascending=False)

    def stock_to_dict(row):
        return {
            "code": row.get("代码", ""),
            "name": row.get("名称", ""),
            "close": float(row.get("最新价", 0)),
            "change_pct": float(row.get("涨跌幅", 0)),
            "amount": float(row.get("成交额", 0)),
        }

    gainers = [stock_to_dict(row) for _, row in df.head(top_n).iterrows()]
    losers = [stock_to_dict(row) for _, row in df.tail(top_n).iterrows()]
    losers.reverse()

    logger.info("Fetched top %d gainers and losers", top_n)
    return {"gainers": gainers, "losers": losers}


def fetch_all_market_data(config: dict) -> dict:
    """
    Fetch all market data in one call.

    Args:
        config: Settings dict from config/settings.yaml

    Returns:
        Dict with keys: indices, sectors, breadth, top_movers, fetch_time
    """
    market_cfg = config.get("market", {})
    sector_cfg = config.get("sectors", {})

    indices = fetch_index_quotes(market_cfg.get("indices", []))
    sectors = fetch_sector_performance(
        top_n_gainers=sector_cfg.get("top_n_gainers", 5),
        top_n_losers=sector_cfg.get("top_n_losers", 5),
    )
    breadth = fetch_market_breadth()
    top_movers = fetch_top_movers(top_n=10)

    return {
        "indices": indices,
        "sectors": sectors,
        "breadth": breadth,
        "top_movers": top_movers,
        "fetch_time": datetime.now().isoformat(),
    }
