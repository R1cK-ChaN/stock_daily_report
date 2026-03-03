"""
Fetch China A-shares market data from AKShare.

Uses Sina as primary source for index quotes (more reliable),
东方财富 for sectors and individual stocks.
Each sub-fetcher is isolated so partial failures don't crash the pipeline.
"""

import logging
import time
from datetime import datetime, date
from typing import Any

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

MAX_RETRIES = 4
RETRY_DELAY = 3  # seconds


def _retry(func, *args, **kwargs):
    """Retry a function call with exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            logger.warning("Attempt %d/%d failed: %s. Retrying...", attempt, MAX_RETRIES, e)
            time.sleep(RETRY_DELAY * attempt)


def fetch_index_quotes(indices: list[dict]) -> list[dict]:
    """
    Fetch real-time quotes for major A-share indices.
    Primary: stock_zh_index_spot_sina (reliable).
    Fallback: stock_zh_index_spot_em.
    """
    logger.info("Fetching index quotes...")

    # Build lookup: sina uses codes like "sh000001", "sz399001"
    # Config uses codes like "1.000001" (market.code_suffix)
    code_map = {}
    for idx in indices:
        cfg_code = idx["code"]
        raw = cfg_code.split(".")[-1] if "." in cfg_code else cfg_code
        prefix = cfg_code.split(".")[0] if "." in cfg_code else ""
        # 1 = Shanghai, 0 = Shenzhen
        sina_code = ("sh" if prefix == "1" else "sz") + raw
        code_map[sina_code] = idx["name"]
        code_map[raw] = idx["name"]  # fallback matching by raw code

    # Try sina first
    try:
        df = _retry(ak.stock_zh_index_spot_sina)
        results = []
        for _, row in df.iterrows():
            code = str(row.get("代码", ""))
            name = code_map.get(code)
            if name is None:
                continue
            results.append({
                "name": name,
                "code": code,
                "close": float(row.get("最新价", 0)),
                "change": float(row.get("涨跌额", 0)),
                "change_pct": float(row.get("涨跌幅", 0)),
                "volume": float(row.get("成交量", 0)),
                "amount": float(row.get("成交额", 0)),
                "open": float(row.get("今开", 0)),
                "high": float(row.get("最高", 0)),
                "low": float(row.get("最低", 0)),
                "prev_close": float(row.get("昨收", 0)),
            })
        if results:
            logger.info("Fetched %d index quotes via Sina", len(results))
            return results
    except Exception as e:
        logger.warning("Sina index fetch failed: %s, trying EM fallback", e)

    # Fallback: 东方财富
    try:
        df = _retry(ak.stock_zh_index_spot_em)
        results = []
        for _, row in df.iterrows():
            raw_code = str(row.get("代码", ""))
            name = code_map.get(raw_code)
            if name is None:
                continue
            results.append({
                "name": name,
                "code": raw_code,
                "close": float(row.get("最新价", 0)),
                "change": float(row.get("涨跌额", 0)),
                "change_pct": float(row.get("涨跌幅", 0)),
                "volume": float(row.get("成交量", 0)),
                "amount": float(row.get("成交额", 0)),
                "open": float(row.get("今开", 0)),
                "high": float(row.get("最高", 0)),
                "low": float(row.get("最低", 0)),
                "prev_close": float(row.get("昨收", 0)),
            })
        logger.info("Fetched %d index quotes via EM fallback", len(results))
        return results
    except Exception as e:
        logger.error("All index quote sources failed: %s", e)
        return []


def fetch_sector_performance(top_n_gainers: int = 5, top_n_losers: int = 5) -> dict:
    """Fetch sector/industry performance rankings.

    Primary: 东方财富 (stock_board_industry_name_em).
    Fallback: 同花顺 (stock_board_industry_summary_ths) when EM is down.
    """
    logger.info("Fetching sector performance...")

    # --- Primary: 东方财富 (EM) ---
    try:
        df = _retry(ak.stock_board_industry_name_em)
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
        losers.reverse()

        logger.info("Fetched sector performance: %d gainers, %d losers", len(gainers), len(losers))
        return {"gainers": gainers, "losers": losers}
    except Exception as e:
        logger.warning("EM sectors failed (%s), trying THS fallback...", e)

    # --- Fallback: 同花顺 (THS) ---
    try:
        df = _retry(ak.stock_board_industry_summary_ths)
        df = df.sort_values("涨跌幅", ascending=False)

        def _ths_row_to_dict(row):
            return {
                "name": row.get("板块", ""),
                "change_pct": float(row.get("涨跌幅", 0)),
                "turnover_rate": 0,
                "amount": float(row.get("总成交额", 0)) if pd.notna(row.get("总成交额")) else 0,
                "leader_stock": row.get("领涨股", ""),
                "leader_change_pct": float(row.get("领涨股-涨跌幅", 0)) if pd.notna(row.get("领涨股-涨跌幅")) else 0,
                "num_up": int(row.get("上涨家数", 0)) if pd.notna(row.get("上涨家数")) else 0,
                "num_down": int(row.get("下跌家数", 0)) if pd.notna(row.get("下跌家数")) else 0,
            }

        gainers = [_ths_row_to_dict(row) for _, row in df.head(top_n_gainers).iterrows()]
        losers = [_ths_row_to_dict(row) for _, row in df.tail(top_n_losers).iterrows()]
        losers.reverse()

        logger.info("Fetched sector performance via THS fallback: %d gainers, %d losers", len(gainers), len(losers))
        return {"gainers": gainers, "losers": losers}
    except Exception as e2:
        logger.error("All sector sources failed: %s", e2)
        return {"gainers": [], "losers": []}


def _fetch_em_spot_df():
    """Fetch A-share spot data once, with Sina fallback if EM is down."""
    try:
        df = _retry(ak.stock_zh_a_spot_em)
        logger.info("Fetched A-share spot data via EM (%d rows)", len(df))
        return df
    except Exception as e:
        logger.warning("EM spot failed (%s), trying Sina fallback...", e)
        try:
            df = _retry(ak.stock_zh_a_spot)
            logger.info("Fetched A-share spot data via Sina fallback (%d rows)", len(df))
            return df
        except Exception as e2:
            logger.error("All A-share spot sources failed: %s", e2)
            return None


def _compute_breadth(df) -> dict:
    """Compute market breadth from a pre-fetched spot DataFrame."""
    if df is None or df.empty:
        return {}

    logger.info("Computing market breadth...")
    change_pct = df["涨跌幅"].astype(float)

    up_count = int((change_pct > 0).sum())
    down_count = int((change_pct < 0).sum())
    flat_count = int((change_pct == 0).sum())
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


def _compute_top_movers(df, top_n: int = 10) -> dict:
    """Compute top gainers/losers from a pre-fetched spot DataFrame."""
    if df is None or df.empty:
        return {"gainers": [], "losers": []}

    logger.info("Computing top movers...")
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

    logger.info("Computed top %d gainers and losers", top_n)
    return {"gainers": gainers, "losers": losers}


def fetch_all_market_data(config: dict) -> dict:
    """
    Fetch all market data. Each sub-fetcher is isolated —
    a failure in one (e.g. sectors) won't lose data from others (e.g. indices).
    """
    market_cfg = config.get("market", {})
    sector_cfg = config.get("sectors", {})

    indices = fetch_index_quotes(market_cfg.get("indices", []))

    sectors = fetch_sector_performance(
        top_n_gainers=sector_cfg.get("top_n_gainers", 5),
        top_n_losers=sector_cfg.get("top_n_losers", 5),
    )

    # Fetch EM spot data once, shared by breadth and top_movers
    spot_df = _fetch_em_spot_df()
    breadth = _compute_breadth(spot_df)
    top_movers = _compute_top_movers(spot_df, top_n=10)

    return {
        "indices": indices,
        "sectors": sectors,
        "breadth": breadth,
        "top_movers": top_movers,
        "fetch_time": datetime.now().isoformat(),
    }
