"""
Fetch financial news for China A-shares market report.

Sources:
- stock_info_global_em()  — 东方财富 broad market headlines (primary)
- stock_info_global_cls() — 财联社 real-time flash news (secondary)
- stock_info_global_futu() — 富途 international coverage (tertiary)
- news_cctv()             — 央视 policy signals (weekdays)
"""

import logging
from datetime import datetime
from typing import Any

import akshare as ak

logger = logging.getLogger(__name__)


def fetch_em_news(max_items: int = 50) -> list[dict]:
    """
    Fetch broad market news from 东方财富 via stock_info_global_em().

    Returns:
        List of standardized dicts: {title, content, publish_time, source, url}
    """
    logger.info("Fetching 东方财富 global news...")
    try:
        df = ak.stock_info_global_em()
    except Exception as e:
        logger.error("stock_info_global_em() failed: %s", e)
        return []

    results = []
    for _, row in df.head(max_items).iterrows():
        item = {
            "title": str(row.get("标题", "")),
            "content": str(row.get("摘要", row.get("内容", "")))[:500],
            "publish_time": str(row.get("发布时间", "")),
            "source": "东方财富",
            "url": str(row.get("链接", row.get("新闻链接", ""))),
        }
        if item["title"]:
            results.append(item)

    logger.info("Fetched %d news items from 东方财富", len(results))
    return results


def fetch_cls_news(max_items: int = 20) -> list[dict]:
    """
    Fetch real-time flash news from 财联社 via stock_info_global_cls().

    Returns:
        List of standardized dicts: {title, content, publish_time, source, url}
    """
    logger.info("Fetching 财联社 flash news...")
    try:
        df = ak.stock_info_global_cls()
    except Exception as e:
        logger.error("stock_info_global_cls() failed: %s", e)
        return []

    results = []
    for _, row in df.head(max_items).iterrows():
        title = str(row.get("标题", ""))
        content = str(row.get("内容", ""))[:500]
        # CLS may have date and time in separate columns
        pub_date = str(row.get("发布日期", ""))
        pub_time = str(row.get("发布时间", ""))
        publish_time = f"{pub_date} {pub_time}".strip()

        item = {
            "title": title if title else content[:80],
            "content": content,
            "publish_time": publish_time,
            "source": "财联社",
            "url": "",
        }
        if item["title"]:
            results.append(item)

    logger.info("Fetched %d news items from 财联社", len(results))
    return results


def fetch_futu_news(max_items: int = 50) -> list[dict]:
    """
    Fetch international market news from 富途 via stock_info_global_futu().

    Returns:
        List of standardized dicts: {title, content, publish_time, source, url}
    """
    logger.info("Fetching 富途 news...")
    try:
        df = ak.stock_info_global_futu()
    except Exception as e:
        logger.error("stock_info_global_futu() failed: %s", e)
        return []

    results = []
    for _, row in df.head(max_items).iterrows():
        item = {
            "title": str(row.get("标题", "")),
            "content": str(row.get("内容", row.get("摘要", "")))[:500],
            "publish_time": str(row.get("发布时间", "")),
            "source": "富途",
            "url": str(row.get("链接", "")),
        }
        if item["title"]:
            results.append(item)

    logger.info("Fetched %d news items from 富途", len(results))
    return results


def fetch_cctv_news() -> list[dict]:
    """
    Fetch CCTV financial news via AKShare (联播).

    Returns:
        List of standardized dicts: {title, content, publish_time, source, url}
    """
    logger.info("Fetching CCTV news...")
    try:
        today_str = datetime.now().strftime("%Y%m%d")
        df = ak.news_cctv(date=today_str)
    except Exception as e:
        logger.warning("CCTV news fetch failed: %s", e)
        return []

    results = []
    for _, row in df.iterrows():
        item = {
            "title": str(row.get("title", "")),
            "content": str(row.get("content", ""))[:500],
            "publish_time": today_str,
            "source": "央视新闻联播",
            "url": "",
        }
        if item["title"]:
            results.append(item)

    logger.info("Fetched %d CCTV news items", len(results))
    return results


def fetch_economic_calendar() -> list[dict]:
    """
    Fetch recent economic data releases (CPI, PMI, etc.).

    Returns:
        List of dicts with indicator name, value, period, source.
    """
    logger.info("Fetching economic data...")
    econ_data = []

    # Try to fetch recent CPI
    try:
        df = ak.macro_china_cpi_yearly()
        if not df.empty:
            latest = df.iloc[-1]
            econ_data.append({
                "indicator": "CPI同比",
                "value": str(latest.iloc[-1]),
                "period": str(latest.iloc[0]),
                "source": "国家统计局",
            })
    except Exception as e:
        logger.warning("CPI fetch failed: %s", e)

    # Try to fetch recent PMI
    try:
        df = ak.macro_china_pmi_yearly()
        if not df.empty:
            latest = df.iloc[-1]
            econ_data.append({
                "indicator": "制造业PMI",
                "value": str(latest.iloc[-1]),
                "period": str(latest.iloc[0]),
                "source": "国家统计局",
            })
    except Exception as e:
        logger.warning("PMI fetch failed: %s", e)

    # Try GDP growth
    try:
        df = ak.macro_china_gdp_yearly()
        if not df.empty:
            latest = df.iloc[-1]
            econ_data.append({
                "indicator": "GDP同比增速",
                "value": str(latest.iloc[-1]),
                "period": str(latest.iloc[0]),
                "source": "国家统计局",
            })
    except Exception as e:
        logger.warning("GDP fetch failed: %s", e)

    logger.info("Fetched %d economic indicators", len(econ_data))
    return econ_data


def fetch_all_news(config: dict) -> dict:
    """
    Fetch all news data from multiple sources.

    Args:
        config: Settings dict

    Returns:
        Dict with keys: market_news, cctv_news, economic_data, fetch_time
    """
    news_cfg = config.get("news", {})
    max_headlines = news_cfg.get("max_headlines", 50)

    # Primary: 东方财富 broad market news
    em_news = fetch_em_news(max_items=max_headlines)

    # Secondary: 财联社 flash news
    cls_news = fetch_cls_news(max_items=20)

    # Tertiary: 富途 international coverage
    futu_news = fetch_futu_news(max_items=max_headlines)

    # Combine all market news into a single list
    market_news = em_news + cls_news + futu_news

    # CCTV policy signals
    cctv_news = fetch_cctv_news()

    economic_data = fetch_economic_calendar()

    logger.info(
        "Total news fetched: %d market (%d EM + %d CLS + %d futu), %d CCTV, %d econ",
        len(market_news), len(em_news), len(cls_news), len(futu_news),
        len(cctv_news), len(economic_data),
    )

    return {
        "market_news": market_news,
        "cctv_news": cctv_news,
        "economic_data": economic_data,
        "fetch_time": datetime.now().isoformat(),
    }
