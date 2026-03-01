"""
Fetch financial news for China A-shares market report.

Sources: AKShare (东方财富 news feed), RSS feeds (财联社, etc.).
"""

import logging
from datetime import datetime
from typing import Any

import akshare as ak
import feedparser
import requests

logger = logging.getLogger(__name__)

# RSS feeds for Chinese financial news
RSS_FEEDS = {
    "cls": "https://www.cls.cn/rss",  # 财联社
}


def fetch_eastmoney_news(max_items: int = 20) -> list[dict]:
    """
    Fetch latest financial news from 东方财富 via AKShare.

    Returns:
        List of dicts with title, content, publish_time, source.
    """
    logger.info("Fetching 东方财富 news...")
    try:
        df = ak.stock_news_em(symbol="300059")
        # stock_news_em requires a symbol — use a popular stock to get general news
        # Alternatively try the general financial news endpoint
    except Exception:
        logger.warning("stock_news_em failed, trying alternative news source...")
        try:
            # Try general financial news
            df = ak.news_cctv(date=datetime.now().strftime("%Y%m%d"))
        except Exception as e:
            logger.error("All news fetching failed: %s", e)
            return []

    results = []
    for _, row in df.head(max_items).iterrows():
        item = {
            "title": str(row.get("新闻标题", row.get("title", ""))),
            "content": str(row.get("新闻内容", row.get("content", "")))[:500],
            "publish_time": str(row.get("发布时间", row.get("date", ""))),
            "source": "东方财富",
            "url": str(row.get("新闻链接", row.get("url", ""))),
        }
        if item["title"]:
            results.append(item)

    logger.info("Fetched %d news items from 东方财富", len(results))
    return results


def fetch_cctv_news() -> list[dict]:
    """
    Fetch CCTV financial news via AKShare (联播).

    Returns:
        List of dicts with title, content, source.
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


def fetch_rss_news(feed_url: str, source_name: str, max_items: int = 10) -> list[dict]:
    """
    Fetch news from an RSS feed.

    Returns:
        List of dicts with title, content, publish_time, source, url.
    """
    logger.info("Fetching RSS feed: %s", source_name)
    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        logger.error("RSS fetch failed for %s: %s", source_name, e)
        return []

    results = []
    for entry in feed.entries[:max_items]:
        item = {
            "title": entry.get("title", ""),
            "content": entry.get("summary", "")[:500],
            "publish_time": entry.get("published", ""),
            "source": source_name,
            "url": entry.get("link", ""),
        }
        if item["title"]:
            results.append(item)

    logger.info("Fetched %d items from %s RSS", len(results), source_name)
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
    Fetch all news data.

    Args:
        config: Settings dict

    Returns:
        Dict with keys: eastmoney_news, cctv_news, rss_news, economic_data, fetch_time
    """
    news_cfg = config.get("news", {})
    max_headlines = news_cfg.get("max_headlines", 20)

    eastmoney_news = fetch_eastmoney_news(max_items=max_headlines)
    cctv_news = fetch_cctv_news()

    # Fetch from RSS feeds
    rss_news = []
    for source_name, feed_url in RSS_FEEDS.items():
        items = fetch_rss_news(feed_url, source_name, max_items=10)
        rss_news.extend(items)

    economic_data = fetch_economic_calendar()

    return {
        "eastmoney_news": eastmoney_news,
        "cctv_news": cctv_news,
        "rss_news": rss_news,
        "economic_data": economic_data,
        "fetch_time": datetime.now().isoformat(),
    }
