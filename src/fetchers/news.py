"""
Fetch financial news from curated RSS feeds for A-shares market report.

Uses feedparser + requests to fetch ~20 macro-finance RSS feeds in parallel,
then deduplicates and filters by recency before passing to the ranking pipeline.
"""

import html
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from time import mktime
from typing import Any

import feedparser
import requests

logger = logging.getLogger(__name__)


def _parse_publish_time(entry: dict) -> str | None:
    """Extract ISO 8601 publish time from a feedparser entry.

    Tries published_parsed, updated_parsed, then raw string parsing.
    Returns ISO 8601 string or None.
    """
    for attr in ("published_parsed", "updated_parsed"):
        tp = entry.get(attr)
        if tp:
            try:
                dt = datetime.fromtimestamp(mktime(tp), tz=timezone.utc)
                return dt.isoformat()
            except (OverflowError, ValueError, OSError):
                continue

    # Fallback: try raw string
    for attr in ("published", "updated"):
        raw = entry.get(attr, "")
        if not raw:
            continue
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(raw)
            return dt.isoformat()
        except (ValueError, TypeError):
            pass

    return None


def _extract_source_from_gnews(title: str) -> tuple[str, str]:
    """Google News titles end with ' - SourceName'. Split them.

    Returns (clean_title, source_name).
    """
    parts = title.rsplit(" - ", 1)
    if len(parts) == 2 and len(parts[1]) < 50:
        return parts[0].strip(), parts[1].strip()
    return title, ""


def _clean_html_content(raw: str) -> str:
    """Strip HTML tags, decode entities, truncate to 500 chars."""
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:500]


def _is_within_age(publish_time: str | None, max_age_hours: int) -> bool:
    """Check if a publish time is within the max age window."""
    if not publish_time:
        return True  # Keep items with unknown time (let ranking handle them)

    try:
        dt = datetime.fromisoformat(publish_time)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        return dt >= cutoff
    except (ValueError, TypeError):
        return True


def _fetch_single_feed(feed_cfg: dict, timeout: int) -> list[dict]:
    """Fetch one RSS feed and return standardized items.

    Each item: {title, content, publish_time, source, category, url}
    """
    name = feed_cfg["name"]
    url = feed_cfg["url"]
    category = feed_cfg.get("category", "general")

    try:
        resp = requests.get(url, timeout=timeout, headers={
            "User-Agent": "DailyStockReport/1.0 (RSS reader)"
        })
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Feed '%s' fetch failed: %s", name, e)
        return []

    feed = feedparser.parse(resp.content)

    if feed.bozo and not feed.entries:
        logger.warning("Feed '%s' parse error: %s", name, getattr(feed, "bozo_exception", "unknown"))
        return []

    items = []
    is_gnews = "news.google.com" in url

    for entry in feed.entries:
        title = entry.get("title", "").strip()
        if not title:
            continue

        source = name
        if is_gnews:
            title, extracted_source = _extract_source_from_gnews(title)
            if extracted_source:
                source = extracted_source

        # Content: try summary, then description
        raw_content = entry.get("summary", entry.get("description", ""))
        content = _clean_html_content(raw_content)

        publish_time = _parse_publish_time(entry)

        items.append({
            "title": title,
            "content": content,
            "publish_time": publish_time or "",
            "source": source,
            "category": category,
            "url": entry.get("link", ""),
        })

    logger.debug("Feed '%s': %d items", name, len(items))
    return items


def _deduplicate(items: list[dict], threshold: float) -> list[dict]:
    """Remove near-duplicate titles using SequenceMatcher.

    Keeps the first occurrence (earlier in the sorted list).
    """
    if threshold <= 0:
        return items

    unique = []
    seen_titles: list[str] = []

    for item in items:
        title = item["title"].lower()
        is_dup = False
        for seen in seen_titles:
            if SequenceMatcher(None, title, seen).ratio() >= threshold:
                is_dup = True
                break
        if not is_dup:
            unique.append(item)
            seen_titles.append(title)

    if len(items) != len(unique):
        logger.info("Deduplication: %d items → %d unique", len(items), len(unique))

    return unique


def fetch_all_news(config: dict) -> dict:
    """
    Fetch news from all configured RSS feeds.

    Args:
        config: Settings dict with news.rss_feeds

    Returns:
        Dict with keys: market_news, cctv_news, economic_data, fetch_time
        (cctv_news and economic_data are empty lists for backward compat)
    """
    news_cfg = config.get("news", {})
    feeds = news_cfg.get("rss_feeds", [])
    timeout = news_cfg.get("fetch_timeout", 10)
    max_workers = news_cfg.get("max_workers", 8)
    max_age_hours = news_cfg.get("max_age_hours", 48)
    dedup_threshold = news_cfg.get("dedup_threshold", 0.7)
    max_headlines = news_cfg.get("max_headlines", 50)

    if not feeds:
        logger.warning("No RSS feeds configured in news.rss_feeds")
        return {
            "market_news": [],
            "cctv_news": [],
            "economic_data": [],
            "fetch_time": datetime.now().isoformat(),
        }

    # Fetch all feeds in parallel
    all_items: list[dict] = []
    success_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_feed = {
            executor.submit(_fetch_single_feed, feed, timeout): feed
            for feed in feeds
        }
        for future in as_completed(future_to_feed):
            feed = future_to_feed[future]
            try:
                items = future.result()
                if items:
                    all_items.extend(items)
                    success_count += 1
            except Exception as e:
                logger.warning("Feed '%s' raised exception: %s", feed.get("name", "?"), e)

    logger.info(
        "RSS fetch complete: %d/%d feeds successful, %d total items",
        success_count, len(feeds), len(all_items),
    )

    # Filter by age
    before_filter = len(all_items)
    all_items = [item for item in all_items if _is_within_age(item.get("publish_time"), max_age_hours)]
    if before_filter != len(all_items):
        logger.info("Age filter: %d items → %d within %dh", before_filter, len(all_items), max_age_hours)

    # Sort by publish time (newest first), items without time go last
    def sort_key(item: dict) -> str:
        return item.get("publish_time") or "0000"
    all_items.sort(key=sort_key, reverse=True)

    # Deduplicate
    all_items = _deduplicate(all_items, dedup_threshold)

    # Trim to max
    all_items = all_items[:max_headlines]

    return {
        "market_news": all_items,
        "cctv_news": [],
        "economic_data": [],
        "fetch_time": datetime.now().isoformat(),
    }
