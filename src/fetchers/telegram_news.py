"""
Fetch financial news from Telegram public channel previews.

Scrapes https://t.me/s/<channel> using requests + lxml to extract recent
messages.  Supports pagination via ``?before=<post_id>`` so it can reach
back far enough for high-frequency channels like jin10 (~60 msgs/hour).

Outputs items in the same format as the RSS news fetcher so they feed
cleanly into the ranking pipeline.

Adapted from information/news/src/telegram/provider.py — uses requests
(already a project dependency) instead of httpx.
"""

import hashlib
import html as _html
import logging
import re
import time as _time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import lxml.html
import requests

logger = logging.getLogger(__name__)

_TG_URL_RE = re.compile(r"https?://t\.me/s/(\w+)")

_USER_AGENT = "DailyStockReport/1.0 (telegram-scraper)"

# Average posts per hour — used to estimate the starting post ID when
# jumping back in time.  Jin10 posts ~60/hour on average.
_ESTIMATED_POSTS_PER_HOUR = 65


def _make_item_id(permalink: str) -> str:
    return hashlib.sha256(permalink.encode()).hexdigest()[:16]


def _truncate_title(text: str, max_len: int = 120) -> str:
    """Extract a title from message text: first sentence or truncated first line."""
    first_line = text.split("\n", 1)[0].strip()
    for sep in (". ", "! ", "? ", "。", "！", "？"):
        idx = first_line.find(sep)
        if 0 < idx < max_len:
            return first_line[: idx + 1]
    if len(first_line) <= max_len:
        return first_line
    return first_line[:max_len].rsplit(" ", 1)[0] + "…"


def _extract_external_url(message_el) -> str | None:
    """Find the first external (non-t.me) link in a message element."""
    for a_tag in message_el.cssselect("a[href]"):
        href = a_tag.get("href", "")
        if not href:
            continue
        parsed = urlparse(href)
        if parsed.scheme in ("http", "https") and "t.me" not in parsed.netloc:
            return href
    return None


def _parse_messages(html_text: str, feed_name: str, channel: str, category: str) -> list[dict]:
    """Parse Telegram widget HTML into a list of news items (oldest-first)."""
    doc = lxml.html.fromstring(html_text)
    messages = doc.cssselect("div.tgme_widget_message")

    items: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for msg in messages:
        data_post = msg.get("data-post", "")
        if not data_post:
            continue

        text_els = msg.cssselect(".tgme_widget_message_text")
        if not text_els:
            continue
        text = text_els[0].text_content().strip()
        if not text:
            continue

        permalink = f"https://t.me/{data_post}"

        # Parse publish time
        time_els = msg.cssselect(".tgme_widget_message_date time[datetime]")
        published = now
        if time_els:
            dt_str = time_els[0].get("datetime", "")
            if dt_str:
                try:
                    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                    published = dt.isoformat()
                except ValueError:
                    pass

        external_url = _extract_external_url(msg)
        link = external_url or permalink

        # Extract numeric post ID for pagination
        try:
            post_id = int(data_post.split("/")[-1])
        except (ValueError, IndexError):
            post_id = 0

        # Strip residual HTML tags from text_content() output
        clean_text = re.sub(r"<[^>]+>", "", text).strip()
        clean_text = _html.unescape(clean_text)

        items.append({
            "title": _truncate_title(clean_text),
            "content": clean_text,
            "publish_time": published,
            "source": feed_name or f"@{channel}",
            "category": category,
            "url": link,
            "_post_id": post_id,
        })

    return items  # oldest-first (as in the HTML)


def _fetch_channel_paginated(
    url: str,
    feed_name: str,
    category: str,
    timeout: int,
    max_age_hours: int,
    max_pages: int,
) -> list[dict]:
    """Fetch messages from a Telegram channel with pagination.

    Strategy for high-frequency channels:
    1. Fetch the latest page to get the newest post ID.
    2. Estimate the post ID at the start of the lookback window and
       jump directly there (avoids scraping hundreds of pages sequentially).
    3. Paginate forward from that point, collecting all messages until
       we reach the present or hit max_pages.

    Returns items newest-first, filtered to the lookback window.
    """
    m = _TG_URL_RE.match(url)
    if not m:
        logger.warning("Invalid Telegram channel URL: %s", url)
        return []
    channel = m.group(1)

    session = requests.Session()
    session.headers.update({"User-Agent": _USER_AGENT, "Accept": "text/html"})

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)

    # Step 1: Fetch latest page to get the newest post ID
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Telegram channel '%s' fetch failed: %s", feed_name, e)
        session.close()
        return []

    latest_items = _parse_messages(resp.text, feed_name, channel, category)
    if not latest_items:
        logger.warning("Telegram '%s': no messages found on page", feed_name)
        session.close()
        return []

    latest_post_id = latest_items[-1]["_post_id"]

    # Check if all latest items are already within our window
    # (channel posts slowly enough that a single page covers the window)
    oldest_on_page = latest_items[0]
    try:
        oldest_dt = datetime.fromisoformat(oldest_on_page["publish_time"])
        if oldest_dt.tzinfo is None:
            oldest_dt = oldest_dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        oldest_dt = None

    if oldest_dt and oldest_dt <= cutoff:
        # Single page already covers the lookback window
        all_items = [
            i for i in latest_items
            if _within_cutoff(i["publish_time"], cutoff)
        ]
        session.close()
        all_items.reverse()  # newest-first
        logger.info(
            "Telegram '%s': %d items from single page (within %dh)",
            feed_name, len(all_items), max_age_hours,
        )
        return all_items

    # Step 2: Estimate jump-back point
    # We know the latest post ID and need to go back max_age_hours
    jump_back_posts = int(_ESTIMATED_POSTS_PER_HOUR * max_age_hours * 1.2)  # 20% margin
    estimated_start_id = latest_post_id - jump_back_posts

    all_items: list[dict] = []

    # Step 3: Paginate forward from the estimated start point
    # We use ?before= which gives us messages OLDER than the given ID,
    # but we start from our estimated point and keep fetching newer pages.
    # Actually, ?before=N returns the page of messages just before post N.
    # So we start at estimated_start_id, get that page, then use the
    # last post ID + 21 as the next ?before= to move forward.
    before_id = estimated_start_id
    pages_fetched = 0
    reached_present = False

    while pages_fetched < max_pages and not reached_present:
        try:
            page_url = f"{url}?before={before_id}"
            resp = session.get(page_url, timeout=timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Telegram '%s' page %d failed: %s", feed_name, pages_fetched, e)
            break

        page_items = _parse_messages(resp.text, feed_name, channel, category)
        pages_fetched += 1

        if not page_items:
            break

        # Filter to items within our window
        for item in page_items:
            if _within_cutoff(item["publish_time"], cutoff):
                all_items.append(item)

        last_id_on_page = page_items[-1]["_post_id"]

        # If we've reached or passed the latest post, we're done
        if last_id_on_page >= latest_post_id:
            reached_present = True
            break

        # Move forward: next page starts after the last post we saw
        before_id = last_id_on_page + 21

        _time.sleep(0.3)  # rate-limit courtesy

    # Also include the latest page items (they might not have been covered)
    seen_ids = {i["_post_id"] for i in all_items}
    for item in latest_items:
        if item["_post_id"] not in seen_ids and _within_cutoff(item["publish_time"], cutoff):
            all_items.append(item)

    session.close()

    # Deduplicate by post_id (pages may overlap)
    deduped: dict[int, dict] = {}
    for item in all_items:
        deduped[item["_post_id"]] = item
    all_items = list(deduped.values())

    # Sort newest-first
    all_items.sort(key=lambda x: x.get("publish_time", "0000"), reverse=True)

    # Remove internal _post_id field
    for item in all_items:
        item.pop("_post_id", None)

    logger.info(
        "Telegram '%s': %d items from %d pages (within %dh)",
        feed_name, len(all_items), pages_fetched, max_age_hours,
    )
    return all_items


def _within_cutoff(publish_time: str, cutoff: datetime) -> bool:
    """Check if a publish time is after the cutoff."""
    try:
        dt = datetime.fromisoformat(publish_time)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= cutoff
    except (ValueError, TypeError):
        return True


def fetch_telegram_news(config: dict) -> dict:
    """
    Fetch news from configured Telegram channels.

    Args:
        config: Settings dict with news.telegram_channels list.

    Returns:
        Dict with keys: market_news, cctv_news, economic_data, fetch_time
        (same shape as fetch_all_news for pipeline compatibility)
    """
    news_cfg = config.get("news", {})
    channels = news_cfg.get("telegram_channels", [])
    timeout = news_cfg.get("telegram_fetch_timeout", 15)
    max_age_hours = news_cfg.get("telegram_max_age_hours", 36)
    max_pages = news_cfg.get("telegram_max_pages", 80)

    if not channels:
        logger.info("No Telegram channels configured in news.telegram_channels")
        return {
            "market_news": [],
            "cctv_news": [],
            "economic_data": [],
            "fetch_time": datetime.now().isoformat(),
        }

    all_items: list[dict] = []

    for ch in channels:
        name = ch["name"]
        url = ch["url"]
        category = ch.get("category", "china")

        items = _fetch_channel_paginated(
            url, name, category, timeout, max_age_hours, max_pages,
        )
        all_items.extend(items)

    logger.info(
        "Telegram fetch complete: %d total items from %d channels",
        len(all_items), len(channels),
    )

    return {
        "market_news": all_items,
        "cctv_news": [],
        "economic_data": [],
        "fetch_time": datetime.now().isoformat(),
    }
