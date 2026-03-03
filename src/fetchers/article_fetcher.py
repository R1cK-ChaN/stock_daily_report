"""Fetch full article content from URLs using readability extraction.

Resolves Google News proxy URLs to real article URLs via batchexecute API,
then extracts readable text with readability-lxml + markdownify.

Adapted from information/news/src/article_fetcher.py — uses requests (already
a project dependency) instead of httpx, and exposes simple functions rather
than a class so it integrates cleanly with the ranking pipeline.
"""

from __future__ import annotations

import json
import logging
import re
from urllib.parse import quote, urlparse

import requests
from markdownify import markdownify
from readability import Document

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_GNEWS_SIG_RE = re.compile(r'data-n-a-sg="([^"]+)"')
_GNEWS_TS_RE = re.compile(r'data-n-a-ts="([^"]+)"')

_BATCHEXECUTE_URL = (
    "https://news.google.com/_/DotsSplashUi/data/batchexecute"
)
_BATCHEXECUTE_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
}


def _is_google_news_url(url: str) -> bool:
    try:
        return urlparse(url).hostname == "news.google.com"
    except Exception:
        return False


def resolve_google_news_url(url: str, session: requests.Session, timeout: int = 10) -> str | None:
    """Resolve a Google News proxy URL to the real article URL.

    Steps:
    1. Extract base64 article ID from the URL path.
    2. GET the Google News article page, extract signature + timestamp.
       Falls back to ``/rss/articles/`` if the first page lacks the attrs.
    3. POST to batchexecute API with those params to decode the URL.

    Returns the decoded URL on success, or None on any failure.
    """
    try:
        path = urlparse(url).path.split("/")
        base64_str = path[-1]

        page_url = (
            f"https://news.google.com/articles/{base64_str}"
            "?hl=en-US&gl=US&ceid=US:en"
        )
        resp = session.get(page_url, timeout=timeout)
        if resp.status_code != 200:
            return None

        sig_m = _GNEWS_SIG_RE.search(resp.text)
        ts_m = _GNEWS_TS_RE.search(resp.text)

        # Fallback: /rss/articles/ often has the attrs when /articles/ doesn't
        if not sig_m or not ts_m:
            rss_url = f"https://news.google.com/rss/articles/{base64_str}?hl=en-US&gl=US&ceid=US:en"
            resp = session.get(rss_url, timeout=timeout)
            if resp.status_code == 200:
                sig_m = _GNEWS_SIG_RE.search(resp.text)
                ts_m = _GNEWS_TS_RE.search(resp.text)
            if not sig_m or not ts_m:
                return None

        sig, ts = sig_m.group(1), ts_m.group(1)

        payload = [
            "Fbv4je",
            (
                '["garturlreq",[["X","X",["X","X"],'
                "null,null,1,1,\"US:en\",null,1,null,null,"
                'null,null,null,0,1],"X","X",1,[1,1,1],'
                f'1,1,null,0,0,null,0],"{base64_str}",{ts},"{sig}"]'
            ),
        ]
        body = f"f.req={quote(json.dumps([[payload]]))}"
        resp2 = session.post(
            _BATCHEXECUTE_URL,
            headers=_BATCHEXECUTE_HEADERS,
            data=body,
            timeout=timeout,
        )
        if resp2.status_code != 200 or "garturlres" not in resp2.text:
            return None

        parts = resp2.text.split("\n\n")
        parsed = json.loads(parts[1])[:-2]
        decoded_url = json.loads(parsed[0][2])[1]
        return decoded_url

    except Exception as exc:
        logger.debug("Google News URL resolve failed for %s: %s", url, exc)
        return None


def fetch_article_content(
    url: str,
    fallback_content: str,
    session: requests.Session,
    timeout: int = 15,
    max_chars: int = 2000,
) -> str:
    """Fetch and extract readable article text from *url*.

    For Google News proxy URLs, resolves the real URL first.
    On ANY error, returns *fallback_content* — never loses content.
    """
    try:
        if _is_google_news_url(url):
            real_url = resolve_google_news_url(url, session, timeout=timeout)
            if not real_url:
                return fallback_content
            url = real_url

        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()

        doc = Document(resp.text)
        html_summary = doc.summary()
        text = markdownify(html_summary).strip()

        if not text:
            return fallback_content

        if len(text) > max_chars:
            text = text[:max_chars]

        return text

    except Exception as exc:
        logger.debug("Article fetch failed for %s: %s", url, exc)
        return fallback_content


def enrich_articles(items: list[dict], config: dict) -> list[dict]:
    """Enrich a list of news items with full article content.

    Uses a shared requests.Session for connection reuse.
    Logs success/failure counts.  Returns the same list (mutated in place).
    """
    news_cfg = config.get("news", {})
    timeout = news_cfg.get("article_fetch_timeout", 15)
    max_chars = news_cfg.get("article_max_chars", 2000)

    session = requests.Session()
    session.headers.update({"User-Agent": _USER_AGENT})

    success = 0
    for item in items:
        url = item.get("url", "")
        if not url:
            continue

        original = item.get("content", "")
        enriched = fetch_article_content(
            url, original, session, timeout=timeout, max_chars=max_chars,
        )
        if enriched != original:
            item["content"] = enriched
            success += 1

    session.close()
    logger.info("Article content enriched: %d/%d items", success, len(items))
    return items
