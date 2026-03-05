"""
Fetch PBOC (People's Bank of China) monetary policy data.

Uses AKShare functions:
- repo_rate_query(): daily repo rates (FR001/FR007/FR014)
- macro_china_lpr(): LPR rates
- macro_china_shibor_all(): SHIBOR interbank rates
"""

import logging
import re
from datetime import datetime, date, timedelta
from typing import Any

import time as _time

import akshare as ak
import feedparser
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def _requests_session_with_retry(
    retries: int = 3,
    backoff_factor: float = 1.0,
    timeout: int = 15,
) -> requests.Session:
    """Create a requests session with automatic retry on connection/server errors."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


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


def _parse_omo_html(html: str, title: str, url: str) -> dict | None:
    """
    Parse OMO announcement HTML to extract operation details.

    Strategy:
    1. Locate the header row ("期限", "操作利率", etc.) among all <td> cells
    2. Parse data rows in fixed-width groups after the header
    3. Fallback: extract from prose text (e.g. "190亿元7天期逆回购操作...利率1.40%")

    Returns:
        Parsed OMO dict, or None if parsing fails.
    """
    try:
        # Extract announcement number, e.g. [2026]第39号
        num_match = re.search(r'[(\[（【](\d{4})[)\]）】]\s*第\s*(\d+)\s*号', title)
        announcement_num = f"[{num_match.group(1)}]第{num_match.group(2)}号" if num_match else ""

        # Determine operation type from title/body
        if "逆回购" in title or "逆回购" in html:
            op_type = "逆回购"
        elif "正回购" in title or "正回购" in html:
            op_type = "正回购"
        elif "MLF" in title or "中期借贷便利" in title:
            op_type = "MLF"
        else:
            op_type = "公开市场操作"

        # Extract ALL <td> cells from the page (includes navigation noise)
        td_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL | re.IGNORECASE)
        cells = [re.sub(r'<[^>]+>', '', c).strip() for c in td_pattern.findall(html)]

        operations = []

        # --- Strategy 1: Find header row, then parse fixed-width data rows ---
        # PBOC tables have headers: 期限 | 操作利率 | 投标量 | 中标量
        # The "期限" header may appear at the end of a longer cell (cell 60 in
        # the real page mixes prose text with the header).  We detect the header
        # by looking for a cell that *ends* with "期限" or is exactly "期限".
        header_idx = None
        for i, c in enumerate(cells):
            # Match cells like "期限" or "...期限" (end of prose+header cell)
            if c == "期限" or c.endswith("期限"):
                # Verify the next cells look like column headers
                remaining = cells[i + 1:i + 4]
                remaining_text = " ".join(remaining)
                if "利率" in remaining_text or "投标" in remaining_text or "中标" in remaining_text:
                    header_idx = i
                    break

        if header_idx is not None:
            # Count header columns: 期限, 操作利率, 投标量, 中标量 (typically 4)
            num_cols = 1  # "期限" itself
            for k in range(header_idx + 1, min(header_idx + 6, len(cells))):
                c = cells[k]
                if "利率" in c or "投标" in c or "中标" in c:
                    num_cols += 1
                else:
                    break
            if num_cols < 2:
                num_cols = 4  # safe default

            # Data rows start right after the header columns
            data_start = header_idx + num_cols
            # Parse groups of num_cols cells as data rows
            row_idx = data_start
            while row_idx + num_cols - 1 < len(cells):
                row_cells = cells[row_idx:row_idx + num_cols]
                # First cell should be tenor (e.g. "7天")
                tenor_match = re.search(r'(\d+)\s*天', row_cells[0])
                if not tenor_match:
                    break  # no more data rows

                tenor = f"{tenor_match.group(1)}天"
                rate = None
                bid_amount = None
                win_amount = None

                # Parse remaining cells in order: rate, bid, win
                for val_str in row_cells[1:]:
                    val_str = val_str.replace(',', '').replace('，', '').replace('%', '').replace('亿元', '')
                    num_m = re.search(r'(\d+\.?\d*)', val_str)
                    if not num_m:
                        continue
                    val = float(num_m.group(1))
                    if rate is None:
                        rate = val
                    elif bid_amount is None:
                        bid_amount = val
                    elif win_amount is None:
                        win_amount = val

                if rate is not None:
                    operations.append({
                        "tenor": tenor,
                        "rate": rate,
                        "bid_amount": bid_amount,
                        "win_amount": win_amount if win_amount is not None else bid_amount,
                    })
                row_idx += num_cols

        # --- Strategy 2 (fallback): Extract from prose text ---
        if not operations:
            # Pattern A: "190亿元7天期逆回购操作" + "利率1.40%" (amount before tenor)
            text_ops_a = re.findall(
                r'(\d+\.?\d*)\s*亿元\s*(\d+)\s*天[期]?\S*?操作.*?利率\s*(\d+\.?\d*)%',
                html,
            )
            for amount, tenor_d, rate in text_ops_a:
                operations.append({
                    "tenor": f"{tenor_d}天",
                    "rate": float(rate),
                    "bid_amount": float(amount),
                    "win_amount": float(amount),
                })

            # Pattern B: "7天期逆回购操作19亿元，中标利率1.40%" (tenor before amount)
            if not operations:
                text_ops_b = re.findall(
                    r'(\d+)\s*天[期]?\S*?操作\s*(\d+\.?\d*)\s*亿元.*?利率\s*(\d+\.?\d*)%',
                    html,
                )
                for tenor_d, amount, rate in text_ops_b:
                    operations.append({
                        "tenor": f"{tenor_d}天",
                        "rate": float(rate),
                        "bid_amount": float(amount),
                        "win_amount": float(amount),
                    })

        if not operations:
            logger.warning("Could not parse any operations from OMO announcement")
            return None

        total_amount = sum(op["win_amount"] or 0 for op in operations)

        result = {
            "has_data": True,
            "title": title.strip(),
            "announcement_num": announcement_num,
            "op_type": op_type,
            "operations": operations,
            "total_amount": total_amount,
            "url": url,
            "date": date.today().isoformat(),
        }

        logger.info(
            "Parsed OMO: %s, %d operations, total %.1f亿元",
            op_type, len(operations), total_amount,
        )
        return result

    except Exception as e:
        logger.warning("Failed to parse OMO HTML: %s", e)
        return None


def fetch_omo_via_rss(config: dict) -> dict | None:
    """
    Fetch today's OMO announcement via RSSHub feed (if configured).

    Returns:
        Parsed OMO dict, or None if RSS is not configured or fails.
    """
    rsshub_base = config.get("pboc", {}).get("rsshub_base_url", "")
    if not rsshub_base:
        return None

    feed_url = f"{rsshub_base.rstrip('/')}/pbc/omo"
    logger.info("Fetching OMO via RSS: %s", feed_url)

    try:
        timeout = config.get("pboc", {}).get("request_timeout", 15)
        feed = feedparser.parse(feed_url, request_headers={"User-Agent": "Mozilla/5.0"})

        if feed.bozo and not feed.entries:
            logger.warning("RSS feed parse error: %s", feed.bozo_exception)
            return None

        today_str = date.today().strftime("%Y-%m-%d")
        today_compact = date.today().strftime("%Y%m%d")

        for entry in feed.entries:
            # Match today's entry by date in link or published date
            link = entry.get("link", "")
            published = entry.get("published", "")
            title = entry.get("title", "")

            if today_compact in link or today_str in published:
                html = entry.get("summary", "") or entry.get("content", [{}])[0].get("value", "")
                if html:
                    result = _parse_omo_html(html, title, link)
                    if result:
                        result["source"] = "rss"
                        return result

        logger.info("No OMO entry for today in RSS feed")
        return None

    except Exception as e:
        logger.warning("RSS OMO fetch failed: %s", e)
        return None


def fetch_omo_via_scraping(config: dict) -> dict | None:
    """
    Scrape PBOC website for today's OMO announcement.

    1. Fetch the listing page to find today's announcement link (YYYYMMDD in path)
    2. Fetch the detail page and parse the operation table

    Returns:
        Parsed OMO dict, or None if no announcement found or scraping fails.
    """
    pboc_cfg = config.get("pboc", {})
    listing_url = pboc_cfg.get(
        "listing_url",
        "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/index.html",
    )
    base_url = pboc_cfg.get("base_url", "https://www.pbc.gov.cn")
    timeout = pboc_cfg.get("request_timeout", 15)
    today_compact = date.today().strftime("%Y%m%d")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }

    logger.info("Scraping PBOC OMO listing: %s", listing_url)

    session = _requests_session_with_retry(retries=3, backoff_factor=2.0)

    try:
        resp = session.get(listing_url, headers=headers, timeout=timeout)
        resp.encoding = resp.apparent_encoding or "utf-8"
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Failed to fetch PBOC listing page after retries: %s", e)
        session.close()
        return None

    # Find today's announcement link — PBOC uses paths with /YYYYMMDD/ or dates in href
    # Pattern: href="./t20260302_..." or href="/zhengcehuobisi/.../t20260302_..."
    link_pattern = re.compile(
        r'href=["\']([^"\']*?' + today_compact + r'[^"\']*?\.html?)["\']',
        re.IGNORECASE,
    )
    matches = link_pattern.findall(resp.text)

    if not matches:
        logger.info("No OMO announcement found for today (%s)", today_compact)
        return None

    # Use the first match — typically the most recent announcement
    detail_path = matches[0]
    if detail_path.startswith("./"):
        # Relative to listing page directory
        listing_dir = listing_url.rsplit("/", 1)[0]
        detail_url = f"{listing_dir}/{detail_path[2:]}"
    elif detail_path.startswith("/"):
        detail_url = f"{base_url}{detail_path}"
    elif detail_path.startswith("http"):
        detail_url = detail_path
    else:
        listing_dir = listing_url.rsplit("/", 1)[0]
        detail_url = f"{listing_dir}/{detail_path}"

    logger.info("Fetching OMO detail page: %s", detail_url)

    try:
        resp2 = session.get(detail_url, headers=headers, timeout=timeout)
        resp2.encoding = resp2.apparent_encoding or "utf-8"
        resp2.raise_for_status()
    except Exception as e:
        logger.warning("Failed to fetch OMO detail page after retries: %s", e)
        session.close()
        return None

    # Extract title from the detail page
    title_match = re.search(r'<title[^>]*>(.*?)</title>', resp2.text, re.DOTALL | re.IGNORECASE)
    title = title_match.group(1).strip() if title_match else "公开市场业务交易公告"
    # Clean up common title suffixes
    title = re.sub(r'\s*[-_|]\s*中国人民银行.*$', '', title)

    session.close()
    result = _parse_omo_html(resp2.text, title, detail_url)
    if result:
        result["source"] = "scraping"
    return result


def fetch_omo_data(config: dict) -> dict:
    """
    Fetch today's PBOC open market operation data.

    Tries RSS (if RSSHub is configured) first, then falls back to direct scraping.

    Returns:
        Dict with OMO data, or {"has_data": False} if unavailable.
    """
    # Try RSS first (if configured)
    result = fetch_omo_via_rss(config)
    if result:
        return result

    # Fall back to scraping
    result = fetch_omo_via_scraping(config)
    if result:
        return result

    logger.info("No OMO data available today (may be weekend/holiday or no announcement yet)")
    return {"has_data": False}


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
    omo = fetch_omo_data(config)

    has_data = (
        repo_rates.get("has_data", False)
        or shibor.get("has_data", False)
        or omo.get("has_data", False)
    )

    return {
        "date": today_str,
        "repo_rates": repo_rates,
        "shibor": shibor,
        "lpr": lpr,
        "omo": omo,
        "has_data": has_data,
        "fetch_time": datetime.now().isoformat(),
    }
