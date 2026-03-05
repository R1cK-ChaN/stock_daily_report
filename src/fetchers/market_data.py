"""
Fetch China A-shares market data from AKShare.

Uses Sina as primary source for index quotes (more reliable),
东方财富 for sectors and individual stocks.
Each sub-fetcher is isolated so partial failures don't crash the pipeline.
"""

import logging
import math
import re
import time
from datetime import datetime
from io import StringIO
from typing import Any

import akshare as ak
import pandas as pd
import requests
from akshare.stock.cons import (
    zh_sina_a_stock_count_url,
    zh_sina_a_stock_payload,
    zh_sina_a_stock_url,
)
from akshare.utils import demjson

logger = logging.getLogger(__name__)

MAX_RETRIES = 2
RETRY_DELAY = 2  # seconds

SPOT_NUMERIC_COLUMNS = [
    "最新价",
    "涨跌额",
    "涨跌幅",
    "成交量",
    "成交额",
    "今开",
    "最高",
    "最低",
    "昨收",
]
SPOT_COLUMNS = ["代码", "名称", *SPOT_NUMERIC_COLUMNS]

SPOT_EM_FS_ALL = "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"
SPOT_EM_FS_SEGMENTS = {
    "SH": "m:1 t:2,m:1 t:23",
    "SZ": "m:0 t:6,m:0 t:80",
    "BJ": "m:0 t:81 s:2048",
}


def _retry(
    func,
    *args,
    max_retries: int | None = None,
    retry_delay: float | None = None,
    action_name: str | None = None,
    **kwargs,
):
    """Retry a function call with linear backoff."""
    retries = max_retries if max_retries is not None else MAX_RETRIES
    delay = retry_delay if retry_delay is not None else RETRY_DELAY
    action = action_name or getattr(func, "__name__", "call")

    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == retries:
                raise
            sleep_secs = delay * attempt
            logger.warning(
                "Attempt %d/%d for %s failed: %s. Retrying in %.1fs...",
                attempt,
                retries,
                action,
                e,
                sleep_secs,
            )
            time.sleep(sleep_secs)


def _ensure_spot_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize spot DataFrame into the common schema used by downstream calculations."""
    if df is None or df.empty:
        return pd.DataFrame(columns=SPOT_COLUMNS)

    for col in SPOT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    for col in SPOT_NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[SPOT_COLUMNS]


def _http_get(
    url: str,
    *,
    params: dict | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 6,
) -> requests.Response:
    """Small wrapper to enforce timeout + status checking for all custom requests."""
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp


def _fetch_em_spot_df_fast(
    *,
    request_timeout: int,
    max_retries_per_request: int,
    em_page_size: int,
    fs_filter: str = SPOT_EM_FS_ALL,
    source_label: str = "EM",
    deadline: float | None,
) -> pd.DataFrame:
    """Fetch A-share spot data from EastMoney with controllable page size/timeouts."""
    url = "https://82.push2.eastmoney.com/api/qt/clist/get"
    base_params = {
        "pn": "1",
        "pz": str(em_page_size),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": fs_filter,
        "fields": "f2,f3,f4,f5,f6,f12,f14,f15,f16,f17,f18",
    }

    rows: list[dict[str, Any]] = []
    total_pages = 1
    current_page = 1

    while current_page <= total_pages:
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("EM spot fetch exceeded overall timeout budget")

        params = base_params.copy()
        params["pn"] = str(current_page)
        resp = _retry(
            _http_get,
            url,
            params=params,
            timeout=request_timeout,
            max_retries=max_retries_per_request,
            retry_delay=1,
            action_name=f"{source_label} spot page {current_page}",
        )
        payload = resp.json()
        data = (payload or {}).get("data") or {}
        diff = data.get("diff") or []

        if current_page == 1:
            total = int(data.get("total") or len(diff))
            per_page = max(len(diff), 1)
            total_pages = max(math.ceil(total / per_page), 1)

        rows.extend(diff)
        current_page += 1

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.rename(
        columns={
            "f12": "代码",
            "f14": "名称",
            "f2": "最新价",
            "f3": "涨跌幅",
            "f4": "涨跌额",
            "f5": "成交量",
            "f6": "成交额",
            "f17": "今开",
            "f15": "最高",
            "f16": "最低",
            "f18": "昨收",
        }
    )
    df = _ensure_spot_schema(df)
    logger.info(
        "Fetched A-share spot data via %s fast path (%d rows, %d pages, pz=%s)",
        source_label,
        len(df),
        total_pages,
        em_page_size,
    )
    return df


def _fetch_em_spot_df_segmented_fast(
    *,
    request_timeout: int,
    max_retries_per_request: int,
    em_page_size: int,
    deadline: float | None,
) -> pd.DataFrame:
    """Fetch A-share spot by market segments when full-universe EM query is unstable."""
    segment_frames: list[pd.DataFrame] = []
    for label, fs_filter in SPOT_EM_FS_SEGMENTS.items():
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("EM segmented spot fetch exceeded overall timeout budget")
        df = _fetch_em_spot_df_fast(
            request_timeout=request_timeout,
            max_retries_per_request=max_retries_per_request,
            em_page_size=em_page_size,
            fs_filter=fs_filter,
            source_label=f"EM-{label}",
            deadline=deadline,
        )
        if not df.empty:
            segment_frames.append(df)

    if not segment_frames:
        return pd.DataFrame(columns=SPOT_COLUMNS)

    merged = pd.concat(segment_frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["代码"], keep="first")
    merged = _ensure_spot_schema(merged)
    logger.info("Fetched A-share spot data via EM segmented fallback (%d rows)", len(merged))
    return merged


def _to_float(value: Any, default: float = 0.0) -> float:
    if pd.isna(value):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if pd.isna(value):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _fetch_sina_spot_df_fast(
    *,
    request_timeout: int,
    max_retries_per_request: int,
    deadline: float | None,
) -> pd.DataFrame:
    """Fetch A-share spot data from Sina with explicit timeout control."""
    count_resp = _retry(
        _http_get,
        zh_sina_a_stock_count_url,
        timeout=request_timeout,
        max_retries=max_retries_per_request,
        retry_delay=1,
        action_name="Sina stock count",
    )
    match = re.search(r"\d+", count_resp.text)
    if not match:
        raise ValueError("Could not parse Sina A-share stock count")

    total_stocks = int(match.group(0))
    page_count = max(math.ceil(total_stocks / 80), 1)
    rows: list[dict[str, Any]] = []

    for page in range(1, page_count + 1):
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("Sina spot fetch exceeded overall timeout budget")

        payload = zh_sina_a_stock_payload.copy()
        payload.update({"page": str(page)})
        resp = _retry(
            _http_get,
            zh_sina_a_stock_url,
            params=payload,
            timeout=request_timeout,
            max_retries=max_retries_per_request,
            retry_delay=1,
            action_name=f"Sina spot page {page}",
        )
        page_rows = demjson.decode(resp.text)
        if page_rows:
            rows.extend(page_rows)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.rename(
        columns={
            "symbol": "代码",
            "name": "名称",
            "trade": "最新价",
            "pricechange": "涨跌额",
            "changepercent": "涨跌幅",
            "volume": "成交量",
            "amount": "成交额",
            "open": "今开",
            "high": "最高",
            "low": "最低",
            "settlement": "昨收",
        }
    )
    df = _ensure_spot_schema(df)
    logger.info("Fetched A-share spot data via Sina fast path (%d rows, %d pages)", len(df), page_count)
    return df


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


def _fetch_em_sector_df_fast(
    *,
    board_kind: str,
    request_timeout: int,
    max_retries_per_request: int,
    em_page_size: int,
    deadline: float | None,
) -> pd.DataFrame:
    """Fetch sector board snapshot from EastMoney (industry/concept)."""
    if board_kind == "industry":
        url = "https://17.push2.eastmoney.com/api/qt/clist/get"
        fs = "m:90 t:2 f:!50"
        source_label = "EM industry"
    elif board_kind == "concept":
        url = "https://79.push2.eastmoney.com/api/qt/clist/get"
        fs = "m:90 t:3 f:!50"
        source_label = "EM concept"
    else:
        raise ValueError(f"Unsupported board kind: {board_kind}")

    base_params = {
        "pn": "1",
        "pz": str(em_page_size),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": fs,
        # Keep only fields needed by report
        "fields": "f3,f6,f8,f12,f14,f104,f105,f128,f136",
    }

    rows: list[dict[str, Any]] = []
    total_pages = 1
    current_page = 1

    while current_page <= total_pages:
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(f"{source_label} fetch exceeded overall timeout budget")

        params = base_params.copy()
        params["pn"] = str(current_page)
        resp = _retry(
            _http_get,
            url,
            params=params,
            timeout=request_timeout,
            max_retries=max_retries_per_request,
            retry_delay=1,
            action_name=f"{source_label} page {current_page}",
        )
        payload = resp.json()
        data = (payload or {}).get("data") or {}
        diff = data.get("diff") or []

        if current_page == 1:
            total = int(data.get("total") or len(diff))
            per_page = max(len(diff), 1)
            total_pages = max(math.ceil(total / per_page), 1)

        rows.extend(diff)
        current_page += 1

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).rename(
        columns={
            "f14": "name",
            "f3": "change_pct",
            "f8": "turnover_rate",
            "f6": "amount",
            "f128": "leader_stock",
            "f136": "leader_change_pct",
            "f104": "num_up",
            "f105": "num_down",
        }
    )
    for col in ["change_pct", "turnover_rate", "amount", "leader_change_pct", "num_up", "num_down"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.info("Fetched %s board snapshot (%d rows)", source_label, len(df))
    return df


def _find_column(columns: list[Any], required_keywords: tuple[str, ...]) -> Any | None:
    for col in columns:
        col_text = str(col)
        if all(key in col_text for key in required_keywords):
            return col
    return None


def _fetch_ths_sector_page1_fast(
    *,
    request_timeout: int,
    max_retries_per_request: int,
    deadline: float | None,
) -> pd.DataFrame:
    """Fetch THS industry board page-1 snapshot with explicit timeout (last-resort fallback)."""
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("THS page-1 sector fetch exceeded overall timeout budget")

    url = "http://q.10jqka.com.cn/thshy/index/field/199112/order/desc/page/1/ajax/1/"
    # THS pages require anti-bot token headers to avoid intermittent 401.
    import py_mini_racer
    from akshare.datasets import get_ths_js

    with open(get_ths_js("ths.js"), encoding="utf-8") as f:
        js_content = f.read()
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(js_content)
    v_code = js_code.call("v")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Cookie": f"v={v_code}",
        "hexin-v": v_code,
    }
    resp = _retry(
        _http_get,
        url,
        headers=headers,
        timeout=request_timeout,
        max_retries=max_retries_per_request,
        retry_delay=1,
        action_name="THS industry page 1",
    )

    table = pd.read_html(StringIO(resp.text))[0]
    columns = list(table.columns)
    name_col = _find_column(columns, ("板块",))
    change_col = _find_column(columns, ("涨跌幅",))
    amount_col = _find_column(columns, ("总成交额",))
    up_col = _find_column(columns, ("上涨家数",))
    down_col = _find_column(columns, ("下跌家数",))
    leader_col = _find_column(columns, ("领涨股",))
    leader_change_col = _find_column(columns, ("领涨股", "涨跌幅"))

    if name_col is None or change_col is None:
        raise ValueError("THS sector table missing required columns")

    df = pd.DataFrame(
        {
            "name": table[name_col],
            "change_pct": pd.to_numeric(
                table[change_col].astype(str).str.replace("%", "", regex=False),
                errors="coerce",
            ),
            "turnover_rate": 0.0,
            "amount": pd.to_numeric(
                table[amount_col].astype(str).str.replace(",", "", regex=False)
                if amount_col is not None
                else 0,
                errors="coerce",
            ),
            "leader_stock": table[leader_col] if leader_col is not None else "",
            "leader_change_pct": pd.to_numeric(
                table[leader_change_col].astype(str).str.replace("%", "", regex=False)
                if leader_change_col is not None
                else 0,
                errors="coerce",
            ),
            "num_up": pd.to_numeric(table[up_col], errors="coerce") if up_col is not None else 0,
            "num_down": pd.to_numeric(table[down_col], errors="coerce") if down_col is not None else 0,
        }
    )
    logger.info("Fetched THS industry page-1 snapshot (%d rows)", len(df))
    return df


def _sector_lists_from_df(df: pd.DataFrame, top_n_gainers: int, top_n_losers: int) -> dict:
    if df is None or df.empty:
        return {"gainers": [], "losers": []}

    working = df.copy()
    working["change_pct"] = pd.to_numeric(working["change_pct"], errors="coerce")
    working = working.dropna(subset=["change_pct"]).sort_values("change_pct", ascending=False)

    def row_to_dict(row):
        return {
            "name": str(row.get("name", "")),
            "change_pct": _to_float(row.get("change_pct", 0)),
            "turnover_rate": _to_float(row.get("turnover_rate", 0)),
            "amount": _to_float(row.get("amount", 0)),
            "leader_stock": str(row.get("leader_stock", "")),
            "leader_change_pct": _to_float(row.get("leader_change_pct", 0)),
            "num_up": _to_int(row.get("num_up", 0)),
            "num_down": _to_int(row.get("num_down", 0)),
        }

    gainers = [row_to_dict(row) for _, row in working.head(top_n_gainers).iterrows()]
    losers = [row_to_dict(row) for _, row in working.tail(top_n_losers).iterrows()]
    losers.reverse()
    return {"gainers": gainers, "losers": losers}


def fetch_sector_performance(
    top_n_gainers: int = 5,
    top_n_losers: int = 5,
    sector_fetch_cfg: dict | None = None,
) -> dict:
    """Fetch board performance with bounded fallback chain."""
    logger.info("Fetching sector performance...")
    cfg = sector_fetch_cfg or {}
    source_order = cfg.get("source_order", ["em_industry", "em_concept", "ths_page1"])
    request_timeout = int(cfg.get("request_timeout", 6))
    max_retries_per_request = int(cfg.get("max_retries_per_request", 1))
    em_page_size = int(cfg.get("em_page_size", 300))
    overall_timeout_seconds = int(cfg.get("overall_timeout_seconds", 20))

    deadline = None
    if overall_timeout_seconds > 0:
        deadline = time.monotonic() + overall_timeout_seconds

    last_error = None
    for source in source_order:
        if deadline is not None and time.monotonic() >= deadline:
            logger.warning("Sector fetch stopped: overall timeout budget reached (%ss)", overall_timeout_seconds)
            break

        try:
            if source == "em_industry":
                df = _fetch_em_sector_df_fast(
                    board_kind="industry",
                    request_timeout=request_timeout,
                    max_retries_per_request=max_retries_per_request,
                    em_page_size=em_page_size,
                    deadline=deadline,
                )
            elif source == "em_concept":
                df = _fetch_em_sector_df_fast(
                    board_kind="concept",
                    request_timeout=request_timeout,
                    max_retries_per_request=max_retries_per_request,
                    em_page_size=max(em_page_size, 1000),
                    deadline=deadline,
                )
            elif source == "ths_page1":
                df = _fetch_ths_sector_page1_fast(
                    request_timeout=request_timeout,
                    max_retries_per_request=max_retries_per_request,
                    deadline=deadline,
                )
            else:
                logger.warning("Unknown sector source '%s' in source_order; skipping", source)
                continue

            result = _sector_lists_from_df(df, top_n_gainers=top_n_gainers, top_n_losers=top_n_losers)
            if result["gainers"] or result["losers"]:
                logger.info(
                    "Fetched sector performance via %s: %d gainers, %d losers",
                    source,
                    len(result["gainers"]),
                    len(result["losers"]),
                )
                return result
        except Exception as e:
            last_error = e
            logger.warning("%s sector source failed: %s", source, e)

    if last_error is not None:
        logger.error("All sector sources failed: %s", last_error)
    else:
        logger.error("All sector sources failed")
    return {"gainers": [], "losers": []}


def _fetch_em_spot_df(spot_cfg: dict | None = None):
    """Fetch A-share spot data with bounded retries/timeouts and optional fallback."""
    cfg = spot_cfg or {}
    source_order = cfg.get("source_order", ["em", "em_segmented", "sina"])
    enable_sina_fallback = cfg.get("enable_sina_fallback", True)
    request_timeout = int(cfg.get("request_timeout", 6))
    max_retries_per_request = int(cfg.get("max_retries_per_request", 1))
    em_page_size = int(cfg.get("em_page_size", 2000))
    overall_timeout_seconds = int(cfg.get("overall_timeout_seconds", 30))
    min_rows = int(cfg.get("min_rows", 3000))

    if not enable_sina_fallback:
        source_order = [s for s in source_order if s != "sina"]
    if not source_order:
        source_order = ["em"]

    deadline = None
    if overall_timeout_seconds > 0:
        deadline = time.monotonic() + overall_timeout_seconds

    last_error = None
    best_candidate = None
    for source in source_order:
        if deadline is not None and time.monotonic() >= deadline:
            logger.warning("Spot fetch stopped: overall timeout budget reached (%ss)", overall_timeout_seconds)
            break

        try:
            if source == "em":
                candidate = _fetch_em_spot_df_fast(
                    request_timeout=request_timeout,
                    max_retries_per_request=max_retries_per_request,
                    em_page_size=em_page_size,
                    fs_filter=SPOT_EM_FS_ALL,
                    source_label="EM",
                    deadline=deadline,
                )
            elif source == "em_segmented":
                candidate = _fetch_em_spot_df_segmented_fast(
                    request_timeout=request_timeout,
                    max_retries_per_request=max_retries_per_request,
                    em_page_size=em_page_size,
                    deadline=deadline,
                )
            elif source == "sina":
                candidate = _fetch_sina_spot_df_fast(
                    request_timeout=request_timeout,
                    max_retries_per_request=max_retries_per_request,
                    deadline=deadline,
                )
            else:
                logger.warning("Unknown spot source '%s' in source_order; skipping", source)
                continue

            if best_candidate is None or len(candidate) > len(best_candidate):
                best_candidate = candidate
            if len(candidate) >= min_rows:
                return candidate

            logger.warning(
                "%s spot returned %d rows (< min_rows=%d), trying next source",
                source.upper(),
                len(candidate),
                min_rows,
            )
        except Exception as e:
            last_error = e
            logger.warning("%s spot failed: %s", source.upper(), e)

    if best_candidate is not None and not best_candidate.empty:
        logger.warning(
            "Using partial spot dataset with %d rows (below min_rows=%d)",
            len(best_candidate),
            min_rows,
        )
        return best_candidate

    if last_error is not None:
        logger.error("All A-share spot sources failed: %s", last_error)
    else:
        logger.error("All A-share spot sources failed")
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


def _fetch_breadth_fallback_legu() -> dict:
    """Fallback breadth from 乐咕 market activity when full spot data is unavailable."""
    try:
        df = _retry(
            ak.stock_market_activity_legu,
            max_retries=1,
            action_name="stock_market_activity_legu",
        )
        if df is None or df.empty:
            return {}

        kv = {str(row["item"]).strip(): row["value"] for _, row in df.iterrows()}
        up_count = _to_int(kv.get("上涨"))
        down_count = _to_int(kv.get("下跌"))
        flat_count = _to_int(kv.get("平盘"))
        limit_up = _to_int(kv.get("真实涨停", kv.get("涨停")))
        limit_down = _to_int(kv.get("真实跌停", kv.get("跌停")))
        total_stocks = up_count + down_count + flat_count

        result = {
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "total_stocks": total_stocks,
            "total_volume": None,
            "total_amount": None,
        }
        logger.info(
            "Fetched breadth via Legu fallback: %d up, %d down, %d flat",
            up_count,
            down_count,
            flat_count,
        )
        return result
    except Exception as e:
        logger.warning("Legu breadth fallback failed: %s", e)
        return {}


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
        sector_fetch_cfg=sector_cfg.get("fetch", {}),
    )

    # Fetch spot data once, shared by breadth and top_movers
    spot_df = _fetch_em_spot_df(market_cfg.get("spot_fetch", {}))
    breadth = _compute_breadth(spot_df)
    if not breadth:
        breadth = _fetch_breadth_fallback_legu()
    top_movers = _compute_top_movers(spot_df, top_n=10)

    return {
        "indices": indices,
        "sectors": sectors,
        "breadth": breadth,
        "top_movers": top_movers,
        "fetch_time": datetime.now().isoformat(),
    }
