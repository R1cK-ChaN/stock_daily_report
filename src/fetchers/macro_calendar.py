"""
Lightweight macro calendar fallback chain for report fundamentals.

Priority order:
1. Trading Economics API
2. FX678 macro calendar page
3. Investing economic calendar (best-effort only)
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import lxml.html
import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
USER_AGENT = "DailyStockReport/1.0 (macro-calendar)"
DEFAULT_SOURCE_ORDER = ["tradingeconomics", "fx678", "investing"]
DEFAULT_TIMEOUT = 12
DEFAULT_FALLBACK_CHAIN = ["tradingeconomics", "fx678", "investing"]
TE_COUNTRIES = ["china", "united states", "euro area"]

COUNTRY_ALIASES = {
    "china": "China",
    "中国": "China",
    "prc": "China",
    "united states": "United States",
    "united states of america": "United States",
    "usa": "United States",
    "us": "United States",
    "美国": "United States",
    "euro area": "Euro Area",
    "欧元区": "Euro Area",
    "eurozone": "Euro Area",
}

COUNTRY_LABELS_ZH = {
    "China": "中国",
    "United States": "美国",
    "Euro Area": "欧元区",
}

FX678_FLAG_COUNTRY_MAP = {
    "c_usa": "United States",
    "c_china": "China",
    "c_eu": "Euro Area",
    "c_euro": "Euro Area",
    "c_eurozone": "Euro Area",
}

DOMESTIC_GROUPS = [
    "中国消费情绪方面",
    "中国通胀方面",
    "中国景气方面",
    "中国贸易方面",
    "中国货币方面",
    "中国信用与流动性方面",
]

INTERNATIONAL_GROUPS = [
    "美国资产负债方面",
    "美国房地产方面",
    "美国贸易方面",
    "美国劳动力市场方面",
    "美国通胀与消费方面",
    "欧元区宏观方面",
    "国际补充方面",
]

GROUP_KEYWORDS = {
    "中国消费情绪方面": [
        "consumer sentiment",
        "consumer confidence",
        "消费者信心",
        "消费情绪",
        "pcsi",
        "ipsos",
    ],
    "中国通胀方面": [
        "cpi",
        "ppi",
        "inflation",
        "通胀",
        "居民消费价格",
        "工业生产者出厂价格",
    ],
    "中国景气方面": [
        "pmi",
        "gdp",
        "retail sales",
        "industrial production",
        "consumer spending",
        "固定资产投资",
        "工业增加值",
        "零售销售",
        "景气",
    ],
    "中国贸易方面": [
        "trade balance",
        "exports",
        "imports",
        "balance of trade",
        "出口",
        "进口",
        "贸易",
    ],
    "中国信用与流动性方面": [
        "social financing",
        "aggregate financing",
        "new yuan loans",
        "loan growth",
        "社融",
        "社会融资",
        "贷款",
        "信贷",
        "流动性",
    ],
    "中国货币方面": [
        "m2",
        "m1",
        "money supply",
        "货币供应",
        "货币",
        "存款",
    ],
    "美国资产负债方面": [
        "fed balance sheet",
        "federal reserve balance sheet",
        "美联储资产负债表",
        "quantitative tightening",
        "qt",
    ],
    "美国房地产方面": [
        "building permits",
        "housing starts",
        "existing home sales",
        "new home sales",
        "房屋开工",
        "新屋开工",
        "营建许可",
        "房地产",
        "成屋销售",
        "新屋销售",
    ],
    "美国贸易方面": [
        "trade balance",
        "exports",
        "imports",
        "balance of trade",
        "贸易帐",
        "贸易",
        "出口",
        "进口",
    ],
    "美国劳动力市场方面": [
        "initial jobless claims",
        "continuing jobless claims",
        "non farm payrolls",
        "nonfarm payrolls",
        "unemployment",
        "jobless claims",
        "失业金",
        "非农",
        "失业率",
        "就业",
    ],
    "美国通胀与消费方面": [
        "cpi",
        "ppi",
        "inflation",
        "retail sales",
        "consumer sentiment",
        "consumer confidence",
        "通胀",
        "零售销售",
        "消费情绪",
        "消费者信心",
        "消费",
    ],
    "欧元区宏观方面": [
        "euro area",
        "eurozone",
        "欧元区",
        "cpi",
        "ppi",
        "pmi",
        "gdp",
        "retail sales",
        "industrial production",
        "trade",
        "inflation",
        "通胀",
        "景气",
        "贸易",
    ],
    "国际补充方面": [
        "cpi",
        "ppi",
        "gdp",
        "pmi",
        "inflation",
        "retail sales",
        "trade",
        "通胀",
        "景气",
        "贸易",
    ],
}

PRIORITY_PATTERNS = [
    "consumer sentiment",
    "consumer confidence",
    "消费情绪",
    "消费者信心",
    "fed balance sheet",
    "federal reserve balance sheet",
    "美联储资产负债表",
    "building permits",
    "housing starts",
    "trade balance",
    "balance of trade",
    "exports",
    "imports",
    "initial jobless claims",
    "continuing jobless claims",
    "cpi",
    "ppi",
    "pmi",
    "gdp",
    "retail sales",
    "industrial production",
    "social financing",
    "aggregate financing",
    "money supply",
    "m2",
    "m1",
    "进出口",
    "贸易帐",
    "营建许可",
    "新屋开工",
    "初请失业金",
    "续请失业金",
    "通胀",
    "社融",
    "社会融资",
    "货币供应",
    "出口",
    "进口",
]

CORE_MACRO_PRIORITY_PATTERNS = [
    "cpi",
    "ppi",
    "pmi",
    "gdp",
    "retail sales",
    "industrial production",
    "trade balance",
    "exports",
    "imports",
    "building permits",
    "housing starts",
    "initial jobless claims",
    "continuing jobless claims",
    "fed balance sheet",
    "consumer sentiment",
    "consumer confidence",
    "m2",
    "m1",
    "social financing",
    "aggregate financing",
    "money supply",
    "进出口",
    "贸易帐",
    "营建许可",
    "新屋开工",
    "初请失业金",
    "续请失业金",
    "通胀",
    "社融",
    "社会融资",
    "货币供应",
    "消费者信心",
    "消费情绪",
    "工业增加值",
    "零售销售",
]

COUNTRY_SORT_PRIORITY = {
    "China": 0,
    "United States": 1,
    "Euro Area": 2,
}

BLACKLIST_PATTERNS = [
    "auction",
    "竞拍",
    "bill auction",
    "bond auction",
    "treasury auction",
    "spdr",
    "ishares",
    "comex",
    "库存-每日更新",
    "持仓-每日更新",
    "speech",
    "讲话",
    "boiler",
]


def _request_timeout(config: dict | None) -> int:
    macro_cfg = (config or {}).get("macro_calendar", {})
    return int(macro_cfg.get("request_timeout", DEFAULT_TIMEOUT))


def _source_order(config: dict | None) -> list[str]:
    macro_cfg = (config or {}).get("macro_calendar", {})
    order = macro_cfg.get("source_order", DEFAULT_SOURCE_ORDER)
    if not isinstance(order, list) or not order:
        return list(DEFAULT_SOURCE_ORDER)
    return [str(item).strip().lower() for item in order if str(item).strip()]


def _cache_enabled(config: dict | None) -> bool:
    macro_cfg = (config or {}).get("macro_calendar", {})
    return bool(macro_cfg.get("cache_enabled", True))


def _cache_path(date_str: str) -> Path:
    output_dir = PROJECT_ROOT / "output" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / "macro_calendar_cache.json"


def _empty_grouped(source_used: str = "") -> dict:
    return {
        "domestic": [],
        "international": {group: [] for group in INTERNATIONAL_GROUPS[:-1]},
        "source_used": source_used,
    }


def _empty_result(
    date_str: str,
    *,
    fallback_chain: list[str] | None = None,
    fallback_reason: str = "",
    empty_reason: str = "",
) -> dict:
    return {
        "events": [],
        "grouped": _empty_grouped(),
        "source_used": "",
        "fallback_chain": fallback_chain or list(DEFAULT_FALLBACK_CHAIN),
        "fallback_reason": fallback_reason,
        "empty_reason": empty_reason,
        "has_data": False,
        "cache_hit": False,
        "date": date_str,
    }


def _load_cached_result(date_str: str, config: dict | None) -> dict | None:
    if not _cache_enabled(config):
        return None

    cache_path = _cache_path(date_str)
    if not cache_path.exists():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Macro calendar cache read failed: %s", exc)
        return None

    if not isinstance(payload, dict) or payload.get("date") != date_str:
        return None

    payload["cache_hit"] = True
    logger.info(
        "Macro calendar cache hit: source=%s, events=%d",
        payload.get("source_used", ""),
        len(payload.get("events", [])),
    )
    return payload


def _save_cached_result(date_str: str, payload: dict, config: dict | None) -> None:
    if not _cache_enabled(config) or not payload.get("has_data"):
        return

    cache_path = _cache_path(date_str)
    try:
        cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Macro calendar cache write failed: %s", exc)


def _normalize_country(raw: str) -> str:
    cleaned = str(raw or "").strip()
    if not cleaned:
        return ""
    key = cleaned.lower()
    return COUNTRY_ALIASES.get(key, cleaned)


def _country_label(country: str) -> str:
    return COUNTRY_LABELS_ZH.get(country, country or "国际")


def _clean_text(value: str | None) -> str:
    text = re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()
    return text


def _extract_unit_from_event(event: str) -> str:
    match = re.search(r"\(([^()]+)\)", event or "")
    return _clean_text(match.group(1)) if match else ""


def _numeric_field_present(event: dict) -> bool:
    return bool(_clean_text(event.get("actual")) or _clean_text(event.get("forecast")) or _clean_text(event.get("previous")))


def _has_enough_key_fields(events: list[dict]) -> bool:
    if not events:
        return False
    valid = [
        event
        for event in events
        if _clean_text(event.get("country")) and _clean_text(event.get("event")) and _numeric_field_present(event)
    ]
    return len(valid) >= 1 and len(valid) >= max(1, len(events) // 3)


def _detect_country_from_text(event_text: str) -> str:
    text = _clean_text(event_text)
    lowered = text.lower()
    if text.startswith("中国香港") or "hong kong" in lowered or "香港" in text:
        return ""
    if text.startswith("美国") or " united states" in lowered or lowered.startswith("us "):
        return "United States"
    if text.startswith("中国") or lowered.startswith("china "):
        return "China"
    if text.startswith("欧元区") or lowered.startswith("euro area"):
        return "Euro Area"
    return ""


def _event_text(event: dict) -> str:
    return f"{event.get('event', '')} {event.get('category', '')}".strip().lower()


def _importance_as_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = _clean_text(str(value)).lower()
    if text in {"high", "高"}:
        return 3
    if text in {"medium", "med", "中"}:
        return 2
    if text in {"low", "低"}:
        return 1
    match = re.search(r"\d+", text)
    return int(match.group()) if match else 0


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    )
    return session


def fetch_te_calendar(date_str: str, config: dict | None = None) -> list[dict]:
    """Fetch macro events from Trading Economics."""
    api_key = os.environ.get("TRADINGECONOMICS_API_KEY", "").strip()
    macro_cfg = (config or {}).get("macro_calendar", {})
    if not api_key and macro_cfg.get("te_guest_fallback", False):
        api_key = "guest:guest"

    if not api_key:
        logger.info("Trading Economics key missing, skipping TE source")
        return []

    countries = quote(",".join(TE_COUNTRIES))
    url = (
        "https://api.tradingeconomics.com/calendar/country/"
        f"{countries}/{date_str}/{date_str}"
    )
    try:
        response = _session().get(
            url,
            params={"c": api_key, "f": "json", "lang": "zh"},
            timeout=_request_timeout(config),
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return payload
    except Exception as exc:
        logger.warning("Trading Economics fetch failed: %s", exc)
    return []


def _fx678_country_from_cell(country_cell, event_text: str) -> str:
    for node in country_cell.xpath(".//*[@class]"):
        classes = _clean_text(node.get("class", "")).split()
        for item in classes:
            if item in FX678_FLAG_COUNTRY_MAP:
                return FX678_FLAG_COUNTRY_MAP[item]
    return _detect_country_from_text(event_text)


def _fx678_importance(row_class: str, raw_text: str) -> int:
    importance = _importance_as_int(raw_text)
    if "red_color_s" in (row_class or ""):
        return max(importance, 3)
    return importance


def fetch_fx678_calendar(date_str: str, config: dict | None = None) -> list[dict]:
    """Fetch macro events from FX678 calendar HTML."""
    compact_date = date_str.replace("-", "")
    url = f"https://rl.fx678.com/date/{compact_date}.html"

    try:
        response = _session().get(url, timeout=_request_timeout(config))
        response.raise_for_status()
        doc = lxml.html.fromstring(response.text)
    except Exception as exc:
        logger.warning("FX678 fetch failed: %s", exc)
        return []

    events: list[dict] = []
    for row in doc.xpath("//table[@id='current_data']//tr[td]"):
        cells = row.xpath("./td")
        if len(cells) < 7:
            continue

        has_time_and_country = len(cells) >= 9 and _clean_text(cells[0].text_content())
        if not has_time_and_country:
            continue

        event_text = _clean_text(cells[2].text_content())
        country = _fx678_country_from_cell(cells[1], event_text)
        hrefs = cells[2].xpath(".//a/@href")
        detail_url = f"https://rl.fx678.com{hrefs[0]}" if hrefs and hrefs[0].startswith("/") else (hrefs[0] if hrefs else "")

        events.append(
            {
                "time": _clean_text(cells[0].text_content()),
                "country": country,
                "event": event_text,
                "category": "",
                "previous": _clean_text(cells[3].text_content()),
                "forecast": _clean_text(cells[4].text_content()),
                "actual": _clean_text(cells[5].text_content()),
                "importance": _fx678_importance(row.get("class", ""), _clean_text(cells[6].text_content())),
                "unit": _extract_unit_from_event(event_text),
                "reference": date_str,
                "url": detail_url,
            }
        )

    return events


def fetch_investing_calendar(date_str: str, config: dict | None = None) -> list[dict]:
    """Best-effort Investing fallback. Fast-fails on blocked requests."""
    url = "https://www.investing.com/economic-calendar"
    headers = {
        "Referer": "https://www.google.com/",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        response = _session().get(url, headers=headers, timeout=min(_request_timeout(config), 8))
        if response.status_code != 200:
            logger.warning("Investing fetch unavailable: status=%s", response.status_code)
            return []
        doc = lxml.html.fromstring(response.text)
    except Exception as exc:
        logger.warning("Investing fetch failed: %s", exc)
        return []

    events: list[dict] = []
    for row in doc.xpath("//tr[contains(@class,'js-event-item')]"):
        country = _normalize_country(_clean_text(" ".join(row.xpath(".//td[contains(@class,'flagCur')]//text()"))))
        event_text = _clean_text(" ".join(row.xpath(".//td[contains(@class,'event')]//text()")))
        if not event_text:
            continue
        events.append(
            {
                "country": country or _detect_country_from_text(event_text),
                "event": event_text,
                "category": "",
                "actual": _clean_text(" ".join(row.xpath(".//td[contains(@class,'act')]//text()"))),
                "forecast": _clean_text(" ".join(row.xpath(".//td[contains(@class,'fore')]//text()"))),
                "previous": _clean_text(" ".join(row.xpath(".//td[contains(@class,'prev')]//text()"))),
                "importance": len(row.xpath(".//*[contains(@class,'grayFullBullishIcon')]")),
                "unit": _extract_unit_from_event(event_text),
                "reference": date_str,
                "url": url,
            }
        )
    return events


def normalize_macro_events(events: list[dict], source: str, date_str: str) -> list[dict]:
    """Normalize raw source payloads into the shared event schema."""
    normalized: list[dict] = []

    for raw in events or []:
        if not isinstance(raw, dict):
            continue

        if source == "tradingeconomics":
            country = _normalize_country(raw.get("OCountry") or raw.get("Country"))
            event_text = _clean_text(raw.get("Event") or raw.get("OEvent"))
            category = _clean_text(raw.get("Category") or raw.get("OCategory"))
            reference = _clean_text(raw.get("Reference"))
            unit = _clean_text(raw.get("Unit"))
            date_value = _clean_text(raw.get("Date")) or date_str
            source_url = _clean_text(raw.get("SourceURL"))
        else:
            country = _normalize_country(raw.get("country"))
            event_text = _clean_text(raw.get("event"))
            category = _clean_text(raw.get("category"))
            reference = _clean_text(raw.get("reference")) or date_str
            unit = _clean_text(raw.get("unit"))
            date_value = _clean_text(raw.get("date")) or date_str
            source_url = _clean_text(raw.get("url"))

        if not country:
            country = _detect_country_from_text(event_text)

        normalized.append(
            {
                "source": source,
                "date": date_value,
                "country": country,
                "event": event_text,
                "category": category,
                "actual": _clean_text(raw.get("Actual") if source == "tradingeconomics" else raw.get("actual")),
                "forecast": _clean_text(raw.get("Forecast") if source == "tradingeconomics" else raw.get("forecast")),
                "previous": _clean_text(raw.get("Previous") if source == "tradingeconomics" else raw.get("previous")),
                "importance": _importance_as_int(raw.get("Importance") if source == "tradingeconomics" else raw.get("importance")),
                "unit": unit,
                "reference": reference,
                "source_url": source_url,
            }
        )

    return normalized


def _event_matches_priority(event: dict) -> bool:
    haystack = _event_text(event)
    return any(pattern in haystack for pattern in PRIORITY_PATTERNS)


def _event_blacklisted(event: dict) -> bool:
    haystack = _event_text(event)
    return any(pattern in haystack for pattern in BLACKLIST_PATTERNS)


def _event_core_priority_rank(event: dict) -> int:
    haystack = _event_text(event)
    return 0 if any(pattern in haystack for pattern in CORE_MACRO_PRIORITY_PATTERNS) else 1


def filter_macro_events(events: list[dict]) -> list[dict]:
    """Filter the normalized events to the report-worthy macro subset."""
    filtered: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    for event in events or []:
        country = _normalize_country(event.get("country"))
        if country not in {"China", "United States", "Euro Area"}:
            continue
        if "香港" in _clean_text(event.get("event")) or "hong kong" in _event_text(event):
            continue
        if not _clean_text(event.get("event")):
            continue
        if not _clean_text(event.get("actual")):
            continue
        if _event_blacklisted(event):
            continue

        importance = _importance_as_int(event.get("importance"))
        if importance < 2 and not _event_matches_priority(event):
            continue
        if not _event_matches_priority(event):
            continue

        dedupe_key = (
            country,
            _clean_text(event.get("event")).lower(),
            _clean_text(event.get("reference")).lower(),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        filtered.append(
            {
                **event,
                "country": country,
                "importance": importance,
            }
        )

    filtered.sort(
        key=lambda item: (
            COUNTRY_SORT_PRIORITY.get(item.get("country"), 9),
            _event_core_priority_rank(item),
            -_importance_as_int(item.get("importance")),
            _clean_text(item.get("event")).lower(),
        )
    )
    return filtered


def _classify_event_group(event: dict) -> str:
    text = _event_text(event)
    country = event.get("country")

    if country == "China":
        candidates = DOMESTIC_GROUPS
    elif country == "United States":
        candidates = INTERNATIONAL_GROUPS[:-1]
    elif country == "Euro Area":
        candidates = ["欧元区宏观方面", "国际补充方面"]
    else:
        candidates = ["国际补充方面"]

    for group in candidates:
        if any(keyword in text for keyword in GROUP_KEYWORDS[group]):
            return group

    if country == "China":
        return "中国景气方面"
    if country == "United States":
        return "国际补充方面"
    if country == "Euro Area":
        return "欧元区宏观方面"
    return "国际补充方面"


def _build_event_brief(event: dict) -> str:
    event_name = _clean_text(event.get("event"))
    country_label = _country_label(event.get("country"))
    prefix = event_name if event_name.startswith(country_label) else f"{country_label}{event_name}"
    value_parts = []
    if _clean_text(event.get("actual")):
        value_parts.append(f"今值为{event['actual']}")
    if _clean_text(event.get("forecast")):
        value_parts.append(f"预测值为{event['forecast']}")
    if _clean_text(event.get("previous")):
        value_parts.append(f"前值为{event['previous']}")
    reference = _clean_text(event.get("reference"))
    reference_text = f"（参考期：{reference}）" if reference else ""
    return f"{prefix}，{'，'.join(value_parts)}{reference_text}" if value_parts else f"{prefix}{reference_text}"


def group_macro_events_for_report(events: list[dict]) -> dict:
    """Group macro events into researcher-style domestic/international buckets."""
    domestic_map = {group: [] for group in DOMESTIC_GROUPS}
    international_map = {group: [] for group in INTERNATIONAL_GROUPS}

    for event in events or []:
        group = _classify_event_group(event)
        entry = {
            "category": group,
            "country": event.get("country", ""),
            "event": event.get("event", ""),
            "summary": _build_event_brief(event),
            "actual": event.get("actual", ""),
            "forecast": event.get("forecast", ""),
            "previous": event.get("previous", ""),
            "importance": _importance_as_int(event.get("importance")),
            "reference": event.get("reference", ""),
            "source": event.get("source", ""),
        }

        if event.get("country") == "China":
            domestic_map.setdefault(group, []).append(entry)
        else:
            international_map.setdefault(group, []).append(entry)

    domestic = [
        {"category": group, "items": domestic_map[group]}
        for group in DOMESTIC_GROUPS
        if domestic_map.get(group)
    ]
    international = {
        group: international_map[group]
        for group in INTERNATIONAL_GROUPS
        if international_map.get(group)
    }

    return {
        "domestic": domestic,
        "international": international,
        "source_used": events[0]["source"] if events else "",
    }


def fetch_macro_calendar(date_str: str, config: dict | None = None) -> dict:
    """Fetch macro calendar data with source fallback, cache, and normalization."""
    cached = _load_cached_result(date_str, config)
    if cached is not None:
        return cached

    source_order = _source_order(config)
    fallback_notes: list[str] = []

    fetchers = {
        "tradingeconomics": fetch_te_calendar,
        "fx678": fetch_fx678_calendar,
        "investing": fetch_investing_calendar,
    }

    for source in source_order:
        fetcher = fetchers.get(source)
        if fetcher is None:
            fallback_notes.append(f"{source}:unsupported")
            continue

        raw_events = fetcher(date_str, config)
        if not raw_events:
            fallback_notes.append(f"{source}:empty")
            continue

        normalized = normalize_macro_events(raw_events, source, date_str)
        filtered = filter_macro_events(normalized)
        if not filtered:
            fallback_notes.append(f"{source}:filtered_empty")
            continue
        if not _has_enough_key_fields(filtered):
            fallback_notes.append(f"{source}:insufficient_fields")
            continue

        grouped = group_macro_events_for_report(filtered)
        payload = {
            "events": filtered,
            "grouped": grouped,
            "source_used": source,
            "fallback_chain": list(source_order),
            "fallback_reason": " | ".join(fallback_notes),
            "empty_reason": "",
            "has_data": True,
            "cache_hit": False,
            "date": date_str,
        }
        _save_cached_result(date_str, payload, config)
        logger.info(
            "Macro calendar source selected: %s (%d events)",
            source,
            len(filtered),
        )
        return payload

    empty_reason = "all_sources_failed_or_empty"
    fallback_reason = " | ".join(fallback_notes)
    logger.warning(
        "Macro calendar fallback exhausted: empty_reason=%s, fallback_reason=%s",
        empty_reason,
        fallback_reason or "none",
    )
    return _empty_result(
        date_str,
        fallback_chain=list(source_order),
        fallback_reason=fallback_reason,
        empty_reason=empty_reason,
    )
