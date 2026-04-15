"""
Shared helpers for news recency buckets and time-decay metadata.
"""

from __future__ import annotations

from datetime import datetime, timezone


DEFAULT_TIME_DECAY_BUCKETS = [
    {"label": "0-24h", "max_age_hours": 24.0, "multiplier": 1.0, "report_eligible": True},
    {"label": "24-48h", "max_age_hours": 48.0, "multiplier": 0.55, "report_eligible": True},
    {"label": "48-72h", "max_age_hours": 72.0, "multiplier": 0.25, "report_eligible": True},
]

DEFAULT_STALE_BUCKET = {
    "label": ">72h",
    "multiplier": 0.0,
    "report_eligible": False,
}

UNKNOWN_TIME_BUCKET = {
    "label": "unknown",
    "multiplier": 0.0,
    "report_eligible": False,
}


def parse_publish_time(publish_time: str | None) -> datetime | None:
    """Parse an ISO publish time into a timezone-aware datetime."""
    if not publish_time:
        return None

    try:
        dt = datetime.fromisoformat(str(publish_time))
    except (TypeError, ValueError):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_time_decay_config(config: dict | None) -> dict:
    """Normalize time-decay settings with safe defaults."""
    ranking_cfg = (config or {}).get("news", {}).get("ranking", {})
    time_cfg = ranking_cfg.get("time_decay", {})
    enabled = bool(time_cfg.get("enabled", True))

    raw_buckets = time_cfg.get("buckets") or DEFAULT_TIME_DECAY_BUCKETS
    buckets: list[dict] = []
    previous_max = 0.0
    for raw in raw_buckets:
        max_age_hours = float(raw.get("max_age_hours", 0))
        if max_age_hours <= previous_max:
            continue
        label = raw.get("label") or f"{int(previous_max)}-{int(max_age_hours)}h"
        buckets.append({
            "label": str(label),
            "max_age_hours": max_age_hours,
            "multiplier": float(raw.get("multiplier", 1.0)),
            "report_eligible": bool(raw.get("report_eligible", True)),
        })
        previous_max = max_age_hours

    if not buckets:
        buckets = [bucket.copy() for bucket in DEFAULT_TIME_DECAY_BUCKETS]

    raw_default = time_cfg.get("default", DEFAULT_STALE_BUCKET)
    default_bucket = {
        "label": str(raw_default.get("label", DEFAULT_STALE_BUCKET["label"])),
        "multiplier": float(raw_default.get("multiplier", DEFAULT_STALE_BUCKET["multiplier"])),
        "report_eligible": bool(
            raw_default.get("report_eligible", DEFAULT_STALE_BUCKET["report_eligible"])
        ),
    }
    if not enabled:
        default_bucket = {"label": "all", "multiplier": 1.0, "report_eligible": True}

    report_buckets = [bucket for bucket in buckets if bucket["report_eligible"]]
    max_report_age_hours = max(
        (bucket["max_age_hours"] for bucket in report_buckets),
        default=0.0,
    )

    return {
        "enabled": enabled,
        "buckets": buckets,
        "default": default_bucket,
        "priority_labels": [bucket["label"] for bucket in report_buckets],
        "max_report_age_hours": max_report_age_hours,
    }


def annotate_news_freshness(
    item: dict,
    config: dict | None,
    *,
    now: datetime | None = None,
) -> dict:
    """Attach recency metadata to a news item."""
    profile = get_time_decay_config(config)
    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    now_dt = now_dt.astimezone(timezone.utc)

    publish_dt = parse_publish_time(item.get("publish_time"))
    if publish_dt is None:
        return {
            **item,
            "age_hours": None,
            "recency_bucket": UNKNOWN_TIME_BUCKET["label"],
            "recency_multiplier": UNKNOWN_TIME_BUCKET["multiplier"],
            "report_eligible": False,
        }

    age_hours = max((now_dt - publish_dt).total_seconds() / 3600.0, 0.0)
    for bucket in profile["buckets"]:
        if age_hours < bucket["max_age_hours"]:
            return {
                **item,
                "age_hours": round(age_hours, 2),
                "recency_bucket": bucket["label"],
                "recency_multiplier": bucket["multiplier"],
                "report_eligible": bool(bucket["report_eligible"]),
            }

    return {
        **item,
        "age_hours": round(age_hours, 2),
        "recency_bucket": profile["default"]["label"],
        "recency_multiplier": profile["default"]["multiplier"],
        "report_eligible": bool(profile["default"]["report_eligible"]),
    }


def count_items_by_bucket(items: list[dict]) -> dict[str, int]:
    """Count items by their recency bucket."""
    counts: dict[str, int] = {}
    for item in items:
        label = item.get("recency_bucket", "unknown")
        counts[label] = counts.get(label, 0) + 1
    return counts
