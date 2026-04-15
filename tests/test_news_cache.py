import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import src.main as main_module


def _config() -> dict:
    return {
        "news": {
            "ranking": {
                "time_decay": {
                    "enabled": True,
                    "buckets": [
                        {"label": "0-24h", "max_age_hours": 24, "multiplier": 1.0, "report_eligible": True},
                        {"label": "24-48h", "max_age_hours": 48, "multiplier": 0.55, "report_eligible": True},
                        {"label": "48-72h", "max_age_hours": 72, "multiplier": 0.25, "report_eligible": True},
                    ],
                    "default": {"label": ">72h", "multiplier": 0.0, "report_eligible": False},
                }
            }
        }
    }


def _cached_item(title: str, *, age_hours: float, now: datetime, url: str) -> dict:
    return {
        "title": title,
        "content": "",
        "publish_time": (now - timedelta(hours=age_hours)).isoformat(),
        "source": "Jin10",
        "category": "china",
        "url": url,
    }


class NewsCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = _config()
        self.now = datetime(2026, 3, 18, 15, 0, tzinfo=timezone.utc)

    def test_load_recent_cached_news_filters_to_72h_window(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cache_path = root / "output" / "2026-03-17" / main_module.NEWS_CACHE_FILENAME
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_payload = {
                "date": "2026-03-17",
                "saved_at": self.now.isoformat(),
                "market_news": [
                    _cached_item("12小时内", age_hours=12, now=self.now, url="https://example.com/12"),
                    _cached_item("36小时内", age_hours=36, now=self.now, url="https://example.com/36"),
                    _cached_item("80小时外", age_hours=80, now=self.now, url="https://example.com/80"),
                ],
            }
            cache_path.write_text(json.dumps(cache_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with patch.object(main_module, "PROJECT_ROOT", root):
                loaded_items, meta = main_module._load_recent_cached_news(self.config, now=self.now)

        self.assertEqual([item["title"] for item in loaded_items], ["12小时内", "36小时内"])
        self.assertEqual(meta["loaded_item_count"], 2)
        self.assertEqual(meta["max_report_age_hours"], 72.0)

    def test_merge_market_news_with_cache_prefers_live_items_and_deduplicates(self):
        live_items = [
            {
                "title": "实时新闻",
                "content": "",
                "publish_time": self.now.isoformat(),
                "source": "Jin10",
                "category": "china",
                "url": "https://example.com/shared",
            }
        ]
        cached_items = [
            {
                "title": "缓存中的同一条新闻",
                "content": "",
                "publish_time": (self.now - timedelta(hours=2)).isoformat(),
                "source": "Jin10",
                "category": "china",
                "url": "https://example.com/shared",
            },
            {
                "title": "缓存独有新闻",
                "content": "",
                "publish_time": (self.now - timedelta(hours=30)).isoformat(),
                "source": "Jin10",
                "category": "china",
                "url": "https://example.com/unique",
            },
        ]

        merged = main_module._merge_market_news_with_cache(live_items, cached_items)

        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0]["title"], "实时新闻")
        self.assertEqual(merged[1]["title"], "缓存独有新闻")

    def test_save_report_writes_news_cache_and_freshness_audit_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_path = root / "report.md"
            audit_path = root / "audit.json"
            news_data = {
                "ranking_details": {
                    "method": "keyword+llm",
                    "freshness": {
                        "input_bucket_counts": {"0-24h": 6, "24-48h": 2},
                        "final_bucket_counts": {"0-24h": 4, "24-48h": 1},
                    },
                },
                "cache_details": {
                    "live_item_count": 10,
                    "loaded_item_count": 3,
                    "merged_item_count": 12,
                    "saved_path": "/tmp/news_cache.json",
                },
                "macro_calendar": {"events": []},
            }

            main_module.save_report(
                "示例报告",
                {"passed": True, "review_flags": [], "number_check": {}},
                news_data,
                self.config,
                report_path=report_path,
                audit_path=audit_path,
                generated_at=self.now.isoformat(),
            )

            audit = json.loads(audit_path.read_text(encoding="utf-8"))

        self.assertIn("news_cache", audit)
        self.assertEqual(audit["news_cache"]["merged_item_count"], 12)
        self.assertEqual(
            audit["news_ranking"]["freshness"]["final_bucket_counts"],
            {"0-24h": 4, "24-48h": 1},
        )


if __name__ == "__main__":
    unittest.main()
