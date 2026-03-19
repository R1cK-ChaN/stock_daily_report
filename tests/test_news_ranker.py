import unittest
from datetime import datetime, timedelta, timezone

from src.fetchers.news_freshness import annotate_news_freshness
from src.fetchers.news_ranker import (
    _compute_keyword_score,
    _select_report_items_by_bucket_priority,
    keyword_rank,
)


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


def _item(title: str, *, age_hours: float | None, now: datetime, content: str = "") -> dict:
    publish_time = ""
    if age_hours is not None:
        publish_time = (now - timedelta(hours=age_hours)).isoformat()
    return {
        "title": title,
        "content": content,
        "publish_time": publish_time,
        "source": "Jin10",
        "url": f"https://example.com/{abs(hash((title, publish_time))) % 100000}",
    }


class NewsRankerRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = _config()
        self.now = datetime(2026, 3, 18, 15, 0, tzinfo=timezone.utc)

    def test_macro_and_institution_keywords_outweigh_low_value_company_news(self):
        macro_score = _compute_keyword_score(
            "高盛：美国CPI与非农仍偏强，美联储资产负债表收缩延续",
            "",
        )
        company_score = _compute_keyword_score(
            "某公司控股股东变更并发布净利润增长公告",
            "",
        )

        self.assertGreater(macro_score, company_score)

    def test_recency_bucket_boundaries_are_precise(self):
        cases = [
            (23.99, "0-24h", True, 1.0),
            (24.0, "24-48h", True, 0.55),
            (47.99, "24-48h", True, 0.55),
            (48.0, "48-72h", True, 0.25),
            (71.99, "48-72h", True, 0.25),
            (72.0, ">72h", False, 0.0),
        ]

        for age_hours, expected_bucket, expected_eligible, expected_multiplier in cases:
            with self.subTest(age_hours=age_hours):
                annotated = annotate_news_freshness(
                    _item("美国CPI公布", age_hours=age_hours, now=self.now),
                    self.config,
                    now=self.now,
                )
                self.assertEqual(annotated["recency_bucket"], expected_bucket)
                self.assertEqual(annotated["report_eligible"], expected_eligible)
                self.assertEqual(annotated["recency_multiplier"], expected_multiplier)

    def test_keyword_rank_keeps_company_news_only_as_low_priority_fallback(self):
        items = [
            _item("美国2月CPI高于预期，美联储资产负债表收缩路径受关注", age_hours=2, now=self.now),
            _item("中金：美国就业与房地产数据仍有韧性，A股风险偏好修复仍看政策配合", age_hours=4, now=self.now),
            _item("某公司控股股东变更并发布净利润增长公告", age_hours=1, now=self.now),
        ]

        ranked = keyword_rank(items, top_n=3, config=self.config)
        titles = [item["title"] for item in ranked]

        self.assertEqual(titles[-1], "某公司控股股东变更并发布净利润增长公告")
        self.assertIn("美国2月CPI高于预期，美联储资产负债表收缩路径受关注", titles[:2])

    def test_keyword_rank_prioritizes_fresher_news_with_same_keywords(self):
        items = [
            _item("美国CPI高于预期，零售销售仍偏强（旧）", age_hours=36, now=self.now),
            _item("美国CPI高于预期，零售销售仍偏强（新）", age_hours=6, now=self.now),
        ]

        ranked = keyword_rank(items, top_n=2, config=self.config)

        self.assertEqual(ranked[0]["title"], "美国CPI高于预期，零售销售仍偏强（新）")
        self.assertGreater(ranked[0]["keyword_score"], ranked[1]["keyword_score"])
        self.assertEqual(ranked[0]["recency_bucket"], "0-24h")
        self.assertEqual(ranked[1]["recency_bucket"], "24-48h")

    def test_keyword_rank_excludes_stale_and_invalid_publish_time(self):
        items = [
            _item("美国非农意外走强", age_hours=6, now=self.now),
            _item("美国非农意外走强（旧）", age_hours=80, now=self.now),
            _item("美国非农意外走强（无时间）", age_hours=None, now=self.now),
        ]

        ranked = keyword_rank(items, top_n=5, config=self.config)

        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0]["title"], "美国非农意外走强")
        self.assertTrue(ranked[0]["report_eligible"])

    def test_select_report_items_prefers_fresh_buckets_over_older_scores(self):
        ordered_candidates = [
            {
                "title": "能源旧闻",
                "keyword_score": 99.0,
                "recency_bucket": "48-72h",
                "report_eligible": True,
            },
            {
                "title": "美国CPI新消息",
                "keyword_score": 40.0,
                "recency_bucket": "0-24h",
                "report_eligible": True,
            },
            {
                "title": "A股机构观点新消息",
                "keyword_score": 35.0,
                "recency_bucket": "0-24h",
                "report_eligible": True,
            },
            {
                "title": "昨日补位消息",
                "keyword_score": 30.0,
                "recency_bucket": "24-48h",
                "report_eligible": True,
            },
        ]

        top_two = _select_report_items_by_bucket_priority(
            ordered_candidates,
            top_n=2,
            config=self.config,
        )
        top_three = _select_report_items_by_bucket_priority(
            ordered_candidates,
            top_n=3,
            config=self.config,
        )

        self.assertEqual([item["title"] for item in top_two], ["美国CPI新消息", "A股机构观点新消息"])
        self.assertEqual(
            [item["title"] for item in top_three],
            ["美国CPI新消息", "A股机构观点新消息", "昨日补位消息"],
        )

    def test_a_share_strategy_view_outranks_generic_global_central_bank_commentary(self):
        a_share_view_score = _compute_keyword_score(
            "瑞银：更偏好A股而非H股和ADR，交易量高企与两会政策将支撑中国股票表现",
            "",
        )
        global_commentary_score = _compute_keyword_score(
            "机构观点：欧洲央行与英国央行利率路径仍是全球市场关注焦点",
            "",
        )

        self.assertGreater(a_share_view_score, global_commentary_score)


if __name__ == "__main__":
    unittest.main()
