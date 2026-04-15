import unittest
from unittest.mock import patch

from src.fetchers.macro_calendar import (
    fetch_macro_calendar,
    filter_macro_events,
    group_macro_events_for_report,
)


class MacroCalendarFallbackTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "macro_calendar": {
                "cache_enabled": False,
                "source_order": ["tradingeconomics", "fx678", "investing"],
            }
        }

    @patch("src.fetchers.macro_calendar.group_macro_events_for_report")
    @patch("src.fetchers.macro_calendar.filter_macro_events")
    @patch("src.fetchers.macro_calendar.normalize_macro_events")
    @patch("src.fetchers.macro_calendar.fetch_investing_calendar")
    @patch("src.fetchers.macro_calendar.fetch_fx678_calendar")
    @patch("src.fetchers.macro_calendar.fetch_te_calendar")
    def test_fetch_macro_calendar_prefers_te_when_available(
        self,
        mock_te,
        mock_fx678,
        mock_investing,
        mock_normalize,
        mock_filter,
        mock_group,
    ):
        event = {
            "source": "tradingeconomics",
            "country": "United States",
            "event": "Initial Jobless Claims",
            "actual": "220K",
            "forecast": "225K",
            "previous": "221K",
            "importance": 3,
            "reference": "Mar/08",
        }
        mock_te.return_value = [{"CalendarId": "1"}]
        mock_normalize.return_value = [event]
        mock_filter.return_value = [event]
        mock_group.return_value = {
            "domestic": [],
            "international": {"美国劳动力市场方面": [{"summary": "美国初请失业金公布值220K"}]},
            "source_used": "tradingeconomics",
        }

        result = fetch_macro_calendar("2026-03-13", self.config)

        self.assertTrue(result["has_data"])
        self.assertEqual(result["source_used"], "tradingeconomics")
        self.assertEqual(result["events"], [event])
        mock_fx678.assert_not_called()
        mock_investing.assert_not_called()

    @patch("src.fetchers.macro_calendar.group_macro_events_for_report")
    @patch("src.fetchers.macro_calendar.filter_macro_events")
    @patch("src.fetchers.macro_calendar.normalize_macro_events")
    @patch("src.fetchers.macro_calendar.fetch_investing_calendar")
    @patch("src.fetchers.macro_calendar.fetch_fx678_calendar")
    @patch("src.fetchers.macro_calendar.fetch_te_calendar")
    def test_fetch_macro_calendar_falls_back_to_fx678(
        self,
        mock_te,
        mock_fx678,
        mock_investing,
        mock_normalize,
        mock_filter,
        mock_group,
    ):
        event = {
            "source": "fx678",
            "country": "China",
            "event": "中国2月CPI年率",
            "actual": "0.7%",
            "forecast": "0.8%",
            "previous": "0.5%",
            "importance": 3,
            "reference": "2月",
        }
        mock_te.return_value = []
        mock_fx678.return_value = [{"event": "中国2月CPI年率"}]
        mock_normalize.return_value = [event]
        mock_filter.return_value = [event]
        mock_group.return_value = {
            "domestic": [{"category": "中国通胀方面", "items": [{"summary": "中国2月CPI公布值0.7%"}]}],
            "international": {},
            "source_used": "fx678",
        }

        result = fetch_macro_calendar("2026-03-13", self.config)

        self.assertTrue(result["has_data"])
        self.assertEqual(result["source_used"], "fx678")
        self.assertIn("tradingeconomics:empty", result["fallback_reason"])
        mock_investing.assert_not_called()

    @patch("src.fetchers.macro_calendar.group_macro_events_for_report")
    @patch("src.fetchers.macro_calendar.filter_macro_events")
    @patch("src.fetchers.macro_calendar.normalize_macro_events")
    @patch("src.fetchers.macro_calendar.fetch_investing_calendar")
    @patch("src.fetchers.macro_calendar.fetch_fx678_calendar")
    @patch("src.fetchers.macro_calendar.fetch_te_calendar")
    def test_fetch_macro_calendar_uses_investing_as_last_resort(
        self,
        mock_te,
        mock_fx678,
        mock_investing,
        mock_normalize,
        mock_filter,
        mock_group,
    ):
        event = {
            "source": "investing",
            "country": "United States",
            "event": "Retail Sales MoM",
            "actual": "0.4%",
            "forecast": "0.3%",
            "previous": "0.1%",
            "importance": 2,
            "reference": "Mar",
        }
        mock_te.return_value = []
        mock_fx678.return_value = []
        mock_investing.return_value = [{"event": "Retail Sales MoM"}]
        mock_normalize.return_value = [event]
        mock_filter.return_value = [event]
        mock_group.return_value = {
            "domestic": [],
            "international": {"美国通胀与消费方面": [{"summary": "美国零售销售公布值0.4%"}]},
            "source_used": "investing",
        }

        result = fetch_macro_calendar("2026-03-13", self.config)

        self.assertTrue(result["has_data"])
        self.assertEqual(result["source_used"], "investing")
        self.assertIn("fx678:empty", result["fallback_reason"])

    @patch("src.fetchers.macro_calendar.fetch_investing_calendar", return_value=[])
    @patch("src.fetchers.macro_calendar.fetch_fx678_calendar", return_value=[])
    @patch("src.fetchers.macro_calendar.fetch_te_calendar", return_value=[])
    def test_fetch_macro_calendar_returns_empty_payload_when_all_sources_fail(
        self,
        _mock_te,
        _mock_fx678,
        _mock_investing,
    ):
        result = fetch_macro_calendar("2026-03-13", self.config)

        self.assertFalse(result["has_data"])
        self.assertEqual(result["events"], [])
        self.assertEqual(result["source_used"], "")
        self.assertEqual(result["empty_reason"], "all_sources_failed_or_empty")


class MacroCalendarFilterAndGroupingTests(unittest.TestCase):
    def test_filter_macro_events_keeps_high_value_macro_and_drops_noise(self):
        events = [
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "United States",
                "event": "Initial Jobless Claims",
                "category": "Labor",
                "actual": "220K",
                "forecast": "225K",
                "previous": "221K",
                "importance": 3,
                "unit": "K",
                "reference": "Mar/08",
            },
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "China",
                "event": "CPI YoY",
                "category": "Inflation",
                "actual": "0.7%",
                "forecast": "0.8%",
                "previous": "0.5%",
                "importance": 3,
                "unit": "%",
                "reference": "Feb",
            },
            {
                "source": "fx678",
                "date": "2026-03-13",
                "country": "United States",
                "event": "SPDR Gold Holdings Daily Update",
                "category": "",
                "actual": "1075.85",
                "forecast": "",
                "previous": "1077.28",
                "importance": 3,
                "unit": "吨",
                "reference": "2026-03-13",
            },
            {
                "source": "fx678",
                "date": "2026-03-13",
                "country": "United States",
                "event": "Fed Member Speech",
                "category": "",
                "actual": "",
                "forecast": "",
                "previous": "",
                "importance": 1,
                "unit": "",
                "reference": "2026-03-13",
            },
        ]

        filtered = filter_macro_events(events)

        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["country"], "China")
        self.assertEqual(filtered[1]["event"], "Initial Jobless Claims")

    def test_group_macro_events_maps_core_topics(self):
        events = [
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "United States",
                "event": "Fed Balance Sheet",
                "category": "",
                "actual": "6.65T",
                "forecast": "",
                "previous": "6.63T",
                "importance": 3,
                "unit": "T",
                "reference": "Mar/11",
            },
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "United States",
                "event": "Housing Starts",
                "category": "",
                "actual": "1.52M",
                "forecast": "1.48M",
                "previous": "1.50M",
                "importance": 3,
                "unit": "M",
                "reference": "Feb",
            },
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "China",
                "event": "CPI YoY",
                "category": "Inflation",
                "actual": "0.7%",
                "forecast": "0.8%",
                "previous": "0.5%",
                "importance": 3,
                "unit": "%",
                "reference": "Feb",
            },
        ]

        grouped = group_macro_events_for_report(events)

        domestic_categories = [item["category"] for item in grouped["domestic"]]
        self.assertIn("中国通胀方面", domestic_categories)
        self.assertIn("美国资产负债方面", grouped["international"])
        self.assertIn("美国房地产方面", grouped["international"])

    def test_group_macro_events_handles_consumer_sentiment_money_supply_and_euro_area(self):
        events = [
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "China",
                "event": "Consumer Sentiment",
                "category": "",
                "actual": "72.46",
                "forecast": "",
                "previous": "72.82",
                "importance": 3,
                "unit": "",
                "reference": "Mar",
            },
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "China",
                "event": "M2 Money Supply YoY",
                "category": "",
                "actual": "7.1%",
                "forecast": "7.0%",
                "previous": "7.0%",
                "importance": 3,
                "unit": "%",
                "reference": "Feb",
            },
            {
                "source": "tradingeconomics",
                "date": "2026-03-13",
                "country": "Euro Area",
                "event": "CPI YoY",
                "category": "Inflation",
                "actual": "2.4%",
                "forecast": "2.5%",
                "previous": "2.4%",
                "importance": 3,
                "unit": "%",
                "reference": "Feb",
            },
        ]

        grouped = group_macro_events_for_report(events)
        domestic_categories = [item["category"] for item in grouped["domestic"]]

        self.assertIn("中国消费情绪方面", domestic_categories)
        self.assertIn("中国货币方面", domestic_categories)
        self.assertIn("欧元区宏观方面", grouped["international"])


if __name__ == "__main__":
    unittest.main()
