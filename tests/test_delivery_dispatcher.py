import unittest
from pathlib import Path
from unittest.mock import patch

from src.delivery.dispatcher import (
    EVENT_DELIVERY_BLOCKED,
    EVENT_PIPELINE_EXCEPTION,
    deliver_report,
    notify_event,
    summarize_delivery_result,
)


class DeliveryDispatcherTests(unittest.TestCase):
    @patch("src.delivery.dispatcher.notify_feishu_event")
    @patch("src.delivery.dispatcher.deliver_wechat_report")
    def test_deliver_report_preserves_wechat_and_skips_feishu_when_disabled(
        self,
        mock_wechat,
        mock_feishu,
    ):
        mock_wechat.return_value = {
            "provider": "wechat",
            "success": True,
            "skipped": False,
            "event": "report_success",
            "response": {"errcode": 0},
            "error": None,
            "reason": None,
            "status_code": 200,
            "attempts": 1,
        }
        mock_feishu.return_value = {
            "provider": "feishu",
            "success": True,
            "skipped": True,
            "event": "report_success",
            "response": None,
            "error": None,
            "reason": "Feishu notifications disabled",
            "status_code": None,
            "attempts": 1,
        }

        result = deliver_report(
            "report body",
            {"wechat": {"enabled": True, "msg_type": "markdown"}},
            report_path=Path("output/2026-03-10/report.md"),
            fact_check={"passed": True, "review_flags": []},
            generated_at="2026-03-10T15:35:00",
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["attempted_count"], 1)
        self.assertEqual(mock_wechat.call_count, 1)
        self.assertEqual(mock_feishu.call_count, 1)
        feishu_message = mock_feishu.call_args.args[1]
        self.assertIn("[Daily Report]", feishu_message)
        self.assertIn("Path: output/2026-03-10/report.md", feishu_message)

    @patch("src.delivery.dispatcher.notify_feishu_event")
    def test_notify_event_routes_blocked_delivery_to_feishu_only(self, mock_feishu):
        mock_feishu.return_value = {
            "provider": "feishu",
            "success": True,
            "skipped": False,
            "event": EVENT_DELIVERY_BLOCKED,
            "response": {"code": 0},
            "error": None,
            "reason": None,
            "status_code": 200,
            "attempts": 1,
        }

        result = notify_event(
            EVENT_DELIVERY_BLOCKED,
            report_path="output/2026-03-10/report.md",
            review_flags=["[NEEDS REVIEW] example"],
            reason="Fact-check failed on attempt 2/10",
            attempt=2,
            max_attempts=10,
            attempt_mode="same-data retry",
            next_delay_seconds=60,
            is_final_attempt=False,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["attempted_count"], 1)
        self.assertEqual(mock_feishu.call_count, 1)
        message = mock_feishu.call_args.args[1]
        self.assertIn("Delivery blocked after fact-check", message)
        self.assertIn("Attempt: 2/10", message)
        self.assertIn("Mode: same-data retry", message)
        self.assertIn("Next retry in: 60s", message)
        self.assertIn("Final failure: no", message)

    @patch("src.delivery.dispatcher.notify_feishu_event")
    def test_notify_event_formats_pipeline_exception(self, mock_feishu):
        mock_feishu.return_value = {
            "provider": "feishu",
            "success": True,
            "skipped": False,
            "event": EVENT_PIPELINE_EXCEPTION,
            "response": {"code": 0},
            "error": None,
            "reason": None,
            "status_code": 200,
            "attempts": 1,
        }

        notify_event(EVENT_PIPELINE_EXCEPTION, exception=RuntimeError("boom"))

        message = mock_feishu.call_args.args[1]
        self.assertIn("Unhandled exception in stock_daily_report", message)
        self.assertIn("Type: RuntimeError", message)
        self.assertIn("Error: boom", message)

    def test_summarize_delivery_result_compacts_provider_states(self):
        result = {
            "providers": {
                "wechat": {"success": True, "skipped": False},
                "feishu": {"success": True, "skipped": True, "reason": "Feishu notifications disabled"},
            }
        }

        summary = summarize_delivery_result(result)

        self.assertIn("wechat=OK", summary)
        self.assertIn("feishu=Feishu notifications disabled", summary)


if __name__ == "__main__":
    unittest.main()
