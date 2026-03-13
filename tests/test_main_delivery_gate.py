import os
import unittest
from pathlib import Path
from unittest.mock import call, patch

import src.main as main_module


def _config(max_attempts: int = 10) -> dict:
    return {
        "delivery_retry": {
            "enabled": True,
            "max_attempts": max_attempts,
            "initial_backoff_seconds": 30,
            "backoff_multiplier": 2,
            "max_backoff_seconds": 300,
            "notify_each_blocked": True,
        }
    }


def _failed_post_check() -> dict:
    return {
        "passed": False,
        "review_flags": ["[NEEDS REVIEW] example issue"],
        "claim_check": {
            "issues": [
                {
                    "severity": "critical",
                    "claim": "example claim",
                    "explanation": "unsupported",
                }
            ]
        },
        "number_check": {"verification_rate": 1.0},
    }


def _passed_post_check() -> dict:
    return {
        "passed": True,
        "review_flags": [],
        "claim_check": {"issues": []},
        "number_check": {"verification_rate": 1.0},
    }


def _generation_result(label: str) -> dict:
    return {
        "report_text": f"一、A股收评（市场表现）\n\n示例报告正文 {label}",
        "model": "demo-model",
        "usage": {"input_tokens": 10, "output_tokens": 10},
        "generated_at": f"2026-03-11T10:52:{label}",
    }


def _save_report_side_effect(*_args, **kwargs) -> Path:
    return Path(kwargs.get("report_path", Path("output/2026-03-11/report.md")))


class MainDeliveryGateTests(unittest.TestCase):
    def test_run_pipeline_retries_until_successful_delivery(self):
        with (
            patch("src.main.is_trading_day", return_value=True),
            patch("src.main.load_config", return_value=_config(10)),
            patch("src.main.fetch_all_data", return_value=({}, {}, {})) as mock_fetch,
            patch("src.main.rank_news", return_value={"market_news": []}) as mock_rank,
            patch(
                "src.main.run_pre_generation_checks",
                return_value={"passed": True, "warning_count": 0, "critical_count": 0, "issues": []},
            ) as mock_precheck,
            patch(
                "src.main.generate_report",
                side_effect=[
                    _generation_result("13"),
                    _generation_result("14"),
                    _generation_result("15"),
                ],
            ) as mock_generate,
            patch(
                "src.main.run_post_generation_checks",
                side_effect=[_failed_post_check(), _failed_post_check(), _passed_post_check()],
            ),
            patch("src.main.save_report", side_effect=_save_report_side_effect),
            patch("src.main.notify_event", return_value={"providers": {"feishu": {"success": True}}}) as mock_notify,
            patch(
                "src.main.deliver_report",
                return_value={"providers": {"feishu": {"success": True}}},
            ) as mock_deliver,
            patch("src.main.log_report_snapshot"),
            patch("src.main.time.sleep") as mock_sleep,
            patch.dict(os.environ, {"ALLOW_NEEDS_REVIEW_DELIVERY": "false"}, clear=False),
        ):
            result = main_module.run_pipeline()

        self.assertTrue(result["success"])
        self.assertEqual(result["attempts_used"], 3)
        self.assertEqual(result["max_attempts"], 10)
        self.assertFalse(result["retry_exhausted"])
        self.assertEqual(result["final_attempt_mode"], "same-data retry")
        self.assertTrue(result["fact_check"]["passed"])
        self.assertFalse(result["fact_check"]["delivery_blocked"])
        self.assertEqual(mock_fetch.call_count, 1)
        self.assertEqual(mock_rank.call_count, 1)
        self.assertEqual(mock_precheck.call_count, 1)
        self.assertEqual(mock_generate.call_count, 3)
        self.assertEqual(mock_notify.call_count, 2)
        self.assertEqual(mock_deliver.call_count, 1)
        self.assertEqual(mock_sleep.call_args_list, [call(30), call(60)])

        second_call = mock_generate.call_args_list[1]
        self.assertEqual(second_call.kwargs["temperature_override"], 0.0)
        self.assertIn("[NEEDS REVIEW] example issue", second_call.kwargs["regeneration_hints"])
        self.assertIn("example claim: unsupported", second_call.kwargs["regeneration_hints"])

    def test_run_pipeline_returns_failure_after_retry_exhaustion(self):
        with (
            patch("src.main.is_trading_day", return_value=True),
            patch("src.main.load_config", return_value=_config(3)),
            patch("src.main.fetch_all_data", side_effect=[({}, {}, {}), ({}, {}, {})]) as mock_fetch,
            patch(
                "src.main.rank_news",
                side_effect=[{"market_news": []}, {"market_news": []}],
            ) as mock_rank,
            patch(
                "src.main.run_pre_generation_checks",
                side_effect=[
                    {"passed": True, "warning_count": 0, "critical_count": 0, "issues": []},
                    {"passed": True, "warning_count": 0, "critical_count": 0, "issues": []},
                ],
            ) as mock_precheck,
            patch(
                "src.main.generate_report",
                side_effect=[
                    _generation_result("13"),
                    _generation_result("14"),
                    _generation_result("15"),
                ],
            ) as mock_generate,
            patch(
                "src.main.run_post_generation_checks",
                side_effect=[_failed_post_check(), _failed_post_check(), _failed_post_check()],
            ),
            patch("src.main.save_report", side_effect=_save_report_side_effect),
            patch("src.main.notify_event", return_value={"providers": {"feishu": {"success": True}}}) as mock_notify,
            patch("src.main.deliver_report", return_value={"providers": {}}) as mock_deliver,
            patch("src.main.log_report_snapshot"),
            patch("src.main.time.sleep") as mock_sleep,
            patch.dict(os.environ, {"ALLOW_NEEDS_REVIEW_DELIVERY": "false"}, clear=False),
        ):
            result = main_module.run_pipeline()

        self.assertFalse(result["success"])
        self.assertEqual(result["stage"], "delivery_retry_exhausted")
        self.assertEqual(result["attempts_used"], 3)
        self.assertEqual(result["max_attempts"], 3)
        self.assertTrue(result["retry_exhausted"])
        self.assertEqual(result["final_attempt_mode"], "full refresh retry")
        self.assertTrue(result["fact_check"]["delivery_blocked"])
        self.assertIn("/attempts/", result["report_path"])
        self.assertTrue(result["report_path"].endswith("attempt_03_report.md"))
        self.assertEqual(mock_fetch.call_count, 2)
        self.assertEqual(mock_rank.call_count, 2)
        self.assertEqual(mock_precheck.call_count, 2)
        self.assertEqual(mock_generate.call_count, 3)
        self.assertEqual(mock_notify.call_count, 3)
        self.assertEqual(mock_deliver.call_count, 0)
        self.assertEqual(mock_sleep.call_args_list, [call(30), call(60)])
        self.assertIsNone(mock_generate.call_args_list[2].kwargs["temperature_override"])

    def test_run_pipeline_forces_delivery_when_override_is_enabled(self):
        with (
            patch("src.main.is_trading_day", return_value=True),
            patch("src.main.load_config", return_value=_config(10)),
            patch("src.main.fetch_all_data", return_value=({}, {}, {})) as mock_fetch,
            patch("src.main.rank_news", return_value={"market_news": []}),
            patch(
                "src.main.run_pre_generation_checks",
                return_value={"passed": True, "warning_count": 0, "critical_count": 0, "issues": []},
            ),
            patch("src.main.generate_report", return_value=_generation_result("13")) as mock_generate,
            patch("src.main.run_post_generation_checks", return_value=_failed_post_check()),
            patch("src.main.save_report", side_effect=_save_report_side_effect),
            patch("src.main.notify_event", return_value={"providers": {"feishu": {"success": True}}}) as mock_notify,
            patch(
                "src.main.deliver_report",
                return_value={"providers": {"feishu": {"success": True}}},
            ) as mock_deliver,
            patch("src.main.log_report_snapshot"),
            patch("src.main.time.sleep") as mock_sleep,
            patch.dict(os.environ, {"ALLOW_NEEDS_REVIEW_DELIVERY": "true"}, clear=False),
        ):
            result = main_module.run_pipeline()

        self.assertTrue(result["success"])
        self.assertFalse(result["fact_check"]["delivery_blocked"])
        self.assertTrue(result["fact_check"]["delivery_forced"])
        self.assertEqual(result["attempts_used"], 1)
        self.assertEqual(result["max_attempts"], 1)
        self.assertFalse(result["retry_exhausted"])
        self.assertEqual(mock_fetch.call_count, 1)
        self.assertEqual(mock_generate.call_count, 1)
        self.assertEqual(mock_notify.call_count, 0)
        self.assertEqual(mock_deliver.call_count, 1)
        self.assertEqual(mock_sleep.call_count, 0)
        delivered_text = mock_deliver.call_args.args[0]
        self.assertIn("[TEST DELIVERY][NEEDS REVIEW]", delivered_text)
        self.assertIn("ALLOW_NEEDS_REVIEW_DELIVERY=true", delivered_text)


if __name__ == "__main__":
    unittest.main()
