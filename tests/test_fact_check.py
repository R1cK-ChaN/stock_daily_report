import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.checker.fact_check import build_source_numbers, verify_claims_with_llm
from src.fetchers.market_data import _compute_breadth
from src.generator.report_generator import build_generation_prompt


class FactCheckRegressionTests(unittest.TestCase):
    def test_compute_breadth_adds_ratio_fields(self):
        df = pd.DataFrame(
            {
                "涨跌幅": [1.2, 0.0, -0.5, 9.95],
                "成交量": [1, 1, 1, 1],
                "成交额": [10, 20, 30, 40],
            }
        )

        breadth = _compute_breadth(df)

        self.assertEqual(breadth["total_stocks"], 4)
        self.assertEqual(breadth["up_ratio_pct"], 50.0)
        self.assertEqual(breadth["down_ratio_pct"], 25.0)
        self.assertEqual(breadth["flat_ratio_pct"], 25.0)

    def test_build_generation_prompt_includes_explicit_breadth_ratios(self):
        market_data = {
            "indices": [],
            "sectors": {},
            "breadth": {
                "up_count": 4302,
                "down_count": 790,
                "flat_count": 89,
                "limit_up": 65,
                "limit_down": 2,
                "total_stocks": 5181,
                "total_amount": 2400000000000,
                "up_ratio_pct": 83.03,
                "down_ratio_pct": 15.25,
                "flat_ratio_pct": 1.72,
            },
        }

        prompt = build_generation_prompt(market_data, {"market_news": []}, {"has_data": False})

        self.assertIn("上涨占比: 83.03%", prompt)
        self.assertIn("若提及市场上涨/下跌/平盘占比，只能直接引用【市场广度】中明确给出的百分比", prompt)
        self.assertIn("禁止写“较上一交易日放量/缩量”", prompt)

    def test_build_generation_prompt_labels_omo_total_as_operation_total(self):
        prompt = build_generation_prompt(
            {"indices": [], "sectors": {}, "breadth": {}},
            {"market_news": []},
            {
                "has_data": True,
                "date": "2026-03-10",
                "omo": {
                    "has_data": True,
                    "title": "公开市场业务交易公告",
                    "op_type": "逆回购",
                    "operations": [{"tenor": "7天", "rate": 1.40, "win_amount": 395.0}],
                    "total_amount": 395.0,
                    "url": "https://example.com/omo",
                },
                "repo_rates": {},
                "shibor": {},
                "lpr": {},
            },
        )

        self.assertIn("操作总量: 395.0亿元", prompt)
        self.assertIn("禁止把操作总量改写为投放资金", prompt)

    def test_build_source_numbers_includes_explicit_breadth_ratios(self):
        numbers = build_source_numbers(
            {"breadth": {"up_ratio_pct": 83.03, "down_ratio_pct": 15.25, "flat_ratio_pct": 1.72}},
            {},
            {},
        )

        self.assertIn(83.03, numbers)
        self.assertIn(15.25, numbers)
        self.assertIn(1.72, numbers)

    @patch("src.checker.fact_check.openai.OpenAI")
    def test_verify_claims_with_llm_infers_success_when_json_omits_overall_verified(self, mock_openai):
        response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content='{"issues":[],"summary":"核查通过，未发现不支持的声明。"}'
                    )
                )
            ],
            usage=SimpleNamespace(prompt_tokens=100, completion_tokens=20),
        )
        mock_openai.return_value.chat.completions.create.return_value = response

        result = verify_claims_with_llm(
            report_text="示例报告",
            market_data={},
            news_data={},
            pboc_data={},
            config={"llm": {"model": "demo", "base_url": "https://example.com"}},
        )

        self.assertTrue(result["verified"])
        self.assertEqual(result["issues"], [])


if __name__ == "__main__":
    unittest.main()
