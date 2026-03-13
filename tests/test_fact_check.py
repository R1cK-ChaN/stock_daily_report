import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.checker.fact_check import (
    build_source_numbers,
    run_post_generation_checks,
    verify_claims_with_llm,
)
from src.fetchers.market_data import _compute_breadth
from src.generator.report_generator import build_generation_prompt, clean_report_style


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

    def test_build_generation_prompt_adds_domestic_and_international_fundamental_structure(self):
        prompt = build_generation_prompt(
            {"indices": [], "sectors": {}, "breadth": {}},
            {"market_news": []},
            {"has_data": False},
        )

        self.assertIn("一、市场表现", prompt)
        self.assertIn("三、央行动态", prompt)
        self.assertIn("四、市场观察摘要", prompt)
        self.assertIn("国内方面：", prompt)
        self.assertIn("国际方面：", prompt)
        self.assertIn("i. ii. iii.", prompt)
        self.assertIn("国际方面优先解读美国宏观指标及美联储政策信号", prompt)
        self.assertIn("机构、券商、外资、投行", prompt)
        self.assertIn("禁止使用这些表达：", prompt)
        self.assertIn("整体来看", prompt)
        self.assertIn("投资者需关注", prompt)

    def test_build_generation_prompt_prioritizes_macro_calendar_context(self):
        prompt = build_generation_prompt(
            {"indices": [], "sectors": {}, "breadth": {}},
            {
                "market_news": [],
                "macro_calendar": {
                    "has_data": True,
                    "source_used": "tradingeconomics",
                    "events": [
                        {
                            "source": "tradingeconomics",
                            "country": "China",
                            "event": "CPI YoY",
                            "actual": "0.7%",
                            "forecast": "0.8%",
                            "previous": "0.5%",
                            "importance": 3,
                            "reference": "Feb",
                        }
                    ],
                    "grouped": {
                        "domestic": [
                            {
                                "category": "中国通胀方面",
                                "items": [{"summary": "中国CPI同比公布值0.7%，预期0.8%，前值0.5%"}],
                            }
                        ],
                        "international": {
                            "美国劳动力市场方面": [{"summary": "美国初请失业金公布值220K"}]
                        },
                        "source_used": "tradingeconomics",
                    },
                },
            },
            {"has_data": False},
        )

        self.assertIn("【宏观日历】", prompt)
        self.assertIn("命中来源: tradingeconomics", prompt)
        self.assertIn("中国通胀方面", prompt)
        self.assertIn("优先使用【宏观日历】", prompt)

    def test_build_generation_prompt_adds_section_specific_fundamental_and_observation_candidates(self):
        prompt = build_generation_prompt(
            {"indices": [], "sectors": {}, "breadth": {}},
            {
                "ranked_news": [
                    {
                        "title": "据外媒报道，国际能源署讨论释放石油储备",
                        "content": "",
                    },
                    {
                        "title": "瑞银：更偏好A股而非H股和ADR，交易量高企将支撑中国股票表现",
                        "content": "",
                    },
                    {
                        "title": "某公司控股股东变更并发布业绩预告",
                        "content": "",
                    },
                ],
                "market_news": [],
                "macro_calendar": {"has_data": False},
            },
            {"has_data": False},
        )

        self.assertIn("【第二部分候选新闻补充】", prompt)
        self.assertIn("国际能源方面", prompt)
        self.assertIn("【第四部分候选观点补充】", prompt)
        self.assertIn("更偏好A股而非H股和ADR", prompt)
        section_specific_prompt = prompt.split("【第四部分候选观点补充】", 1)[0]
        self.assertNotIn("控股股东变更并发布业绩预告", section_specific_prompt.split("【第二部分候选新闻补充】", 1)[1])

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

    def test_build_source_numbers_includes_macro_calendar_values(self):
        numbers = build_source_numbers(
            {},
            {},
            {
                "macro_calendar": {
                    "events": [
                        {
                            "actual": "6.65万亿美元",
                            "forecast": "",
                            "previous": "6.63万亿美元",
                            "reference": "3月11日",
                        },
                        {
                            "actual": "220K",
                            "forecast": "225K",
                            "previous": "221K",
                            "reference": "Mar/08",
                        },
                    ]
                }
            },
        )

        self.assertIn(6.65, numbers)
        self.assertIn(6.63, numbers)
        self.assertIn(220.0, numbers)
        self.assertIn(225.0, numbers)

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

    def test_clean_report_style_normalizes_titles_and_removes_filler_phrases(self):
        report = """一、A股收评（市场表现）
整体来看，市场震荡整理。

四、总结与展望
短期展望方面，投资者需关注政策节奏，有望改善风险偏好。"""

        cleaned = clean_report_style(report)

        self.assertIn("一、市场表现", cleaned)
        self.assertIn("四、市场观察摘要", cleaned)
        self.assertIn("市场震荡整理。", cleaned)
        for phrase in ("整体来看", "总结与展望", "短期展望", "投资者需关注", "有望"):
            self.assertNotIn(phrase, cleaned)

    @patch("src.checker.fact_check.verify_claims_with_llm")
    def test_run_post_generation_checks_flags_missing_bullets_explanatory_terms_and_global_summary(self, mock_verify):
        mock_verify.return_value = {
            "verified": True,
            "issues": [],
            "summary": "",
            "verifier_response": "",
        }

        report = """一、市场表现
指数震荡整理。
二、基本面分析
国内方面：
中国通胀数据显示出需求改善。
国际方面：
美国就业与通胀说明海外压力仍在。
三、央行动态
央行开展7天逆回购操作。
四、市场观察摘要
欧洲央行与英国央行利率路径仍是全球市场关注重点。"""

        result = run_post_generation_checks(
            report_text=report,
            market_data={},
            news_data={},
            pboc_data={"omo": {"has_data": False}},
            config={"validation": {"number_verification_rate": 0}},
        )

        self.assertFalse(result["passed"])
        issue_explanations = [issue["explanation"] for issue in result["claim_check"]["issues"]]
        self.assertTrue(any("国内方面必须使用 i. ii. iii." in text for text in issue_explanations))
        self.assertTrue(any("国际方面必须使用 i. ii. iii." in text for text in issue_explanations))
        self.assertTrue(any("解释性表达" in text for text in issue_explanations))
        self.assertTrue(any("A股或中国股票锚点" in text for text in issue_explanations))


if __name__ == "__main__":
    unittest.main()
