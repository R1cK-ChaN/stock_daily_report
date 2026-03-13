import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_report_style.py"
SPEC = importlib.util.spec_from_file_location("check_report_style", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class ReportStyleCheckTests(unittest.TestCase):
    def test_inspect_report_style_flags_missing_bullets_and_predictive_summary(self):
        report = """一、市场表现
指数震荡，成交额回落。
二、基本面分析
国内方面：
i. 财政政策维持积极取向。
国际方面：
美国就业数据说明海外压力仍在。
三、央行动态
逆回购利率维持不变。
四、市场观察摘要
预计风险偏好有望继续修复。"""

        result = MODULE.inspect_report_style(report)

        self.assertTrue(any("国际方面" in issue and "i." in issue for issue in result["errors"]))
        self.assertTrue(any("预测/展望词" in issue for issue in result["errors"]))
        self.assertTrue(any("解释性词语" in warning for warning in result["warnings"]))

    def test_inspect_report_style_warns_on_global_summary_without_a_share_anchor(self):
        report = """一、市场表现
指数震荡，成交额回落。
二、基本面分析
国内方面：
i. 中国CPI今值为0.7%，前值为0.5%。
国际方面：
i. 美国初请失业金人数今值为22万，前值为22.1万。
三、央行动态
逆回购利率维持不变。
四、市场观察摘要
欧洲央行与英国央行利率路径仍是全球市场关注重点。"""

        result = MODULE.inspect_report_style(report)

        self.assertTrue(any("A股或中国股票锚点" in warning for warning in result["warnings"]))

    def test_main_returns_zero_for_clean_report(self):
        report = """一、市场表现
主要指数小幅分化，两市成交额维持在结构化数据给出的区间，板块轮动集中在家电与风电。
二、基本面分析
国内方面：
i. 中国财政政策与产业政策继续围绕稳增长和产业升级展开，消费与制造业景气数据是当日主轴。
国际方面：
i. 美国通胀、就业与房地产数据仍是海外定价核心，美联储资产负债表今值为6.65万亿美元，前值为6.63万亿美元。
三、央行动态
央行当日继续开展7天逆回购操作，FR007、SHIBOR与LPR整体维持平稳。
四、市场观察摘要
机构观点普遍认为，A股与中国股票仍需围绕政策落地节奏和交易活跃度来判断风险偏好的修复力度。"""

        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as fh:
            fh.write(report)
            report_path = Path(fh.name)

        try:
            exit_code = MODULE.main(["check_report_style.py", str(report_path)])
        finally:
            report_path.unlink(missing_ok=True)

        self.assertEqual(exit_code, 0)


if __name__ == "__main__":
    unittest.main()
