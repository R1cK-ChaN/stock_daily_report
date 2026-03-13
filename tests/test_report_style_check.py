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
    def test_inspect_report_style_flags_missing_international_and_predictive_summary(self):
        report = """一、市场表现
指数震荡，成交额回落。
二、基本面分析
国内方面：财政政策维持积极取向。
三、央行动态
逆回购利率维持不变。
四、市场观察摘要
预计风险偏好有望继续修复。"""

        result = MODULE.inspect_report_style(report)

        self.assertTrue(any("国际方面" in issue for issue in result["errors"]))
        self.assertTrue(any("预测/展望词" in issue for issue in result["errors"]))

    def test_main_returns_zero_for_clean_report(self):
        report = """一、市场表现
主要指数小幅分化，两市成交额维持在结构化数据给出的区间，板块轮动集中在家电与风电。
二、基本面分析
国内方面：财政政策与产业政策继续围绕稳增长和产业升级展开，消费与制造业景气线索仍是当日主轴。
国际方面：美国通胀、就业与房地产数据仍是海外定价核心，美联储资产负债表变化对全球风险偏好形成约束。
三、央行动态
央行当日继续开展7天逆回购操作，FR007、SHIBOR与LPR整体维持平稳。
四、市场观察摘要
机构观点普遍认为，后续仍需围绕政策落地节奏和海外利率环境来判断风险偏好的修复力度。"""

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
