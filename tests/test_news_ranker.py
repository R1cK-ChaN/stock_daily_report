import unittest

from src.fetchers.news_ranker import _compute_keyword_score, keyword_rank


class NewsRankerRegressionTests(unittest.TestCase):
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

    def test_keyword_rank_keeps_company_news_only_as_low_priority_fallback(self):
        items = [
            {
                "title": "美国2月CPI高于预期，美联储资产负债表收缩路径受关注",
                "content": "",
                "publish_time": "",
            },
            {
                "title": "中金：美国就业与房地产数据仍有韧性，A股风险偏好修复仍看政策配合",
                "content": "",
                "publish_time": "",
            },
            {
                "title": "某公司控股股东变更并发布净利润增长公告",
                "content": "",
                "publish_time": "",
            },
        ]

        ranked = keyword_rank(items, top_n=3)
        titles = [item["title"] for item in ranked]

        self.assertEqual(titles[-1], "某公司控股股东变更并发布净利润增长公告")
        self.assertIn("美国2月CPI高于预期，美联储资产负债表收缩路径受关注", titles[:2])


if __name__ == "__main__":
    unittest.main()
