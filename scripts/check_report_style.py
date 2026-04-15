#!/usr/bin/env python3
"""Lightweight style checks for the generated daily report."""

from __future__ import annotations

import re
import sys
from pathlib import Path

TARGET_SECTIONS = [
    "一、市场表现",
    "二、基本面分析",
    "三、央行动态",
    "四、市场观察摘要",
]

BANNED_PHRASES = [
    "总结与展望",
    "短期展望",
    "整体来看",
    "值得关注",
    "需要注意",
    "有望",
    "或将",
    "投资者需关注",
    "投资者需要关注",
    "短期内",
    "中长期看",
]

LOW_VALUE_TERMS = [
    "控股股东",
    "股东变更",
    "扭亏",
    "净利润",
    "减持",
    "增持",
    "回购",
    "签约",
    "中标",
    "业绩预告",
    "业绩快报",
]

PREDICTIVE_TERMS = ["预计", "预期", "有望", "或将", "展望", "投资建议"]
FUNDAMENTAL_EXPLANATORY_TERMS = ["显示出", "反映出", "说明", "支撑", "压制", "提振", "打压"]
OBSERVATION_A_SHARE_TERMS = [
    "A股", "中国股票", "中国股市", "沪深", "沪深市场", "沪深两市",
    "中国资产", "中国策略", "港股", "H股", "ADR", "美国存托凭证",
]
OBSERVATION_GLOBAL_COMMENTARY_TERMS = [
    "英国央行", "英格兰银行", "Bank of England", "BOE",
    "欧洲央行", "ECB", "全球央行", "全球利率", "全球经济", "全球市场",
]


def extract_sections(report_text: str) -> dict[str, str]:
    """Split report text by the expected numbered section headings."""
    matches = list(
        re.finditer(
            r"^(一、市场表现|二、基本面分析|三、央行动态|四、市场观察摘要)\s*$",
            report_text,
            flags=re.MULTILINE,
        )
    )
    sections: dict[str, str] = {}
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(report_text)
        sections[match.group(1)] = report_text[match.end():end].strip()
    return sections


def inspect_report_style(report_text: str) -> dict[str, list[str]]:
    """Return blocking issues and soft warnings for report style."""
    errors: list[str] = []
    warnings: list[str] = []

    headings = re.findall(r"^[一二三四]、.*$", report_text, flags=re.MULTILINE)
    for title in TARGET_SECTIONS:
        if title not in headings:
            errors.append(f"缺少目标章节：{title}")

    if len(headings) != 4:
        errors.append(f"章节数量异常：检测到 {len(headings)} 个一级章节")

    for phrase in BANNED_PHRASES:
        if phrase in report_text:
            errors.append(f"出现禁用表述：{phrase}")

    sections = extract_sections(report_text)
    fundamental = sections.get("二、基本面分析", "")
    if "国内方面：" not in fundamental or "国际方面：" not in fundamental:
        errors.append("基本面分析缺少“国内方面：”或“国际方面：”分组")
    else:
        domestic_match = re.search(r"国内方面：([\s\S]*?)国际方面：", fundamental)
        international_match = re.search(r"国际方面：([\s\S]*)$", fundamental)
        domestic = domestic_match.group(1).strip() if domestic_match else ""
        international = international_match.group(1).strip() if international_match else ""
        if not re.search(r"^\s*i\.\s+", domestic, flags=re.MULTILINE):
            errors.append("基本面分析的“国内方面：”缺少 i. 分点编号")
        if not re.search(r"^\s*i\.\s+", international, flags=re.MULTILINE):
            errors.append("基本面分析的“国际方面：”缺少 i. 分点编号")

    hits = [term for term in FUNDAMENTAL_EXPLANATORY_TERMS if term in fundamental]
    if hits:
        warnings.append(f"基本面分析包含解释性词语：{'、'.join(hits[:4])}")

    low_value_hits = sum(report_text.count(term) for term in LOW_VALUE_TERMS)
    if low_value_hits > 1:
        errors.append(f"低价值公司新闻痕迹偏多：命中 {low_value_hits} 次")
    elif low_value_hits == 1:
        warnings.append("出现 1 次低价值公司新闻词，属于允许的 fallback 上限")

    observation = sections.get("四、市场观察摘要", "")
    if observation:
        if len(observation) > 220:
            errors.append("市场观察摘要过长，超过 220 字符")
        for term in PREDICTIVE_TERMS:
            if term in observation:
                errors.append(f"市场观察摘要包含预测/展望词：{term}")
        has_a_share_anchor = any(term.lower() in observation.lower() for term in OBSERVATION_A_SHARE_TERMS)
        has_global_commentary = any(
            term.lower() in observation.lower() for term in OBSERVATION_GLOBAL_COMMENTARY_TERMS
        )
        if has_global_commentary and not has_a_share_anchor:
            warnings.append("市场观察摘要出现泛全球央行/利率评论，但缺少A股或中国股票锚点")

    return {"errors": errors, "warnings": warnings}


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: check_report_style.py <report_path>")
        return 2

    report_path = Path(argv[1])
    if not report_path.exists():
        print(f"Report not found: {report_path}")
        return 2

    report_text = report_path.read_text(encoding="utf-8")
    result = inspect_report_style(report_text)

    print(f"Style check: {report_path}")
    if result["errors"]:
        print("FAIL")
        for issue in result["errors"]:
            print(f"- {issue}")
    else:
        print("PASS")

    for warning in result["warnings"]:
        print(f"- Warning: {warning}")

    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
