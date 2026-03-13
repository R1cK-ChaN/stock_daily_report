"""
Report generator using OpenRouter chat completions.

Assembles structured prompts from fetched data and generates the 4-section
daily market report. All LLM outputs are constrained by the provided data
to minimize hallucination.
"""

import logging
import os
import re
from datetime import datetime

import openai

logger = logging.getLogger(__name__)


REPORT_SECTION_TITLES = [
    "一、市场表现",
    "二、基本面分析",
    "三、央行动态",
    "四、市场观察摘要",
]

STYLE_BANNED_PHRASES = (
    "整体来看",
    "值得关注",
    "需要注意",
    "有望",
    "或将",
    "投资者需关注",
    "投资者需要关注",
    "短期内",
    "中长期看",
    "总结与展望",
    "短期展望",
)
STYLE_BANNED_PHRASES_TEXT = "、".join(STYLE_BANNED_PHRASES)

SECTION_TITLE_PATTERNS = (
    (r"^\s*一、A股收评(?:（市场表现）)?\s*$", "一、市场表现"),
    (r"^\s*一、市场表现\s*$", "一、市场表现"),
    (r"^\s*二、基本面分析(?:（重要新闻与经济数据）)?\s*$", "二、基本面分析"),
    (r"^\s*三、央行逆回购(?:（公开市场操作）)?\s*$", "三、央行动态"),
    (r"^\s*三、央行公开市场操作\s*$", "三、央行动态"),
    (r"^\s*四、总结与展望\s*$", "四、市场观察摘要"),
    (r"^\s*四、市场观察摘要\s*$", "四、市场观察摘要"),
)

LINE_PREFIX_PATTERNS = (
    r"^(整体来看|总体来看|总体而言|整体上看)[，,：:]?",
    r"^(值得关注的是|需要注意的是|需要注意|值得关注)[，,：:]?",
    r"^(短期展望方面|短期展望|展望短期市场|短期来看|后续来看)[，,：:]?",
)

LINE_SUBSTRING_REPLACEMENTS = (
    ("投资者需关注", "关注"),
    ("投资者需要关注", "关注"),
    ("有望", ""),
    ("或将", ""),
    ("短期内", ""),
    ("中长期看", ""),
)

LOW_VALUE_NEWS_TERMS = (
    "控股股东", "股东变更", "扭亏", "净利润", "减持", "增持",
    "回购", "签约", "中标", "业绩预告", "业绩快报", "分红",
)

FUNDAMENTAL_CORE_DATA_KEYWORDS = {
    ("domestic", "中国消费情绪方面"): ("中国", "消费情绪", "消费者信心", "consumer sentiment", "consumer confidence", "pcsi", "ipsos"),
    ("domestic", "中国通胀方面"): ("中国", "cpi", "ppi", "inflation", "通胀"),
    ("domestic", "中国货币方面"): ("中国", "m2", "m1", "货币供应", "money supply"),
    ("domestic", "中国信用与流动性方面"): ("中国", "社融", "社会融资", "aggregate financing", "social financing", "贷款", "信贷"),
    ("domestic", "中国贸易方面"): ("中国", "贸易", "贸易帐", "进出口", "出口", "进口", "trade balance", "exports", "imports"),
    ("domestic", "中国景气方面"): ("中国", "pmi", "gdp", "零售销售", "工业增加值", "固定资产投资", "retail sales", "industrial production"),
    ("international", "美国资产负债方面"): ("美国", "美联储资产负债表", "fed balance sheet", "quantitative tightening", "qt"),
    ("international", "美国房地产方面"): ("美国", "营建许可", "新屋开工", "住房", "房地产", "building permits", "housing starts", "existing home sales", "new home sales"),
    ("international", "美国贸易方面"): ("美国", "贸易", "贸易帐", "出口", "进口", "trade balance", "exports", "imports"),
    ("international", "美国劳动力市场方面"): ("美国", "初请失业金", "续请失业金", "非农", "失业率", "initial jobless claims", "continuing jobless claims", "nonfarm", "unemployment"),
    ("international", "美国通胀与消费方面"): ("美国", "cpi", "ppi", "通胀", "零售销售", "消费情绪", "消费者信心", "consumer sentiment", "consumer confidence", "retail sales"),
    ("international", "欧元区宏观方面"): ("欧元区", "euro area", "eurozone", "cpi", "ppi", "pmi", "gdp", "零售销售", "工业生产", "industrial production"),
}

FUNDAMENTAL_MAJOR_EVENT_GROUPS = {
    ("domestic", "国内宏观事件方面"): ("中国", "政策", "财政政策", "货币政策", "监管"),
    ("international", "国际能源方面"): ("油价", "原油", "石油", "能源", "iea", "国际能源署", "opec"),
    ("international", "国际地缘事件方面"): ("地缘政治", "霍尔木兹", "制裁", "贸易调查", "贸易冲突", "关税"),
    ("international", "国际宏观事件方面"): ("央行政策", "美联储", "欧洲央行", "英国央行", "利率", "global central bank"),
}

OBSERVATION_A_SHARE_KEYWORDS = (
    "a股", "中国股票", "中国股市", "沪深", "沪深市场", "沪深两市",
    "中国资产", "中国策略", "港股", "h股", "h shares", "h-share",
    "adr", "美国存托凭证", "a-share", "a shares", "ashare", "ashares",
    "china equities", "china equity", "china stocks",
)

OBSERVATION_INSTITUTION_KEYWORDS = (
    "机构观点", "券商", "外资", "投行", "分析师", "首席", "策略师",
    "高盛", "摩根士丹利", "摩根大通", "瑞银", "花旗", "美银",
    "凯投宏观", "中金", "中信证券", "中信建投", "华泰证券", "申万宏源",
    "国泰海通", "广发证券", "招商证券", "兴业证券", "天风证券",
)


def _contains_any(text: str, keywords: tuple[str, ...] | list[str]) -> bool:
    lowered = text.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _get_news_display_text(item: dict) -> str:
    content = item.get("content", "")
    title = item.get("title", "")
    return content if len(content) > len(title) else title


def _iter_ranked_or_raw_news(news_data: dict) -> list[dict]:
    ranked = news_data.get("ranked_news", [])
    return ranked if ranked else news_data.get("market_news", [])


def _classify_fundamental_news_group(text: str) -> tuple[str, str] | None:
    lowered = text.lower()
    for group_key, keywords in FUNDAMENTAL_CORE_DATA_KEYWORDS.items():
        if keywords[0].lower() in lowered and _contains_any(lowered, keywords[1:]):
            return group_key
    for group_key, keywords in FUNDAMENTAL_MAJOR_EVENT_GROUPS.items():
        if _contains_any(lowered, keywords):
            return group_key
    return None


def _collect_fundamental_news_candidates(news_data: dict) -> dict[str, dict[str, list[str]]]:
    grouped = {"domestic": {}, "international": {}}

    for item in _iter_ranked_or_raw_news(news_data):
        display_text = _get_news_display_text(item).strip()
        if not display_text:
            continue
        if _contains_any(display_text, LOW_VALUE_NEWS_TERMS):
            continue
        group = _classify_fundamental_news_group(display_text)
        if not group:
            continue

        side, category = group
        side_bucket = grouped[side].setdefault(category, [])
        if display_text in side_bucket or len(side_bucket) >= 2:
            continue
        side_bucket.append(display_text)

    return grouped


def _format_fundamental_news_candidates(news_data: dict) -> str:
    grouped = _collect_fundamental_news_candidates(news_data)
    lines = ["【第二部分候选新闻补充】"]

    domestic = grouped.get("domestic", {})
    if domestic:
        lines.append("国内方面：")
        for category, items in domestic.items():
            lines.append(f"- {category}")
            for item in items:
                lines.append(f"  - {item}")

    international = grouped.get("international", {})
    if international:
        lines.append("国际方面：")
        for category, items in international.items():
            lines.append(f"- {category}")
            for item in items:
                lines.append(f"  - {item}")

    if len(lines) == 1:
        lines.append("（未命中可补充的重大宏观/市场热点新闻）")

    return "\n".join(lines)


def _format_observation_candidates(news_data: dict) -> str:
    lines = ["【第四部分候选观点补充】"]
    count = 0

    for item in _iter_ranked_or_raw_news(news_data):
        display_text = _get_news_display_text(item).strip()
        if not display_text:
            continue
        if not _contains_any(display_text, OBSERVATION_INSTITUTION_KEYWORDS):
            continue
        if not _contains_any(display_text, OBSERVATION_A_SHARE_KEYWORDS):
            continue
        lines.append(f"- {display_text}")
        count += 1
        if count >= 3:
            break

    if count == 0:
        lines.append("（未命中直接A股/中国股票机构观点。第四部分只能对前三部分已确认内容做1-2句A股归纳，不得展开全球评论。）")

    return "\n".join(lines)


SYSTEM_PROMPT = f"""你是一位A股卖方策略研究员，负责撰写收盘后的研究型日报，不是财经新闻播报员。

写作目标：
- 信息选择以宏观、政策、流动性、机构观点为主线，不追求新闻覆盖率
- 语言像人工研究员的盘后纪要，克制、简洁、少套话、少预测
- 只保留对A股定价、风险偏好和资金面判断有帮助的信息

关键规则：
1. 你只能使用提供给你的数据来撰写报告，不得编造任何数字或新闻
2. 所有数字必须与提供的数据完全一致
3. 如果某项数据缺失，直接跳过该内容，不要提及"数据暂缺"，也不要猜测
4. 使用研究员口径的专业、简洁财经语言，不要写成新闻串讲
5. 报告使用中文撰写
6. 每个观点都必须有数据支撑
7. 即使新闻标题中提到了某些数字（如涨停数、连板数），除非这些数字也出现在对应的结构化数据字段中，否则不得将其作为精确统计数据引用。新闻中的数字只能作为新闻引用，必须注明来源。
8. 禁止使用“接近八成/超过九成/不足五成”等模糊比例口径，若要描述占比必须给出可由结构化数据直接计算的明确百分比。
9. 对于市场广度占比，只能原样引用结构化数据里明确给出的百分比，禁止自行心算、估算或改写。
10. 若未提供上一交易日、历史对比或环比字段，禁止写“较上一交易日放量/缩量”“连续X日”“刷新纪录”等比较性表述。
11. 央行公开市场操作中的 total_amount 仅表示当日操作总量/中标总量，禁止改写为“净投放”“净回笼”或“投放资金”。
12. 报告必须固定为四节：一、市场表现；二、基本面分析；三、央行动态；四、市场观察摘要。
13. 第二节必须按“国内方面：”“国际方面：”组织，且两个小标题下都必须使用 i. ii. iii. 分点；国内优先中国宏观、政策、消费和产业景气，国际优先美国与欧元区宏观、美联储资产负债表、房地产、贸易、就业及重大能源/地缘事件。
14. 普通公司业绩、股东变更、弱相关公告默认降权，只有当宏观与政策信息明显不足时才可补充，且最多一条。
15. 第四节优先引用直接谈A股、中国股票、H股、ADR或中国资产的机构、券商、外资、投行观点；若没有可靠观点源，只允许用1-2句已验证的A股归纳补位，不得写展望、预测或投资建议。
16. 禁止使用这些表达：{STYLE_BANNED_PHRASES_TEXT}。"""


def _fmt_amount(yuan: float) -> str:
    """Format an amount in yuan to 亿元 or 万亿元."""
    yi = yuan / 1e8
    if yi >= 10000:
        return f"{yuan / 1e12:.2f}万亿元"
    return f"{yi:.2f}亿元"


def _format_index_data(indices: list[dict]) -> str:
    """Format index data into a readable string for the prompt."""
    if not indices:
        return "（指数数据暂缺）"

    lines = []
    for idx in indices:
        direction = "↑" if idx["change_pct"] > 0 else "↓" if idx["change_pct"] < 0 else "→"
        lines.append(
            f"- {idx['name']}: {idx['close']:.2f} {direction} "
            f"{idx['change_pct']:+.2f}% (涨跌额: {idx['change']:+.2f}, "
            f"成交额: {_fmt_amount(idx['amount'])})"
        )
    return "\n".join(lines)


def _format_sector_data(sectors: dict) -> str:
    """Format sector performance data."""
    if not sectors.get("gainers") and not sectors.get("losers"):
        return "（板块表现数据暂缺，请勿编造板块涨跌信息）"

    lines = ["【领涨板块】"]
    for s in sectors.get("gainers", [])[:3]:
        lines.append(
            f"- {s['name']}: {s['change_pct']:+.2f}% "
            f"(领涨股: {s['leader_stock']} {s['leader_change_pct']:+.2f}%)"
        )

    lines.append("\n【领跌板块】")
    for s in sectors.get("losers", [])[:3]:
        lines.append(
            f"- {s['name']}: {s['change_pct']:+.2f}% "
            f"(领涨股: {s['leader_stock']} {s['leader_change_pct']:+.2f}%)"
        )
    return "\n".join(lines)


def _format_breadth_data(breadth: dict) -> str:
    """Format market breadth data."""
    if not breadth:
        return (
            "（市场广度数据暂缺。严禁从新闻标题推测涨跌家数、涨停跌停数量或连板数据。"
            '如需提及市场广度，请写\u201c数据暂缺\u201d。）'
        )
    total_amount = breadth.get("total_amount")
    amount_text = _fmt_amount(float(total_amount)) if total_amount is not None else "N/A"
    ratio_parts = []
    for label, key in (
        ("上涨占比", "up_ratio_pct"),
        ("下跌占比", "down_ratio_pct"),
        ("平盘占比", "flat_ratio_pct"),
    ):
        value = breadth.get(key)
        if value is None:
            continue
        ratio_parts.append(f"{label}: {float(value):.2f}%")

    ratio_line = "\n" + "，".join(ratio_parts) if ratio_parts else ""
    return (
        f"两市总成交额（唯一可用的全市场成交额）: {amount_text}\n"
        f"市场广度: 上涨{breadth.get('up_count', 'N/A')}家, 下跌{breadth.get('down_count', 'N/A')}家, "
        f"平盘{breadth.get('flat_count', 'N/A')}家\n"
        f"辅助信息: 涨停{breadth.get('limit_up', 'N/A')}家, 跌停{breadth.get('limit_down', 'N/A')}家"
        f"{ratio_line}\n"
        "约束: 若描述市场广度占比，只能直接引用以上百分比，禁止自行换算。"
    )


def _format_news_data(news_data: dict) -> str:
    """Format pre-ranked news data for the prompt."""
    lines = []

    # Use pre-ranked news if available (from news_ranker)
    ranked = news_data.get("ranked_news", [])
    if ranked:
        lines.append("【以下为金十快讯全局参考，已按宏观优先、机构观点优先、公司新闻降权的口径预排序】")
        lines.append("写作要求：第二部分优先使用宏观、政策、消费与产业景气、美国/欧元区宏观与美联储线索；第四部分优先提炼直接谈A股/中国股票的机构观点。")
        lines.append("低优先级：普通公司业绩、股东变更、弱相关公告，仅当宏观素材明显不足时才可补一条。")
        for i, item in enumerate(ranked, 1):
            reason = item.get("llm_reason", "")
            reason_str = f" | 理由: {reason}" if reason else ""
            display_text = _get_news_display_text(item)
            lines.append(
                f"[重要性: {i}{reason_str}]\n"
                f"  {display_text}"
            )
    else:
        # Fallback: use raw market_news if ranking wasn't run
        lines.append("【以下为金十快讯，请直接用中文概述要点】")
        for item in news_data.get("market_news", [])[:10]:
            lines.append(f"- {_get_news_display_text(item)}")

    return "\n".join(lines) if lines else "（新闻数据暂缺）"


def _format_macro_calendar_data(news_data: dict) -> str:
    """Format grouped macro calendar data for the prompt."""
    macro_calendar = news_data.get("macro_calendar", {})
    if not macro_calendar.get("has_data"):
        return "【宏观日历】\n（未命中可用宏观日历事件，第二部分回退至金十快讯）"

    grouped = macro_calendar.get("grouped", {})
    lines = [
        "【宏观日历】",
        (
            f"命中来源: {macro_calendar.get('source_used', '') or 'unknown'}"
            f" | 事件数: {len(macro_calendar.get('events', []))}"
        ),
    ]

    domestic = grouped.get("domestic", [])
    if domestic:
        lines.append("国内方面：")
        for block in domestic:
            category = block.get("category", "")
            items = block.get("items", [])
            if not items:
                continue
            lines.append(f"- {category}")
            for item in items[:2]:
                lines.append(f"  - {item.get('summary', '')}")

    international = grouped.get("international", {})
    if international:
        lines.append("国际方面：")
        for category, items in international.items():
            if not items:
                continue
            lines.append(f"- {category}")
            for item in items[:2]:
                lines.append(f"  - {item.get('summary', '')}")

    if macro_calendar.get("fallback_reason"):
        lines.append(f"备注: fallback={macro_calendar['fallback_reason']}")

    return "\n".join(lines)


def _format_pboc_data(pboc_data: dict) -> str:
    """Format PBOC monetary data for the prompt."""
    if not pboc_data.get("has_data"):
        return "（央行货币市场数据暂缺）"

    lines = [f"数据日期: {pboc_data['date']}"]

    # OMO (Open Market Operations)
    omo = pboc_data.get("omo", {})
    if omo.get("has_data"):
        lines.append(f"\n【公开市场操作公告】")
        lines.append(f"公告: {omo.get('title', '')}")
        lines.append(f"操作类型: {omo.get('op_type', '未知')}")
        for op in omo.get("operations", []):
            parts = [f"期限: {op['tenor']}", f"利率: {op['rate']:.2f}%"]
            if op.get("bid_amount") is not None:
                parts.append(f"投放量: {op['bid_amount']:.1f}亿元")
            if op.get("win_amount") is not None:
                parts.append(f"中标量: {op['win_amount']:.1f}亿元")
            lines.append(f"- {', '.join(parts)}")
        lines.append(f"操作总量: {omo.get('total_amount', 0):.1f}亿元")
        if (
            omo.get("maturity_amount") is None
            and omo.get("net_injection") is None
            and omo.get("net_amount") is None
        ):
            lines.append("约束: 未提供到期与净投放/净回笼数据，禁止补充此类数字，也禁止把操作总量改写为投放资金")
        lines.append(f"来源: {omo.get('url', '')}")
    else:
        lines.append("\n【公开市场操作公告】今日无公开市场操作公告")

    # Repo rates
    repo = pboc_data.get("repo_rates", {})
    if repo.get("has_data"):
        lines.append(f"\n【银行间回购利率】(截至 {repo['latest_date']})")
        lines.append(
            f"- FR001 (隔夜): {repo['FR001']:.2f}% | "
            f"FR007 (7天): {repo['FR007']:.2f}% | "
            f"FR014 (14天): {repo['FR014']:.2f}%"
        )

    # SHIBOR
    shibor = pboc_data.get("shibor", {})
    if shibor.get("has_data"):
        lines.append(f"\n【SHIBOR利率】(截至 {shibor['latest_date']})")
        lines.append(
            f"- 隔夜: {shibor['overnight']:.3f}% | 1周: {shibor['1W']:.3f}% | "
            f"1月: {shibor['1M']:.3f}% | 3月: {shibor['3M']:.3f}%"
        )

    # LPR
    lpr = pboc_data.get("lpr", {})
    if lpr.get("has_data"):
        lines.append(f"\n【LPR贷款市场报价利率】(截至 {lpr['latest_date']})")
        lines.append(f"- 1年期LPR: {lpr['LPR_1Y']:.2f}% | 5年期以上LPR: {lpr['LPR_5Y']:.2f}%")

    return "\n".join(lines)


def clean_report_style(report_text: str) -> str:
    """Light-touch post processing to normalize headings and remove AI filler."""
    text = report_text
    for pattern, replacement in SECTION_TITLE_PATTERNS:
        text = re.sub(pattern, replacement, text, flags=re.MULTILINE)

    cleaned_lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if cleaned_lines and cleaned_lines[-1] != "":
                cleaned_lines.append("")
            continue

        if stripped in REPORT_SECTION_TITLES or stripped in {"国内方面：", "国际方面："}:
            cleaned_lines.append(stripped)
            continue

        for pattern in LINE_PREFIX_PATTERNS:
            line = re.sub(pattern, "", line)
        for old, new in LINE_SUBSTRING_REPLACEMENTS:
            line = line.replace(old, new)

        line = re.sub(r"[，,]{2,}", "，", line)
        line = re.sub(r"^[，,：:；;\s]+", "", line)
        line = re.sub(r"[，,：:；;]\s*([。！？])", r"\1", line)
        line = re.sub(r"\s+", " ", line).strip()
        if not line:
            continue
        cleaned_lines.append(line)

    return "\n".join(cleaned_lines).strip()


def _build_missing_data_warnings(market_data: dict, news_data: dict, pboc_data: dict) -> str:
    """Scan all data dicts and build a consolidated warning block for missing fields."""
    warnings = []

    if not market_data.get("indices"):
        warnings.append("- 主要指数数据缺失：跳过指数相关内容，勿编造")
    if not market_data.get("breadth"):
        warnings.append("- 市场广度数据缺失：跳过涨跌家数、涨停跌停相关内容，勿编造")
    sectors = market_data.get("sectors", {})
    if not sectors.get("gainers") and not sectors.get("losers"):
        warnings.append("- 板块表现数据缺失：跳过板块涨跌相关内容，勿编造")
    if (
        not news_data.get("market_news")
        and not news_data.get("ranked_news")
        and not news_data.get("macro_calendar", {}).get("has_data")
    ):
        warnings.append("- 基本面素材缺失：跳过第二部分相关内容，勿编造")
    if not pboc_data.get("has_data"):
        warnings.append("- 央行公开市场操作数据缺失：跳过逆回购相关内容，勿编造")
    elif not pboc_data.get("omo", {}).get("has_data"):
        warnings.append("- 今日无央行公开市场操作公告：可简要注明今日无操作公告，勿编造操作金额")

    if not warnings:
        return ""

    header = '⚠️ 以下数据字段缺失，对应内容严禁编造，直接跳过即可，不要在报告中提及"数据暂缺"：\n'
    return header + "\n".join(warnings) + "\n\n"


def build_generation_prompt(market_data: dict, news_data: dict, pboc_data: dict) -> str:
    """
    Build the full generation prompt with all data attached.

    Returns:
        Formatted prompt string with data sections.
    """
    today_str = datetime.now().strftime("%Y年%m月%d日")

    missing_warnings = _build_missing_data_warnings(market_data, news_data, pboc_data)

    prompt = f"""请根据以下数据撰写{today_str}的A股每日市场报告。报告必须包含以下四个部分。
所有数字和事实必须严格来源于下方提供的数据，不得编造。

===== 原始数据 =====

{missing_warnings}【一、主要指数行情】
{_format_index_data(market_data.get('indices', []))}

【市场广度】
{_format_breadth_data(market_data.get('breadth', {}))}

【板块表现】
{_format_sector_data(market_data.get('sectors', {}))}

【二、今日财经新闻与经济数据】
{_format_macro_calendar_data(news_data)}

【金十快讯全局参考】
{_format_news_data(news_data)}

{_format_fundamental_news_candidates(news_data)}

{_format_observation_candidates(news_data)}

【三、央行公开市场操作】
{_format_pboc_data(pboc_data)}

===== 报告要求 =====

请按以下结构撰写报告：

一、市场表现
- 用研究员盘后纪要口径概述主要指数、两市成交额与2-3个关键板块轮动
- 仅可使用【一、主要指数行情】中的指数成交额和【市场广度】中的两市总成交额；若未提供上一交易日或环比字段，禁止写“较上一交易日放量/缩量”
- 市场广度、涨停跌停只作从属补充，不要把这一节写成市场数据流水账
- 不要堆砌过多市场广度数字，不要逐项播报全部板块
- 字数：220-360字

二、基本面分析
- 必须严格输出如下结构：先写“国内方面：”，再写“国际方面：”，两个小标题都必须保留
- “国内方面：”和“国际方面：”下都必须使用 i. ii. iii. 这样的分点编号；即使只有一条，也必须写成 i.
- 优先使用【宏观日历】中的国内/国际分组，再视素材缺口补充【第二部分候选新闻补充】
- 国内方面优先：中国宏观变量、政策变化、消费/产业规模/景气数据、国内资本流动与监管变化
- 国际方面优先：美国宏观数据、美联储资产负债表、美国房地产、贸易、就业、欧元区核心宏观数据，以及重大能源/地缘/贸易冲突事件新闻
- 若同时存在多条国际宏观信息，国际方面优先解读美国宏观指标及美联储政策信号，重点关注美国CPI、PPI、非农、失业率、零售、贸易、住房与Fed政策信号
- 若【宏观日历】已提供中国/美国/欧元区高价值宏观指标，必须优先写入，不要再用普通公司新闻、个股业绩、股东变更等内容填充第二部分
- 普通公司业绩、股东变更、弱相关公告默认降权，仅在宏观素材不足时最多补1条，且不要占主要篇幅
- 宏观数据优先采用“今值 / 前值 / 预测值”播报句式，不做市场影响分析，不做总结性评论
- 若使用重大宏观新闻/市场热点事件补位，可用新闻播报句式，但不得写影响解读
- 禁止使用“显示出”“反映出”“说明”“支撑”“压制”“提振”“打压”等解释性表达
- 直接陈述已给出的事实，不要提及新闻来源名称，不要补充未出现的具体数字或细节
- 分析以宏观框架为主，不要写成新闻并列罗列，更不要把国内和国际分别写成大段落
- 字数：220-360字

三、央行动态
- 简明写今日操作情况（中标总量/操作总量、期限、利率）
- 到期与净投放/回笼情况仅当【三】数据明确提供该字段时才可写具体数字；否则跳过
- 仅基于回购利率、SHIBOR、LPR与公开市场操作数据进行资金面解读，不要扩展成资金利率流水账
- 字数：120-220字

四、市场观察摘要
- 优先引用【第四部分候选观点补充】中的机构、券商、外资、投行等观点，且观点必须直接围绕A股、中国股票、H股、ADR或中国资产
- 若没有可靠观点源，只允许用1-2句已验证的A股归纳补位，不得泛化为全球经济评论
- 英国央行、欧洲央行、全球央行、全球利率等评论若不直接涉及A股或中国股票，不要写入第四部分
- 不要写“总结与展望”，不要写预测句、投资建议或评论员腔
- 字数：80-180字

注意：
- 所有数字必须与提供的数据一致
- 如果某项数据缺失或不足以支撑分析，直接跳过，不需要提及缺失
- 禁止使用“创出新高/新低、历史新高/新低、阶段性新高/新低”等需要历史序列支撑的表述
- 禁止使用“接近X成/超过X成/不足X成”等模糊比例描述；若描述占比，必须给出明确百分比
- 若提及市场上涨/下跌/平盘占比，只能直接引用【市场广度】中明确给出的百分比，禁止根据家数自行估算
- 若未提供上一交易日、环比或历史序列字段，禁止出现“较上一交易日放量/缩量”“连续X日”“创纪录”等比较性表述
- 央行公开市场操作的 `操作总量` 不是 `净投放/净回笼`，也不要改写成“投放资金”
- 禁止使用这些表达：{STYLE_BANNED_PHRASES_TEXT}
- 使用专业财经术语
- 语气客观中立
- 输出纯文本，不要使用任何Markdown格式符号（如##、**、*、-等），只用普通文字和换行

严格数据分区（极其重要）：
- 第一部分"市场表现"只能使用【一、主要指数行情】【市场广度】【板块表现】的结构化数据，禁止引用金十快讯中的任何内容
- 第一部分禁止引用新闻中的“较上一交易日缩量/放量”“南向/北向资金”“刷新纪录”等内容
- 第二部分"基本面分析"只能使用【二、今日财经新闻与经济数据】中的【宏观日历】和【第二部分候选新闻补充】内容；若【宏观日历】有可用宏观指标，优先使用【宏观日历】中的事实和数字
- 第三部分"央行动态"只能使用【三、央行公开市场操作】的结构化数据，禁止从金十快讯中提取央行操作信息
- 若【三】未提供到期或净投放字段，禁止出现“到期X亿元/净投放(回笼)X亿元”等具体数字
- 第三部分若引用 `操作总量`，必须表述为“操作总量”或“中标总量”，禁止改写成“投放资金”
- 第三部分禁止引用“两会表态、降准降息预期、稳增长政策”等新闻口径内容
- 第四部分"市场观察摘要"优先引用【第四部分候选观点补充】中的A股/中国股票相关机构观点；若无可靠观点，只能对前三部分已确认内容做1-2句A股归纳，不得延伸预测"""

    return prompt


def generate_report(
    market_data: dict,
    news_data: dict,
    pboc_data: dict,
    config: dict,
    regeneration_hints: list[str] | None = None,
    temperature_override: float | None = None,
) -> dict:
    """
    Generate the daily market report using OpenRouter chat completions.

    Args:
        market_data: Output from market_data.fetch_all_market_data()
        news_data: Output from news.fetch_all_news()
        pboc_data: Output from pboc.fetch_pboc_data()
        config: Settings dict
        regeneration_hints: If provided, specific fact-check failures to fix
        temperature_override: Optional runtime override for sampling temperature

    Returns:
        Dict with 'report_text', 'model', 'usage', 'prompt_data'
    """
    llm_cfg = config.get("llm", config.get("claude", {}))
    model = llm_cfg.get("model", "anthropic/claude-sonnet-4-20250514")
    max_tokens = llm_cfg.get("max_tokens", 4096)
    temperature = (
        temperature_override
        if temperature_override is not None
        else llm_cfg.get("temperature", 0.3)
    )
    base_url = llm_cfg.get("base_url", "https://openrouter.ai/api/v1")
    api_key = os.environ.get("OPENROUTER_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))

    prompt = build_generation_prompt(market_data, news_data, pboc_data)

    if regeneration_hints:
        hint_lines = "\n".join(f"- {h}" for h in regeneration_hints)
        prompt += (
            f"\n\n===== 重要：上一次生成的报告被事实核查驳回 =====\n"
            f"请特别注意修正以下问题：\n{hint_lines}\n"
            f"严格使用上方提供的结构化数据，不得从新闻标题推测数字。"
        )

    logger.info(
        "Generating report with model=%s, max_tokens=%d, temperature=%s",
        model,
        max_tokens,
        temperature,
    )

    client = openai.OpenAI(base_url=base_url, api_key=api_key)

    response = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )

    report_text = response.choices[0].message.content
    # Strip any markdown formatting the LLM may have included
    report_text = re.sub(r'^#{1,6}\s*', '', report_text, flags=re.MULTILINE)  # ## headers
    report_text = re.sub(r'\*\*(.+?)\*\*', r'\1', report_text)               # **bold**
    report_text = re.sub(r'\*(.+?)\*', r'\1', report_text)                    # *italic*
    report_text = clean_report_style(report_text)
    usage = response.usage

    result = {
        "report_text": report_text,
        "model": model,
        "usage": {
            "input_tokens": usage.prompt_tokens if usage else 0,
            "output_tokens": usage.completion_tokens if usage else 0,
        },
        "prompt_data": {
            "market_data_summary": {
                "num_indices": len(market_data.get("indices", [])),
                "has_sectors": bool(market_data.get("sectors")),
                "has_breadth": bool(market_data.get("breadth")),
            },
            "news_data_summary": {
                "num_market_news": len(news_data.get("market_news", [])),
                "num_ranked": len(news_data.get("ranked_news", [])),
            },
            "pboc_has_data": pboc_data.get("has_data", False),
        },
        "generated_at": datetime.now().isoformat(),
    }

    logger.info(
        "Report generated: %d chars, %d input tokens, %d output tokens",
        len(report_text),
        result["usage"]["input_tokens"],
        result["usage"]["output_tokens"],
    )
    return result
