"""
News ranking module — two-stage hybrid ranking for Jin10 Telegram feed.

Stage A: Deterministic keyword scoring (<10ms)
Stage B: LLM re-ranking via Gemini Flash (~700 tokens)

Sits between data fetch and report generation to surface the most
market-relevant headlines and suppress noise from the high-volume
Jin10 feed (~1000 msgs/day).
"""

import json
import logging
import os
import re
from datetime import datetime

import openai

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────
# Keyword Dictionary (5 tiers + noise) — Chinese-first
# ────────────────────────────────────────────────────────────

KEYWORD_TIERS = {
    # Tier 1 (weight 10): Monetary/fiscal policy
    10: [
        # Chinese
        "央行", "降准", "降息", "加息", "货币政策", "逆回购",
        "国务院", "证监会", "银保监", "金融委",
        "两会", "政治局", "国常会", "财政政策",
        "mlf", "slf", "lpr",
        # English (jin10 sometimes mixes)
        "federal reserve", "fed", "fomc", "rate cut", "rate hike",
        "pboc", "ecb", "boj", "central bank", "monetary policy",
        "stimulus",
    ],
    # Tier 2 (weight 8): Economic data
    8: [
        # Chinese
        "非农", "失业率", "通胀", "社融", "社会融资",
        "进出口", "贸易顺差", "贸易逆差", "零售",
        "工业增加值", "固定资产投资",
        # English
        "gdp", "cpi", "ppi", "pmi", "nonfarm", "unemployment",
        "inflation", "retail sales", "m1", "m2",
    ],
    # Tier 3 (weight 6): Market structure / China flows
    6: [
        # Chinese
        "北向资金", "南向资金", "沪深港通", "融资融券",
        "涨停", "跌停", "关税", "制裁", "贸易战",
        "a股", "上证", "深证", "创业板", "科创板",
        "大宗交易", "股票回购", "减持", "增持",
        # English
        "northbound", "stock connect", "qfii", "ipo",
        "etf", "tariff", "sanctions", "trade war",
    ],
    # Tier 4 (weight 4): Hot sectors
    4: [
        # Chinese
        "半导体", "芯片", "人工智能", "新能源", "锂电",
        "光伏", "房地产", "医药", "原油", "黄金",
        "稀土", "军工", "白酒", "算力", "机器人",
        # English
        "semiconductor", "chip", "ai", "ev", "battery",
        "oil", "crude", "gold", "copper",
    ],
    # Tier 5 (weight 3): Bellwether companies
    3: [
        # Chinese
        "比亚迪", "宁德时代", "阿里巴巴", "腾讯", "华为",
        "茅台", "中芯国际", "小米", "字节跳动", "百度",
        # English
        "byd", "catl", "alibaba", "tencent", "huawei",
        "apple", "nvidia", "tsmc", "tesla",
    ],
}

# Noise keywords (penalty -5)
NOISE_KEYWORDS = [
    # Chinese
    "娱乐", "体育", "综艺", "选秀", "广告", "招聘",
    "正在直播", "立即观看", "敬请管理风险",
    "已在金十数据中心更新",
    # English
    "donation", "charity", "sports", "entertainment",
    "advertisement",
]

INSTITUTION_VIEW_KEYWORDS = [
    "机构观点", "券商", "外资", "投行", "分析师", "首席", "策略师",
    "高盛", "摩根士丹利", "摩根大通", "瑞银", "花旗", "美银",
    "凯投宏观", "中金", "中信证券", "中信建投", "华泰证券", "申万宏源",
    "国泰海通", "广发证券", "招商证券", "兴业证券", "天风证券",
]

US_MACRO_PRIORITY_KEYWORDS = [
    "美国cpi", "美国ppi", "美国pmi", "美国非农", "美国失业率", "美国零售销售",
    "美国贸易", "美国房地产", "美国住房", "新屋开工", "成屋销售", "耐用品订单",
    "美联储资产负债表", "fed balance sheet", "quantitative tightening", "qt",
    "cpi", "ppi", "nonfarm", "unemployment", "retail sales", "housing starts",
    "existing home sales", "trade balance", "jobless claims",
]

LOW_VALUE_COMPANY_KEYWORDS = [
    "净利润", "扭亏", "控股股东", "股东变更", "减持", "增持", "回购",
    "公告", "签约", "中标", "预增", "预亏", "业绩快报", "业绩预告",
    "分红", "解禁", "股权激励",
]


def _count_keyword_matches(text: str, keywords: list[str]) -> int:
    return sum(1 for kw in keywords if kw in text)


def _compute_keyword_score(title: str, content: str) -> float:
    """Compute keyword-based relevance score for a single news item."""
    text = (title + " " + content).lower()
    score = 0.0
    tier1_matches = 0

    for weight, keywords in KEYWORD_TIERS.items():
        for kw in keywords:
            if kw in text:
                score += weight
                if weight == 10:
                    tier1_matches += 1

    # Noise penalty
    for kw in NOISE_KEYWORDS:
        if kw in text:
            score -= 5

    institution_matches = _count_keyword_matches(text, INSTITUTION_VIEW_KEYWORDS)
    us_macro_matches = _count_keyword_matches(text, US_MACRO_PRIORITY_KEYWORDS)
    low_value_company_matches = _count_keyword_matches(text, LOW_VALUE_COMPANY_KEYWORDS)

    score += institution_matches * 6
    score += us_macro_matches * 4
    score -= low_value_company_matches * 5

    has_macro_signal = tier1_matches > 0 or _count_keyword_matches(text, KEYWORD_TIERS[8]) > 0
    if low_value_company_matches and not has_macro_signal and institution_matches == 0:
        score *= 0.6

    # Compounding: 2+ tier-1 matches -> x1.5
    if tier1_matches >= 2:
        score *= 1.5

    return max(score, 0)


def _compute_recency_multiplier(publish_time: str) -> float:
    """Compute recency multiplier based on publish time."""
    if not publish_time:
        return 0.4

    now = datetime.now()
    today = now.date()

    try:
        dt = datetime.fromisoformat(publish_time)
        pub_date = dt.date()
        delta = (today - pub_date).days
        if delta <= 0:
            return 1.0
        elif delta == 1:
            return 0.7
        else:
            return 0.4
    except (ValueError, TypeError):
        pass

    return 0.4


def keyword_rank(news_items: list[dict], top_n: int = 15) -> list[dict]:
    """
    Stage A: Rank news items by keyword score.

    Args:
        news_items: List of standardized news dicts.
        top_n: Number of top items to return.

    Returns:
        Top N news items sorted by score, each with 'keyword_score' added.
    """
    scored = []
    for item in news_items:
        base_score = _compute_keyword_score(item.get("title", ""), item.get("content", ""))
        recency_mult = _compute_recency_multiplier(item.get("publish_time", ""))

        final_score = base_score * recency_mult
        scored_item = {**item, "keyword_score": round(final_score, 2)}
        scored.append(scored_item)

    scored.sort(key=lambda x: x["keyword_score"], reverse=True)
    return scored[:top_n]


def llm_rank(
    top_items: list[dict],
    config: dict,
    top_n: int = 5,
) -> list[dict]:
    """
    Stage B: Use LLM to re-rank top keyword-scored headlines.

    Sends title + content snippet for each item since jin10 titles
    are sometimes just numbered list headers.
    Falls back to keyword-only ranking on failure.
    """
    if not top_items:
        return []

    # Build item list with title + content for better LLM judgement
    # Always send full content since jin10 titles can be very short
    # ("1.", "美元1.") while the actual info is in the body
    item_lines = []
    for i, item in enumerate(top_items, 1):
        title = item["title"]
        content = item.get("content", "")
        # Use the longer of title/content to ensure full info is visible
        text = content if len(content) > len(title) else title
        item_lines.append(f"{i}. {text}")

    items_text = "\n".join(item_lines)

    prompt = f"""你是A股市场策略研究员。请从以下金十快讯中选出对A股市场最值得写入日报的{top_n}条，按优先级从高到低排序。

选择标准：
1. 国内宏观政策、经济数据、中国政策动向 > 美国宏观、美联储、美国就业/房地产/贸易 > 行业政策 > 机构/券商/外资/投行观点 > 个股事件
2. 若出现机构、券商、外资、投行观点，优先保留至少1条，供“市场观察摘要”使用
3. 普通公司业绩、股东变更、减持增持、回购、签约、中标等公告只有在宏观素材不足时才补位
4. 优先选择有实质内容的新闻（政策变化、数据发布、重大事件），不要追求覆盖面
5. 过滤掉：日程预告、数据中心更新通知、直播推广、外汇期权到期提示
6. 如果是综合摘要类消息（编号列表），评估其中最重要的单条信息

只返回前{top_n}条，JSON格式：[{{"rank": 1, "id": N, "reason": "10字以内理由"}}, ...]

{items_text}"""

    llm_cfg = config.get("llm", {})
    base_url = llm_cfg.get("base_url", "https://openrouter.ai/api/v1")
    model = llm_cfg.get("model", "google/gemini-3-flash-preview")
    api_key = os.environ.get("OPENROUTER_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
    ranking_cfg = config.get("news", {}).get("ranking", {})
    max_tokens = ranking_cfg.get("llm_max_tokens", 500)

    try:
        client = openai.OpenAI(base_url=base_url, api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            max_tokens=max_tokens,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = response.choices[0].message.content

        # Parse JSON from response
        json_match = re.search(r'\[[\s\S]*?\]', response_text)
        if not json_match:
            logger.warning("LLM ranking: no JSON array found in response, falling back")
            return top_items[:top_n]

        rankings = json.loads(json_match.group())

        # Map LLM rankings back to items
        ranked_result = []
        for entry in rankings[:top_n]:
            idx = entry.get("id", 0) - 1  # 1-indexed to 0-indexed
            if 0 <= idx < len(top_items):
                item = {
                    **top_items[idx],
                    "llm_rank": entry.get("rank", 0),
                    "llm_reason": entry.get("reason", ""),
                }
                ranked_result.append(item)

        if ranked_result:
            logger.info("LLM ranking selected %d items", len(ranked_result))
            return ranked_result

        logger.warning("LLM ranking returned empty results, falling back")
        return top_items[:top_n]

    except Exception as e:
        logger.warning("LLM ranking failed (%s), falling back to keyword-only", e)
        return top_items[:top_n]


def rank_news(news_data: dict, config: dict) -> dict:
    """
    Two-stage news ranking pipeline.

    Args:
        news_data: Output from fetch_telegram_news() with 'market_news'.
        config: Settings dict.

    Returns:
        Modified news_data with added 'ranked_news' key containing top items
        with scores and reasons.
    """
    ranking_cfg = config.get("news", {}).get("ranking", {})
    keyword_top_n = ranking_cfg.get("keyword_top_n", 15)
    llm_top_n = ranking_cfg.get("llm_top_n", 5)
    llm_enabled = ranking_cfg.get("llm_ranking_enabled", True)

    all_news = news_data.get("market_news", [])

    if not all_news:
        logger.warning("No news items to rank")
        news_data["ranked_news"] = []
        news_data["ranking_details"] = {"total_input": 0, "method": "none"}
        return news_data

    # Stage A: Keyword scoring
    logger.info("Stage A: Keyword scoring %d items -> top %d", len(all_news), keyword_top_n)
    keyword_top = keyword_rank(all_news, top_n=keyword_top_n)

    for item in keyword_top[:5]:
        logger.info("  [%.1f] %s", item["keyword_score"], item["title"][:60])

    # Stage B: LLM re-ranking (optional)
    if llm_enabled and keyword_top:
        logger.info("Stage B: LLM re-ranking top %d -> top %d", len(keyword_top), llm_top_n)
        ranked = llm_rank(keyword_top, config, top_n=llm_top_n)
        method = "keyword+llm"
    else:
        ranked = keyword_top[:llm_top_n]
        method = "keyword_only"

    news_data["ranked_news"] = ranked
    news_data["ranking_details"] = {
        "total_input": len(all_news),
        "keyword_top_n": len(keyword_top),
        "final_count": len(ranked),
        "method": method,
        "keyword_scores": [
            {"title": item["title"][:50], "score": item["keyword_score"]}
            for item in keyword_top
        ],
    }

    logger.info(
        "Ranking complete: %d input -> %d keyword -> %d final (%s)",
        len(all_news), len(keyword_top), len(ranked), method,
    )
    return news_data
