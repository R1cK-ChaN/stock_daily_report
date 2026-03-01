"""
News ranking module — two-stage hybrid ranking.

Stage A: Deterministic keyword scoring (<10ms)
Stage B: LLM pre-ranking via Gemini Flash (~700 tokens)

Sits between data fetch and report generation to surface the most
market-relevant headlines and suppress noise.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any

import openai

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────
# Keyword Dictionary (5 tiers + noise)
# ────────────────────────────────────────────────────────────

KEYWORD_TIERS = {
    # Tier 1 (weight 10): Monetary/fiscal policy
    10: [
        "央行", "降准", "降息", "加息", "LPR", "MLF", "逆回购", "SLF", "PSL",
        "国务院", "证监会", "银保监", "金融委", "财政部", "发改委",
        "货币政策", "财政政策", "公开市场", "再贷款", "存款准备金",
    ],
    # Tier 2 (weight 8): Economic data
    8: [
        "GDP", "CPI", "PPI", "PMI", "社融", "M1", "M2", "信贷",
        "进出口", "贸易顺差", "贸易逆差", "外汇储备", "失业率",
        "工业增加值", "固定资产投资", "社会消费品零售",
    ],
    # Tier 3 (weight 6): Market structure
    6: [
        "北向资金", "南向资金", "外资", "涨停", "跌停", "熔断",
        "IPO", "ETF", "注册制", "退市", "增持", "减持", "回购",
        "融资融券", "两融", "大宗交易", "股权质押", "解禁",
    ],
    # Tier 4 (weight 4): Hot sectors
    4: [
        "新能源", "光伏", "锂电", "半导体", "芯片", "AI", "人工智能",
        "房地产", "地产", "医药", "消费", "白酒", "军工", "国防",
        "数字经济", "数据要素", "碳中和", "储能", "氢能",
    ],
    # Tier 5 (weight 3): Bellwether companies
    3: [
        "茅台", "宁德时代", "比亚迪", "中芯国际", "腾讯", "阿里",
        "华为", "中国平安", "招商银行", "工商银行", "中国石油",
        "隆基", "药明康德", "迈瑞", "海康威视",
    ],
}

# Noise keywords (penalty -5)
NOISE_KEYWORDS = [
    "捐赠", "慈善", "体育", "娱乐", "招聘", "校招", "广告",
    "综艺", "明星", "八卦", "选秀", "真人秀",
]

# Source credibility multipliers
SOURCE_MULTIPLIERS = {
    "央视新闻联播": 1.4,
    "财联社": 1.2,
    "东方财富": 1.0,
    "富途": 1.0,
}


def _compute_keyword_score(title: str, content: str) -> float:
    """Compute keyword-based relevance score for a single news item."""
    text = title + " " + content
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

    # Compounding: 2+ tier-1 matches → ×1.5
    if tier1_matches >= 2:
        score *= 1.5

    return max(score, 0)


def _compute_recency_multiplier(publish_time: str) -> float:
    """Compute recency multiplier based on publish time."""
    if not publish_time:
        return 0.4

    now = datetime.now()
    today = now.date()

    # Try various date formats
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y%m%d"]:
        try:
            dt = datetime.strptime(publish_time[:len(fmt.replace("%", "x"))], fmt)
            pub_date = dt.date()
            delta = (today - pub_date).days
            if delta <= 0:
                return 1.0
            elif delta == 1:
                return 0.7
            else:
                return 0.4
        except (ValueError, TypeError):
            continue

    # If we can't parse, check if today's date string appears
    today_str = today.strftime("%Y-%m-%d")
    today_str2 = today.strftime("%Y%m%d")
    if today_str in publish_time or today_str2 in publish_time:
        return 1.0

    return 0.4


def keyword_rank(news_items: list[dict], top_n: int = 10) -> list[dict]:
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
        source_mult = SOURCE_MULTIPLIERS.get(item.get("source", ""), 1.0)
        recency_mult = _compute_recency_multiplier(item.get("publish_time", ""))

        final_score = base_score * source_mult * recency_mult
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

    Sends titles only (no content) to minimize token cost.
    Falls back to keyword-only ranking on failure.

    Args:
        top_items: Keyword-ranked news items (typically top 10).
        config: Settings dict with LLM config.
        top_n: Number of items to return.

    Returns:
        Top N items with 'llm_rank' and 'llm_reason' added.
    """
    if not top_items:
        return []

    # Build title list for LLM
    title_lines = []
    for i, item in enumerate(top_items, 1):
        title_lines.append(f"{i}. [{item['source']}] {item['title']}")

    titles_text = "\n".join(title_lines)

    prompt = f"""请对以下新闻按A股市场影响力从高到低排序。
评判标准：宏观政策 > 经济数据 > 行业政策 > 个股事件
只返回前{top_n}条，JSON格式：[{{"rank": 1, "id": N, "reason": "10字理由"}}, ...]

{titles_text}"""

    llm_cfg = config.get("llm", {})
    base_url = llm_cfg.get("base_url", "https://openrouter.ai/api/v1")
    model = llm_cfg.get("model", "google/gemini-3-flash-preview")
    api_key = os.environ.get("OPENROUTER_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
    ranking_cfg = config.get("news", {}).get("ranking", {})
    max_tokens = ranking_cfg.get("llm_max_tokens", 300)

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
        news_data: Output from fetch_all_news() with 'market_news' and 'cctv_news'.
        config: Settings dict.

    Returns:
        Modified news_data with added 'ranked_news' key containing top 5 items
        with scores and reasons.
    """
    ranking_cfg = config.get("news", {}).get("ranking", {})
    keyword_top_n = ranking_cfg.get("keyword_top_n", 10)
    llm_top_n = ranking_cfg.get("llm_top_n", 5)
    llm_enabled = ranking_cfg.get("llm_ranking_enabled", True)

    # Combine all news for ranking
    all_news = (
        news_data.get("market_news", [])
        + news_data.get("cctv_news", [])
    )

    if not all_news:
        logger.warning("No news items to rank")
        news_data["ranked_news"] = []
        news_data["ranking_details"] = {"total_input": 0, "method": "none"}
        return news_data

    # Stage A: Keyword scoring
    logger.info("Stage A: Keyword scoring %d items → top %d", len(all_news), keyword_top_n)
    keyword_top = keyword_rank(all_news, top_n=keyword_top_n)

    for item in keyword_top[:5]:
        logger.info("  [%.1f] %s", item["keyword_score"], item["title"][:60])

    # Stage B: LLM re-ranking (optional)
    if llm_enabled and keyword_top:
        logger.info("Stage B: LLM re-ranking top %d → top %d", len(keyword_top), llm_top_n)
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
        "Ranking complete: %d input → %d keyword → %d final (%s)",
        len(all_news), len(keyword_top), len(ranked), method,
    )
    return news_data
