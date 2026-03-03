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
from datetime import datetime, timedelta, timezone
from typing import Any

import openai

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────
# Keyword Dictionary (5 tiers + noise) — English
# ────────────────────────────────────────────────────────────

KEYWORD_TIERS = {
    # Tier 1 (weight 10): Monetary/fiscal policy
    10: [
        "federal reserve", "fed", "fomc", "rate cut", "rate hike",
        "pboc", "ecb", "boj", "central bank", "monetary policy",
        "state council", "csrc", "stimulus",
    ],
    # Tier 2 (weight 8): Economic data
    8: [
        "gdp", "cpi", "ppi", "pmi", "nonfarm", "unemployment",
        "inflation", "trade deficit", "trade surplus", "retail sales",
        "m1", "m2",
    ],
    # Tier 3 (weight 6): Market structure / China flows
    6: [
        "northbound", "stock connect", "qfii", "a-shares", "ipo",
        "etf", "tariff", "sanctions", "trade war", "margin",
        "short selling",
    ],
    # Tier 4 (weight 4): Hot sectors
    4: [
        "semiconductor", "chip", "ai", "ev", "battery", "lithium",
        "solar", "real estate", "pharma", "oil", "crude", "gold",
        "copper",
    ],
    # Tier 5 (weight 3): Bellwether companies
    3: [
        "byd", "catl", "alibaba", "tencent", "huawei", "apple",
        "nvidia", "tsmc", "tesla", "kweichow moutai",
    ],
}

# Noise keywords (penalty -5)
NOISE_KEYWORDS = [
    "donation", "charity", "sports", "entertainment", "celebrity",
    "recruitment", "advertisement", "reality show",
]

# Source credibility multipliers (partial-match keys)
SOURCE_MULTIPLIERS = {
    "federal reserve": 1.5,
    "wsj": 1.3,
    "wall street journal": 1.3,
    "reuters": 1.3,
    "bloomberg": 1.3,
    "ecb": 1.3,
    "cnbc": 1.2,
    "scmp": 1.2,
    "south china morning post": 1.2,
    "xinhua": 1.1,
    "china daily": 1.1,
    "cgtn": 1.0,
    "bbc": 1.1,
    "nikkei": 1.1,
    "yahoo": 1.0,
}


def _get_source_multiplier(source: str) -> float:
    """Look up source multiplier via case-insensitive partial match."""
    source_lower = source.lower()
    for key, mult in SOURCE_MULTIPLIERS.items():
        if key in source_lower:
            return mult
    return 1.0


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

    # Try ISO 8601 first (from RSS feeds)
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
        source_mult = _get_source_multiplier(item.get("source", ""))
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

    prompt = f"""请对以下英文新闻按A股市场影响力从高到低排序。
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

    # Stage A.5: Enrich top items with full article content
    if config.get("news", {}).get("article_fetch_enabled", True):
        from src.fetchers.article_fetcher import enrich_articles
        keyword_top = enrich_articles(keyword_top, config)

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
