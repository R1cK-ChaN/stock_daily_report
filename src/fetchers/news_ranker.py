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

import openai

from src.fetchers.news_freshness import (
    annotate_news_freshness,
    count_items_by_bucket,
    get_time_decay_config,
)

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

CORE_MACRO_PRIORITY_KEYWORDS = [
    "中国cpi", "中国ppi", "中国pmi", "中国gdp", "中国零售销售", "中国工业增加值",
    "中国进出口", "中国贸易", "中国m2", "中国m1", "中国社融", "中国社会融资",
    "中国货币供应", "中国消费情绪", "中国消费者信心", "ipsos",
    "美国cpi", "美国ppi", "美国pmi", "美国非农", "美国失业率", "美国零售销售",
    "美国贸易", "美国贸易帐", "美国出口", "美国进口", "美国房地产", "美国住房",
    "美国营建许可", "美国新屋开工", "美国初请失业金", "美国续请失业金",
    "美联储资产负债表", "fed balance sheet", "consumer sentiment", "consumer confidence",
    "building permits", "housing starts", "initial jobless claims", "continuing jobless claims",
    "industrial production", "trade balance", "exports", "imports", "retail sales",
    "social financing", "aggregate financing", "货币供应", "消费者信心",
    "欧元区cpi", "欧元区ppi", "欧元区pmi", "欧元区gdp", "欧元区零售销售",
    "欧元区工业产出", "欧元区工业生产", "欧元区贸易", "euro area cpi", "euro area ppi",
    "euro area pmi", "euro area gdp", "euro area retail sales", "euro area industrial production",
]

US_MACRO_PRIORITY_KEYWORDS = [
    "美国cpi", "美国ppi", "美国pmi", "美国非农", "美国失业率", "美国零售销售",
    "美国贸易", "美国房地产", "美国住房", "新屋开工", "成屋销售", "耐用品订单",
    "美联储资产负债表", "fed balance sheet", "quantitative tightening", "qt",
    "cpi", "ppi", "nonfarm", "unemployment", "retail sales", "housing starts",
    "existing home sales", "trade balance", "jobless claims",
]

MAJOR_MACRO_EVENT_KEYWORDS = [
    "霍尔木兹", "海峡", "国际能源署", "iea", "石油储备", "原油供应",
    "能源供应", "能源冲击", "油价", "原油", "opec", "地缘政治",
    "贸易调查", "贸易冲突", "关税", "制裁", "央行政策",
]

A_SHARE_OBSERVATION_KEYWORDS = [
    "a股", "中国股票", "中国股市", "沪深", "沪深市场", "沪深两市",
    "中国资产", "中国策略", "a shares", "a-share", "ashare", "ashares",
    "china equities", "china equity", "china stocks", "h股", "h shares", "h-share",
    "adr", "美国存托凭证", "港股", "h股 vs a股", "adr vs a股",
]

GLOBAL_COMMENTARY_KEYWORDS = [
    "英国央行", "英格兰银行", "bank of england", "boe",
    "全球央行", "全球利率", "全球经济", "全球市场",
    "欧洲央行", "ecb", "加央行", "瑞士央行", "发达市场利率",
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
    core_macro_matches = _count_keyword_matches(text, CORE_MACRO_PRIORITY_KEYWORDS)
    us_macro_matches = _count_keyword_matches(text, US_MACRO_PRIORITY_KEYWORDS)
    major_macro_event_matches = _count_keyword_matches(text, MAJOR_MACRO_EVENT_KEYWORDS)
    a_share_observation_matches = _count_keyword_matches(text, A_SHARE_OBSERVATION_KEYWORDS)
    global_commentary_matches = _count_keyword_matches(text, GLOBAL_COMMENTARY_KEYWORDS)
    low_value_company_matches = _count_keyword_matches(text, LOW_VALUE_COMPANY_KEYWORDS)

    score += institution_matches * 6
    score += core_macro_matches * 5
    score += us_macro_matches * 4
    score += major_macro_event_matches * 3
    score += a_share_observation_matches * 4
    score -= low_value_company_matches * 5
    if institution_matches > 0 and a_share_observation_matches > 0:
        score += min(institution_matches, a_share_observation_matches) * 6
    if (
        global_commentary_matches > 0
        and a_share_observation_matches == 0
        and core_macro_matches == 0
        and major_macro_event_matches == 0
    ):
        score -= global_commentary_matches * 4

    has_macro_signal = (
        tier1_matches > 0
        or _count_keyword_matches(text, KEYWORD_TIERS[8]) > 0
        or core_macro_matches > 0
        or major_macro_event_matches > 0
    )
    if low_value_company_matches and not has_macro_signal and institution_matches == 0:
        score *= 0.6

    # Compounding: 2+ tier-1 matches -> x1.5
    if tier1_matches >= 2 or core_macro_matches >= 2:
        score *= 1.5

    return max(score, 0)


def _freshness_tag(item: dict) -> str:
    """Return a short freshness tag for prompts and logs."""
    bucket = item.get("recency_bucket", "unknown")
    age_hours = item.get("age_hours")
    if age_hours is None:
        return f"[{bucket}]"
    return f"[{bucket} | {age_hours:.2f}h]"


def _select_report_items_by_bucket_priority(
    items: list[dict],
    *,
    top_n: int,
    config: dict,
) -> list[dict]:
    """Select final report items by recency bucket priority, preserving intra-bucket order."""
    if top_n <= 0:
        return []

    profile = get_time_decay_config(config)
    selected = []
    for bucket_label in profile["priority_labels"]:
        bucket_items = [
            item for item in items
            if item.get("report_eligible") and item.get("recency_bucket") == bucket_label
        ]
        remaining_slots = top_n - len(selected)
        if remaining_slots <= 0:
            break
        selected.extend(bucket_items[:remaining_slots])

    return selected[:top_n]


def keyword_rank(news_items: list[dict], top_n: int = 15, config: dict | None = None) -> list[dict]:
    """
    Stage A: Rank news items by keyword score.

    Args:
        news_items: List of standardized news dicts.
        top_n: Number of top items to return.

    Returns:
        Top N news items sorted by score, each with freshness metadata.
    """
    scored = []
    for item in news_items:
        annotated = item
        if "recency_bucket" not in annotated or "recency_multiplier" not in annotated:
            annotated = annotate_news_freshness(item, config)
        if not annotated.get("report_eligible"):
            continue

        base_score = _compute_keyword_score(item.get("title", ""), item.get("content", ""))
        recency_mult = float(annotated.get("recency_multiplier", 0.0))
        final_score = base_score * recency_mult
        scored_item = {
            **annotated,
            "base_keyword_score": round(base_score, 2),
            "keyword_score": round(final_score, 2),
        }
        scored.append(scored_item)

    scored.sort(
        key=lambda item: (
            -item["keyword_score"],
            item.get("age_hours", float("inf")),
            item.get("title", ""),
        )
    )
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
        item_lines.append(f"{i}. {_freshness_tag(item)} {text}")

    items_text = "\n".join(item_lines)

    prompt = f"""你是A股市场策略研究员。请从以下金十快讯中选出对A股市场最值得写入日报的{top_n}条，按优先级从高到低排序。

选择标准：
1. 中国、美国、欧元区宏观经济数据优先级最高，尤其是CPI/PPI/PMI/GDP、零售销售、工业生产、贸易、房地产、失业金、美联储资产负债表、M2/社融等
2. 若核心宏观数据不足，可补充重大宏观/市场热点事件，如能源、油价、地缘政治、贸易冲突、央行政策等
3. “市场观察摘要”所需观点优先保留直接谈A股、中国股票、H股、ADR、中国资产、中国策略的机构/券商/外资/投行观点
4. 不直接涉及A股或中国股票的泛全球央行评论、英国央行/欧洲央行利率评论优先级更低
5. 普通公司业绩、股东变更、减持增持、回购、签约、中标等公告只有在宏观素材不足时才补位
6. 优先选择有实质内容的新闻（政策变化、数据发布、重大事件），不要追求覆盖面
7. 过滤掉：日程预告、数据中心更新通知、直播推广、外汇期权到期提示
8. 如果是综合摘要类消息（编号列表），评估其中最重要的单条信息
9. 时间权重必须严格执行：`[0-24h]` 优先级最高；`[24-48h]` 只有在 0-24h 素材不足时才补位；`[48-72h]` 仅作最后兜底；没有有效发布时间或超过72小时的新闻不得入选
10. 输出顺序必须与最终建议优先级一致，让更新鲜的同类素材排在更前面

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
        selected_indices: set[int] = set()
        for entry in rankings[:top_n]:
            idx = entry.get("id", 0) - 1  # 1-indexed to 0-indexed
            if 0 <= idx < len(top_items):
                selected_indices.add(idx)
                item = {
                    **top_items[idx],
                    "llm_rank": entry.get("rank", 0),
                    "llm_reason": entry.get("reason", ""),
                }
                ranked_result.append(item)

        for idx, item in enumerate(top_items):
            if idx in selected_indices:
                continue
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

    all_news = [
        annotate_news_freshness(item, config)
        for item in news_data.get("market_news", [])
    ]
    news_data["market_news"] = all_news

    if not all_news:
        logger.warning("No news items to rank")
        news_data["ranked_news"] = []
        news_data["ranking_details"] = {
            "total_input": 0,
            "eligible_input": 0,
            "method": "none",
            "freshness": {"input_bucket_counts": {}},
        }
        return news_data

    eligible_input = [item for item in all_news if item.get("report_eligible")]
    if not eligible_input:
        logger.warning("No report-eligible news items after time filtering")
        news_data["ranked_news"] = []
        news_data["ranking_details"] = {
            "total_input": len(all_news),
            "eligible_input": 0,
            "method": "time_filtered_none",
            "freshness": {
                "input_bucket_counts": count_items_by_bucket(all_news),
                "eligible_bucket_counts": {},
                "final_bucket_counts": {},
            },
        }
        return news_data

    # Stage A: Keyword scoring
    logger.info("Stage A: Keyword scoring %d eligible items -> top %d", len(eligible_input), keyword_top_n)
    keyword_top = keyword_rank(all_news, top_n=keyword_top_n, config=config)

    for item in keyword_top[:5]:
        logger.info(
            "  [%.2f]%s %s",
            item["keyword_score"],
            _freshness_tag(item),
            item["title"][:60],
        )

    # Stage B: LLM re-ranking (optional)
    if llm_enabled and keyword_top:
        logger.info("Stage B: LLM re-ranking top %d candidates", len(keyword_top))
        ordered_candidates = llm_rank(keyword_top, config, top_n=len(keyword_top))
        method = "keyword+llm"
    else:
        ordered_candidates = keyword_top
        method = "keyword_only"

    ranked = _select_report_items_by_bucket_priority(
        ordered_candidates,
        top_n=llm_top_n,
        config=config,
    )

    news_data["ranked_news"] = ranked
    news_data["ranking_details"] = {
        "total_input": len(all_news),
        "eligible_input": len(eligible_input),
        "keyword_top_n": len(keyword_top),
        "final_count": len(ranked),
        "method": method,
        "keyword_scores": [
            {
                "title": item["title"][:50],
                "score": item["keyword_score"],
                "base_score": item.get("base_keyword_score", 0),
                "bucket": item.get("recency_bucket", "unknown"),
                "age_hours": item.get("age_hours"),
            }
            for item in keyword_top
        ],
        "freshness": {
            "input_bucket_counts": count_items_by_bucket(all_news),
            "eligible_bucket_counts": count_items_by_bucket(eligible_input),
            "keyword_bucket_counts": count_items_by_bucket(keyword_top),
            "llm_candidate_bucket_counts": count_items_by_bucket(ordered_candidates),
            "final_bucket_counts": count_items_by_bucket(ranked),
        },
    }

    logger.info(
        "Ranking complete: %d input -> %d keyword -> %d final (%s)",
        len(all_news), len(keyword_top), len(ranked), method,
    )
    return news_data
