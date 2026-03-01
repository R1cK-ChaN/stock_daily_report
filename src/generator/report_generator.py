"""
Report generator using Claude API.

Assembles structured prompts from fetched data and generates the 4-section
daily market report. All LLM outputs are constrained by the provided data
to minimize hallucination.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any

import openai

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """你是一位专业的中国A股市场分析师，负责撰写每日收盘市场报告。

关键规则：
1. 你只能使用提供给你的数据来撰写报告，不得编造任何数字或新闻
2. 所有数字必须与提供的数据完全一致
3. 如果某项数据缺失，明确注明"数据暂缺"，不要猜测
4. 使用专业、简洁的财经语言
5. 报告使用中文撰写
6. 每个观点都必须有数据支撑"""


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
            f"成交额: {idx['amount']/1e8:.2f}亿元)"
        )
    return "\n".join(lines)


def _format_sector_data(sectors: dict) -> str:
    """Format sector performance data."""
    lines = ["【领涨板块】"]
    for s in sectors.get("gainers", []):
        lines.append(
            f"- {s['name']}: {s['change_pct']:+.2f}% "
            f"(领涨股: {s['leader_stock']} {s['leader_change_pct']:+.2f}%, "
            f"上涨{s['num_up']}家/下跌{s['num_down']}家)"
        )

    lines.append("\n【领跌板块】")
    for s in sectors.get("losers", []):
        lines.append(
            f"- {s['name']}: {s['change_pct']:+.2f}% "
            f"(领涨股: {s['leader_stock']} {s['leader_change_pct']:+.2f}%, "
            f"上涨{s['num_up']}家/下跌{s['num_down']}家)"
        )
    return "\n".join(lines)


def _format_breadth_data(breadth: dict) -> str:
    """Format market breadth data."""
    if not breadth:
        return "（市场广度数据暂缺）"
    return (
        f"上涨: {breadth.get('up_count', 'N/A')}家, 下跌: {breadth.get('down_count', 'N/A')}家, "
        f"平盘: {breadth.get('flat_count', 'N/A')}家\n"
        f"涨停: {breadth.get('limit_up', 'N/A')}家, 跌停: {breadth.get('limit_down', 'N/A')}家\n"
        f"两市总成交额: {breadth.get('total_amount', 0)/1e8:.2f}亿元"
    )


def _format_news_data(news_data: dict) -> str:
    """Format news data for the prompt."""
    lines = []

    for item in news_data.get("eastmoney_news", [])[:10]:
        lines.append(f"- [{item['source']}] {item['title']}")
        if item.get("content"):
            lines.append(f"  摘要: {item['content'][:200]}")

    for item in news_data.get("cctv_news", [])[:5]:
        lines.append(f"- [{item['source']}] {item['title']}")

    for item in news_data.get("rss_news", [])[:5]:
        lines.append(f"- [{item['source']}] {item['title']}")

    if news_data.get("economic_data"):
        lines.append("\n【经济数据】")
        for d in news_data["economic_data"]:
            lines.append(f"- {d['indicator']}: {d['value']} ({d['period']})")

    return "\n".join(lines) if lines else "（新闻数据暂缺）"


def _format_pboc_data(pboc_data: dict) -> str:
    """Format PBOC monetary data for the prompt."""
    if not pboc_data.get("has_data"):
        return "（央行货币市场数据暂缺）"

    lines = [f"数据日期: {pboc_data['date']}"]

    # Repo rates
    repo = pboc_data.get("repo_rates", {})
    if repo.get("has_data"):
        lines.append(f"\n【银行间回购利率】(截至 {repo['latest_date']})")
        lines.append(f"- FR001 (隔夜): {repo['FR001']:.2f}%")
        lines.append(f"- FR007 (7天): {repo['FR007']:.2f}%")
        lines.append(f"- FR014 (14天): {repo['FR014']:.2f}%")
        if repo.get("recent_trend"):
            lines.append("近5日FR007走势:")
            for r in repo["recent_trend"]:
                lines.append(f"  {r['date']}: {r['FR007']:.2f}%")

    # SHIBOR
    shibor = pboc_data.get("shibor", {})
    if shibor.get("has_data"):
        lines.append(f"\n【SHIBOR利率】(截至 {shibor['latest_date']})")
        lines.append(f"- 隔夜: {shibor['overnight']:.3f}%")
        lines.append(f"- 1周: {shibor['1W']:.3f}%")
        lines.append(f"- 1月: {shibor['1M']:.3f}%")
        lines.append(f"- 3月: {shibor['3M']:.3f}%")

    # LPR
    lpr = pboc_data.get("lpr", {})
    if lpr.get("has_data"):
        lines.append(f"\n【LPR贷款市场报价利率】(截至 {lpr['latest_date']})")
        lines.append(f"- 1年期LPR: {lpr['LPR_1Y']:.2f}%")
        lines.append(f"- 5年期以上LPR: {lpr['LPR_5Y']:.2f}%")

    return "\n".join(lines)


def build_generation_prompt(market_data: dict, news_data: dict, pboc_data: dict) -> str:
    """
    Build the full generation prompt with all data attached.

    Returns:
        Formatted prompt string with data sections.
    """
    today_str = datetime.now().strftime("%Y年%m月%d日")

    prompt = f"""请根据以下数据撰写{today_str}的A股每日市场报告。报告必须包含以下四个部分。
所有数字和事实必须严格来源于下方提供的数据，不得编造。

===== 原始数据 =====

【一、主要指数行情】
{_format_index_data(market_data.get('indices', []))}

【市场广度】
{_format_breadth_data(market_data.get('breadth', {}))}

【板块表现】
{_format_sector_data(market_data.get('sectors', {}))}

【二、今日财经新闻与经济数据】
{_format_news_data(news_data)}

【三、央行公开市场操作】
{_format_pboc_data(pboc_data)}

===== 报告要求 =====

请按以下结构撰写报告：

## 一、A股收评 (市场表现)
- 概述今日大盘走势（上证、深证、创业板等主要指数涨跌）
- 分析成交量变化
- 点评板块轮动（领涨/领跌板块及原因分析）
- 市场情绪（涨跌家数、涨停跌停）
- 字数：300-500字

## 二、基本面分析 (重要新闻与经济数据)
- 从提供的新闻中挑选2-3条最具市场影响力的新闻进行解读
- 分析对A股市场的潜在影响
- 如有经济数据发布，进行解读
- 字数：200-400字

## 三、央行逆回购 (公开市场操作)
- 今日操作情况（金额、期限、利率）
- 到期与净投放/回笼情况
- 资金面分析与政策信号解读
- 字数：150-300字

## 四、总结与展望
- 综合以上三部分，给出今日市场总结
- 短期展望（基于数据，不做过度预测）
- 字数：150-250字

注意：
- 所有数字必须与提供的数据一致
- 如果某项数据不足以支撑分析，请注明
- 使用专业财经术语
- 语气客观中立"""

    return prompt


def generate_report(
    market_data: dict,
    news_data: dict,
    pboc_data: dict,
    config: dict,
) -> dict:
    """
    Generate the daily market report using Claude API.

    Args:
        market_data: Output from market_data.fetch_all_market_data()
        news_data: Output from news.fetch_all_news()
        pboc_data: Output from pboc.fetch_pboc_data()
        config: Settings dict

    Returns:
        Dict with 'report_text', 'model', 'usage', 'prompt_data'
    """
    llm_cfg = config.get("llm", config.get("claude", {}))
    model = llm_cfg.get("model", "anthropic/claude-sonnet-4-20250514")
    max_tokens = llm_cfg.get("max_tokens", 4096)
    temperature = llm_cfg.get("temperature", 0.3)
    base_url = llm_cfg.get("base_url", "https://openrouter.ai/api/v1")
    api_key = os.environ.get("OPENROUTER_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))

    prompt = build_generation_prompt(market_data, news_data, pboc_data)

    logger.info("Generating report with model=%s, max_tokens=%d", model, max_tokens)

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
                "num_eastmoney": len(news_data.get("eastmoney_news", [])),
                "num_cctv": len(news_data.get("cctv_news", [])),
                "num_econ": len(news_data.get("economic_data", [])),
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
