"""
Report generator using Claude API.

Assembles structured prompts from fetched data and generates the 4-section
daily market report. All LLM outputs are constrained by the provided data
to minimize hallucination.
"""

import json
import logging
from datetime import datetime
from typing import Any

import anthropic

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
    return (
        f"上涨: {breadth['up_count']}家, 下跌: {breadth['down_count']}家, "
        f"平盘: {breadth['flat_count']}家\n"
        f"涨停: {breadth['limit_up']}家, 跌停: {breadth['limit_down']}家\n"
        f"两市总成交额: {breadth['total_amount']/1e8:.2f}亿元"
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
    """Format PBOC repo data for the prompt."""
    if not pboc_data.get("has_data"):
        return "（今日无公开市场操作数据）"

    lines = [f"操作日期: {pboc_data['date']}"]

    for op in pboc_data.get("today_operations", []):
        op_desc = "投放" if op["is_injection"] else "回笼"
        lines.append(
            f"- {op['type']}: {op['tenor_days']}天期, "
            f"{op['volume_billion']}亿元, 利率{op['rate_pct']}% ({op_desc})"
        )

    lines.append(f"\n今日投放: {pboc_data['today_injection_billion']}亿元")
    lines.append(f"今日到期: {pboc_data['maturing_volume_billion']}亿元")
    lines.append(f"净投放/回笼: {pboc_data['net_injection_billion']:+.0f}亿元")

    if pboc_data.get("recent_rates"):
        lines.append("\n【近期利率走势】")
        for r in pboc_data["recent_rates"][:5]:
            lines.append(f"- {r['date']}: {r['rate_pct']}%")

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
    claude_cfg = config.get("claude", {})
    model = claude_cfg.get("model", "claude-sonnet-4-20250514")
    max_tokens = claude_cfg.get("max_tokens", 4096)
    temperature = claude_cfg.get("temperature", 0.3)

    prompt = build_generation_prompt(market_data, news_data, pboc_data)

    logger.info("Generating report with model=%s, max_tokens=%d", model, max_tokens)

    client = anthropic.Anthropic()  # Uses ANTHROPIC_API_KEY env var

    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    report_text = message.content[0].text

    result = {
        "report_text": report_text,
        "model": model,
        "usage": {
            "input_tokens": message.usage.input_tokens,
            "output_tokens": message.usage.output_tokens,
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
