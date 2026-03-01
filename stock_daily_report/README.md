# Stock Daily Report — A股每日市场报告

Automated pipeline that generates a daily China A-shares market report after market close (15:00 CST). Every data point is fetched from deterministic sources (AKShare) before being passed to an LLM, with a 3-layer fact-checking system to prevent hallucination.

## Report Sections

1. **A股收评** — Index prices, sector performance, market breadth
2. **基本面分析** — Key financial news & economic data
3. **央行逆回购** — PBOC open market operations & net liquidity
4. **总结与展望** — Synthesis and short-term outlook

## Project Structure

```
stock_daily_report/
├── src/
│   ├── main.py                     # Pipeline entry point
│   ├── fetchers/
│   │   ├── market_data.py          # AKShare: indices, sectors, breadth, top movers
│   │   ├── news.py                 # 东方财富 news, CCTV, RSS, economic data
│   │   └── pboc.py                 # PBOC reverse repo & net injection calc
│   ├── generator/
│   │   └── report_generator.py     # Claude API prompt assembly & generation
│   ├── checker/
│   │   └── fact_check.py           # 3-layer verification
│   └── delivery/
│       └── wechat.py               # WeChat webhook push
├── config/
│   └── settings.yaml               # API keys, schedule, validation thresholds
├── template/
│   └── daily_market_report.md      # Report template
├── output/                         # Generated reports (date-stamped)
└── requirements.txt
```

## Setup

```bash
cd stock_daily_report
pip install -r requirements.txt
```

Set environment variables:

```bash
export ANTHROPIC_API_KEY="your-key"

# Optional — for WeChat delivery
export WECHAT_WEBHOOK_URL="your-webhook-url"
```

If using WeChat delivery, also set `wechat.enabled: true` in `config/settings.yaml`.

## Usage

```bash
# Run the full pipeline
python src/main.py
```

The pipeline will:

1. Fetch market data, news, and PBOC data in parallel (via AKShare)
2. Validate data freshness, completeness, and value ranges
3. Generate report via Claude API with all data attached to the prompt
4. Fact-check: cross-verify numbers + LLM claim verification
5. Save to `output/daily_report_YYYY-MM-DD.md` and optionally push to WeChat

## Scheduling (Cron)

Run daily at 15:30 CST on weekdays:

```
30 15 * * 1-5 cd /path/to/stock_daily_report && python src/main.py
```

## Fact-Checking Layers

| Layer | When | Method |
|---|---|---|
| Data validation | Pre-LLM | Freshness check, completeness, range bounds |
| Number cross-check | Post-LLM | Regex extraction → exact match against source data |
| Claim verification | Post-LLM | Second LLM call verifies every claim is grounded |

Reports with unverified numbers or ungrounded claims are flagged `[NEEDS REVIEW]`.

## Data Sources

| Data | Source | API |
|---|---|---|
| Index prices & volume | 东方财富 | `akshare.stock_zh_index_spot_em()` |
| Sector performance | 东方财富 | `akshare.stock_board_industry_name_em()` |
| Market breadth | 东方财富 | `akshare.stock_zh_a_spot_em()` |
| PBOC repo operations | 央行 | `akshare.macro_china_gksccz()` |
| Financial news | 东方财富 / CCTV | `akshare.stock_news_em()` / `akshare.news_cctv()` |
| Economic indicators | 国家统计局 | `akshare.macro_china_cpi_yearly()` etc. |

## Configuration

Edit `config/settings.yaml` to customize:

- Tracked indices and sector count
- Claude model, temperature, and token limits
- Validation thresholds (max daily change %, index ranges)
- News source count and selection
- WeChat delivery toggle
