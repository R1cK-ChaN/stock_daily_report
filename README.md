# Stock Daily Report — A股每日市场报告

Automated pipeline that generates a daily China A-shares market report after market close (15:00 CST). Every data point is fetched from deterministic sources (AKShare) before being passed to an LLM, with a 3-layer fact-checking system to prevent hallucination.

## Report Sections

1. **A股收评** — Index prices, sector performance, market breadth
2. **基本面分析** — Key financial news & economic data
3. **央行公开市场操作** — PBOC repo rates, SHIBOR, LPR & monetary policy signals
4. **总结与展望** — Synthesis and short-term outlook

## Architecture

```
TRIGGER: cron at 15:30 CST (or manual run)
    │
    ├── Trading day? (Sina calendar) ── No ──► Skip
    │
    ▼ Yes
┌──────────┐  ┌──────────┐  ┌──────────┐
│  Market   │  │   News   │  │   PBOC   │   ← Parallel fetch
│  (Sina)   │  │  (东方财富) │  │ (AKShare)│
└────┬─────┘  └────┬─────┘  └────┬─────┘
     └──────────┬──┴─────────────┘
                ▼
       Data Validation (pre-LLM)
       - Freshness / completeness / range checks
                │
                ▼
       LLM Report Generation (OpenRouter)
       - Structured prompt with all data attached
                │
                ▼
       Fact-Check (post-LLM)
       - Number cross-check (regex vs source)
       - LLM claim verification (second call)
                │
                ▼
       Save → output/YYYY-MM-DD/report.md
       Deliver → WeChat (optional)
```

## Project Structure

```
├── src/
│   ├── main.py                     # Pipeline orchestrator
│   ├── fetchers/
│   │   ├── market_data.py          # Index quotes (Sina primary, EM fallback),
│   │   │                           #   sectors, breadth, top movers
│   │   ├── news.py                 # 东方财富 news, CCTV, RSS, economic data
│   │   └── pboc.py                 # Repo rates (FR001/007/014), SHIBOR, LPR
│   ├── generator/
│   │   └── report_generator.py     # Prompt assembly + OpenRouter API call
│   ├── checker/
│   │   └── fact_check.py           # 3-layer verification
│   └── delivery/
│       └── wechat.py               # WeChat group webhook push
├── config/
│   └── settings.yaml               # Model, indices, thresholds, toggles
├── template/
│   └── daily_market_report.md      # Report structure reference
├── docs/
│   └── PROJECT_STATUS.md           # Development status & known issues
├── output/                         # Generated reports by date
│   └── YYYY-MM-DD/
│       ├── report.md               # The generated report
│       ├── audit.json              # Fact-check results
│       └── pipeline_*.log          # Run logs
├── .env                            # API keys (git-ignored)
├── .env.example                    # Template for .env
└── requirements.txt
```

## Setup

```bash
# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure API key
cp .env.example .env
# Edit .env and set your OpenRouter API key
```

The `.env` file:
```
OPENROUTER_API_KEY=sk-or-v1-your-key-here
WECHAT_WEBHOOK_URL=              # optional
```

For WeChat delivery, also set `wechat.enabled: true` in `config/settings.yaml`.

## Usage

```bash
source .venv/bin/activate
python src/main.py
```

The pipeline will:
1. Check the Sina trading calendar — skip if not a trading day (weekends + Chinese holidays)
2. Fetch market data, news, and PBOC data in parallel
3. Validate data freshness, completeness, and value ranges
4. Generate report via Gemini 3 Flash on OpenRouter
5. Fact-check: cross-verify numbers + LLM claim verification
6. Save to `output/YYYY-MM-DD/report.md` and optionally push to WeChat

## Scheduling (Cron)

```
30 15 * * * cd /path/to/stock_daily_report && .venv/bin/python src/main.py
```

The trading day check is built-in — safe to run daily including weekends.

## Data Sources

| Data | Source | AKShare Function | Reliability |
|---|---|---|---|
| Index quotes | Sina 财经 | `stock_zh_index_spot_sina()` | High |
| Sector performance | 东方财富 | `stock_board_industry_name_em()` | Medium (rate-limited) |
| Market breadth | 东方财富 | `stock_zh_a_spot_em()` | Medium (rate-limited) |
| Trading calendar | Sina | `tool_trade_date_hist_sina()` | High |
| Repo rates | 全国银行间同业拆借中心 | `repo_rate_query()` | High |
| SHIBOR | 同上 | `macro_china_shibor_all()` | High |
| LPR | 同上 | `macro_china_lpr()` | High |
| Financial news | 东方财富 | `stock_news_em()` | High |
| CCTV news | 央视 | `news_cctv()` | Medium |
| CPI / PMI / GDP | 国家统计局 | `macro_china_cpi_yearly()` etc. | High |

## Fact-Checking

| Layer | Stage | Method |
|---|---|---|
| Data validation | Pre-LLM | Assert freshness (today's date), completeness (all fields), range bounds |
| Number cross-check | Post-LLM | Regex-extract every number from report → match against source data |
| Claim verification | Post-LLM | Second LLM call checks every claim is grounded in provided data |

Reports with unverified numbers or ungrounded claims are flagged `[NEEDS REVIEW]` in the output metadata.

## Configuration

`config/settings.yaml` controls:

- **LLM**: model (`google/gemini-3-flash-preview`), temperature, max tokens, OpenRouter base URL
- **Market indices**: list of index codes to track (Shanghai/Shenzhen)
- **Sectors**: how many top gainers/losers to include
- **Validation**: max daily change threshold, index value ranges
- **News**: max headlines to fetch, number to select for report
- **WeChat**: enable/disable delivery, message format
