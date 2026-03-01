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
│  (Sina)   │  │(EM/CLS/富途)│  │ (AKShare)│
└────┬─────┘  └────┬─────┘  └────┬─────┘
     └──────────┬──┴─────────────┘
                ▼
       News Ranking (2-stage)
       - Stage A: keyword scoring (5-tier dictionary)
       - Stage B: LLM pre-ranking (titles only → top 5)
                │
                ▼
       Data Validation (pre-LLM)
       - Freshness / completeness / range checks
                │
                ▼
       LLM Report Generation (OpenRouter)
       - Structured prompt with ranked news + all data
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
│   │   ├── news.py                 # Multi-source news: 东方财富, 财联社, 富途, CCTV
│   │   ├── news_ranker.py          # 2-stage ranking: keyword scoring + LLM re-rank
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
│       ├── audit.json              # Fact-check results + news ranking details
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
3. **Rank news** — keyword scoring (87 headlines → top 10) then LLM re-ranking (top 10 → top 5 with reasons)
4. Validate data freshness, completeness, and value ranges
5. Generate report via Gemini 3 Flash on OpenRouter (prompt includes only pre-ranked headlines)
6. Fact-check: cross-verify numbers + LLM claim verification
7. Save to `output/YYYY-MM-DD/report.md` and optionally push to WeChat

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
| Financial news (primary) | 东方财富 | `stock_info_global_em()` | High |
| Financial news (secondary) | 财联社 | `stock_info_global_cls()` | High |
| Financial news (tertiary) | 富途 | `stock_info_global_futu()` | High |
| CCTV news | 央视 | `news_cctv()` | Medium (empty on weekends) |
| CPI / PMI / GDP | 国家统计局 | `macro_china_cpi_yearly()` etc. | High |

## News Ranking

A two-stage hybrid ranking system sits between data fetch and report generation:

**Stage A — Keyword Scoring (deterministic)**
- 5-tier keyword dictionary: monetary policy (10) → economic data (8) → market structure (6) → hot sectors (4) → bellwether companies (3)
- Noise penalty (-5) for irrelevant topics (entertainment, sports, etc.)
- Multipliers: source credibility (央视 1.4×, 财联社 1.2×) × recency (today 1.0, yesterday 0.7, older 0.4)
- 2+ tier-1 keyword matches trigger a 1.5× compounding bonus

**Stage B — LLM Pre-Ranking (~700 tokens)**
- Sends only titles (no content) to Gemini Flash for cost efficiency
- Prompt: rank by A-share impact (宏观政策 > 经济数据 > 行业政策 > 个股事件)
- Returns top 5 with 10-character reasons
- Falls back to keyword-only ranking on LLM failure

Rankings are logged in `audit.json` for transparency.

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
- **News**: max headlines (50), sources (eastmoney_global, cls, futu), ranking config (keyword_top_n, llm_top_n, llm_ranking_enabled)
- **WeChat**: enable/disable delivery, message format
