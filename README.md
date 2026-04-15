./scripts/run_full_pipeline.sh

# Stock Daily Report

English-first project homepage for a daily China A-shares post-close report generator.

The service runs on trading days, combines structured market data with Jin10 Telegram news, macro calendar events, and PBOC open market data, then generates a four-section research-style report through an OpenRouter-compatible model. Delivery is gated by fact-checking and can push to WeChat and Feishu when enabled.

## What It Does

- Generates a post-close China A-shares daily report after the market shuts.
- Uses structured inputs instead of free-form news prompting: market data, ranked Jin10 items, macro calendar events, and PBOC liquidity data.
- Ranks high-volume Telegram news before generation so the prompt stays focused on macro, policy, liquidity, and market-relevant items.
- Runs pre-generation validation and post-generation fact checks before delivery.
- Saves a report, audit trail, logs, and retry artifacts under `output/YYYY-MM-DD/`.
- Optionally delivers success messages to WeChat and Feishu, plus Feishu alerts for blocked delivery, pipeline failures, and uncaught exceptions.

## Report Shape

The generated report is fixed to four sections:

1. `一、市场表现` / Market Performance
2. `二、基本面分析` / Fundamental Analysis
3. `三、央行动态` / PBOC Dynamics
4. `四、市场观察摘要` / Market Observation Summary

Recent runs in `output/` show the current tone and structure: research-style Chinese prose with a strong bias toward macro, liquidity, and market-impacting events rather than news recap.

## Pipeline Overview

```text
Trigger: manual run or optional macOS launchd on weekdays at 15:01 Asia/Shanghai
    |
    v
Trading day gate (Sina calendar, weekday fallback)
    |
    v
Parallel fetch
- Market data (Sina / EastMoney / bounded fallbacks)
- Telegram Jin10 news (`https://t.me/s/jin10data`)
- PBOC OMO / repo / SHIBOR / LPR
    |
    v
News ranking
- Stage A: deterministic keyword scoring
- Stage B: LLM re-ranking on the top subset
    |
    v
Macro calendar enrichment
- TradingEconomics -> FX678 -> Investing fallback chain
    |
    v
Pre-generation validation
    |
    v
LLM report generation
    |
    v
Post-generation fact checks
    |
    +--> PASS
    |    Save `report.md` + `audit.json`
    |    Deliver to enabled providers
    |
    +--> NEEDS REVIEW
         Save attempt artifacts
         Send Feishu blocked alert if configured
         Retry with controller
         Optional forced test delivery via `ALLOW_NEEDS_REVIEW_DELIVERY=true`
```

The retry controller reuses the original data for same-data retries and refreshes source data on the final retry attempt.

## Repository Layout

```text
├── src/
│   ├── main.py                     # Pipeline orchestration and retry gate
│   ├── fetchers/
│   │   ├── market_data.py          # Indices, sectors, breadth, top movers
│   │   ├── telegram_news.py        # Telegram public preview scraping for Jin10
│   │   ├── macro_calendar.py       # Macro calendar fallback chain + cache
│   │   ├── news_ranker.py          # Keyword ranking + LLM re-ranking
│   │   └── pboc.py                 # OMO announcement, repo, SHIBOR, LPR
│   ├── generator/
│   │   └── report_generator.py     # Prompt assembly + OpenRouter call
│   ├── checker/
│   │   └── fact_check.py           # Pre/post generation validation
│   └── delivery/
│       ├── dispatcher.py           # Success delivery + alert routing
│       ├── wechat.py               # WeChat webhook delivery
│       └── feishu.py               # Feishu custom bot delivery
├── config/settings.yaml            # Runtime configuration
├── scripts/
│   ├── run_full_pipeline.sh       # Manual one-click end-to-end runner
│   ├── run_daily_report.sh         # launchd-safe wrapper with preflight
│   └── install_launch_agent.sh     # Installs the LaunchAgent
├── launchd/com.kingjason.stock-daily-report.plist.template
├── template/daily_market_report.md # Reference output structure
├── output/YYYY-MM-DD/
│   ├── report.md                   # Final report for the day
│   ├── audit.json                  # Fact-check + ranking summary
│   ├── macro_calendar_cache.json   # Macro calendar cache for that date
│   ├── pipeline_*.log              # Run logs
│   └── attempts/...                # Retry attempt reports and audits
├── .env.example
└── tests/
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
```

Required and commonly used environment variables:

```dotenv
OPENROUTER_API_KEY=             # required
TRADINGECONOMICS_API_KEY=       # recommended for primary macro calendar source

WECHAT_WEBHOOK_URL=             # optional, used when wechat.enabled=true

FEISHU_ENABLED=false            # optional master switch
FEISHU_WEBHOOK_URL=             # optional Feishu custom bot webhook
FEISHU_SECRET=                  # optional signature secret
FEISHU_TIMEOUT_SECONDS=10
FEISHU_RETRY_COUNT=2

ALLOW_NEEDS_REVIEW_DELIVERY=false  # keep false by default
```

Notes:

- `.env.example` also keeps `ANTHROPIC_API_KEY` as a backward-compatibility fallback, but OpenRouter is the intended runtime path.
- Feishu delivery is controlled by environment variables, not by a YAML toggle.

## Run Locally

```bash
source .venv/bin/activate
python src/main.py
```

## One-Click Full Run

For a manual end-to-end run without the scheduler-only Feishu preflight:

```bash
./scripts/run_full_pipeline.sh
```

This wrapper loads `.env`, checks `.venv` and `OPENROUTER_API_KEY`, then runs the full pipeline via `src/main.py`.

At runtime the service will:

1. Check whether the current date is an A-share trading day.
2. Fetch market data, Telegram news, and PBOC data in parallel.
3. Rank Jin10 items for market relevance.
4. Enrich the prompt with the macro calendar fallback chain.
5. Run pre-generation validation.
6. Generate the report through the configured OpenRouter-compatible model.
7. Run post-generation fact checks.
8. Save outputs under `output/YYYY-MM-DD/`.
9. Deliver or block delivery based on the fact-check result.

## Generated Outputs

The runtime entrypoint is `python src/main.py`.

Each run writes into `output/YYYY-MM-DD/`:

- `output/YYYY-MM-DD/report.md`: final report for the date, with metadata headers showing generation time and fact-check status.
- `output/YYYY-MM-DD/audit.json`: audit trail with fact-check summary, ranking details, and macro calendar metadata.
- `output/YYYY-MM-DD/pipeline_*.log`: per-run pipeline logs.
- `output/YYYY-MM-DD/macro_calendar_cache.json`: cached macro calendar payload for the report date.
- `output/YYYY-MM-DD/attempts/<run_id>/attempt_XX_report.md`: saved retry attempt report when delivery is blocked.
- `output/YYYY-MM-DD/attempts/<run_id>/attempt_XX_audit.json`: saved retry attempt audit.

The final `report.md` for a date is overwritten by the successful final version of that day; detailed run-by-run output is preserved in logs and attempt artifacts.

## Configuration

`config/settings.yaml` is the main configuration surface. The most important knobs are:

| Key | Purpose |
|---|---|
| `llm.*` | Base URL, model, token limit, and temperature for report generation |
| `news.telegram_channels` | Telegram public preview sources to scrape |
| `news.ranking.*` | Keyword-stage size, LLM-stage size, and ranking toggle |
| `macro_calendar.source_order` | Macro calendar fallback chain |
| `delivery_retry.*` | Retry controller for fact-check-blocked delivery |
| `wechat.enabled` | Enables WeChat delivery when `WECHAT_WEBHOOK_URL` is set |

Important behavior notes:

- The `schedule` block in `config/settings.yaml` is informational only. The runtime does not read it.
- The actual scheduled trigger is owned by macOS `launchd` when installed.
- Feishu remains env-driven even though retry behavior itself is configured in YAML.

## Delivery Behavior

### Success path

- WeChat sends the generated report only when `wechat.enabled: true` and `WECHAT_WEBHOOK_URL` is present.
- Feishu can send the successful report notification when `FEISHU_ENABLED=true`.

### Alert path

Feishu is also the alerting channel for:

- fact-check-blocked delivery attempts,
- pipeline failures,
- uncaught exceptions,
- scheduled-wrapper preflight failures.

### Fact-check gate and retries

- Reports that pass post-generation checks are delivered immediately.
- Reports that fail post-generation checks are marked `NEEDS REVIEW` and delivery is blocked by default.
- Blocked runs are retried according to `delivery_retry.*` with a fixed 60-second interval by default.
- Retry artifacts are saved under `output/YYYY-MM-DD/attempts/`.
- Setting `ALLOW_NEEDS_REVIEW_DELIVERY=true` forces a clearly labeled test delivery of a flagged report and disables the retry loop for that run.

## Optional macOS Scheduling

```bash
./scripts/install_launch_agent.sh
```

This installs a LaunchAgent that runs the wrapper script on weekdays at `15:01` Asia/Shanghai time and logs scheduler stdout/stderr under `output/scheduler_logs/`.
If you already installed the LaunchAgent, rerun `./scripts/install_launch_agent.sh` after pulling these changes so the existing job is replaced.

Useful commands:

```bash
launchctl print gui/$(id -u)/com.kingjason.stock-daily-report
launchctl kickstart -k gui/$(id -u)/com.kingjason.stock-daily-report
```

The scheduled wrapper is opinionated. Its preflight expects:

- `.venv/bin/python` to exist,
- `.env` to be present,
- `OPENROUTER_API_KEY` to be set,
- `FEISHU_ENABLED=true` and `FEISHU_WEBHOOK_URL` to be configured so preflight failures can alert.

## Current Data Sources

| Area | Current implementation | Notes |
|---|---|---|
| Trading day gate | Sina trading calendar | Falls back to weekday-only logic if the calendar fetch fails |
| Index quotes | Sina primary, EastMoney fallback | Used for major indices |
| Breadth / top movers | EastMoney with bounded fallbacks | Breadth also has a Legu fallback path |
| Sector performance | EastMoney / concept / THS fallback chain | Best-effort; third-party stability varies |
| News | Jin10 via Telegram public preview | Scraped from `t.me/s/jin10data` |
| Macro calendar | TradingEconomics -> FX678 -> Investing | Cached by report date |
| PBOC OMO | PBOC listing page, optional RSSHub feed | OMO announcement plus repo / SHIBOR / LPR |
| Delivery | WeChat webhook, Feishu custom bot | Feishu handles both success and alert events |

## Ranking and Fact-Checking

### News ranking

The Jin10 feed is high-volume, so the pipeline filters it in two stages before generation:

1. Deterministic keyword scoring over title and content.
2. Optional LLM re-ranking on the top subset using the configured OpenRouter-compatible model.

Ranking metadata is stored in `audit.json`.

### Fact-checking

The pipeline applies checks both before and after generation:

- pre-generation validation checks freshness, completeness, and value ranges on source data,
- post-generation checks verify report numbers against source data and flag unsupported claims,
- delivery stays blocked unless post-generation checks pass or the manual override is enabled.

Generated `report.md` files include metadata headers such as `PASSED` or `NEEDS REVIEW`.

## Known Limits

- Telegram scraping depends on the public preview page remaining accessible and structurally stable.
- Third-party finance and macro sources can rate-limit, return partial data, or change HTML/API behavior without notice.
- Scheduled runs depend on the local macOS `launchd` environment and the wrapper preflight assumptions.
- Delivery may be blocked even when a report is generated successfully if the fact-check gate does not pass.
