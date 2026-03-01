# Project Status — Daily Stock Report

> Last updated: 2026-03-01

## Current State: Core Pipeline Functional

The end-to-end pipeline runs from data fetching through validation to report generation. Tested on 2026-03-01 (weekend — some data sources return last trading day data as expected).

## Module Status

| Module | Status | Notes |
|---|---|---|
| `src/fetchers/market_data.py` | Working | Sina (primary) fetches all 5 indices reliably. EM (fallback) intermittently rate-limited. |
| `src/fetchers/news.py` | Working | 东方财富 news OK. CCTV returns 0 on weekends. RSS/CLS may need URL update. |
| `src/fetchers/pboc.py` | Working | Repo rates, SHIBOR, LPR all fetch correctly. |
| `src/generator/report_generator.py` | Working | Requires `ANTHROPIC_API_KEY` env var. |
| `src/checker/fact_check.py` | Working | Pre-validation passes. Post-validation (number cross-check + LLM verifier) ready. |
| `src/delivery/wechat.py` | Untested | Needs `WECHAT_WEBHOOK_URL` env var and `wechat.enabled: true` in config. |
| `src/main.py` | Working | Parallel data fetch, sequential validate → generate → check → save. |

## Data Source Reliability (from testing)

| Source | Function | Reliability | Notes |
|---|---|---|---|
| Sina index quotes | `stock_zh_index_spot_sina()` | High | Primary source, fast and stable |
| EM index quotes | `stock_zh_index_spot_em()` | Medium | Fallback, sometimes rate-limited |
| EM sector performance | `stock_board_industry_name_em()` | Medium | Frequently connection-reset by 东方财富 server |
| EM individual stocks | `stock_zh_a_spot_em()` | Medium | Same connection issues as sectors |
| Repo rates | `repo_rate_query()` | High | Reliable, returns FR001/FR007/FR014 |
| SHIBOR | `macro_china_shibor_all()` | High | Reliable |
| LPR | `macro_china_lpr()` | High | Reliable |
| 东方财富 news | `stock_news_em()` | High | Returns 10+ headlines |
| CCTV news | `news_cctv()` | Medium | Empty on weekends/holidays |
| Economic data (CPI) | `macro_china_cpi_yearly()` | High | Latest available data point |
| Economic data (PMI/GDP) | `macro_china_pmi_yearly()` etc. | Medium | TLS cert issues in some envs |

## Known Challenges

### 1. 东方财富 Rate Limiting
**Problem**: `stock_board_industry_name_em()` and `stock_zh_a_spot_em()` frequently return `RemoteDisconnected` errors, especially on weekends or with rapid consecutive calls.

**Mitigation**: Added 3-retry with exponential backoff. Functions return empty data instead of crashing. Pipeline continues with partial data.

**Future fix**: Add request delays between EM calls, or find Sina alternatives for sector/breadth data.

### 2. AKShare API Changes
**Problem**: Original design assumed `macro_china_gksccz()` for PBOC open market operations — this function doesn't exist in AKShare v1.18.

**Resolution**: Replaced with `repo_rate_query()` (repo rates), `macro_china_shibor_all()` (SHIBOR), and `macro_china_lpr()` (LPR). These provide comparable monetary policy data.

### 3. Weekend/Holiday Data
**Problem**: Market is closed on weekends. Data sources return last trading day's data.

**Mitigation**: Pre-validation checks data freshness but doesn't abort on stale data (reports as warning). The pipeline can still generate a valid report using last-available data.

### 4. TLS Certificate Issues
**Problem**: Some AKShare macro functions fail with `Could not find a suitable TLS CA certificate bundle` in venv environments.

**Mitigation**: Only affects PMI and GDP fetchers (non-critical). CPI still works. Other data sources unaffected.

## Environment Requirements

- Python 3.12+
- `ANTHROPIC_API_KEY` environment variable (required for report generation)
- `WECHAT_WEBHOOK_URL` environment variable (optional, for delivery)
- Network access to: finance.sina.com.cn, eastmoney.com, data.eastmoney.com

## Next Steps

- [ ] Test full pipeline on a weekday with `ANTHROPIC_API_KEY` set
- [ ] Add Sina-based alternatives for sector and breadth data
- [ ] Update `.env.example` with correct variables
- [ ] Test WeChat delivery with a real webhook
- [ ] Add cron job setup script
- [ ] Consider adding request delays between EM API calls to reduce rate-limiting
