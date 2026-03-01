# A股每日市场报告

> 日期：{date}
> 生成时间：{generated_at}
> 数据来源：东方财富、央行公开市场操作公告

---

## 一、A股收评（市场表现）

### 主要指数

| 指数 | 收盘价 | 涨跌额 | 涨跌幅 | 成交额(亿) |
|------|--------|--------|--------|------------|
| 上证指数 | {sse_close} | {sse_change} | {sse_change_pct}% | {sse_amount} |
| 深证成指 | {szse_close} | {szse_change} | {szse_change_pct}% | {szse_amount} |
| 创业板指 | {chinext_close} | {chinext_change} | {chinext_change_pct}% | {chinext_amount} |

### 市场广度

- 上涨：{up_count}家 | 下跌：{down_count}家 | 平盘：{flat_count}家
- 涨停：{limit_up}家 | 跌停：{limit_down}家
- 两市总成交额：{total_amount}亿元

### 板块表现

**领涨板块：**
{top_gainers}

**领跌板块：**
{top_losers}

### 走势点评

{market_commentary}

---

## 二、基本面分析（重要新闻与经济数据）

{fundamental_analysis}

---

## 三、央行逆回购（公开市场操作）

| 操作类型 | 期限 | 金额(亿) | 利率 |
|----------|------|----------|------|
{pboc_operations_table}

- 今日投放：{injection}亿元
- 今日到期：{maturing}亿元
- **净投放/回笼：{net_injection}亿元**

{pboc_commentary}

---

## 四、总结与展望

{summary}

---

> 免责声明：本报告由AI基于公开市场数据自动生成，仅供参考，不构成投资建议。
> 数据来源可能存在延迟，请以官方发布为准。
