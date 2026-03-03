# 聚合数据逻辑梳理

本文档说明从 **activities** 到 **仪表盘/账户聚合指标** 的完整计算链路。

---

## 1. 数据源与层次

```
Polymarket API (activities)
    → DuckDB activities 表（按 address + ts 存储）
    → 账户表 accounts（account_id, wallet_address, display_name, initial_balance）
```

- 所有聚合均为 **现场计算**，不依赖 `daily_net_value` 等预计算缓存。
- 单钱包维度在 `valuation.rs` 中由 activities 算出「每日净值序列」；API 层再按「全账户 / 单账户」做汇总或筛选。

---

## 2. 单钱包每日净值（valuation 层）

### 2.1 入口

- `compute_daily_series_for_wallet(db_path, wallet_address, from_date, to_date)`  
  从 DB 拉取该钱包在区间内的 activities，返回  
  `Vec<(date, net_value, pnl_delta, volume)>`。
- `compute_daily_series_for_wallet_with_deposit_sum(...)`  
  在上述基础上多返回一个 **存款−提款合计** `sum_deposit_withdraw`，用于判断是否展示总余额（见 3.2）。

### 2.2 会计规则（现金流 → running_usdc）

| type     | 对「运行 USDC」的贡献 |
|----------|------------------------|
| DEPOSIT  | + 金额（usdc_size 或 share） |
| WITHDRAW | − 金额 |
| BUY      | − 成本（usdc_size 或 share×price） |
| SELL / REDEEM / CLAIM | + 变现额（usdc_size 或 share×price，REDEEM 有 effective_share 兜底） |

- 按 **时间顺序** 遍历 activities，得到每个时刻的 `running_usdc`。
- **REDEEM** 若 API 未给 token_id，用 `positions::resolve_token_for_reduce` 按 condition_id 反查应减仓的 token；赎回份额以「该 token 当前持仓」为 effective_share 兜底，避免 API 错报导致虚增。

### 2.3 每日三个输出

对区间内每一天（UTC 日）：

1. **net_value（总资产）**  
   - 该日末：`running_usdc`（到当日末累计现金流）+ **position_value**（当日末持仓市值）。  
   - 持仓来自 `positions::compute_positions(&activities[..idx])`，按 token 汇总 size、cur_price，`position_value = Σ(size × cur_price)`。

2. **pnl_delta（当日盈亏）**  
   - `raw_change = 当日末 total_assets − 前一日末 total_assets`  
   - `pnl_delta = raw_change − day_deposit_withdraw`  
   - 即：总资产变动减去当日「存款−提款」，存/取款不算盈利。

3. **volume（当日交易额）**  
   - 仅统计当日 **BUY** 的买入金额（`volume_contribution`）。

区间首日的 `pnl_delta` 用「前一日末总资产」初始化 `prev_total_assets` 再算，避免把首日整日总资产当成当日盈亏。

### 2.4 与账户表的结合

- 展示用净值 = 计算得到的 `net_value` + 该账户的 `initial_balance`（入金调整，API 无 DEPOSIT 时用）。
- `computed_daily_net_value_in_range` / `computed_daily_net_value_for_account` 在写 DailyNetValueRow 时都会加上 `initial_balance`。

---

## 3. 是否展示「总余额」（total_value_visible）

### 3.1 问题

若某钱包 **从未在 activities 中出现 DEPOSIT/WITHDRAW**（Polymarket API 对部分钱包不返回），则「运行 USDC + 仓位」只是相对值，没有绝对本金基准，总余额含义不清。

### 3.2 规则

- `sum_deposit_withdraw = sum_deposit_withdraw_from_activities(activities)`（存款−提款合计）。
- `total_value_visible = (sum_deposit_withdraw.abs() > 1e-9) || (initial_balance.abs() > 1e-9)`  
  即：**有历史存/提款记录，或配置了 initial_balance** 才视为「数据齐全」，才展示总余额/总盈亏。

### 3.3 使用处

- **computed_latest_net_value_per_account**  
  返回 `Vec<(DailyNetValueRow, bool)>`，第二个即 `total_value_visible`。
- **仪表盘汇总**  
  `total_assets` 只累加 `visible == true` 的账户的最新净值。
- **账户列表 / 仪表盘账户卡片**  
  当 `!total_value_visible` 时，`total_value`、`total_pnl` 返回 `None`（前端显示为 —）。

---

## 4. 总盈亏基准（first_net）

若用 0 做基准，会出现「总盈亏 = 总资产」的误导。因此统一用 **「首个净值为正日」的净值** 作为基准。

### 4.1 仪表盘汇总（全账户）

- 先按日汇总：`by_date[date] = 各账户该日 net_value 之和`（只含 visible 的已在 3.3 中体现）。
- `first_net = by_date[首个 date 使得 by_date[date] > 0]`，若无则为 0。
- `total_pnl = total_assets − first_net`。  
- `monthly_return_rate = (total_assets − month_start_net) / month_start_net`，其中 `month_start_net` 用当月首日净值，若无则用 `first_net`。

### 4.2 单账户（列表 / 卡片 / 详情）

- 按账户：对该账户的每日净值序列，取 **首个净值为正日** 的净值作为 `first_net`。
- `total_pnl = latest_net − first_net`（仅当 `total_value_visible` 时展示）。

---

## 5. 各接口用到的聚合

| 接口 | 用的数据 | 说明 |
|------|----------|------|
| GET /dashboard/summary | computed_latest_net_value_per_account + computed_daily_net_value_in_range | total_assets 只加 visible；first_net 为全账户首个总净值为正日；today_pnl/today_volume 取当日 sum；max_drawdown 按 by_date 的 peak 算 |
| GET /dashboard/net-value | computed_daily_net_value_in_range | 按日合并多账户 net_value → 一条曲线 |
| GET /dashboard/accounts | 同上 + latest_map + first_per_account | 每账户 total_value/total_pnl（仅 visible）、monthly_roi |
| GET /accounts | list_accounts + computed_latest + range(当月) | 列表含 total_value/total_pnl/monthly_roi/synced（逻辑同 dashboard 账户，基准为首个净值为正日） |
| GET /dashboard/accounts/:id/summary | computed_daily_net_value_for_account | 单账户 total_pnl、日/月回报、max_drawdown，基准为首个净值为正日 |

---

## 6. 流程小结（单钱包 → 聚合）

1. **DB**：按 `wallet_address` 取 activities（`list_activities_in_range`）。
2. **valuation**：  
   - 现金流 → running_usdc；  
   - 持仓 → position_value（positions）；  
   - 按日 → (net_value, pnl_delta, volume)；  
   - 顺带得到 sum_deposit_withdraw → total_value_visible。
3. **API**：  
   - 加 initial_balance 得到展示净值；  
   - 多账户按日合并或按账户取 first_net；  
   - 仅对 visible 账户参与 total_assets 与 total_value/total_pnl 展示。

这样，聚合数据逻辑就形成一条从「活动流 + 账户配置」到「总资产、总盈亏、回报率、回撤」的清晰链路。
