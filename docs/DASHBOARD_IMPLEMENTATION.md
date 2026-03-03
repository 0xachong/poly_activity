# 仪表盘数据聚合实现说明

本文档记录仪表盘数据聚合 API 的实现细节，便于后续维护与扩展。

## 1. 整体架构

- **写入路径**：Polymarket 同步写入 `activities` 后，对本次涉及且已登记为「账户」的钱包，从本批最小日期到今日重算净值并**增量写入** `daily_net_value`。
- **读取路径**：仪表盘所有 GET 接口**只读** `daily_net_value`（及必要时 `accounts`），在内存中做汇总计算，不维护预汇总表（懒聚合）。

## 2. 数据模型

### 2.1 表结构（DuckDB）

- **accounts**  
  - `account_id` VARCHAR PRIMARY KEY  
  - `wallet_address` VARCHAR NOT NULL（对应 `activities.address`）  
  - `display_name` VARCHAR  
  - `created_at` / `updated_at` TIMESTAMP  

- **daily_net_value**  
  - `account_id` VARCHAR  
  - `date` DATE（仅日期，存为 `YYYY-MM-DD` 字符串写入）  
  - `net_value` DOUBLE  
  - `pnl_delta` DOUBLE  
  - `volume` DOUBLE  
  - PRIMARY KEY (account_id, date)  

多账户通过 `accounts.wallet_address = activities.address` 与活动数据关联；一个钱包对应一个账户。

### 2.2 估值与盈亏规则（valuation.rs）

- **买入（BUY）**：不改变净值，不参与净值与成交量计算。  
- **卖出 / 赎回（SELL、REDEEM）**：净值增加 = 当笔 `share * price`（变现额）；只对 SELL/REDEEM 累加。  
- **存款 / 提款**：`type_` 为 DEPOSIT、WITHDRAWAL、DEPOSITS、WITHDRAWALS 等一律**不参与**净值与盈亏、成交量计算。  

据此：

- `net_value(date)` = 到该日结束为止，该账户所有 SELL/REDEEM 的 `share * price` 之和（从 0 起累计）。  
- `pnl_delta` = 当日所有 SELL/REDEEM 的 `share * price` 之和。  
- `volume` = 当日所有 BUY/SELL/REDEEM 的 `|share * price|` 之和（仅买卖/赎回，不含存款提款）。  

活动类型识别：按 `type_` 字符串（如 sync 层合并后的 "BUY"/"SELL" 及原始 "REDEEM" 等）判断；存款/提款通过 `IGNORED_TYPES` 列表排除。

## 3. 增量保存

- **触发时机**：在 `sync::fetch_segment` 内，`insert_activities_batch` 成功后，对本批 `rows` 中出现的**去重后的 address** 查 `accounts`，得到受影响的账户列表；对每个账户取本批中该 address 的 **min(ts)** 转为日期 `from_date`，`to_date` = 当日（UTC）。  
- **写入范围**：仅对该账户、`[from_date, to_date]` 重算并 `upsert_daily_net_value_batch`；不同账户、不同日期互不影响。  
- **重算逻辑**：`valuation::recompute_and_upsert_daily_net_value` 从 DB 拉取该钱包 **ts ∈ [0, to_ts]** 的全部 activities（上限 500_000 条），用 `compute_daily_series` 得到 [from_date, to_date] 的每日 (net_value, pnl_delta, volume)，再整批写回 `daily_net_value`。这样保证任意日期的净值都是「从最早活动到该日」的累计，与是否分批同步无关。

## 4. 懒聚合（读时计算）

以下均在 GET 处理函数内完成，不写表：

- **总资产**：各账户在 `daily_net_value` 中**最新日期**的 `net_value` 之和（通过 `get_latest_net_value_per_account` 一次查询）。  
- **总盈亏**：各账户「最新净值 − 该账户在 daily_net_value 中**最早日期**的净值」之和；净值本身只含 SELL/REDEEM，即累计交易盈亏。  
- **今日盈亏 / 今日成交量**：从 `get_daily_net_value_in_range(today, today)` 或从 latest 行中取 `date == today` 的 `pnl_delta`/`volume` 按账户汇总。  
- **月回报率**：当前实现为「本月至今」：(最新总净值 − 本月 1 号总净值) / 本月 1 号总净值；如需几何平均月化可改为 `(1 + R_total)^(1/n_months) - 1`。  
- **最大回撤**：对「全账户按日汇总净值」序列按日期排序后，遍历维护历史峰值，回撤 = (peak - value) / peak，取最大值。  
- **净值走势**：仅日线；按 `range=1w|1m|all` 确定日期区间，从 `daily_net_value` 按 date 汇总多账户 `net_value`，返回 `[{ date, net_value }, ...]`。

实现上对「全量按日汇总」可只查一次 `get_daily_net_value_in_range`，在内存中按 date 聚合并同时算总资产、总盈亏、月回报、最大回撤与走势序列，避免多次扫表。

## 5. API 端点一览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /accounts | Body: account_id, wallet_address, display_name；创建/更新账户 |
| GET  | /accounts | 账户列表，可选带 total_value、total_pnl、monthly_roi（懒算） |
| GET  | /dashboard/summary | total_assets, total_pnl, today_pnl, today_volume, monthly_return_rate, max_drawdown |
| GET  | /dashboard/net-value | Query: range=1w\|1m\|all；日线序列 data: [{ date, net_value }] |
| GET  | /dashboard/accounts | 各账户 total_value、total_pnl、monthly_roi |
| POST | /dashboard/refresh | Body 可选 { account_ids: ["id1",...] } 或空；对指定或全部账户重算并写 daily_net_value |

## 6. 关键代码位置

- **Schema 与 CRUD**：[src/db.rs](src/db.rs) — `init_schema` 中 accounts、daily_net_value 建表；`list_accounts`、`get_accounts_by_wallet_addresses`、`upsert_account`、`upsert_daily_net_value_batch`、`get_daily_net_value_in_range`、`get_latest_net_value_per_account`、`list_activities_in_range`。  
- **估值与日序列**：[src/valuation.rs](src/valuation.rs) — `compute_daily_series`（纯函数）、`recompute_and_upsert_daily_net_value`（读 activities + 写 daily_net_value）。  
- **同步后增量写**：[src/sync.rs](src/sync.rs) — `fetch_segment` 内 `insert_activities_batch` 之后，按本批 address 查账户并调用 `recompute_and_upsert_daily_net_value`。  
- **路由与懒聚合**：[src/api.rs](src/api.rs) — `/accounts`、`/dashboard/*` 的 handler；汇总指标均在 handler 内从 DB 读出后内存计算。

## 7. 注意事项

- **DATE 类型**：写入时用字符串 `YYYY-MM-DD`；若 DuckDB Rust 驱动读 DATE 非字符串，需在 SQL 中 `CAST(date AS VARCHAR)` 或读成整数再转日期字符串。  
- **活动类型**：Polymarket 若新增类型（如其他赎回/结算），需在 valuation 中统一视为「对净值有贡献」或「仅成交量」并在 `is_sell_or_redeem` / `is_trade_for_volume` 中扩展。  
- **月化收益**：当前为单月简单收益率；若需「按平均计算」的月化，使用 `(1 + R_total)^(1/n_months) - 1`，其中 R_total 为区间累计收益、n_months 为区间月数。

## 8. 前端预览

项目根目录下的 **dashboard.html** 为独立前端页面，用于调用仪表盘 API 并展示数据。

### 如何打开

- **方式一**：用浏览器直接打开本地文件 `dashboard.html`（`file://` 协议）。此时请求会发往后端，需保证后端已启用 CORS（见下）。
- **方式二**：通过任意静态服务器提供该文件，例如在项目根执行 `python3 -m http.server 8080`，然后访问 `http://localhost:8080/dashboard.html`。

### 配置 API 地址

- 页面顶部有「API 地址」输入框，默认值为 `http://localhost:7001`（与后端默认端口一致）。
- 修改后点击「保存」，会写入 `localStorage`，下次打开自动带出。
- 点击「加载数据」会使用当前输入的地址重新请求 summary、净值走势、账户明细。

### CORS 说明

- 后端已在 **main.rs** 中通过 `tower_http::cors::CorsLayer` 允许任意来源（`Allow-Origin: *`）、任意方法与请求头，便于前端从其他端口或域名访问。
- 若部署时需限制来源，可改为 `.allow_origin(Origin::from("https://your-frontend-domain.com"))` 等。
