# API 简化规格与可清除功能审计清单

## 一、保留的两个接口规格

### 1. 获取某钱包某段时间的历史交易记录

- **路径**：`GET /wallets/:address/activity`（或统一改为 `GET /activity` + query 传 `wallet`，由你定）
- **路径参数**：`address` — 钱包地址（0x...）
- **Query 参数**：
  - `from_ts`（必填）：起始时间戳，Unix 秒
  - `to_ts`（可选）：结束时间戳，默认当前时间
  - `limit`（可选）：条数上限，默认 50000
  - `type`（可选）：按活动类型过滤，如 BUY、SELL
  - **`force_refresh`（可选）**：为 `true` 时，先**清除该钱包在 DB 中的 activities**，再从 Polymarket 从 0 拉取到 to_ts，再返回本次查询区间内的数据
- **响应**：JSON
  - `total`: 符合区间的总条数
  - `from_time`, `to_time`: 可读时间（若有数据）
  - `data`: 数组，每项含 `ts`, `type`, `share`, `price`, `title`, `outcome`, `condition_id`, `token_id`, `transaction_hash` 等

**说明**：若保留「按钱包」语义，路径用 `GET /wallets/:address/activity` 即可；若希望 REST 更中性，可改为 `GET /activity?wallet=0x...&from_ts=...&to_ts=...&force_refresh=true`。

---

### 2. 获取指定时间区间内「每日」的交易额与利润

- **路径**：`GET /daily-stats` 或 `GET /stats/daily`（需带钱包与时间区间）
- **Query 参数**：
  - `wallet` 或 `address`（必填）：钱包地址
  - `from_date`（必填）：起始日期，如 `2025-01-01`
  - `to_date`（必填）：结束日期，如 `2025-01-31`
- **响应**：JSON
  - `data`: 数组，按日一条
    - `date`: 日期 `YYYY-MM-DD`
    - **`volume`**：当日**交易额**，**只统计买入**（当日 BUY 的买入金额之和，即你现在的 `volume_contribution`）
    - **`profit`**：当日**利润**，**只考虑卖出后的赚/亏**（当日 SELL/REDEEM/CLAIM 的「变现收入 − 对应成本」之和；成本按该 token 卖出前的持仓成本算，即已实现盈亏）

**说明**：  
- 交易额 = 仅 BUY 的金额，现有 `valuation::volume_contribution` 已是只统计 BUY。  
- 利润 = 仅「卖出实现的盈亏」，需要按 token 追踪成本、在每次 SELL/REDEEM/CLAIM 时算 (收入 - 成本)，再按日汇总。当前代码里的 `pnl_delta` 是「总资产日变动 − 存提款」，包含未实现，若你要严格「只算卖出赚亏」，需要新增一个「每日已实现利润」的计算（例如在 valuation 或单独模块里按日汇总 sell 收入 − 对应成本）。

---

## 二、可清除功能审计清单

以下按「层级」列出；你审计后决定是否全部删除或保留个别项。

### A. HTTP 路由与 handler（api.rs）

| # | 路由 | 方法 | 说明 | 建议 |
|---|------|------|------|------|
| 1 | `/llms.txt` | GET | 面向 LLM 的服务说明 | 可删 |
| 2 | `/cache/summary` | GET | 缓存概览（地址数、条数、时间范围） | 可删 |
| 3 | `/cache/by-address` | GET | 按地址的缓存条数 | 可删 |
| 4 | `/wallets` | GET | 已缓存钱包列表 | 可删（若不再维护「钱包列表」概念） |
| 5 | `/wallets` | POST | 注册钱包 | 可删 |
| 6 | `/wallets/:address/activity` | GET | **历史交易记录**（保留并加 `force_refresh`） | **保留** |
| 7 | `/accounts` | GET | 账户列表（仅存储字段） | 可删 |
| 8 | `/accounts` | POST | 创建/更新单账户 | 可删 |
| 9 | `/accounts/batch` | POST | 批量创建账户 | 可删 |
| 10 | `/accounts/:account_id` | PATCH | 更新账户（如 initial_balance） | 可删 |
| 11 | `/accounts/:account_id` | DELETE | 删除账户 | 可删 |
| 12 | `/dashboard/summary` | GET | 全账户汇总（总资产、总盈亏等） | 可删 |
| 13 | `/dashboard/net-value` | GET | 多账户合并净值曲线 | 可删 |
| 14 | `/dashboard/accounts` | GET | 各账户卡片（total_value、total_pnl、monthly_roi） | 可删 |
| 15 | `/sync/run` | POST | 手动触发全量/指定账户同步 | 可删（拉数逻辑保留，由「历史交易」接口的 force_refresh 触发） |
| 16 | `/dashboard/refresh` | POST | 兼容用，无实际操作 | 可删 |
| 17 | `/dashboard/accounts/:id/summary` | GET | 单账户汇总（总盈亏、日/月回报、回撤） | 可删 |
| 18 | `/dashboard/accounts/:id/daily` | GET | 单账户每日 pnl_delta、volume | 可删（由「每日交易额+利润」新接口替代） |
| 19 | `/dashboard/accounts/:id/positions` | GET | 单账户当前持仓 | 可删 |
| 20 | `/dashboard/accounts/:id/debug` | GET | 诊断（activity 条数、deposit_withdraw、types） | 可删 |
| 21 | `/dashboard/accounts/:id/valuation-trace` | GET | 估值追踪（daily breakdown + cash_flow） | 可删 |

**小结**：只保留「历史交易记录」接口（并加 `force_refresh`），其余路由均可删；另新增「每日交易额+利润」接口。

---

### B. 前端页面与脚本

| # | 文件/入口 | 说明 | 建议 |
|---|-----------|------|------|
| 22 | `dashboard.html` | 仪表盘：汇总、净值图、账户卡片、同步按钮 | 可删（或仅保留一个「调两个 API 的简单测试页」） |
| 23 | `account-detail.html` | 单账户详情（总盈亏、日/月回报、回撤、每日、持仓） | 可删 |
| 24 | `account-manage.html` | 账户管理（列表、批量导入） | 可删 |
| 25 | `scripts/import_accounts.py` | 批量导入账户 | 可删 |
| 26 | `scripts/accounts_import.txt` | 导入用账户列表示例 | 可删 |
| 27 | `scripts/export_kent02_activities.py` | 导出 kent02 活动（若存在） | 可删或保留作调试 |
| 28 | `scripts/kent02_activities_by_time.txt` | 导出结果示例 | 可删 |

---

### C. 数据层（db.rs）

| # | 表/函数/概念 | 说明 | 建议 |
|---|--------------|------|------|
| 29 | 表 `wallets` | 钱包地址 + latest_activity_ts | 若不再有「钱包列表」接口可删；若 sync 仍要写进度可保留表或简化为「仅 activities」 |
| 30 | 表 `accounts` | account_id、wallet_address、display_name、initial_balance | 若完全不做多账户/仪表盘可删 |
| 31 | 表 `daily_net_value` | 预计算的每日净值缓存 | 若全部改为现场计算且无别处用可删 |
| 32 | `list_wallets` / `register_wallet` / `upsert_wallet_ts` | 钱包读写 | 若删 wallets 表则删 |
| 33 | `list_accounts` / `get_account_by_id` / `upsert_account` / `delete_account` 等 | 账户 CRUD | 若删 accounts 表则删 |
| 34 | `get_cache_summary` / `get_cache_by_address` | 缓存统计 | 可删 |
| 35 | `delete_activities_for_address` | 按地址清空 activities（供 force_refresh 用） | **保留** |
| 36 | `list_activities` / `list_activities_in_range` | 按时间区间查 activities | **保留**（历史交易 + 每日统计都依赖） |
| 37 | `insert_activities_batch` | 写入 activities | **保留**（sync 用） |
| 38 | 其他 daily_net_value / 账户相关查询 | 若不再用可删 | 按上面表是否保留决定 |

---

### D. 同步与配置（sync.rs / main.rs）

| # | 功能 | 说明 | 建议 |
|---|------|------|------|
| 39 | `sync::sync_range` | 按时间区间从 Polymarket 拉取并写入 | **保留**（历史交易接口在 force_refresh 时先清空再调它） |
| 40 | `sync::sync_by_market` | 按市场补拉（REDEEM 等） | 若你不再需要「按市场补拉」可删 |
| 41 | POST `/sync/run` 的 body（account_ids、resync_account_ids、condition_ids） | 多种同步策略 | 可删；force_refresh 仅「清空该钱包 + sync_range(0, now)」即可 |
| 42 | main 里 `--export-kent02` | 导出 kent02 活动到文件 | 可删或保留作调试 |
| 43 | main 里 `--trace-account=xxx` | 估值追踪打 JSON 到 stdout | 可删 |

---

### E. 估值与持仓（valuation.rs / positions.rs）

| # | 功能 | 说明 | 建议 |
|---|------|------|------|
| 44 | `valuation::compute_daily_series` 等 | 按日 net_value、pnl_delta、volume（总资产变动、BUY 额） | **部分保留**：只保留「按钱包 + 日期区间」算**每日 volume（仅 BUY）**；若新增「每日已实现利润」需在这里或新模块算 sell 收入 − 成本 |
| 45 | `valuation::compute_daily_series_for_wallet_with_deposit_sum` | 带存款提款合计、用于 total_value_visible | 若不做总余额/总盈亏展示可删 |
| 46 | 总盈亏基准 first_net、total_value_visible、max_drawdown、monthly_return | 仪表盘/账户汇总用 | 可删 |
| 47 | `positions::compute_positions` | 当前持仓（供净值、持仓页） | 若删持仓接口且不做「每日利润」里的成本可考虑删；若「每日利润」要算卖出成本则需保留持仓成本逻辑（或只在 valuation 里按 token 维护成本） |
| 48 | `positions::resolve_token_for_reduce` | REDEEM 无 token_id 时反查 token | 若保留 valuation 里对 REDEEM 的处理则保留 |

---

### F. 文档与配置

| # | 文件/内容 | 说明 | 建议 |
|---|-----------|------|------|
| 49 | `docs/API.md` | 当前完整 API 列表 | 简化为只描述两个保留接口 |
| 50 | `docs/AGGREGATION_LOGIC.md` | 聚合/净值/总盈亏逻辑 | 可删或缩成「每日 volume + 已实现利润」说明 |
| 51 | `docs/DATA_INTEGRITY.md` | 数据完整性 | 可删或保留作参考 |
| 52 | `docs/DASHBOARD_IMPLEMENTATION.md` | 仪表盘实现 | 可删 |
| 53 | `README.md` | 项目说明 | 更新为「仅两个接口 + 本地运行方式」 |

---

## 三、实现顺序建议

1. **先做接口与调用约定**  
   - 确定两个接口的最终路径与 query 命名（如 `force_refresh`、`from_date`/`to_date`）。  
   - 在「历史交易」接口中增加 `force_refresh`：为 true 时调用 `delete_activities_for_address` + `sync_range(0, now)`，再按原逻辑返回区间内数据。

2. **再实现「每日交易额 + 利润」**  
   - 交易额：直接复用 `volume_contribution`（仅 BUY）按日汇总。  
   - 利润：在 valuation 或新模块中，按时间顺序维护每 token 成本，对每条 SELL/REDEEM/CLAIM 算 (收入 − 成本)，再按日汇总为 `profit`。

3. **最后按审计结果删代码**  
   - 按上面清单逐项删除或保留；先删路由与 handler，再删 db/valuation/positions 中未引用部分，最后删前端与脚本、更新文档。

---

## 四、审计后你可回复的格式（示例）

- 「A 全部删；B 全部删；C 保留 35、36、37，其余删；D 保留 39，删 40–43；E 保留 44 中 volume 与 47/48 用于利润计算，其余删；F 按你建议。」  
我可以按你的勾选给出具体删改 patch 或分步修改说明。
