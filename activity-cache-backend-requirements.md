# 钱包 Activity 缓存后端 — 需求文档

> 独立维护的后端服务，用于缓存 Polymarket 钱包地址的 Activity。**同步策略：仅在对某个钱包发起查询时才触发该钱包的 Activity 数据同步**。若数据库中无该钱包的任何记录，则从 start=0 同步到当前时间；若有记录，则从库内该地址最大时间戳（latest_activity_ts）开始同步到当前时间。

---

## 1. 目标与背景

- **目标**：为多个钱包地址维护一份 **Activity 缓存**，避免重复请求 Polymarket API，并支持按需增量更新。
- **数据源**：Polymarket Data API（如 `https://data-api.polymarket.com/activity` 或 `/v1/activity`），按 `user`（钱包地址）和时间区间查询。
- **同步范围**：**所有类型的 Activity 事件均需同步**（含 TRADE、REDEEM、存取款等），不按类型过滤、不排除任何事件。
- **约束**：Polymarket 在**同一时间区间内**最多返回 **3000 条** 数据，需通过「正序 + 移动起始时间」的方式分批拉取；Data API **限流**为 **1,000 请求 / 10 秒**（见 3.5），多钱包并发同步时需全局限流。

---

## 2. 功能需求

### 2.1 同步触发策略（按需同步）

- **触发时机**：仅当**查询某个钱包**的 Activity 时，才触发该钱包的 Activity 数据同步（不预拉、不按定时任务全量扫）。
- **无记录**（数据库中无该地址或该地址无任何 Activity 记录）：
  - **起始时间 start = 0**（或平台支持的最早时间戳）。
  - **结束时间 end = 当前时间**。
  - 全量拉取并写入存储，并记录该地址的**最新 Activity 时间戳**（`latest_activity_ts`），供后续增量使用。
- **有记录**（数据库中已有该地址的 Activity）：
  - **起始时间 start** = 数据库中该地址**已存储的最大时间戳**（即 `latest_activity_ts` 或从 activities 表按该 address 取 `max(ts)`）。
  - **结束时间 end = 当前时间**。
  - 仅拉取 `(start, end]` 区间的数据，写入存储并更新「最新 Activity 时间戳」。

### 2.2 钱包登记（可选）

- 可选支持**登记**钱包地址（如提前录入待查地址）；未登记时，首次查询该地址会按「无记录」策略触发同步并落库。

### 2.3 数据存储与去重

- 拉取与存储时**不按事件类型过滤**，所有 Activity 事件（交易、赎回、存取款等）均需同步并持久化。
- 所有拉取到的 Activity 需**持久化**到本服务使用的数据库中。
- 单条 Activity 在业务上由「钱包地址 + 交易哈希 + conditionId + tokenId」等唯一标识，**写入时需去重**，避免重复插入（如断点续传、重复触发同步时）。

### 2.4 查询能力

- 支持按**钱包地址**查询该地址的**已缓存 Activity 列表**。
- **查询时先按 2.1 策略触发该钱包的同步**（无记录则 0→now，有记录则 latest_activity_ts→now），再返回当前已缓存数据。
- 支持**分页**（如 `limit` + `offset` 或游标），便于前端或其它服务消费。
- 可选：按时间范围、类型（TRADE / REDEEM 等）过滤。

---

## 3. 与 Polymarket API 的交互规则

### 3.1 单次请求限制

- 在**同一组** `start` / `end` 参数下，API 最多返回 **3000 条** 数据（不论 `offset` 多大，超过 3000 即无更多数据）。

### 3.2 拉取策略（满足 3000 条限制）

- **排序**：请求参数使用**按时间正序**（`sortBy=TIMESTAMP`, `sortDirection=ASC`），即从早到晚。
- **时间区间**：每次请求的 `start` = 本批起始时间，`end` = 本批结束时间（首次全量时 end = 当前时间，增量时 end = 当前时间）。
- **分页方式**：
  1. 在**同一 [start, end]** 内用 `offset` 分批请求（如每批 100 或 500 条），直到本区间内拿满 3000 条或接口返回条数 &lt; 每批大小。
  2. 取本批中**最大时间戳** `T_max`，下一批的 **start = T_max + 1**（避免重复），end 不变。
  3. 重复直到某次返回 0 条，表示该 [start, end] 已拉完。

### 3.3 请求参数要点（参考）

- `user`: 钱包地址（0x...）
- `start` / `end`: Unix 时间戳（秒）
- `sortBy`: `TIMESTAMP`
- `sortDirection`: `ASC`
- `limit`: 单次请求条数（如 500）
- `offset`: 偏移
- `excludeDepositsWithdrawals`: 须为 **false**（或等效：不排除），以拉取包含存取款在内的**全部** Activity 事件

### 3.4 无记录时「起始时间为 0」

- 当**查询某钱包且库中无该地址任何 Activity 记录**时，同步的 **start = 0**（若 API 对 0 有特殊处理，则改用平台支持的最小时间戳，如 2025-01-01 00:00:00 UTC 对应的时间戳）。

### 3.5 限流（Rate Limit）— 按官方策略设计

- **官方限制**（Polymarket Data API）：
  - Base URL: `https://data-api.polymarket.com`
  - **General：1,000 请求 / 10 秒**（全站该 Base URL 下所有端点共享此配额）。
- **并发模型**：不同钱包同步时，**每个钱包一个请求线程/任务**，但所有请求都发往同一 Data API，因此必须按**全局限流**控制，不能按钱包各自限流。
- **限流策略**：
  - **全局限流器**：进程内**唯一**的限流实例，所有对 `data-api.polymarket.com` 的请求在发出前必须先通过该限流器**扣减配额**（acquire），再发 HTTP 请求。
  - **窗口与配额**：采用**滑动窗口**或**固定窗口**（10 秒），在任意连续 10 秒内请求数不超过 1,000。可选预留余量（如 950 请求 / 10 秒）以降低边界踩线导致 429 的概率。
  - **实现要点**：多线程共享同一限流器（如 Mutex + 滑动窗口计数，或 token bucket：每 10 秒补充 1000 个 token，每次请求消耗 1 token，不足时阻塞或返回“需等待”由调用方重试/排队）。
- **请求流程**：`同步线程发起请求 → 向全局限流器 acquire → 成功则发请求；若被限流则等待或短暂 sleep 后重试`。
- **429 处理**：若 API 返回 **429 Too Many Requests**，应**退避重试**（如 exponential backoff 或读取 `Retry-After` 头），并避免在退避期间重复占用限流配额；重试前再次 acquire 限流器。

---

## 4. 存储选型：DuckDB

- **选用 DuckDB** 作为唯一存储：嵌入式、列存、无独立进程，与后端服务同一进程内运行，部署时无额外依赖。
- **形态**：单文件（如 `activity_cache.duckdb`）或指定数据目录，备份/迁移即拷贝文件。
- **Rust**：通过 `duckdb` crate 嵌入，打开同一路径即复用已有数据；单写多读，批量 `INSERT` 与按时间范围查询均高效。

---

## 5. 数据模型（逻辑 + DuckDB）

### 5.1 钱包（用于记录同步进度）

| 字段 | 类型（建议） | 说明 |
|------|----------------|------|
| address | VARCHAR PRIMARY KEY | 钱包地址，统一小写 |
| latest_activity_ts | BIGINT | 该地址已缓存的最新 Activity 时间戳（秒），用于下次增量 start |
| created_at | TIMESTAMP | 首次注册时间 |
| updated_at | TIMESTAMP | 最近一次同步完成时间 |

### 5.2 Activity（单条记录）

与 Polymarket 返回字段对齐，列存下按时间范围查询友好：

| 字段 | 类型（建议） | 说明 |
|------|----------------|------|
| address | VARCHAR | 钱包地址 |
| ts | BIGINT | 活动时间戳（秒） |
| type | VARCHAR | 如 TRADE, REDEEM 等 |
| side | VARCHAR | BUY / SELL（若有） |
| size | DOUBLE | 数量 |
| usdc_size | DOUBLE | USDC 金额（若有） |
| price | DOUBLE | 价格（若有） |
| title | VARCHAR | 市场命题（若有） |
| outcome | VARCHAR | 选项（若有） |
| condition_id | VARCHAR | 条件 ID |
| token_id | VARCHAR | 代币 ID |
| transaction_hash | VARCHAR | 交易哈希 |

**唯一约束**：`(address, transaction_hash, condition_id, token_id)` 或等价组合，保证去重；插入时使用 `INSERT ... ON CONFLICT DO NOTHING` 或先查再插，避免重复。

### 5.3 DuckDB 建表示例（参考）

```sql
-- 钱包同步进度
CREATE TABLE wallets (
  address VARCHAR PRIMARY KEY,
  latest_activity_ts BIGINT,
  created_at TIMESTAMP DEFAULT current_timestamp,
  updated_at TIMESTAMP DEFAULT current_timestamp
);

-- Activity 缓存（列存，按 ts 查询多时可考虑按 address 分区或仅依赖索引）
CREATE TABLE activities (
  address VARCHAR,
  ts BIGINT,
  type VARCHAR,
  side VARCHAR,
  size DOUBLE,
  usdc_size DOUBLE,
  price DOUBLE,
  title VARCHAR,
  outcome VARCHAR,
  condition_id VARCHAR,
  token_id VARCHAR,
  transaction_hash VARCHAR,
  PRIMARY KEY (address, transaction_hash, condition_id, token_id)
);

-- 按地址 + 时间范围查询时，对 (address, ts) 建索引更佳
CREATE INDEX idx_activities_address_ts ON activities(address, ts);
```

---

## 6. API 设计（建议）

### 6.1 查询某地址的 Activity（主入口，触发同步）

- **GET** `/wallets/:address/activity`
- Query：`limit`、`offset`（或 `before_ts` 游标）；可选 `type`、`from_ts`、`to_ts`。
- **行为**：先按 2.1 策略对该地址做一次同步（库无记录则 start=0→now，有记录则 start=库内最大 ts→now），再返回当前已缓存的 Activity 列表。
- 返回：Activity 列表（JSON），按时间倒序或正序可约定。

### 6.2 登记钱包（可选）

- **POST** `/wallets`
- Body: `{ "address": "0x..." }`
- 行为：仅登记地址，不主动拉取；实际同步在首次通过 6.1 查询时触发。

### 6.3 手动触发某地址同步（可选）

- **POST** `/wallets/:address/sync`
- 行为：start = 该地址在库中的 `latest_activity_ts`（无则 0），end = 当前时间；拉取后写入并更新 `latest_activity_ts`。
- 返回：本次新增条数或简要状态。若希望严格「仅查询时同步」，可省略此接口。

### 6.4 列出已登记/已缓存钱包（可选）

- **GET** `/wallets`
- 返回：地址列表及可选的 `latest_activity_ts`、`updated_at`。

---

## 7. 非功能需求（建议）

- **配置**：Polymarket API 根地址、请求超时、每批 limit、代理（如需要）等可配置（环境变量或配置文件）；DuckDB 文件路径可配置（如 `DUCKDB_PATH=./data/activity_cache.duckdb`）；**限流参数**可配置（如 `RATE_LIMIT_MAX_REQUESTS=950`、`RATE_LIMIT_WINDOW_SECS=10`），便于在不改代码的前提下微调余量。
- **幂等与错误**：同一地址并发同步时，应通过唯一约束与「以 latest_activity_ts 为起点」保证数据不重复、不丢；单次同步失败可保留上次 `latest_activity_ts`，下次重试继续。
- **部署**：单机运行或容器化；**仅依赖 DuckDB 嵌入式库**，无独立数据库进程，备份即拷贝 DuckDB 文件。

---

## 8. 总结

| 触发时机 | 条件 | 起始时间 start | 结束时间 end |
|----------|------|----------------|--------------|
| **查询某钱包时** | 库中无该地址/无任何 Activity 记录 | 0（或最小支持时间戳） | 当前时间 |
| **查询某钱包时** | 库中已有该地址的 Activity 记录 | 库内该地址最大时间戳（latest_activity_ts） | 当前时间 |

- **同步仅在对某个钱包发起查询时触发**，不做预拉或定时全量同步。
- 拉取时始终使用 **时间正序 + 每区间最多 3000 条 + 用本批最大时间作为下一批起点** 的方式与 Polymarket API 交互；本服务只负责缓存、去重、按地址查询与分页，为后续 Rust 实现提供清晰需求边界。
