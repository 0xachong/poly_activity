# poly_activity

Polymarket 钱包 Activity 缓存后端：按查询触发同步、时间区间 API、紧凑 JSON。

## 构建与运行

```bash
cargo build --release   # 首次构建含 DuckDB bundled 会较慢
./target/release/poly_activity
```

若之前已有 `data/activity_cache.duckdb`，表结构升级后需删除该文件再启动，否则会沿用旧表结构。

## 环境变量（可选）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `POLY_API_BASE` | `https://data-api.polymarket.com` | Polymarket Data API 根地址 |
| `DUCKDB_PATH` | `./data/activity_cache.duckdb` | DuckDB 文件路径 |
| `RATE_LIMIT_MAX_REQUESTS` | `950` | 每窗口最大请求数 |
| `RATE_LIMIT_WINDOW_SECS` | `10` | 限流窗口（秒） |
| `HTTP_PORT` | `8080` | HTTP 监听端口 |
| `REQUEST_TIMEOUT_SECS` | `30` | 请求 Polymarket 超时 |
| `FETCH_BATCH_LIMIT` | `500` | 单批拉取条数 |

## HTTP API

### 按时间区间查某地址 Activity（主入口，会先触发同步）

```http
GET /wallets/:address/activity?from_ts=0&to_ts=9999999999&limit=100
```

- **from_ts**（必填）、**to_ts**（可选）：Unix 秒；不传 to_ts 时使用当前时间。
- **limit**（可选）：单页条数，默认 50000。
- **type**（可选）：按活动类型过滤，如 `TRADE`、`REDEEM`。

响应 JSON：`{ "total", "from_time", "to_time", "data": [...] }`，其中 `from_time`/`to_time` 为数组内最小/最大时间戳的可读时间（如 `2024-01-15 12:30:45 UTC`），单条字段为 `ts`、`type`、`share`、`price`、`title`、`outcome`、`condition_id`、`token_id`、`transaction_hash`。其中 `type` 对交易会细化为 `BUY` / `SELL`（原 TRADE + side），其余类型保持原样（如 REDEEM、SPLIT 等）。

### 列出已缓存钱包

```http
GET /wallets
```

### 登记钱包（仅写入，不触发同步）

```http
POST /wallets
Content-Type: application/json
{ "address": "0x..." }
```

## 示例

```bash
# 查某地址 2024 年活动（先同步再查）
curl "http://127.0.0.1:8080/wallets/0xYourAddress/activity?from_ts=1704067200&to_ts=1735689599&limit=50"
```
