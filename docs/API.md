# poly_activity API 列表

基地址示例：`http://127.0.0.1:7001`（以实际 `HTTP_PORT` 为准）。响应均为 JSON。

---

## 1. 获取某钱包某段时间的历史交易记录

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/wallets/:address/activity` | 某钱包在时间区间内的活动记录。Query：`from_ts`（必填）、`to_ts`（可选，默认当前）、`limit`（可选，默认 50000）、`type`（可选，按类型过滤）、**`force_refresh`**（可选，为 `true` 时先清空该钱包本地数据，再从 Polymarket 从 0 重拉后返回）。返回 `total`、`from_time`/`to_time`、`data`（ts, type, share, price, title, outcome, condition_id, token_id, transaction_hash 等） |

---

## 2. 获取某钱包当前未平仓 token 列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/wallets/:address/open-positions` | 该钱包当前未平仓 token。Path：`address`（0x 地址）。返回 `address`、`total`、`data`：[{ `token_id`, `condition_id`, `share` }]。数据在每次同步后按活动顺序重算维护。 |

---

## 3. 获取指定时间区间内每日交易额与利润（聚合数据）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/daily-stats` | 单钱包或逗号分隔多钱包。Query：`wallet` 或 `address`（必填，可 `0x1,0x2`）、`from_date`、`to_date`。返回 `data`: [{ `wallet`, `daily`: [{ `date`, `volume`, `profit` }] }] |
| POST | `/daily-stats` | **批量钱包**。Body: `{ "wallets": ["0x...", "0x..."], "from_date": "2025-01-01", "to_date": "2025-01-31" }`。返回同 GET：`data` 按钱包顺序，每项含 `wallet`、`daily`（每日 `volume` 仅买入、`profit` 仅卖出赚亏） |

---

## 小结

- **历史交易**：`GET /wallets/:address/activity`，可选 `force_refresh` 清空重拉。
- **未平仓**：`GET /wallets/:address/open-positions`，返回当前未平仓 token 列表。
- **每日统计**：`GET /daily-stats?wallet=0x...&from_date=...&to_date=...`，得到每日 volume（仅 BUY）与 profit（仅卖出赚亏）。

前端页面已移至 `frontend/` 目录（dashboard、account-detail、account-manage）。
