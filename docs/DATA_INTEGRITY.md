# 后端数据完整性说明（是否漏数据）

## 结论概览

| 风险点 | 是否会导致漏数据 | 当前状态 / 缓解 |
|--------|------------------|------------------|
| API 响应 JSON 解析失败 | **会**：整批静默丢弃 | 已修复：解析失败时打 error 日志并返回错误，不再静默吞掉 |
| 分页同一秒多条只拉一批 | **会**：同一秒超过一页时漏后面的 | 已修复：下一批从 `max_ts` 开始（不 +1），重复由 DB 去重 |
| 已覆盖时间区间不再拉 | **会**：区间内后来新增的 REDEEM 等不会补拉 | 已缓解：提供按 `condition_ids` 的 sync_by_market 强制补拉某市场 |
| 主键冲突 DO NOTHING | **可能**：同 (tx_hash, condition_id, token_id) 只保留一条 | 依赖 API 唯一性；REDEEM 常无 token_id，多笔同 condition 同 tx 理论上会丢一条 |
| type 未识别写成空串 | 不丢行，但**不计入净值/成交量** | 仅当 API 返回非 TRADE/REDEEM 等已知 type 时发生，需看日志 |
| **DEPOSIT/WITHDRAW 不在接口内** | **会**：存款/提款不会被拉到 | Polymarket Data API `/activity` 的 type 仅有 TRADE/SPLIT/MERGE/REDEEM/REWARD/CONVERSION/MAKER_REBATE，**不包含 DEPOSIT/WITHDRAW**；存款由 Bridge 等其它系统处理，当前无法从 activity 同步到 |

---

## 1. API 响应解析失败（已修复）

- **位置**：`src/sync.rs` 中 `fetch_segment`、`sync_by_market` 的 `serde_json::from_str(&body)`。
- **原逻辑**：解析失败时 `unwrap_or_default()` 得到空 Vec，**整批不写入且无日志**，等于静默漏数据。
- **现逻辑**：解析失败时记录 `tracing::error`（含错误和 body 前 300 字符），并 `return Err(...)`，上层会打 warn，便于发现并排查。

---

## 2. 分页导致同一秒多条漏拉（已修复）

- **位置**：`src/sync.rs` 的 `fetch_segment` 分页循环。
- **原逻辑**：每批取 `limit` 条（如 500），下一批 `batch_start = max_ts + 1`。若同一秒内有多条（如多条 REDEEM），且数量 ≥ 一页，则同一秒内排在后面的会永远不被请求到。
- **现逻辑**：下一批使用 `batch_start = max_ts`（不 +1），同一秒的数据会再被拉一次，重复写入由表主键 `ON CONFLICT DO NOTHING` 去重，不会漏。

---

## 3. 已覆盖时间区间不会重拉（设计如此，有补救手段）

- **位置**：`sync_range` 只补「左缺口」和「右缺口」，中间区间 `(from1, to1]` 不会再次请求 API。
- **影响**：若某次同步时 API 尚未返回某条 REDEEM（例如延迟或事后补数据），之后常规 sync 不会重拉这段区间，会一直缺这条。
- **缓解**：对已知缺数据的市场，用「按市场强制补拉」：
  - `POST /sync/run` body 里传 `condition_ids: ["0x..."]`，会对每个账户按该 condition_id 再拉一遍该市场全部活动并写入。

---

## 4. 主键冲突 (address, transaction_hash, condition_id, token_id)

- **位置**：`src/db.rs` 的 `insert_activities_batch`，`ON CONFLICT ... DO NOTHING`。
- **影响**：同一主键只保留第一次插入的那条。若 API 对「同一笔链上操作」返回多条结构不同但主键相同的记录（或我们映射后主键相同），会只存一条。
- **REDEEM**：很多 REDEEM 的 `token_id` 为空，主键为 `(addr, tx_hash, condition_id, "")`。同一 condition、同一 tx、两条 REDEEM 且都无 token_id 时，会丢一条。实际是否发生取决于 Polymarket API 是否保证主键唯一。

---

## 5. type 未识别（type_ 为空串）

- **位置**：`src/sync.rs` 里对 `(type_, side)` 的 match，未命中时 `type_merged = String::new()`。
- **影响**：该行仍会写入 DB，但 `valuation` 里 `is_realized_cash("")`、`is_trade_for_volume("")` 为 false，**不会计入净值与成交量**，看起来像「有记录但没数」。
- **建议**：若发现某类活动没参与统计，查日志里 `types = ?type_counts` 是否有未识别的 type，再决定是否扩展 match。

---

## 如何自检是否漏数据

1. **看日志**  
   - 搜索 `poly response JSON parse failed`：若有，说明曾整批解析失败，该批未写入。  
   - 看每次 poly 请求的 `types = ?type_counts`，确认是否有 REDEEM/TRADE 等预期类型。

2. **对单市场补拉**  
   - 对怀疑缺 REDEEM 的市场，用该市场的 condition_id（0x 格式）调一次：  
     `POST /sync/run` body: `{"condition_ids":["0x..."]}`  
   - 再查该账户在该市场的活动条数 / 导出，对比预期。

3. **和链上或 Polymarket 前端对比**  
   - 选几笔已知的 REDEEM/TRADE，按 transaction_hash 或 condition_id 在 DB 里查是否存在、条数是否一致。

---

## 小结

- **会直接导致漏数据的**：JSON 解析失败（已改为报错+日志）、分页同一秒多条（已改为用 max_ts 不 +1）。
- **可能漏的**：主键冲突时后写入的被忽略；已覆盖时间区间内 API 事后才有的数据，需用 `condition_ids` 补拉。
- **不会丢行但可能不参与统计的**：type 未识别写成空串，需根据日志扩展类型映射。

若你提供具体缺数据的 condition_id 或 transaction_hash，可以再针对那条链路逐段对一下 API 与 DB。

---

## 6. 存款/提款（DEPOSIT/WITHDRAW）无法从 Activity 接口获取

- **原因**：Polymarket 官方 [Data API `/activity`](https://docs.polymarket.com/api-reference/core/get-user-activity) 的 `type` 枚举只包含：TRADE、SPLIT、MERGE、REDEEM、REWARD、CONVERSION、MAKER_REBATE，**没有 DEPOSIT 或 WITHDRAW**。存款/提款走的是 Bridge 等其它接口，当前同步只调 `/activity`，因此**抓不到任何存款或提款记录**。
- **表现**：某钱包（如 jinlai03 / 0x2890977d1ed98f38c6611c33b4529a5ee9832283）在 Polymarket 有入金，但库里没有对应 type 为 DEPOSIT 的活动，净值里也就没有这笔“入金”的 USDC 增加。
- **可选方案**：若官方后续在 Data API 中提供存款/提款活动，可再加一类请求或参数拉取并写入；或若有单独的 Bridge/Deposit 查询接口，可另写同步逻辑并入同一套净值计算。
