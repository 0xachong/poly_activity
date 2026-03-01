//! DuckDB 存储：schema、钱包进度、Activity 读写

use duckdb::{params, Connection, OptionalExt, Result};
use std::path::Path;

fn ts_to_utc_str(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_default()
}

/// 单条 Activity 记录（与表结构一致）
#[derive(Clone, Debug)]
pub struct ActivityRow {
    pub address: String,
    pub ts: i64,
    pub type_: String,
    pub share: Option<f64>,
    pub price: Option<f64>,
    pub title: Option<String>,
    pub outcome: Option<String>,
    pub condition_id: Option<String>,
    pub token_id: Option<String>,
    pub transaction_hash: Option<String>,
    pub ts_utc: Option<String>,
}

fn open(path: &str) -> Result<Connection> {
    let parent = Path::new(path).parent();
    if let Some(p) = parent {
        let _ = std::fs::create_dir_all(p);
    }
    Connection::open(path)
}

/// 初始化 schema，若表已存在则跳过
pub fn init_schema(path: &str) -> Result<()> {
    let conn = open(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS wallets (
            address VARCHAR PRIMARY KEY,
            latest_activity_ts BIGINT,
            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS activities (
            address VARCHAR,
            ts BIGINT,
            type VARCHAR,
            share DOUBLE,
            price DOUBLE,
            title VARCHAR,
            outcome VARCHAR,
            condition_id VARCHAR,
            token_id VARCHAR,
            transaction_hash VARCHAR,
            ts_utc VARCHAR,
            PRIMARY KEY (address, transaction_hash, condition_id, token_id)
        );
        CREATE INDEX IF NOT EXISTS idx_activities_address_ts ON activities(address, ts);",
    )?;
    Ok(())
}

/// 取该地址在库中的 ts 区间 (min, max)；无记录返回 None
pub fn get_ts_range(path: &str, address: &str) -> Result<Option<(i64, i64)>> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    let min_ts: Option<i64> = conn.query_row(
        "SELECT min(ts) FROM activities WHERE address = ?",
        [&addr],
        |r| r.get::<_, Option<i64>>(0),
    )?;
    let max_ts: Option<i64> = conn.query_row(
        "SELECT max(ts) FROM activities WHERE address = ?",
        [&addr],
        |r| r.get::<_, Option<i64>>(0),
    )?;
    Ok(match (min_ts, max_ts) {
        (Some(mi), Some(ma)) => Some((mi, ma)),
        _ => None,
    })
}

/// 取该地址已缓存的最大时间戳；无记录返回 None
pub fn get_latest_ts(path: &str, address: &str) -> Result<Option<i64>> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    let out: Option<Option<i64>> = conn
        .query_row(
            "SELECT latest_activity_ts FROM wallets WHERE address = ?",
            [&addr],
            |r| r.get::<_, Option<i64>>(0),
        )
        .optional()?;
    if let Some(Some(ts)) = out {
        return Ok(Some(ts));
    }
    // max(ts) 无记录时为 NULL，需按 Option 读取
    let max_ts: Option<i64> = conn.query_row(
        "SELECT max(ts) FROM activities WHERE address = ?",
        [&addr],
        |r| r.get::<_, Option<i64>>(0),
    )?;
    Ok(max_ts)
}

/// 更新钱包的 latest_activity_ts
pub fn upsert_wallet_ts(path: &str, address: &str, ts: i64) -> Result<()> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    conn.execute(
        "INSERT INTO wallets (address, latest_activity_ts, created_at, updated_at)
         VALUES (?, ?, now(), now())
         ON CONFLICT (address) DO UPDATE SET latest_activity_ts = excluded.latest_activity_ts, updated_at = now()",
        params![addr, ts],
    )?;
    Ok(())
}

/// 批量写入 Activity，唯一冲突则忽略
pub fn insert_activities_batch(path: &str, rows: &[ActivityRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let conn = open(path)?;
    for r in rows {
        let addr = r.address.to_lowercase();
        let cond_id = r.condition_id.as_deref().unwrap_or("");
        let tok_id = r.token_id.as_deref().unwrap_or("");
        let ts_utc = r.ts_utc.clone().unwrap_or_else(|| ts_to_utc_str(r.ts));
        let _ = conn.execute(
            "INSERT INTO activities (address, ts, type, share, price, title, outcome, condition_id, token_id, transaction_hash, ts_utc)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (address, transaction_hash, condition_id, token_id) DO NOTHING",
            params![
                addr,
                r.ts,
                r.type_,
                r.share,
                r.price,
                r.title.as_deref(),
                r.outcome.as_deref(),
                cond_id,
                tok_id,
                r.transaction_hash.as_deref(),
                &ts_utc,
            ],
        );
    }
    Ok(())
}

/// 按时间区间查询：ts BETWEEN from_ts AND to_ts，倒序，取前 limit 条；type_filter 为空表示不过滤
pub fn list_activities(
    path: &str,
    address: &str,
    from_ts: i64,
    to_ts: i64,
    limit: u32,
    type_filter: Option<&str>,
) -> Result<(Vec<ActivityRow>, u64)> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    let limit_i = limit.min(50_000) as i64;

    let total: i64 = if let Some(t) = type_filter {
        conn.query_row(
            "SELECT COUNT(*) FROM activities WHERE address = ? AND ts >= ? AND ts <= ? AND type = ?",
            params![addr, from_ts, to_ts, t],
            |r| r.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT COUNT(*) FROM activities WHERE address = ? AND ts >= ? AND ts <= ?",
            params![addr, from_ts, to_ts],
            |r| r.get(0),
        )?
    };

    let rows: Vec<ActivityRow> = if let Some(t) = type_filter {
        let mut stmt = conn.prepare(
            "SELECT address, ts, type, share, price, title, outcome, condition_id, token_id, transaction_hash, ts_utc FROM activities WHERE address = ? AND ts >= ? AND ts <= ? AND type = ? ORDER BY ts DESC LIMIT ?",
        )?;
        stmt.query_map(params![addr, from_ts, to_ts, t, limit_i], map_row)?.collect::<Result<Vec<_>>>()?
    } else {
        let mut stmt = conn.prepare(
            "SELECT address, ts, type, share, price, title, outcome, condition_id, token_id, transaction_hash, ts_utc FROM activities WHERE address = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC LIMIT ?",
        )?;
        stmt.query_map(params![addr, from_ts, to_ts, limit_i], map_row)?.collect::<Result<Vec<_>>>()?
    };

    Ok((rows, total as u64))
}

fn map_row(row: &duckdb::Row) -> Result<ActivityRow> {
    Ok(ActivityRow {
        address: row.get(0)?,
        ts: row.get(1)?,
        type_: row.get(2)?,
        share: row.get(3)?,
        price: row.get(4)?,
        title: row.get(5)?,
        outcome: row.get(6)?,
        condition_id: row.get(7)?,
        token_id: row.get(8)?,
        transaction_hash: row.get(9)?,
        ts_utc: row.get(10)?,
    })
}

/// 列出所有已登记/已缓存钱包
pub fn list_wallets(path: &str) -> Result<Vec<(String, Option<i64>)>> {
    let conn = open(path)?;
    let mut stmt = conn.prepare("SELECT address, latest_activity_ts FROM wallets ORDER BY updated_at DESC")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?)))?.collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// 登记钱包（仅 INSERT，若已存在则忽略）
pub fn register_wallet(path: &str, address: &str) -> Result<()> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    conn.execute(
        "INSERT INTO wallets (address, latest_activity_ts) VALUES (?, NULL) ON CONFLICT (address) DO NOTHING",
        [&addr],
    )?;
    Ok(())
}
