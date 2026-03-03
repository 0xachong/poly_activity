//! DuckDB 存储：仅 activities 表及读写

use duckdb::{params, Connection, Result};
use std::path::Path;

/// 单条 Activity 记录（与表结构一致）
#[derive(Clone, Debug)]
pub struct ActivityRow {
    pub address: String,
    pub ts: i64,
    pub type_: String,
    pub share: Option<f64>,
    pub price: Option<f64>,
    pub usdc_size: Option<f64>,
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

/// 初始化 schema：仅 activities 表
pub fn init_schema(path: &str) -> Result<()> {
    let conn = open(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS activities (
            address VARCHAR,
            ts BIGINT,
            type VARCHAR,
            share DOUBLE,
            price DOUBLE,
            usdc_size DOUBLE,
            title VARCHAR,
            outcome VARCHAR,
            condition_id VARCHAR,
            token_id VARCHAR,
            transaction_hash VARCHAR,
            ts_utc VARCHAR,
            PRIMARY KEY (address, transaction_hash, condition_id, token_id)
        );
        CREATE INDEX IF NOT EXISTS idx_activities_address_ts ON activities(address, ts);
        ALTER TABLE activities ADD COLUMN IF NOT EXISTS usdc_size DOUBLE;",
    )?;
    Ok(())
}

/// 该地址在库中的 ts 区间 (min, max)；无记录返回 None
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

/// 删除该地址下全部 activities（用于 force_refresh 时清空后重拉）
pub fn delete_activities_for_address(path: &str, address: &str) -> Result<u64> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    let n = conn.execute("DELETE FROM activities WHERE address = ?", [&addr])?;
    Ok(n as u64)
}

/// 批量写入 Activity，唯一冲突则更新
pub fn insert_activities_batch(path: &str, rows: &[ActivityRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let conn = open(path)?;
    fn ts_to_utc_str(ts: i64) -> String {
        chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_default()
    }
    for r in rows {
        let addr = r.address.to_lowercase();
        let cond_id = r.condition_id.as_deref().unwrap_or("");
        let tok_id = r.token_id.as_deref().unwrap_or("");
        let ts_utc = r.ts_utc.clone().unwrap_or_else(|| ts_to_utc_str(r.ts));
        let _ = conn.execute(
            "INSERT INTO activities (address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash, ts_utc)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (address, transaction_hash, condition_id, token_id) DO UPDATE SET
                ts = excluded.ts,
                type = excluded.type,
                share = COALESCE(share, excluded.share),
                price = CASE WHEN (price IS NULL OR price = 0) AND excluded.price IS NOT NULL THEN excluded.price ELSE price END,
                usdc_size = COALESCE(usdc_size, excluded.usdc_size),
                title = COALESCE(title, excluded.title),
                outcome = COALESCE(outcome, excluded.outcome),
                ts_utc = COALESCE(ts_utc, excluded.ts_utc)",
            params![
                addr,
                r.ts,
                r.type_,
                r.share,
                r.price,
                r.usdc_size,
                r.title.as_deref(),
                r.outcome.as_deref(),
                cond_id,
                tok_id,
                r.transaction_hash.as_deref(),
                &ts_utc,
            ],
        )?;
    }
    Ok(())
}

/// 按时间区间查询：ts 在 [from_ts, to_ts]，按 ts 倒序，最多 limit 条；type_filter 为空则不过滤
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
            "SELECT address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash, ts_utc FROM activities WHERE address = ? AND ts >= ? AND ts <= ? AND type = ? ORDER BY ts DESC LIMIT ?",
        )?;
        stmt.query_map(params![addr, from_ts, to_ts, t, limit_i], map_row)?.collect::<Result<Vec<_>>>()?
    } else {
        let mut stmt = conn.prepare(
            "SELECT address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash, ts_utc FROM activities WHERE address = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC LIMIT ?",
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
        usdc_size: row.get(5)?,
        title: row.get(6)?,
        outcome: row.get(7)?,
        condition_id: row.get(8)?,
        token_id: row.get(9)?,
        transaction_hash: row.get(10)?,
        ts_utc: row.get(11)?,
    })
}

/// 按时间区间查活动，按 ts 升序，limit 可大（供 valuation 用）
pub fn list_activities_in_range(
    path: &str,
    address: &str,
    from_ts: i64,
    to_ts: i64,
    limit: u32,
) -> Result<Vec<ActivityRow>> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    let limit_i = limit.min(5_000_000) as i64;
    let mut stmt = conn.prepare(
        "SELECT address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash, ts_utc
         FROM activities WHERE address = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC LIMIT ?",
    )?;
    let rows = stmt
        .query_map(params![addr, from_ts, to_ts, limit_i], map_row)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}
