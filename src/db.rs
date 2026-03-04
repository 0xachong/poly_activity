//! DuckDB 存储：activities 表、open_positions 表及读写

use duckdb::{params, Connection, Result};
use fs4::FileExt;
use std::path::Path;

/// 跨进程写锁：打开 path.lock 并加排他锁，返回的 File 在 drop 时自动解锁。
/// 多实例写同一 DuckDB 文件时必须串行，仅进程内 Mutex 不够。
/// 供 sync 层在整段 sync_range 内持有一把锁时使用，减少锁竞争。
pub(crate) fn open_write_lock(path: &str) -> std::io::Result<std::fs::File> {
    let lock_path = format!("{}.lock", path);
    if let Some(p) = Path::new(path).parent() {
        let _ = std::fs::create_dir_all(p);
    }
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&lock_path)?;
    f.lock_exclusive()?;
    Ok(f)
}

fn io_to_duckdb(e: std::io::Error) -> duckdb::Error {
    duckdb::Error::ToSqlConversionFailure(Box::new(e))
}

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
    /// REDEEM 时兑付前持仓（同步时写入），展示用
    pub effective_share: Option<f64>,
}

/// 未平仓 token：address + token_id 唯一，同步时按活动顺序维护
#[derive(Clone, Debug)]
pub struct OpenPositionRow {
    pub address: String,
    pub token_id: String,
    pub condition_id: String,
    pub share: f64,
}

fn open(path: &str) -> Result<Connection> {
    let parent = Path::new(path).parent();
    if let Some(p) = parent {
        let _ = std::fs::create_dir_all(p);
    }
    Connection::open(path)
}

/// 初始化 schema：activities 表、open_positions 表
pub fn init_schema(path: &str) -> Result<()> {
    let _guard = open_write_lock(path).map_err(io_to_duckdb)?;
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
        ALTER TABLE activities ADD COLUMN IF NOT EXISTS usdc_size DOUBLE;
        ALTER TABLE activities ADD COLUMN IF NOT EXISTS effective_share DOUBLE;
        CREATE TABLE IF NOT EXISTS open_positions (
            address VARCHAR,
            token_id VARCHAR,
            condition_id VARCHAR,
            share DOUBLE,
            PRIMARY KEY (address, token_id)
        );
        CREATE INDEX IF NOT EXISTS idx_open_positions_address ON open_positions(address);",
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

/// 删除该地址下全部 activities 及 open_positions（用于 force_refresh 时清空后重拉）
pub fn delete_activities_for_address(path: &str, address: &str) -> Result<u64> {
    let _guard = open_write_lock(path).map_err(io_to_duckdb)?;
    let conn = open(path)?;
    let addr = address.to_lowercase();
    let _ = conn.execute("DELETE FROM open_positions WHERE address = ?", [&addr])?;
    let n = conn.execute("DELETE FROM activities WHERE address = ?", [&addr])?;
    Ok(n as u64)
}

fn ts_to_utc_str(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_default()
}

/// 批量写入 Activity（内部实现，不加锁）。调用方需已持有 open_write_lock 或仅单写场景使用。
fn insert_activities_batch_impl(path: &str, rows: &[ActivityRow]) -> Result<()> {
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

/// 批量写入 Activity，唯一冲突则更新
pub fn insert_activities_batch(path: &str, rows: &[ActivityRow]) -> Result<()> {
    let _guard = open_write_lock(path).map_err(io_to_duckdb)?;
    insert_activities_batch_impl(path, rows)
}

/// 批量写入 Activity（不加锁）。仅当调用方已持有 open_write_lock 时使用，如 sync_range 内。
pub(crate) fn insert_activities_batch_unlocked(path: &str, rows: &[ActivityRow]) -> Result<()> {
    insert_activities_batch_impl(path, rows)
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
            "SELECT address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash, ts_utc, effective_share FROM activities WHERE address = ? AND ts >= ? AND ts <= ? AND type = ? ORDER BY ts DESC LIMIT ?",
        )?;
        stmt.query_map(params![addr, from_ts, to_ts, t, limit_i], map_row)?.collect::<Result<Vec<_>>>()?
    } else {
        let mut stmt = conn.prepare(
            "SELECT address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash, ts_utc, effective_share FROM activities WHERE address = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC LIMIT ?",
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
        effective_share: row.get(12)?,
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
        "SELECT address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash, ts_utc, effective_share
         FROM activities WHERE address = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC LIMIT ?",
    )?;
    let rows = stmt
        .query_map(params![addr, from_ts, to_ts, limit_i], map_row)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// 更新一批 REDEEM 行的 effective_share（内部实现，不加锁）。
fn update_effective_share_for_redeems_impl(
    path: &str,
    address: &str,
    updates: &[(&str, &str, &str, f64)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let conn = open(path)?;
    let addr = address.to_lowercase();
    for (tx, cond, tok, eff) in updates.iter() {
        conn.execute(
            "UPDATE activities SET effective_share = ? WHERE address = ? AND COALESCE(transaction_hash,'') = ? AND COALESCE(condition_id,'') = ? AND COALESCE(token_id,'') = ?",
            params![eff, &addr, tx, cond, tok],
        )?;
    }
    Ok(())
}

/// 更新一批 REDEEM 行的 effective_share（由同步后重算写入）。key 为 (transaction_hash, condition_id, token_id)，空用 ""。
pub fn update_effective_share_for_redeems(
    path: &str,
    address: &str,
    updates: &[(&str, &str, &str, f64)],
) -> Result<()> {
    let _guard = open_write_lock(path).map_err(io_to_duckdb)?;
    update_effective_share_for_redeems_impl(path, address, updates)
}

/// 更新一批 REDEEM 行的 effective_share（不加锁）。仅当调用方已持有 open_write_lock 时使用。
pub(crate) fn update_effective_share_for_redeems_unlocked(
    path: &str,
    address: &str,
    updates: &[(&str, &str, &str, f64)],
) -> Result<()> {
    update_effective_share_for_redeems_impl(path, address, updates)
}

/// 查询该地址当前未平仓 token 列表
pub fn list_open_positions(path: &str, address: &str) -> Result<Vec<OpenPositionRow>> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    let mut stmt = conn.prepare(
        "SELECT address, token_id, condition_id, share FROM open_positions WHERE address = ? ORDER BY token_id",
    )?;
    let rows = stmt
        .query_map([&addr], |row| {
            Ok(OpenPositionRow {
                address: row.get(0)?,
                token_id: row.get(1)?,
                condition_id: row.get(2)?,
                share: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// 替换该地址的未平仓列表（内部实现，不加锁）。
fn replace_open_positions_impl(
    path: &str,
    address: &str,
    positions: &[(String, String, f64)],
) -> Result<()> {
    let conn = open(path)?;
    let addr = address.to_lowercase();
    conn.execute("DELETE FROM open_positions WHERE address = ?", [&addr])?;
    for (token_id, condition_id, share) in positions.iter() {
        if share.abs() < 1e-9 {
            continue;
        }
        conn.execute(
            "INSERT INTO open_positions (address, token_id, condition_id, share) VALUES (?, ?, ?, ?)",
            params![&addr, token_id, condition_id, share],
        )?;
    }
    Ok(())
}

/// 替换该地址的未平仓列表（同步后重算：先删后插）
pub fn replace_open_positions(
    path: &str,
    address: &str,
    positions: &[(String, String, f64)],
) -> Result<()> {
    let _guard = open_write_lock(path).map_err(io_to_duckdb)?;
    replace_open_positions_impl(path, address, positions)
}

/// 替换该地址的未平仓列表（不加锁）。仅当调用方已持有 open_write_lock 时使用。
pub(crate) fn replace_open_positions_unlocked(
    path: &str,
    address: &str,
    positions: &[(String, String, f64)],
) -> Result<()> {
    replace_open_positions_impl(path, address, positions)
}
