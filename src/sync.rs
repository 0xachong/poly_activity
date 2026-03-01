//! 单钱包同步：按 start→end 分批拉取 Polymarket Activity，限流、去重写入

use crate::config::Config;
use crate::db::{self, ActivityRow};
use crate::rate_limit::RateLimiter;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct PolyActivityItem {
    #[serde(rename = "proxyWallet")]
    proxy_wallet: Option<String>,
    timestamp: Option<i64>,
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    #[serde(rename = "type")]
    type_: Option<String>,
    size: Option<f64>,
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    price: Option<f64>,
    asset: Option<String>,
    side: Option<String>,
    title: Option<String>,
    outcome: Option<String>,
}

/// 拉取 [seg_start, seg_end] 从 Polymarket 并写入 DB，返回本段新增条数
async fn fetch_segment(
    config: &Config,
    db_path: &str,
    rate_limiter: &RateLimiter,
    client: &Client,
    addr: &str,
    seg_start: i64,
    seg_end: i64,
) -> Result<u64, String> {
    if seg_start > seg_end {
        return Ok(0);
    }
    let mut total_inserted: u64 = 0;
    let mut batch_start = seg_start;
    let timeout = Duration::from_secs(config.request_timeout_secs);
    let limit = config.fetch_batch_limit as u32;

    loop {
        rate_limiter.acquire().await;

        let url = format!(
            "{}/activity?user={}&start={}&end={}&sortBy=TIMESTAMP&sortDirection=ASC&limit={}&excludeDepositsWithdrawals=false",
            config.poly_api_base.trim_end_matches('/'),
            addr,
            batch_start,
            seg_end,
            limit
        );
        tracing::info!(url = %url, "poly request");

        let resp = client
            .get(&url)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{}", e);
                tracing::warn!(err = %e, url = %url, "poly request failed");
                msg
            })?;

        if resp.status().as_u16() == 429 {
            let retry_after = resp
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(2);
            tokio::time::sleep(Duration::from_secs(retry_after)).await;
            continue;
        }

        let body = resp.text().await.map_err(|e| {
            let msg = format!("{}", e);
            tracing::warn!(err = %e, "poly response body failed");
            msg
        })?;
        let items: Vec<PolyActivityItem> = serde_json::from_str(&body).unwrap_or_default();
        let type_counts: std::collections::HashMap<String, usize> =
            items.iter().fold(std::collections::HashMap::new(), |mut m, i| {
                *m.entry(i.type_.clone().unwrap_or_default()).or_insert(0) += 1;
                m
            });
        tracing::info!(count = items.len(), types = ?type_counts, "poly response");

        if items.is_empty() {
            break;
        }

        let rows: Vec<ActivityRow> = items
            .into_iter()
            .map(|i| {
                let ts = i.timestamp.unwrap_or(0);
                let addr_use = i.proxy_wallet.as_deref().unwrap_or(addr).to_string();
                let type_merged = match (i.type_.as_deref(), i.side.as_deref()) {
                    (Some("TRADE"), Some(s)) if s == "BUY" || s == "SELL" => s.to_string(),
                    (Some(t), _) => t.to_string(),
                    _ => String::new(),
                };
                ActivityRow {
                    address: addr_use.to_lowercase(),
                    ts,
                    type_: type_merged,
                    share: i.size,
                    price: i.price,
                    title: i.title,
                    outcome: i.outcome,
                    condition_id: i.condition_id,
                    token_id: i.asset,
                    transaction_hash: i.transaction_hash,
                    ts_utc: None,
                }
            })
            .collect();

        let n = rows.len() as u64;
        db::insert_activities_batch(db_path, &rows).map_err(|e| e.to_string())?;
        total_inserted += n;

        let max_ts = rows.iter().map(|r| r.ts).max().unwrap_or(batch_start);
        db::upsert_wallet_ts(db_path, addr, max_ts).map_err(|e| e.to_string())?;

        if n < limit as u64 {
            break;
        }
        batch_start = max_ts + 1;
        if batch_start > seg_end {
            break;
        }
    }

    Ok(total_inserted)
}

/// 按请求区间 [from_ts, to_ts] 同步：取库中已有区间 [from1, to1]，先拉左缺口 [from_ts, from1)，再拉右缺口 (to1, to_ts]，最后查库即得三段合并结果。
pub async fn sync_range(
    config: &Config,
    db_path: &str,
    rate_limiter: &RateLimiter,
    client: &Client,
    address: &str,
    from_ts: i64,
    to_ts: i64,
) -> Result<u64, String> {
    let addr = address.to_lowercase();
    if from_ts > to_ts {
        return Ok(0);
    }

    let range = db::get_ts_range(db_path, &addr).map_err(|e| e.to_string())?;
    let mut total = 0u64;

    match range {
        None => {
            total += fetch_segment(config, db_path, rate_limiter, client, &addr, from_ts, to_ts).await?;
        }
        Some((from1, to1)) => {
            if to_ts < from1 {
                total += fetch_segment(config, db_path, rate_limiter, client, &addr, from_ts, to1).await?;
            } else if from_ts > to1 {
                total += fetch_segment(config, db_path, rate_limiter, client, &addr, to1, to_ts).await?;
            } else {
                if from_ts < from1 {
                    total += fetch_segment(config, db_path, rate_limiter, client, &addr, from_ts, from1).await?;
                }
                if to_ts > to1 {
                    total += fetch_segment(config, db_path, rate_limiter, client, &addr, to1, to_ts).await?;
                }
            }
        }
    }

    Ok(total)
}
