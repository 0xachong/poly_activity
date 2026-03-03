//! 单钱包同步：按 start→end 分批拉取 Polymarket Activity，限流、去重写入。
//! 官方 API 仅返回 REDEEM（兑付/领奖），无 CLAIM 类型。

use crate::config::Config;
use crate::db::{self, ActivityRow};
use crate::rate_limit::RateLimiter;
use crate::valuation;
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
    #[serde(rename = "usdcSize")]
    usdc_size: Option<f64>,
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    price: Option<f64>,
    asset: Option<String>,
    side: Option<String>,
    title: Option<String>,
    outcome: Option<String>,
}

/// 存款/提款不落库，仅用于 API 拉取时的过滤
fn is_deposit_or_withdraw(type_str: Option<&str>) -> bool {
    let u = type_str.unwrap_or("").to_uppercase();
    matches!(
        u.as_str(),
        "DEPOSIT" | "DEPOSITS" | "WITHDRAW" | "WITHDRAWAL" | "WITHDRAWALS"
    )
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

        // 注意：Polymarket Data API /activity 的 type 仅含 TRADE/SPLIT/MERGE/REDEEM/REWARD/CONVERSION/MAKER_REBATE，不包含 DEPOSIT/WITHDRAW，存款无法从此接口拉到
        let url = format!(
            "{}/activity?user={}&start={}&end={}&sortBy=TIMESTAMP&sortDirection=ASC&limit={}&excludeDepositsWithdrawals=true",
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
        let items: Vec<PolyActivityItem> = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                let preview = body.chars().take(300).collect::<String>();
                tracing::error!(err = %e, body_preview = %preview, "poly response JSON parse failed，本批数据未写入，可能漏数据");
                return Err(format!("poly response JSON parse failed: {}", e));
            }
        };
        let type_counts: std::collections::HashMap<String, usize> =
            items.iter().fold(std::collections::HashMap::new(), |mut m, i| {
                *m.entry(i.type_.clone().unwrap_or_default()).or_insert(0) += 1;
                m
            });
        tracing::info!(count = items.len(), types = ?type_counts, "poly response");

        if items.is_empty() {
            break;
        }

        // 统一按请求时的钱包地址 addr 写入，不按 API 返回的 proxy_wallet，否则按 account.wallet_address 查不到活动，资产/交易会对不上；存款/提款不存储
        let rows: Vec<ActivityRow> = items
            .into_iter()
            .filter(|i| !is_deposit_or_withdraw(i.type_.as_deref()))
            .map(|i| {
                let ts = i.timestamp.unwrap_or(0);
                let type_merged = match (i.type_.as_deref(), i.side.as_deref()) {
                    (Some("TRADE"), Some(s)) if s == "BUY" || s == "SELL" => s.to_string(),
                    (Some(t), _) => t.to_string(),
                    _ => String::new(),
                };
                ActivityRow {
                    address: addr.to_string(),
                    ts,
                    type_: type_merged,
                    share: i.size,
                    price: i.price,
                    usdc_size: i.usdc_size,
                    title: i.title,
                    outcome: i.outcome,
                    condition_id: i.condition_id,
                    token_id: i.asset,
                    transaction_hash: i.transaction_hash,
                    ts_utc: None,
                    effective_share: None,
                }
            })
            .collect();

        let n = rows.len() as u64;
        db::insert_activities_batch(db_path, &rows).map_err(|e| e.to_string())?;
        total_inserted += n;

        let max_ts = rows.iter().map(|r| r.ts).max().unwrap_or(batch_start);

        if n < limit as u64 {
            break;
        }
        // 下一批从 max_ts 开始（不 +1），避免同一秒有多条时漏拉；重复项由 ON CONFLICT DO NOTHING 去重
        batch_start = max_ts;
        if batch_start > seg_end {
            break;
        }
    }

    Ok(total_inserted)
}

/// 按请求区间 [from_ts, to_ts] 同步：取库中已有区间补缺口拉取并写入。
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

    // 同步后按时间序重算：REDEEM 的 effective_share 写入 activities，当前未平仓写入 open_positions
    if let Err(e) = recompute_effective_share_and_open_positions(db_path, &addr) {
        tracing::warn!(address = %addr, err = %e, "recompute effective_share and open_positions failed");
    }

    Ok(total)
}

/// 按该地址全量活动重算 REDEEM 的 effective_share 与当前未平仓，并落库。
fn recompute_effective_share_and_open_positions(db_path: &str, address: &str) -> Result<(), String> {
    let (min_ts, max_ts) = match db::get_ts_range(db_path, address).map_err(|e| e.to_string())? {
        Some(r) => r,
        None => return Ok(()),
    };
    let activities =
        db::list_activities_in_range(db_path, address, min_ts, max_ts, 5_000_000).map_err(|e| e.to_string())?;
    let (effective, open_positions) = valuation::effective_redeem_shares_and_open_positions(&activities);

    let updates: Vec<(String, String, String, f64)> = activities
        .iter()
        .enumerate()
        .filter_map(|(i, row)| {
            if row.type_.eq_ignore_ascii_case("REDEEM") {
                effective.get(i).copied().flatten().map(|eff| {
                    (
                        row.transaction_hash.as_deref().unwrap_or("").to_string(),
                        row.condition_id.as_deref().unwrap_or("").to_string(),
                        row.token_id.as_deref().unwrap_or("").to_string(),
                        eff,
                    )
                })
            } else {
                None
            }
        })
        .collect();

    let updates_ref: Vec<(&str, &str, &str, f64)> = updates
        .iter()
        .map(|(a, b, c, d)| (a.as_str(), b.as_str(), c.as_str(), *d))
        .collect();
    db::update_effective_share_for_redeems(db_path, address, &updates_ref).map_err(|e| e.to_string())?;
    db::replace_open_positions(db_path, address, &open_positions).map_err(|e| e.to_string())?;
    Ok(())
}
