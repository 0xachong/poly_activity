//! HTTP API：仅两个接口——某钱包某段时间历史交易、指定区间每日交易额与已实现利润

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::Response,
    Json, Router,
};
use axum::body::Body;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::Config;
use crate::db;
use crate::rate_limit::RateLimiter;
use crate::sync;
use crate::valuation;
use reqwest::Client;

pub struct AppState {
    pub config: Config,
    pub rate_limiter: Arc<RateLimiter>,
    pub client: Client,
}

#[derive(Debug, Deserialize)]
pub struct ActivityQuery {
    pub from_ts: Option<i64>,
    pub to_ts: Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub r#type: Option<String>,
    /// 为 true 时先清空该钱包本地数据，再从 Polymarket 从 0 重拉后返回
    #[serde(default)]
    pub force_refresh: bool,
}

fn default_limit() -> u32 {
    50_000
}

#[derive(Serialize)]
pub struct ActivityItemCompact {
    pub ts: i64,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_hash: Option<String>,
}

#[derive(Serialize)]
pub struct ActivityResponse {
    pub total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_time: Option<String>,
    pub data: Vec<ActivityItemCompact>,
}

#[derive(Debug, Deserialize)]
pub struct BatchActivityBody {
    pub addresses: Vec<String>,
    pub from_ts: i64,
    #[serde(default)]
    pub to_ts: Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub force_refresh: bool,
}

#[derive(Serialize)]
pub struct WalletActivityItem {
    pub address: String,
    pub total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_time: Option<String>,
    pub data: Vec<ActivityItemCompact>,
}

#[derive(Serialize)]
pub struct BatchActivityResponse {
    pub data: Vec<WalletActivityItem>,
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

/// GET /wallets/:address/activity — 某钱包某段时间历史交易；force_refresh=true 时清空后从 Polymarket 重拉
async fn get_wallet_activity(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(q): Query<ActivityQuery>,
) -> Result<Json<ActivityResponse>, (StatusCode, Json<ErrorBody>)> {
    let from_ts = q.from_ts.ok_or((
        StatusCode::BAD_REQUEST,
        Json(ErrorBody {
            code: "BAD_REQUEST".into(),
            message: "missing from_ts".into(),
        }),
    ))?;
    let to_ts = q.to_ts.unwrap_or_else(|| chrono::Utc::now().timestamp());

    let addr = address.to_lowercase();
    if addr.is_empty() || !addr.starts_with("0x") {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                code: "BAD_REQUEST".into(),
                message: "invalid address".into(),
            }),
        ));
    }

    let path = &state.config.duckdb_path;
    if q.force_refresh {
        let n = db::delete_activities_for_address(path, &addr).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorBody {
                    code: "DB_ERROR".into(),
                    message: e.to_string(),
                }),
            )
        })?;
        tracing::info!(address = %addr, deleted = n, "force_refresh: cleared activities");
        sync::sync_range(
            &state.config,
            path,
            state.rate_limiter.as_ref(),
            &state.client,
            &addr,
            0,
            to_ts,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody {
                    code: "SYNC_ERROR".into(),
                    message: e,
                }),
            )
        })?;
    } else {
        sync::sync_range(
            &state.config,
            path,
            state.rate_limiter.as_ref(),
            &state.client,
            &addr,
            from_ts,
            to_ts,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody {
                    code: "SYNC_ERROR".into(),
                    message: e,
                }),
            )
        })?;
    }

    let (rows, total) = db::list_activities(
        path,
        &addr,
        from_ts,
        to_ts,
        q.limit,
        q.r#type.as_deref(),
    )
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                code: "DB_ERROR".into(),
                message: e.to_string(),
            }),
        )
    })?;

    let d: Vec<ActivityItemCompact> = rows
        .into_iter()
        .map(|r| ActivityItemCompact {
            ts: r.ts,
            type_: r.type_,
            share: r.share,
            price: r.price,
            title: r.title,
            outcome: r.outcome,
            condition_id: r.condition_id,
            token_id: r.token_id,
            transaction_hash: r.transaction_hash,
        })
        .collect();

    let from_time = d.iter().map(|x| x.ts).min().and_then(|ts| {
        chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
    });
    let to_time = d.iter().map(|x| x.ts).max().and_then(|ts| {
        chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
    });

    Ok(Json(ActivityResponse {
        total,
        from_time,
        to_time,
        data: d,
    }))
}

/// POST /wallets/activity — 批量钱包历史交易；body.addresses 多个地址，同一 from_ts/to_ts；force_refresh 对所有地址生效
async fn post_wallets_activity(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BatchActivityBody>,
) -> Result<Json<BatchActivityResponse>, (StatusCode, Json<ErrorBody>)> {
    let to_ts = body.to_ts.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let path = &state.config.duckdb_path;
    let mut out = Vec::with_capacity(body.addresses.len());
    for address in &body.addresses {
        let addr = address.trim().to_lowercase();
        if addr.is_empty() || !addr.starts_with("0x") {
            continue;
        }
        if body.force_refresh {
            let _ = db::delete_activities_for_address(path, &addr);
            if let Err(e) = sync::sync_range(
                &state.config,
                path,
                state.rate_limiter.as_ref(),
                &state.client,
                &addr,
                0,
                to_ts,
            )
            .await
            {
                tracing::warn!(address = %addr, err = %e, "batch force_refresh sync failed");
            }
        } else if let Err(e) = sync::sync_range(
            &state.config,
            path,
            state.rate_limiter.as_ref(),
            &state.client,
            &addr,
            body.from_ts,
            to_ts,
        )
        .await
        {
            tracing::warn!(address = %addr, err = %e, "batch sync failed");
        }
        let (rows, total) = db::list_activities(
            path,
            &addr,
            body.from_ts,
            to_ts,
            body.limit,
            None,
        )
        .unwrap_or((vec![], 0));
        let d: Vec<ActivityItemCompact> = rows
            .into_iter()
            .map(|r| ActivityItemCompact {
                ts: r.ts,
                type_: r.type_,
                share: r.share,
                price: r.price,
                title: r.title,
                outcome: r.outcome,
                condition_id: r.condition_id,
                token_id: r.token_id,
                transaction_hash: r.transaction_hash,
            })
            .collect();
        let from_time = d.iter().map(|x| x.ts).min().and_then(|ts| {
            chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        });
        let to_time = d.iter().map(|x| x.ts).max().and_then(|ts| {
            chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        });
        out.push(WalletActivityItem {
            address: addr,
            total,
            from_time,
            to_time,
            data: d,
        });
    }
    Ok(Json(BatchActivityResponse { data: out }))
}

#[derive(Debug, Deserialize)]
pub struct DailyStatsQuery {
    pub wallet: Option<String>,
    pub address: Option<String>,
    pub from_date: String,
    pub to_date: String,
}

#[derive(Serialize)]
pub struct DailyStatsPoint {
    pub date: String,
    /// 当日交易额，仅统计买入金额
    pub volume: f64,
    /// 当日已实现利润，仅考虑卖出/兑付的赚亏（收入 − 对应成本）
    pub profit: f64,
}

/// 单钱包的每日统计
#[derive(Serialize)]
pub struct WalletDailyStats {
    pub wallet: String,
    pub daily: Vec<DailyStatsPoint>,
}

#[derive(Serialize)]
pub struct DailyStatsResponse {
    /// 单钱包时仅一个元素；多钱包时按请求顺序返回
    pub data: Vec<WalletDailyStats>,
}

/// GET /daily-stats — 指定钱包（可逗号分隔多个）、日期区间内每日交易额（仅 BUY）与已实现利润
async fn get_daily_stats(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DailyStatsQuery>,
) -> Result<Json<DailyStatsResponse>, (StatusCode, Json<ErrorBody>)> {
    let raw = q
        .wallet
        .or(q.address)
        .ok_or((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                code: "BAD_REQUEST".into(),
                message: "missing wallet or address".into(),
            }),
        ))?;
    let addresses: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty() && s.starts_with("0x"))
        .collect();
    if addresses.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                code: "BAD_REQUEST".into(),
                message: "invalid address(es)".into(),
            }),
        ));
    }

    let path = &state.config.duckdb_path;
    let from_ts = 0i64;
    let to_ts = chrono::NaiveDate::parse_from_str(q.to_date.trim(), "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(23, 59, 59))
        .map(|ndt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp());
    let from_date = q.from_date.trim().to_string();
    let to_date = q.to_date.trim().to_string();

    let mut data = Vec::with_capacity(addresses.len());
    for addr in addresses {
        let activities = db::list_activities_in_range(path, &addr, from_ts, to_ts, 5_000_000).unwrap_or_default();
        let daily = valuation::compute_daily_volume_and_realized_pnl(&activities, &from_date, &to_date);
        let daily_points: Vec<DailyStatsPoint> = daily
            .into_iter()
            .map(|(date, volume, profit)| DailyStatsPoint {
                date,
                volume,
                profit,
            })
            .collect();
        data.push(WalletDailyStats {
            wallet: addr,
            daily: daily_points,
        });
    }
    Ok(Json(DailyStatsResponse { data }))
}

#[derive(Debug, Deserialize)]
pub struct BatchDailyStatsBody {
    pub wallets: Vec<String>,
    pub from_date: String,
    pub to_date: String,
}

/// POST /daily-stats — 批量钱包聚合数据（每日交易额与已实现利润），Body: wallets、from_date、to_date
async fn post_daily_stats(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BatchDailyStatsBody>,
) -> Result<Json<DailyStatsResponse>, (StatusCode, Json<ErrorBody>)> {
    let addresses: Vec<String> = body
        .wallets
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty() && s.starts_with("0x"))
        .collect();
    if addresses.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                code: "BAD_REQUEST".into(),
                message: "wallets required and must be valid 0x addresses".into(),
            }),
        ));
    }

    let path = &state.config.duckdb_path;
    let from_ts = 0i64;
    let to_ts = chrono::NaiveDate::parse_from_str(body.to_date.trim(), "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(23, 59, 59))
        .map(|ndt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp());
    let from_date = body.from_date.trim().to_string();
    let to_date = body.to_date.trim().to_string();

    let mut data = Vec::with_capacity(addresses.len());
    for addr in addresses {
        let activities = db::list_activities_in_range(path, &addr, from_ts, to_ts, 5_000_000).unwrap_or_default();
        let daily = valuation::compute_daily_volume_and_realized_pnl(&activities, &from_date, &to_date);
        let daily_points: Vec<DailyStatsPoint> = daily
            .into_iter()
            .map(|(date, volume, profit)| DailyStatsPoint {
                date,
                volume,
                profit,
            })
            .collect();
        data.push(WalletDailyStats {
            wallet: addr,
            daily: daily_points,
        });
    }
    Ok(Json(DailyStatsResponse { data }))
}

/// GET /llms.txt — 面向 LLM 的本服务说明（Markdown）
async fn get_llms_txt() -> Response {
    let base = "";
    let body = format!(
        r#"## poly_activity API

Polymarket 钱包活动缓存后端，仅两个接口。

### 1. 历史交易

- **GET** `{base}/wallets/{{address}}/activity`
- Query: `from_ts`（必填）、`to_ts`、`limit`、`type`、`force_refresh`（可选，true 时清空该钱包本地数据并从 Polymarket 重拉）
- 返回: `total`、`from_time`/`to_time`、`data`（ts, type, share, price, title, outcome, condition_id, token_id, transaction_hash 等）

### 2. 每日交易额与利润（聚合）

- **GET** `{base}/daily-stats` — Query: `wallet` 或 `address`（可逗号分隔）、`from_date`、`to_date`
- **POST** `{base}/daily-stats` — Body: `{{ "wallets": ["0x...", "0x..."], "from_date": "2025-01-01", "to_date": "2025-01-31" }}`
- 返回: `data`: [{{ wallet, daily: [{{ date, volume（仅买入）, profit（仅卖出赚亏） }}] }}]

基地址示例: http://localhost:7001
"#
    );
    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "text/markdown; charset=utf-8")
        .body(Body::from(body))
        .unwrap()
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/llms.txt", axum::routing::get(get_llms_txt))
        .route(
            "/wallets/:address/activity",
            axum::routing::get(get_wallet_activity),
        )
        .route("/daily-stats", axum::routing::get(get_daily_stats).post(post_daily_stats))
        .with_state(state)
}
