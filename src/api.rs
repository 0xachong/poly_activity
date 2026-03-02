//! HTTP API：时间区间查询、紧凑 JSON

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

#[derive(Serialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

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

    sync::sync_range(
        &state.config,
        &state.config.duckdb_path,
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

    let (rows, total) = db::list_activities(
        &state.config.duckdb_path,
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
                type_: r.type_.clone(),
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

#[derive(Serialize)]
pub struct WalletItem {
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_activity_ts: Option<i64>,
}

#[derive(Serialize)]
pub struct WalletListResponse {
    pub data: Vec<WalletItem>,
}

/// 每个地址的缓存条数
#[derive(Serialize)]
pub struct AddressRecordCount {
    pub address: String,
    pub record_count: u64,
}

#[derive(Serialize)]
pub struct CacheByAddressResponse {
    pub data: Vec<AddressRecordCount>,
}

/// 缓存概况响应：地址数、起止时间（可读）、数据条数
#[derive(Serialize)]
pub struct CacheSummaryResponse {
    /// 已缓存的钱包地址数量
    pub address_count: u64,
    /// 缓存数据条数
    pub record_count: u64,
    /// 最早一条数据的时间（可读），无数据时为 null
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_start: Option<String>,
    /// 最晚一条数据的时间（可读），无数据时为 null
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_end: Option<String>,
}

async fn get_cache_summary(
    State(state): State<Arc<AppState>>,
) -> Result<Json<CacheSummaryResponse>, (StatusCode, Json<ErrorBody>)> {
    let summary = db::get_cache_summary(&state.config.duckdb_path).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                code: "DB_ERROR".into(),
                message: e.to_string(),
            }),
        )
    })?;
    let time_start = summary.min_ts.and_then(|ts| {
        chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
    });
    let time_end = summary.max_ts.and_then(|ts| {
        chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
    });
    Ok(Json(CacheSummaryResponse {
        address_count: summary.address_count,
        record_count: summary.record_count,
        time_start,
        time_end,
    }))
}

async fn get_cache_by_address(
    State(state): State<Arc<AppState>>,
) -> Result<Json<CacheByAddressResponse>, (StatusCode, Json<ErrorBody>)> {
    let rows = db::list_address_record_counts(&state.config.duckdb_path).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                code: "DB_ERROR".into(),
                message: e.to_string(),
            }),
        )
    })?;
    Ok(Json(CacheByAddressResponse {
        data: rows
            .into_iter()
            .map(|(address, record_count)| AddressRecordCount {
                address,
                record_count,
            })
            .collect(),
    }))
}

async fn get_wallets(
    State(state): State<Arc<AppState>>,
) -> Result<Json<WalletListResponse>, (StatusCode, Json<ErrorBody>)> {
    let rows = db::list_wallets(&state.config.duckdb_path).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                code: "DB_ERROR".into(),
                message: e.to_string(),
            }),
        )
    })?;
    Ok(Json(WalletListResponse {
        data: rows
            .into_iter()
            .map(|(a, ts)| WalletItem {
                address: a,
                latest_activity_ts: ts,
            })
            .collect(),
    }))
}

#[derive(Deserialize)]
pub struct RegisterWalletBody {
    pub address: String,
}

async fn post_wallets(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RegisterWalletBody>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let addr = body.address.to_lowercase();
    if addr.is_empty() || !addr.starts_with("0x") {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                code: "BAD_REQUEST".into(),
                message: "invalid address".into(),
            }),
        ));
    }
    db::register_wallet(&state.config.duckdb_path, &addr).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                code: "DB_ERROR".into(),
                message: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::CREATED)
}

/// GET /llms.txt — llms.txt 规范：面向 LLM 的本服务调用说明（Markdown）
async fn get_llms_txt() -> Response {
    let base = ""; // 相对路径，适用于任意 host/port
    let body = format!(
        r#"## API 端点

- [缓存概览]({base}/cache/summary): GET。返回缓存统计：地址数、记录数、时间范围（time_start/time_end）。
- [按地址缓存]({base}/cache/by-address): GET。各钱包地址及其记录条数。
- [钱包列表]({base}/wallets): GET。已缓存钱包列表及每地址最新活动时间戳。
- [注册钱包]({base}/wallets): POST。Body JSON: `{{"address":"0x..."}}`。将地址纳入缓存（后续活动查询时会按需同步）。
- [钱包活动]({base}/wallets/{{address}}/activity): GET。按时间区间查询某地址的 Polymarket 活动。查询参数：`from_ts`（必填，Unix 秒）、`to_ts`（可选，默认当前时间）、`limit`（可选，默认 50000）、`type`（可选，活动类型过滤）。返回紧凑 JSON：`total`、`from_time`/`to_time`、`data`（ts, type, share, price, title, outcome, condition_id, token_id, transaction_hash 等）。

## 使用说明

本服务为 Polymarket 钱包活动缓存后端，按查询触发与 Polymarket API 的同步，数据落 DuckDB。所有接口返回 JSON；时间戳均为 Unix 秒。活动接口首次查询某地址某时间区间时会自动拉取并写入缓存再返回。

> Polymarket 钱包活动缓存服务，提供按时间区间的活动查询与缓存概览。

# poly_activity
"#
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/markdown; charset=utf-8")
        .body(Body::from(body))
        .unwrap()
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/llms.txt", axum::routing::get(get_llms_txt))
        .route("/cache/summary", axum::routing::get(get_cache_summary))
        .route("/cache/by-address", axum::routing::get(get_cache_by_address))
        .route("/wallets", axum::routing::get(get_wallets).post(post_wallets))
        .route(
            "/wallets/:address/activity",
            axum::routing::get(get_wallet_activity),
        )
        .with_state(state)
}
