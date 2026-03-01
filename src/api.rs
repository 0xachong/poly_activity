//! HTTP API：时间区间查询、紧凑 JSON

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json, Router,
};
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

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/wallets", axum::routing::get(get_wallets).post(post_wallets))
        .route(
            "/wallets/:address/activity",
            axum::routing::get(get_wallet_activity),
        )
        .with_state(state)
}
