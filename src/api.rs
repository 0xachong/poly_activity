//! HTTP API：仅两个接口——某钱包某段时间历史交易、指定区间每日交易额与已实现利润

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use axum::body::Body;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

use crate::config::Config;
use crate::db;
use crate::rate_limit::RateLimiter;
use crate::sync;
use crate::valuation;
use reqwest::Client;

// ---------- 写任务队列：单一 worker 串行执行所有写库，替代各处持锁 ----------

/// 需要独占写库的操作，由后台 worker 串行执行
pub enum WriteJob {
    /// 同步单个地址，完成后通过 oneshot 返回结果
    SyncOne {
        address: String,
        from_ts: i64,
        to_ts: i64,
        force_refresh: bool,
        respond: oneshot::Sender<Result<u64, String>>,
    },
    /// 同步多个地址（小批量），全部完成后返回 Ok(()) 或首个 Err
    SyncMany {
        items: Vec<(String, i64, i64, bool)>, // (address, from_ts, to_ts, force_refresh)
        respond: oneshot::Sender<Result<(), String>>,
    },
    /// 同步到昨日并聚合：用于 GET daily-stats 多地址；worker 会更新 JobState
    SyncManyUntilYesterday {
        items: Vec<String>, // addresses
        respond: oneshot::Sender<Result<(), String>>,
    },
    /// 批量 activity 任务（超限 202）：worker 内完成 sync + 读库 + 写 JobState
    BatchActivity {
        job_id: String,
        addresses: Vec<String>,
        from_ts: i64,
        to_ts: i64,
        limit: u32,
        force_refresh: bool,
    },
    /// 批量 daily_stats 任务（超限 202）
    BatchDailyStats {
        job_id: String,
        addresses: Vec<String>,
        from_date: String,
        to_date: String,
    },
}

/// 后台写 worker：从 channel 取任务并串行执行，无锁竞争
pub async fn run_write_worker(mut job_rx: mpsc::Receiver<WriteJob>, state: Arc<AppState>) {
    let config = &state.config;
    let path = &state.config.duckdb_path;
    let rate_limiter = state.rate_limiter.as_ref();
    let client = &state.client;

    while let Some(job) = job_rx.recv().await {
        match job {
            WriteJob::SyncOne {
                address,
                from_ts,
                to_ts,
                force_refresh,
                respond,
            } => {
                let addr = address.to_lowercase();
                let result = if force_refresh {
                    let _ = db::delete_activities_for_address(path, &addr);
                    sync::sync_range(config, path, rate_limiter, client, &addr, 0, to_ts).await
                } else {
                    sync::sync_range(config, path, rate_limiter, client, &addr, from_ts, to_ts).await
                };
                let _ = respond.send(result);
            }
            WriteJob::SyncMany { items, respond } => {
                let mut first_err: Option<String> = None;
                for (addr, from_ts, to_ts, force_refresh) in items {
                    let res = if force_refresh {
                        let _ = db::delete_activities_for_address(path, &addr);
                        sync::sync_range(config, path, rate_limiter, client, &addr, 0, to_ts).await
                    } else {
                        sync::sync_range(config, path, rate_limiter, client, &addr, from_ts, to_ts).await
                    };
                    if let Err(e) = res {
                        first_err = Some(e);
                        break;
                    }
                }
                let _ = respond.send(first_err.map_or(Ok(()), Err));
            }
            WriteJob::SyncManyUntilYesterday { items, respond } => {
                let sync_to_ts = end_of_yesterday_ts();
                let mut first_err: Option<String> = None;
                for addr in items {
                    if let Err(e) =
                        sync::sync_range(config, path, rate_limiter, client, &addr, 0, sync_to_ts).await
                    {
                        first_err = Some(e);
                        break;
                    }
                }
                let _ = respond.send(first_err.map_or(Ok(()), Err));
            }
            WriteJob::BatchActivity {
                job_id,
                addresses,
                from_ts,
                to_ts,
                limit,
                force_refresh,
            } => {
                let job_ref = match state.jobs.read().await.get(&job_id).cloned() {
                    Some(r) => r,
                    None => continue,
                };
                let mut out = Vec::with_capacity(addresses.len());
                for (i, addr) in addresses.iter().enumerate() {
                    if force_refresh {
                        let _ = db::delete_activities_for_address(path, addr);
                        if let Err(e) =
                            sync::sync_range(config, path, rate_limiter, client, addr, 0, to_ts).await
                        {
                            let mut j = job_ref.write().await;
                            j.status = JobStatus::Failed;
                            j.error = Some(e);
                            return;
                        }
                    } else if let Err(e) = sync::sync_range(config, path, rate_limiter, client, addr, from_ts, to_ts)
                        .await
                    {
                        let mut j = job_ref.write().await;
                        j.status = JobStatus::Failed;
                        j.error = Some(e);
                        return;
                    }
                    {
                        let mut j = job_ref.write().await;
                        j.completed = i + 1;
                        j.status = JobStatus::Running;
                    }
                    let (rows, total) =
                        db::list_activities(path, addr, from_ts, to_ts, limit, None).unwrap_or((vec![], 0));
                    let need_fallback =
                        rows.iter().any(|r| r.type_.eq_ignore_ascii_case("REDEEM") && r.effective_share.is_none());
                    let eff_ts = rows.iter().map(|r| r.ts).max().unwrap_or(to_ts);
                    let effective_redeem = if need_fallback {
                        effective_redeem_share_map(path, addr, eff_ts)
                    } else {
                        HashMap::new()
                    };
                    let d: Vec<ActivityItemCompact> = rows
                        .into_iter()
                        .map(|r| {
                            let share = if r.type_.eq_ignore_ascii_case("REDEEM") {
                                r.effective_share
                                    .or_else(|| {
                                        effective_redeem.get(&(
                                            r.ts,
                                            r.transaction_hash.clone(),
                                            r.condition_id.clone(),
                                            r.token_id.clone(),
                                        ))
                                        .copied()
                                    })
                                    .or(r.share)
                            } else {
                                r.share
                            };
                            ActivityItemCompact {
                                ts: r.ts,
                                type_: r.type_,
                                share,
                                price: r.price,
                                title: r.title,
                                outcome: r.outcome,
                                condition_id: r.condition_id,
                                token_id: r.token_id,
                                transaction_hash: r.transaction_hash,
                            }
                        })
                        .collect();
                    let from_time = d.iter().map(|x| x.ts).min().and_then(|ts| {
                        chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    });
                    let to_time = d.iter().map(|x| x.ts).max().and_then(|ts| {
                        chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    });
                    out.push(WalletActivityItem {
                        address: addr.clone(),
                        total,
                        from_time,
                        to_time,
                        data: d,
                    });
                }
                let response = BatchActivityResponse { data: out };
                if let Ok(v) = serde_json::to_value(&response) {
                    let mut j = job_ref.write().await;
                    j.result = Some(v);
                    j.status = JobStatus::Done;
                    j.message = None;
                }
            }
            WriteJob::BatchDailyStats {
                job_id,
                addresses,
                from_date,
                to_date,
            } => {
                let job_ref = match state.jobs.read().await.get(&job_id).cloned() {
                    Some(r) => r,
                    None => continue,
                };
                let sync_to_ts = end_of_yesterday_ts();
                let from_ts = 0i64;
                let to_ts = chrono::NaiveDate::parse_from_str(to_date.trim(), "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.and_hms_opt(23, 59, 59))
                    .map(|ndt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp())
                    .unwrap_or_else(|| chrono::Utc::now().timestamp());

                for (i, addr) in addresses.iter().enumerate() {
                    if let Err(e) =
                        sync::sync_range(config, path, rate_limiter, client, addr, 0, sync_to_ts).await
                    {
                        let mut j = job_ref.write().await;
                        j.status = JobStatus::Failed;
                        j.error = Some(e);
                        return;
                    }
                    let mut j = job_ref.write().await;
                    j.completed = i + 1;
                    j.status = JobStatus::Running;
                }
                let mut data = Vec::with_capacity(addresses.len());
                for addr in &addresses {
                    let activities =
                        db::list_activities_in_range(path, addr, from_ts, to_ts, 5_000_000).unwrap_or_default();
                    let daily =
                        valuation::compute_daily_volume_and_realized_pnl(&activities, &from_date, &to_date);
                    let daily_points: Vec<DailyStatsPoint> = daily
                        .into_iter()
                        .map(|(date, volume, profit)| DailyStatsPoint {
                            date,
                            volume,
                            profit,
                        })
                        .collect();
                    data.push(WalletDailyStats {
                        wallet: addr.clone(),
                        daily: daily_points,
                    });
                }
                let response = DailyStatsResponse { data };
                if let Ok(v) = serde_json::to_value(&response) {
                    let mut j = job_ref.write().await;
                    j.result = Some(v);
                    j.status = JobStatus::Done;
                    j.message = None;
                }
            }
        }
    }
}

/// 批量任务状态（地址数超过上限时异步处理，前端轮询进度）
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobState {
    pub id: String,
    pub kind: String, // "daily_stats" | "activity"
    pub total: usize,
    pub completed: usize,
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    pub created_at: i64,
}

pub struct AppState {
    pub config: Config,
    pub rate_limiter: Arc<RateLimiter>,
    pub client: Client,
    /// 批量任务：job_id -> 任务状态（超过 max_batch_addresses 时创建）
    pub jobs: Arc<RwLock<HashMap<String, Arc<RwLock<JobState>>>>>,
    /// 写任务队列：所有写库操作由此 channel 入队，由 run_write_worker 串行执行，无锁
    pub write_job_tx: mpsc::Sender<WriteJob>,
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

/// 昨日 23:59:59 UTC 的 Unix 时间戳。统计接口同步基础数据只拉到此时间，避免每次请求都拉当天数据。
fn end_of_yesterday_ts() -> i64 {
    let yesterday = (chrono::Utc::now() - chrono::Duration::days(1)).date_naive();
    let end = yesterday.and_hms_opt(23, 59, 59).unwrap();
    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(end, chrono::Utc).timestamp()
}

/// REDEEM 展示用：按历史持仓算出的有效份额。key = (ts, tx_hash, condition_id, token_id)
/// 性能：会拉取该地址 [min_ts, to_ts] 的全量活动（一次 list_activities_in_range），仅当本页含 REDEEM 时才调用。
fn effective_redeem_share_map(
    path: &str,
    address: &str,
    to_ts: i64,
) -> HashMap<(i64, Option<String>, Option<String>, Option<String>), f64> {
    let (min_ts, _) = match db::get_ts_range(path, address) {
        Ok(Some(range)) => range,
        _ => return HashMap::new(),
    };
    let activities = match db::list_activities_in_range(path, address, min_ts, to_ts, 5_000_000) {
        Ok(a) => a,
        Err(_) => return HashMap::new(),
    };
    let effective = valuation::effective_redeem_shares(&activities);
    let mut map = HashMap::new();
    for (i, row) in activities.iter().enumerate() {
        if row.type_.eq_ignore_ascii_case("REDEEM") {
            if let Some(s) = effective.get(i).copied().flatten() {
                let key = (
                    row.ts,
                    row.transaction_hash.clone(),
                    row.condition_id.clone(),
                    row.token_id.clone(),
                );
                map.insert(key, s);
            }
        }
    }
    map
}

/// 本页是否包含至少一条 REDEEM（用于决定是否拉全量算 effective_redeem，避免无 REDEEM 时的额外查询）
fn page_has_redeem(rows: &[db::ActivityRow]) -> bool {
    rows.iter().any(|r| r.type_.eq_ignore_ascii_case("REDEEM"))
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

/// 地址数超限时返回 202，前端轮询 progress_url 获取进度与结果
#[derive(Serialize)]
pub struct JobAccepted {
    pub job_id: String,
    pub message: String,
    pub progress_url: String,
}

#[derive(Serialize)]
pub struct OpenPositionItem {
    pub token_id: String,
    pub condition_id: String,
    pub share: f64,
}

#[derive(Serialize)]
pub struct OpenPositionsResponse {
    pub address: String,
    pub total: u64,
    pub data: Vec<OpenPositionItem>,
}

/// GET /wallets/:address/open-positions — 该钱包当前未平仓 token 列表（同步时维护）
async fn get_open_positions(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> Result<Json<OpenPositionsResponse>, (StatusCode, Json<ErrorBody>)> {
    let addr = address.trim().to_lowercase();
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
    let rows = db::list_open_positions(path, &addr).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                code: "DB_ERROR".into(),
                message: e.to_string(),
            }),
        )
    })?;
    let data: Vec<OpenPositionItem> = rows
        .into_iter()
        .map(|r| OpenPositionItem {
            token_id: r.token_id,
            condition_id: r.condition_id,
            share: r.share,
        })
        .collect();
    let total = data.len() as u64;
    Ok(Json(OpenPositionsResponse {
        address: addr,
        total,
        data,
    }))
}

/// GET /jobs/:job_id — 查询批量任务进度与结果（地址数超限时返回 202，前端轮询此接口）
async fn get_job_progress(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobState>, (StatusCode, Json<ErrorBody>)> {
    let jobs = state.jobs.read().await;
    let job_ref = jobs.get(&job_id).ok_or((
        StatusCode::NOT_FOUND,
        Json(ErrorBody {
            code: "NOT_FOUND".into(),
            message: "job not found or expired".into(),
        }),
    ))?;
    let job = job_ref.read().await.clone();
    Ok(Json(job))
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
    let (tx, rx) = oneshot::channel();
    state
        .write_job_tx
        .send(WriteJob::SyncOne {
            address: addr.clone(),
            from_ts,
            to_ts,
            force_refresh: q.force_refresh,
            respond: tx,
        })
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody {
                    code: "QUEUE_FULL".into(),
                    message: "write worker unavailable".into(),
                }),
            )
        })?;
    let sync_result = rx.await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody {
                code: "SYNC_ERROR".into(),
                message: "write worker dropped".into(),
            }),
        )
    })?;
    sync_result.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody {
                code: "SYNC_ERROR".into(),
                message: e,
            }),
        )
    })?;
    if q.force_refresh {
        tracing::info!(address = %addr, "force_refresh: sync completed");
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

    // 仅当本页有 REDEEM 且存在 effective_share 未落库的旧数据时才拉全量重算
    let need_redeem_fallback =
        page_has_redeem(&rows) && rows.iter().any(|r| r.type_.eq_ignore_ascii_case("REDEEM") && r.effective_share.is_none());
    let effective_redeem = if need_redeem_fallback {
        let eff_ts = rows.iter().map(|r| r.ts).max().unwrap_or(to_ts);
        effective_redeem_share_map(path, &addr, eff_ts)
    } else {
        HashMap::new()
    };

    let d: Vec<ActivityItemCompact> = rows
        .into_iter()
        .map(|r| {
            let share = if r.type_.eq_ignore_ascii_case("REDEEM") {
                r.effective_share
                    .or_else(|| {
                        effective_redeem.get(&(
                            r.ts,
                            r.transaction_hash.clone(),
                            r.condition_id.clone(),
                            r.token_id.clone(),
                        )).copied()
                    })
                    .or(r.share)
            } else {
                r.share
            };
            ActivityItemCompact {
                ts: r.ts,
                type_: r.type_,
                share,
                price: r.price,
                title: r.title,
                outcome: r.outcome,
                condition_id: r.condition_id,
                token_id: r.token_id,
                transaction_hash: r.transaction_hash,
            }
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

/// POST /wallets/activity — 批量钱包历史交易；body.addresses 多个地址，同一 from_ts/to_ts；force_refresh 对所有地址生效；地址数超限时返回 202 + job_id
async fn post_wallets_activity(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BatchActivityBody>,
) -> Result<Response, (StatusCode, Json<ErrorBody>)> {
    let addresses: Vec<String> = body
        .addresses
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty() && s.starts_with("0x"))
        .collect();
    if addresses.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                code: "BAD_REQUEST".into(),
                message: "addresses required and must be valid 0x".into(),
            }),
        ));
    }
    let to_ts = body.to_ts.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let from_ts = body.from_ts;

    if addresses.len() > state.config.max_batch_addresses {
        let job_id = Uuid::new_v4().to_string();
        let job_state = JobState {
            id: job_id.clone(),
            kind: "activity".to_string(),
            total: addresses.len(),
            completed: 0,
            status: JobStatus::Pending,
            message: Some("处理中，请稍后轮询进度或重试".to_string()),
            error: None,
            result: None,
            created_at: chrono::Utc::now().timestamp(),
        };
        let job_ref = Arc::new(RwLock::new(job_state));
        state.jobs.write().await.insert(job_id.clone(), job_ref.clone());

        state
            .write_job_tx
            .send(WriteJob::BatchActivity {
                job_id: job_id.clone(),
                addresses: addresses.clone(),
                from_ts,
                to_ts,
                limit: body.limit,
                force_refresh: body.force_refresh,
            })
            .await
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorBody {
                        code: "QUEUE_FULL".into(),
                        message: "write worker unavailable".into(),
                    }),
                )
            })?;

        return Ok((
            StatusCode::ACCEPTED,
            Json(JobAccepted {
                job_id: job_id.clone(),
                message: "地址较多，正在后台处理，请轮询进度或稍后重试".to_string(),
                progress_url: format!("/jobs/{}", job_id),
            }),
        )
            .into_response());
    }

    let path = &state.config.duckdb_path;
    let items: Vec<(String, i64, i64, bool)> = addresses
        .iter()
        .map(|a| (a.clone(), from_ts, to_ts, body.force_refresh))
        .collect();
    let (tx, rx) = oneshot::channel();
    state
        .write_job_tx
        .send(WriteJob::SyncMany { items, respond: tx })
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody {
                    code: "QUEUE_FULL".into(),
                    message: "write worker unavailable".into(),
                }),
            )
        })?;
    if let Ok(Err(e)) = rx.await {
        tracing::warn!(err = %e, "batch sync failed");
    }
    let mut out = Vec::with_capacity(addresses.len());
    for addr in &addresses {
        let (rows, total) = db::list_activities(
            path,
            addr,
            from_ts,
            to_ts,
            body.limit,
            None,
        )
        .unwrap_or((vec![], 0));
        let need_redeem_fallback =
            page_has_redeem(&rows) && rows.iter().any(|r| r.type_.eq_ignore_ascii_case("REDEEM") && r.effective_share.is_none());
        let effective_redeem = if need_redeem_fallback {
            let eff_ts = rows.iter().map(|r| r.ts).max().unwrap_or(to_ts);
            effective_redeem_share_map(path, &addr, eff_ts)
        } else {
            HashMap::new()
        };
        let d: Vec<ActivityItemCompact> = rows
            .into_iter()
            .map(|r| {
                let share = if r.type_.eq_ignore_ascii_case("REDEEM") {
                    r.effective_share
                        .or_else(|| {
                            effective_redeem.get(&(
                                r.ts,
                                r.transaction_hash.clone(),
                                r.condition_id.clone(),
                                r.token_id.clone(),
                            )).copied()
                        })
                        .or(r.share)
                } else {
                    r.share
                };
                ActivityItemCompact {
                    ts: r.ts,
                    type_: r.type_,
                    share,
                    price: r.price,
                    title: r.title,
                    outcome: r.outcome,
                    condition_id: r.condition_id,
                    token_id: r.token_id,
                    transaction_hash: r.transaction_hash,
                }
            })
            .collect();
        let from_time = d.iter().map(|x| x.ts).min().and_then(|ts| {
            chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        });
        let to_time = d.iter().map(|x| x.ts).max().and_then(|ts| {
            chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        });
        out.push(WalletActivityItem {
            address: addr.clone(),
            total,
            from_time,
            to_time,
            data: d,
        });
    }
    Ok((StatusCode::OK, Json(BatchActivityResponse { data: out })).into_response())
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

/// GET /daily-stats — 指定钱包（可逗号分隔多个）、日期区间内每日交易额（仅 BUY）与已实现利润；地址数超限时返回 202 + job_id
async fn get_daily_stats(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DailyStatsQuery>,
) -> Result<Response, (StatusCode, Json<ErrorBody>)> {
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

    let from_date = q.from_date.trim().to_string();
    let to_date = q.to_date.trim().to_string();
    let to_ts = chrono::NaiveDate::parse_from_str(q.to_date.trim(), "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(23, 59, 59))
        .map(|ndt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp());

    if addresses.len() > state.config.max_batch_addresses {
        let job_id = Uuid::new_v4().to_string();
        let job_state = JobState {
            id: job_id.clone(),
            kind: "daily_stats".to_string(),
            total: addresses.len(),
            completed: 0,
            status: JobStatus::Pending,
            message: Some("处理中，请稍后轮询进度或重试".to_string()),
            error: None,
            result: None,
            created_at: chrono::Utc::now().timestamp(),
        };
        let job_ref = Arc::new(RwLock::new(job_state));
        state.jobs.write().await.insert(job_id.clone(), job_ref.clone());

        state
            .write_job_tx
            .send(WriteJob::BatchDailyStats {
                job_id: job_id.clone(),
                addresses: addresses.clone(),
                from_date: from_date.clone(),
                to_date: to_date.clone(),
            })
            .await
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorBody {
                        code: "QUEUE_FULL".into(),
                        message: "write worker unavailable".into(),
                    }),
                )
            })?;

        return Ok((
            StatusCode::ACCEPTED,
            Json(JobAccepted {
                job_id: job_id.clone(),
                message: "地址较多，正在后台处理，请轮询进度或稍后重试".to_string(),
                progress_url: format!("/jobs/{}", job_id),
            }),
        )
            .into_response());
    }

    let path = &state.config.duckdb_path;
    let from_ts = 0i64;
    let to_ts = chrono::NaiveDate::parse_from_str(q.to_date.trim(), "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(23, 59, 59))
        .map(|ndt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp());

    let (tx, rx) = oneshot::channel();
    state
        .write_job_tx
        .send(WriteJob::SyncManyUntilYesterday {
            items: addresses.clone(),
            respond: tx,
        })
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody {
                    code: "QUEUE_FULL".into(),
                    message: "write worker unavailable".into(),
                }),
            )
        })?;
    let sync_ok = rx.await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody {
                code: "SYNC_ERROR".into(),
                message: "write worker dropped".into(),
            }),
        )
    })?;
    sync_ok.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody {
                code: "SYNC_ERROR".into(),
                message: e,
            }),
        )
    })?;

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
    Ok((StatusCode::OK, Json(DailyStatsResponse { data })).into_response())
}

#[derive(Debug, Deserialize)]
pub struct BatchDailyStatsBody {
    pub wallets: Vec<String>,
    pub from_date: String,
    pub to_date: String,
}

/// POST /daily-stats — 批量钱包聚合数据（每日交易额与已实现利润），Body: wallets、from_date、to_date；地址数超限时返回 202 + job_id，前端轮询 GET /jobs/:job_id
async fn post_daily_stats(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BatchDailyStatsBody>,
) -> Result<Response, (StatusCode, Json<ErrorBody>)> {
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

    let path = state.config.duckdb_path.clone();
    let from_ts = 0i64;
    let to_ts = chrono::NaiveDate::parse_from_str(body.to_date.trim(), "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(23, 59, 59))
        .map(|ndt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp());
    let from_date = body.from_date.trim().to_string();
    let to_date = body.to_date.trim().to_string();

    if addresses.len() > state.config.max_batch_addresses {
        let job_id = Uuid::new_v4().to_string();
        let total = addresses.len();
        let job_state = JobState {
            id: job_id.clone(),
            kind: "daily_stats".to_string(),
            total,
            completed: 0,
            status: JobStatus::Pending,
            message: Some("处理中，请稍后轮询进度或重试".to_string()),
            error: None,
            result: None,
            created_at: chrono::Utc::now().timestamp(),
        };
        let job_ref = Arc::new(RwLock::new(job_state));
        state.jobs.write().await.insert(job_id.clone(), job_ref.clone());

        state
            .write_job_tx
            .send(WriteJob::BatchDailyStats {
                job_id: job_id.clone(),
                addresses: addresses.clone(),
                from_date: from_date.clone(),
                to_date: to_date.clone(),
            })
            .await
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorBody {
                        code: "QUEUE_FULL".into(),
                        message: "write worker unavailable".into(),
                    }),
                )
            })?;

        return Ok((
            StatusCode::ACCEPTED,
            Json(JobAccepted {
                job_id: job_id.clone(),
                message: "地址较多，正在后台处理，请轮询进度或稍后重试".to_string(),
                progress_url: format!("/jobs/{}", job_id),
            }),
        )
            .into_response());
    }

    let path = &state.config.duckdb_path;
    let (tx, rx) = oneshot::channel();
    state
        .write_job_tx
        .send(WriteJob::SyncManyUntilYesterday {
            items: addresses.clone(),
            respond: tx,
        })
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody {
                    code: "QUEUE_FULL".into(),
                    message: "write worker unavailable".into(),
                }),
            )
        })?;
    let sync_ok = rx.await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody {
                code: "SYNC_ERROR".into(),
                message: "write worker dropped".into(),
            }),
        )
    })?;
    sync_ok.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorBody {
                code: "SYNC_ERROR".into(),
                message: e,
            }),
        )
    })?;

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
    Ok((StatusCode::OK, Json(DailyStatsResponse { data })).into_response())
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
        .route(
            "/wallets/:address/open-positions",
            axum::routing::get(get_open_positions),
        )
        .route("/jobs/:job_id", axum::routing::get(get_job_progress))
        .route("/daily-stats", axum::routing::get(get_daily_stats).post(post_daily_stats))
        .with_state(state)
}
