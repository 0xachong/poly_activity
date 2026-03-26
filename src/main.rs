//! Polymarket 钱包 Activity 缓存后端：仅两个 API（历史交易 + 每日交易额/利润）

mod api;
mod config;
mod db;
mod rate_limit;
mod sync;
mod valuation;
mod positions;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use api::{router, run_write_worker, AppState};
use config::Config;
use rate_limit::RateLimiter;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

/// 尝试抢占「单写实例」leader 锁（非阻塞）。成功则返回持锁的 File，进程需长期持有；失败则返回 None。
fn try_acquire_leader_lock(duckdb_path: &str) -> Option<std::fs::File> {
    use fs4::FileExt;
    let leader_path = format!("{}.leader", duckdb_path);
    if let Some(p) = std::path::Path::new(duckdb_path).parent() {
        let _ = std::fs::create_dir_all(p);
    }
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&leader_path)
        .ok()?;
    f.try_lock_exclusive().ok()?;
    Some(f)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("poly_activity=info".parse()?),
        )
        .init();

    let config = Config::from_env();
    db::init_schema(&config.duckdb_path)?;

    let rate_limiter = Arc::new(RateLimiter::new(
        config.rate_limit_max_requests,
        config.rate_limit_window_secs,
    ));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(config.request_timeout_secs))
        .user_agent("poly_activity/1.0")
        .build()?;

    let (write_job_tx, write_job_rx) = mpsc::channel(1024);
    let (is_write_leader, _leader_lock_file) = if config.single_writer {
        match try_acquire_leader_lock(&config.duckdb_path) {
            Some(f) => {
                tracing::info!("single_writer: acquired leader lock, this instance is the sole writer");
                (true, Some(f))
            }
            None => {
                tracing::info!("single_writer: leader lock held by another instance, this instance is read-only for writes");
                (false, None)
            }
        }
    } else {
        (true, None)
    };

    // 非写主实例：强制所有 DuckDB 连接以只读方式打开，避免多进程对同一文件以 RW 打开造成元数据一致性问题。
    if !is_write_leader {
        std::env::set_var("POLY_ACTIVITY_DB_READ_ONLY", "1");
    }

    let state = Arc::new(AppState {
        config: config.clone(),
        rate_limiter,
        client,
        jobs: Arc::new(RwLock::new(HashMap::new())),
        write_job_tx,
        worker_current_job_id: Arc::new(RwLock::new(None)),
        is_write_leader,
        _leader_lock_file,
    });

    if is_write_leader {
        tokio::spawn(run_write_worker(write_job_rx, state.clone()));
    }

    let app = router(state)
        .layer(
            TraceLayer::new_for_http()
                .on_request(|req: &axum::http::Request<axum::body::Body>, _span: &tracing::Span| {
                    tracing::info!(method = %req.method(), uri = %req.uri(), "request");
                })
                .on_response(|_res: &axum::http::Response<_>, _latency: std::time::Duration, _span: &tracing::Span| {
                    tracing::info!(status = %_res.status(), "response");
                }),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    let bind = format!("0.0.0.0:{}", config.http_port);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("listening on http://{}", bind);
    axum::serve(listener, app).await?;
    Ok(())
}
