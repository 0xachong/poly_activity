//! Polymarket 钱包 Activity 缓存后端：按查询触发同步、时间区间 API、紧凑 JSON

mod api;
mod config;
mod db;
mod rate_limit;
mod sync;

use std::sync::Arc;

use api::{router, AppState};
use config::Config;
use rate_limit::RateLimiter;

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

    let state = Arc::new(AppState {
        config: config.clone(),
        rate_limiter,
        client,
    });

    let app = router(state);
    let bind = format!("0.0.0.0:{}", config.http_port);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("listening on http://{}", bind);
    axum::serve(listener, app).await?;
    Ok(())
}
