//! 环境变量配置

use std::env;

#[derive(Clone)]
pub struct Config {
    pub poly_api_base: String,
    pub duckdb_path: String,
    pub rate_limit_max_requests: u32,
    pub rate_limit_window_secs: u64,
    pub http_port: u16,
    pub request_timeout_secs: u64,
    pub fetch_batch_limit: u32,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            poly_api_base: env::var("POLY_API_BASE")
                .unwrap_or_else(|_| "https://data-api.polymarket.com".to_string()),
            duckdb_path: env::var("DUCKDB_PATH")
                .unwrap_or_else(|_| "./data/activity_cache.duckdb".to_string()),
            rate_limit_max_requests: env::var("RATE_LIMIT_MAX_REQUESTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(950),
            rate_limit_window_secs: env::var("RATE_LIMIT_WINDOW_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
            http_port: env::var("HTTP_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(7001),
            request_timeout_secs: env::var("REQUEST_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            fetch_batch_limit: env::var("FETCH_BATCH_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(500),
        }
    }
}
