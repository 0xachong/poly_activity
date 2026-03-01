//! 全局限流：滑动窗口，任意 window_secs 内不超过 max_requests 次

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub struct RateLimiter {
    max_requests: u32,
    window: Duration,
    /// 最近每次 acquire 成功的时间点（单调递增清理过期）
    times: Mutex<VecDeque<Instant>>,
}

impl RateLimiter {
    pub fn new(max_requests: u32, window_secs: u64) -> Self {
        Self {
            max_requests,
            window: Duration::from_secs(window_secs),
            times: Mutex::new(VecDeque::new()),
        }
    }

    /// 在发 Polymarket 请求前调用；若超限则 sleep 到可再请求
    pub async fn acquire(&self) {
        loop {
            let now = Instant::now();
            let deadline = {
                let mut g = self.times.lock().unwrap();
                let window_start = now - self.window;
                while g.front().map_or(false, |&t| t < window_start) {
                    g.pop_front();
                }
                if (g.len() as u32) < self.max_requests {
                    g.push_back(now);
                    return;
                }
                g.front().copied().map(|t| t + self.window)
            };
            if let Some(d) = deadline {
                let wait = (d - now).max(Duration::from_millis(1));
                sleep(wait).await;
            }
        }
    }
}
