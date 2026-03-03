//! 从 activities 计算：每日交易额（仅 BUY）、每日已实现利润（卖出收入 − 对应成本）

use crate::db::ActivityRow;
use crate::positions;
use std::collections::HashMap;

fn ts_to_date_str(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

fn buy_cost(row: &ActivityRow) -> f64 {
    if let Some(c) = row.usdc_size {
        return c;
    }
    let s = row.share.unwrap_or(0.0);
    let p = row.price.unwrap_or(0.0);
    s * p
}

fn is_realized_cash(t: &str) -> bool {
    let u = t.to_uppercase();
    u == "SELL" || u == "REDEEM" || u == "CLAIM"
}

/// 卖出/兑付收入；REDEEM 可传 effective_share 兜底
fn sell_redeem_cash_with_share(row: &ActivityRow, effective_share: Option<f64>) -> f64 {
    if let Some(c) = row.usdc_size {
        return c;
    }
    let t = row.type_.to_uppercase();
    let api_share = row.share.unwrap_or(0.0);
    let eff = effective_share.unwrap_or(api_share);
    let (s, p) = match (t.as_str(), row.price) {
        ("REDEEM", None) | ("REDEEM", Some(0.0)) => (eff.min(api_share.max(eff)), 1.0),
        ("REDEEM", Some(p)) => (eff, p),
        ("CLAIM", None) => (eff, 1.0),
        (_, Some(p)) => (eff, p),
        _ => (eff, 0.0),
    };
    s * p
}

/// 按顺序计算每条 REDEEM 的有效 share（该 token 当前持仓）
fn effective_redeem_shares(activities: &[ActivityRow]) -> Vec<Option<f64>> {
    let mut out = vec![None; activities.len()];
    let mut token_to_share: HashMap<String, f64> = HashMap::new();
    let mut token_to_condition: HashMap<String, String> = HashMap::new();
    for (i, r) in activities.iter().enumerate() {
        let type_upper = r.type_.to_uppercase();
        let t = type_upper.as_str();
        if t != "BUY" && t != "SELL" && t != "REDEEM" && t != "CLAIM" {
            continue;
        }
        let cond = r.condition_id.as_deref().unwrap_or("");
        let mut tok = r.token_id.as_deref().unwrap_or("").to_string();
        if tok.is_empty() && !cond.is_empty() && (t == "SELL" || t == "REDEEM" || t == "CLAIM") {
            tok = positions::resolve_token_for_reduce(
                cond,
                r.share.unwrap_or(0.0),
                &token_to_share,
                &token_to_condition,
            );
        }
        if tok.is_empty() {
            continue;
        }
        let share = r.share.unwrap_or(0.0);
        if t == "REDEEM" {
            let pos = token_to_share.get(&tok).copied().unwrap_or(0.0).max(0.0);
            out[i] = Some(pos);
            token_to_share.entry(tok.clone()).and_modify(|s| *s -= pos);
        } else {
            let sign = if t == "BUY" { 1.0 } else { -1.0 };
            *token_to_share.entry(tok.clone()).or_insert(0.0) += sign * share;
        }
        if !cond.is_empty() {
            token_to_condition.entry(tok).or_insert_with(|| cond.to_string());
        }
    }
    out
}

/// 单条活动对「交易额」的贡献：仅 BUY 的买入金额
pub fn volume_contribution(row: &ActivityRow) -> f64 {
    if row.type_.to_uppercase().as_str() != "BUY" {
        return 0.0;
    }
    buy_cost(row).abs()
}

/// 根据 activities 计算 [from_date, to_date] 内每日的「交易额」（仅 BUY）和「已实现利润」（卖出收入 − 对应成本）。
/// 返回按日期升序的 (date, volume, realized_pnl)。
pub fn compute_daily_volume_and_realized_pnl(
    activities: &[ActivityRow],
    from_date: &str,
    to_date: &str,
) -> Vec<(String, f64, f64)> {
    let from_s = from_date.to_string();
    let to_s = to_date.to_string();
    let effective_redeem = effective_redeem_shares(activities);

    let mut token_to_share: HashMap<String, f64> = HashMap::new();
    let mut token_to_cost: HashMap<String, f64> = HashMap::new();
    let mut token_to_condition: HashMap<String, String> = HashMap::new();
    let mut day_volume: HashMap<String, f64> = HashMap::new();
    let mut day_realized_pnl: HashMap<String, f64> = HashMap::new();

    for (i, row) in activities.iter().enumerate() {
        let date = ts_to_date_str(row.ts);
        let u = row.type_.to_uppercase();
        let t = u.as_str();

        let cond = row.condition_id.as_deref().unwrap_or("");
        let mut tok = row.token_id.as_deref().unwrap_or("").to_string();
        if tok.is_empty() && !cond.is_empty() && (t == "SELL" || t == "REDEEM" || t == "CLAIM") {
            tok = positions::resolve_token_for_reduce(
                cond,
                row.share.unwrap_or(0.0),
                &token_to_share,
                &token_to_condition,
            );
        }
        if !cond.is_empty() && !tok.is_empty() {
            token_to_condition.entry(tok.clone()).or_insert_with(|| cond.to_string());
        }

        if t == "BUY" {
            if tok.is_empty() {
                continue;
            }
            let share = row.share.unwrap_or(0.0);
            let cost = buy_cost(row);
            *token_to_share.entry(tok.clone()).or_insert(0.0) += share;
            *token_to_cost.entry(tok.clone()).or_insert(0.0) += cost;
            if date >= from_s && date <= to_s {
                *day_volume.entry(date).or_insert(0.0) += cost.abs();
            }
            continue;
        }

        if is_realized_cash(t) {
            if tok.is_empty() {
                continue;
            }
            let eff = effective_redeem.get(i).copied().flatten();
            let revenue = sell_redeem_cash_with_share(row, eff);
            let share_sold = if t == "REDEEM" {
                eff.unwrap_or_else(|| row.share.unwrap_or(0.0))
            } else {
                row.share.unwrap_or(0.0)
            };
            let pos = token_to_share.get(&tok).copied().unwrap_or(0.0);
            if pos < 1e-9 {
                if date >= from_s && date <= to_s {
                    *day_realized_pnl.entry(date).or_insert(0.0) += revenue;
                }
                continue;
            }
            let cost_total = token_to_cost.get(&tok).copied().unwrap_or(0.0);
            let cost_basis = if share_sold >= pos - 1e-9 {
                cost_total
            } else {
                cost_total * (share_sold / pos)
            };
            let pnl = revenue - cost_basis;
            let s = token_to_share.get_mut(&tok).unwrap();
            *s = (*s - share_sold).max(0.0);
            let c = token_to_cost.get_mut(&tok).unwrap();
            *c = (*c - cost_basis).max(0.0);
            if date >= from_s && date <= to_s {
                *day_realized_pnl.entry(date).or_insert(0.0) += pnl;
            }
        }
    }

    let from_naive = chrono::NaiveDate::parse_from_str(from_date, "%Y-%m-%d")
        .unwrap_or_else(|_| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    let to_naive = chrono::NaiveDate::parse_from_str(to_date, "%Y-%m-%d")
        .unwrap_or_else(|_| chrono::NaiveDate::from_ymd_opt(2100, 12, 31).unwrap());
    let mut out = Vec::new();
    let mut current = from_naive;
    while current <= to_naive {
        let d = current.format("%Y-%m-%d").to_string();
        let vol = day_volume.get(&d).copied().unwrap_or(0.0);
        let pnl = day_realized_pnl.get(&d).copied().unwrap_or(0.0);
        out.push((d, vol, pnl));
        current = current.succ_opt().unwrap_or(current);
    }
    out
}
