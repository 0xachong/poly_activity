//! REDEEM/SELL 无 token_id 时，用 condition_id 反查应减的 token_id（供 valuation 用）

use std::collections::HashMap;

/// REDEEM/SELL 无 token_id 时，用 condition_id 从已记录的 token->condition 反查应减的 token_id。
/// 同一 condition 下若有多个 token 有持仓，选**持仓最大**的（即被兑付/卖出的那一侧），不按 API 的 share 精确匹配，避免 API 报 50 而实际持仓 100 时选错。
pub fn resolve_token_for_reduce(
    condition_id: &str,
    _share: f64,
    token_to_share: &HashMap<String, f64>,
    token_to_condition: &HashMap<String, String>,
) -> String {
    let mut candidates: Vec<(String, f64)> = token_to_condition
        .iter()
        .filter(|(_, c)| c.as_str() == condition_id)
        .filter_map(|(tok, _)| token_to_share.get(tok).copied().map(|s| (tok.clone(), s)))
        .filter(|(_, s)| *s > 1e-9)
        .collect();
    if candidates.is_empty() {
        return String::new();
    }
    // 按持仓从大到小排序，取持仓最大的 token（兑付/卖出时应减的是该侧）
    candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    candidates.first().map(|(t, _)| t.clone()).unwrap_or_default()
}
