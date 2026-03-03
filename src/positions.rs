//! REDEEM/SELL 无 token_id 时，用 condition_id 反查应减的 token_id（供 valuation 用）

use std::collections::HashMap;

/// REDEEM/SELL 无 token_id 时，用 condition_id 从已记录的 token->condition 反查应减的 token_id
pub fn resolve_token_for_reduce(
    condition_id: &str,
    share: f64,
    token_to_share: &HashMap<String, f64>,
    token_to_condition: &HashMap<String, String>,
) -> String {
    let mut candidates: Vec<(String, f64)> = token_to_condition
        .iter()
        .filter(|(_, c)| c.as_str() == condition_id)
        .filter_map(|(tok, _)| token_to_share.get(tok).copied().map(|s| (tok.clone(), s)))
        .filter(|(_, s)| *s > 0.0 && *s >= share - 1e-9)
        .collect();
    if candidates.is_empty() {
        return String::new();
    }
    candidates.sort_by(|a, b| {
        let exact_a = (a.1 - share).abs() < 1e-9;
        let exact_b = (b.1 - share).abs() < 1e-9;
        match (exact_a, exact_b) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.0.cmp(&b.0),
        }
    });
    candidates.first().map(|(t, _)| t.clone()).unwrap_or_default()
}
