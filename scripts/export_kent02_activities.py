#!/usr/bin/env python3
"""
导出 kent02 钱包的 activities 按时间顺序，并标注每条对 USDC 现金流/成交量的贡献（与 valuation.rs 一致）。
逻辑：DEPOSIT USDC+，WITHDRAW USDC-，BUY USDC-，SELL USDC+，REDEEM USDC+；running = 累计现金流。
"""
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(PROJECT_DIR, "data", "activity_cache.duckdb")
# 默认导出 kent02；可通过参数指定 display_name，如 python export_kent02_activities.py jinlai03
DEFAULT_ACCOUNT = "kent02"

def _amount(share: float | None, usdc_size: float | None) -> float:
    if usdc_size is not None:
        return float(usdc_size)
    return float(share or 0.0)

def _buy_cost(share: float, price: float | None, usdc_size: float | None) -> float:
    if usdc_size is not None:
        return float(usdc_size)
    return share * (float(price) if price is not None else 0.0)

def _sell_redeem_cash(share: float, price: float | None, usdc_size: float | None, type_: str) -> float:
    if usdc_size is not None:
        return float(usdc_size)
    u = (type_ or "").upper()
    if u == "REDEEM" and (price is None or float(price or 0) == 0.0):
        p = 1.0
    else:
        p = float(price) if price is not None else (1.0 if u == "CLAIM" else 0.0)
    return share * p

# 单条对「运行 USDC」的贡献；REDEEM 可传入 effective_share（兜底：以持仓为准）
def cash_flow_contribution(
    type_: str,
    share: float,
    price: float | None,
    usdc_size: float | None,
    effective_redeem_share: float | None = None,
) -> float:
    u = (type_ or "").upper()
    if u in ("DEPOSIT", "DEPOSITS"):
        return _amount(share, usdc_size)
    if u in ("WITHDRAW", "WITHDRAWAL", "WITHDRAWALS"):
        return -_amount(share, usdc_size)
    if u == "BUY":
        return -_buy_cost(share or 0.0, price, usdc_size)
    if u in ("SELL", "REDEEM", "CLAIM"):
        # REDEEM 无 usdc_size 且 price=0 时用 min(api_share, effective_share)，既展示部分兑付亏损又防止 API 错报大额虚增利润
        if u == "REDEEM" and usdc_size is None and (price is None or (price is not None and float(price) == 0.0)):
            api_s = share or 0.0
            eff_s = effective_redeem_share if effective_redeem_share is not None else api_s
            s = min(api_s, eff_s) if eff_s > 0 else api_s
        else:
            s = (effective_redeem_share if u == "REDEEM" and effective_redeem_share is not None else None) or (share or 0.0)
        return _sell_redeem_cash(s, price, usdc_size, type_)
    return 0.0

# 交易额 = 仅买入总交易额（BUY 的金额）
def volume_contribution(type_: str, share: float, price: float | None, usdc_size: float | None) -> float:
    u = (type_ or "").upper()
    if u != "BUY":
        return 0.0
    return abs(_buy_cost(share or 0.0, price, usdc_size))

def ts_to_date(ts: int) -> str:
    from datetime import datetime
    try:
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
    except Exception:
        return ""

def main():
    try:
        import duckdb
    except ImportError:
        print("pip install duckdb", file=sys.stderr)
        sys.exit(1)

    account_name = (sys.argv[1] if len(sys.argv) > 1 else "").strip() or DEFAULT_ACCOUNT
    out_path = os.path.join(PROJECT_DIR, "scripts", f"{account_name}_activities_by_time.txt")

    if not os.path.isfile(DB_PATH):
        print(f"DB not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = duckdb.connect(DB_PATH, read_only=True)

    # 按 display_name 或 account_id 匹配（不区分大小写）
    key = account_name.lower()
    row = conn.execute(
        "SELECT account_id, wallet_address, display_name FROM accounts WHERE LOWER(COALESCE(display_name,'')) = ? OR LOWER(account_id) = ? LIMIT 1",
        [key, key],
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT account_id, wallet_address, display_name FROM accounts WHERE display_name = ? OR account_id = ? LIMIT 1",
            [account_name, account_name],
        ).fetchone()
    if not row:
        print(f"Account '{account_name}' not found. Available:", file=sys.stderr)
        for r in conn.execute("SELECT display_name, account_id, wallet_address FROM accounts LIMIT 20").fetchall():
            print(f"  {r[0]} | {r[1]} -> {r[2]}", file=sys.stderr)
        sys.exit(1)

    account_id, wallet_address, display_name = row
    wallet = (wallet_address or account_id or "").lower()
    if not wallet:
        print(f"Account '{account_name}' wallet_address is empty", file=sys.stderr)
        sys.exit(1)

    activities = conn.execute(
        "SELECT address, ts, type, share, price, usdc_size, title, outcome, condition_id, token_id, transaction_hash FROM activities WHERE LOWER(address) = ? ORDER BY ts ASC",
        [wallet],
    ).fetchall()

    lines = []
    lines.append(f"# {account_name}: account_id={account_id} wallet_address={wallet_address}")
    lines.append(f"# 共 {len(activities)} 条活动，按 ts 升序。cash_flow：DEPOSIT+ WITHDRAW- BUY- SELL+ REDEEM+；running=累计USDC。与 valuation 一致（REDEEM 用持仓兜底、min(api,eff) 防虚增）。")
    lines.append("")
    header = "ts\tdate\ttype\tshare\tprice\tusdc_size\tcash_flow\tvolume_contrib\trunning_usdc\ttransaction_hash\tcondition_id\ttoken_id\ttitle"
    lines.append(header)

    # REDEEM 兜底：按时间序维护每个 token 持仓，REDEEM 的 share 以当前持仓为准（全部赎回）
    token_to_share: dict[str, float] = {}
    token_to_condition: dict[str, str] = {}

    def resolve_token_for_reduce(cond: str, fallback_share: float) -> str:
        cands = [(tok, token_to_share.get(tok, 0.0)) for tok, c in token_to_condition.items() if c == cond]
        cands.sort(key=lambda x: -abs(x[1]))
        if cands and cands[0][1] > 0:
            return cands[0][0]
        return ""

    running = 0.0
    for r in activities:
        addr, ts, type_, share, price, usdc_size, title, outcome, cond_id, tok_id, tx_hash = r
        share_f = float(share) if share is not None else 0.0
        price_f = float(price) if price is not None else None
        usdc_f = float(usdc_size) if usdc_size is not None else None
        u = (type_ or "").upper()
        tok = (tok_id or "").strip()
        cond = (cond_id or "").strip()
        if u == "REDEEM" and not tok and cond:
            tok = resolve_token_for_reduce(cond, share_f)
        effective_redeem = None
        if u == "REDEEM" and tok:
            effective_redeem = max(0.0, token_to_share.get(tok, 0.0))
            token_to_share[tok] = token_to_share.get(tok, 0.0) - effective_redeem
        elif u == "BUY" and tok:
            token_to_share[tok] = token_to_share.get(tok, 0.0) + (share_f or 0.0)
        elif u in ("SELL", "CLAIM") and tok:
            token_to_share[tok] = token_to_share.get(tok, 0.0) - (share_f or 0.0)
        if cond and tok:
            token_to_condition.setdefault(tok, cond)
        cf = cash_flow_contribution(type_ or "", share_f, price_f, usdc_f, effective_redeem)
        vol = volume_contribution(type_ or "", share_f, price_f, usdc_f)
        running += cf
        display_share = effective_redeem if (u == "REDEEM" and effective_redeem is not None) else share_f
        date_str = ts_to_date(int(ts)) if ts is not None else ""
        title_short = (title or "")[:40].replace("\t", " ")
        price_str = f"{price_f:.4f}" if price_f is not None else ""
        usdc_str = f"{usdc_f:.4f}" if usdc_f is not None else ""
        tx_s = (tx_hash or "").replace("\t", " ")
        cond_s = (cond_id or "").replace("\t", " ")
        tok_s = (tok_id or "").replace("\t", " ")
        lines.append(f"{ts}\t{date_str}\t{type_ or ''}\t{display_share}\t{price_str}\t{usdc_str}\t{cf:.4f}\t{vol:.4f}\t{running:.4f}\t{tx_s}\t{cond_s}\t{tok_s}\t{title_short}")

    conn.close()

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Wrote {len(activities)} rows to {out_path}")
    print(f"  running_usdc (last) = {running:.4f}")

if __name__ == "__main__":
    main()
