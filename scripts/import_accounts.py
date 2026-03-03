#!/usr/bin/env python3
"""从 accounts_import.txt 解析并 POST 到 /accounts/batch。"""
import json
import os
import re
import sys
import urllib.request

# 默认 API 基地址（可环境变量 BASE_URL 或首个参数覆盖）
BASE = os.environ.get("BASE_URL", "http://127.0.0.1:7001")
if sys.argv[1:]:
    BASE = sys.argv[1].rstrip("/")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
IMPORT_FILE = os.path.join(SCRIPT_DIR, "accounts_import.txt")

# 每行：名称 Tab/空格 0x地址；或 0x地址, 名称
def parse_line(line: str) -> tuple[str | None, str | None]:
    line = line.strip()
    if not line:
        return None, None
    # 0x 地址格式
    addr_match = re.search(r"0x[a-fA-F0-9]{40}", line)
    if not addr_match:
        return None, None
    addr = addr_match.group(0).lower()
    # 名称：Tab/空格前，或逗号后
    if "\t" in line:
        name = line.split("\t")[0].strip()
    elif " " in line:
        parts = line.split(maxsplit=1)
        name = parts[0].strip() if len(parts) > 1 else ""
        if name and not name.lower().startswith("0x"):
            pass
        else:
            name = ""
    else:
        name = ""
    if "," in line and not name:
        after = line.split(",", 1)[1].strip()
        if after and not after.lower().startswith("0x"):
            name = after
    return (name or None), addr

def main():
    if not os.path.isfile(IMPORT_FILE):
        print(f"Missing {IMPORT_FILE}", file=sys.stderr)
        sys.exit(1)
    items = []
    with open(IMPORT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            name, addr = parse_line(line)
            if addr:
                items.append({
                    "wallet_address": addr,
                    "display_name": name if name else None,
                })
    if not items:
        print("No valid items to import.", file=sys.stderr)
        sys.exit(1)
    url = f"{BASE}/accounts/batch"
    body = json.dumps({"items": items}).encode("utf-8")
    print(f"POST {url} ({len(items)} accounts, timeout=120s)...")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            print(f"POST {url} -> {resp.status}")
            print(f"Imported {len(items)} accounts.")
    except urllib.error.HTTPError as e:
        print(f"HTTP error: {e.code} {e.reason}", file=sys.stderr)
        if e.fp:
            print(e.fp.read().decode("utf-8", errors="replace"), file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"Request failed: {e}", file=sys.stderr)
        if "timed out" in str(e).lower():
            print("提示：请先在本机启动后端（如 cargo run），并确认 API 地址正确。", file=sys.stderr)
            print(f"当前地址: {BASE}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
