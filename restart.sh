#!/usr/bin/env bash
# 编译 release、拷贝到根目录、停止旧服务、启动新服务

set -e
cd "$(dirname "$0")"
ROOT="$(pwd)"
BIN_NAME="poly_activity"
RELEASE_BIN="target/release/$BIN_NAME"
PID_FILE="$ROOT/.$BIN_NAME.pid"

echo "[1/4] 编译 release..."
cargo build --release

echo "[2/4] 拷贝到项目根目录..."
cp "$RELEASE_BIN" "$ROOT/$BIN_NAME"

echo "[3/4] 停止之前的服务..."
if [[ -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    kill "$OLD_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$OLD_PID" 2>/dev/null || true
    echo "  已停止进程 $OLD_PID"
  fi
  rm -f "$PID_FILE"
fi
# 兜底：按进程名结束（仅限当前目录启动的）
pkill -f "$ROOT/$BIN_NAME" 2>/dev/null || true
sleep 1

echo "[4/4] 启动当前服务..."
nohup "$ROOT/$BIN_NAME" >> "$ROOT/poly_activity.log" 2>&1 &
echo $! > "$PID_FILE"
echo "  已启动 PID=$(cat $PID_FILE), 日志: $ROOT/poly_activity.log"
