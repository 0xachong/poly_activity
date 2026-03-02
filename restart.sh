#!/bin/sh
# 编译 release、停止旧服务、拷贝到根目录、启动新服务（先停再拷避免 Text file busy）
# POSIX sh，兼容 macOS / Ubuntu；请于项目根目录执行：sh restart.sh 或 ./restart.sh

set -e
# 解析脚本所在目录为项目根（支持 sh restart.sh 与 ./restart.sh）
case "$0" in
  */*) dir_="$0";;
  *) dir_="$(pwd)/$0";;
esac
ROOT="$(cd "$(dirname "$dir_")" && pwd)"
cd "$ROOT"
BIN_NAME="poly_activity"
RELEASE_BIN="target/release/$BIN_NAME"
PID_FILE="$ROOT/.$BIN_NAME.pid"

echo "[1/4] 编译 release..."
cargo build --release

echo "[2/4] 停止之前的服务..."
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    kill "$OLD_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$OLD_PID" 2>/dev/null || true
    echo "  已停止进程 $OLD_PID"
  fi
  rm -f "$PID_FILE"
fi
# 兜底1：按完整路径匹配命令行（对 ./poly_activity 可能不匹配）
pkill -f "$ROOT/$BIN_NAME" 2>/dev/null || true
# 兜底2：用 lsof 找正在执行该二进制文件的进程并结束（无论用何种方式启动）
if [ -f "$ROOT/$BIN_NAME" ]; then
  for pid in $(lsof -t "$ROOT/$BIN_NAME" 2>/dev/null); do
    kill -9 "$pid" 2>/dev/null || true
    echo "  已停止进程 $pid (lsof)"
  done
fi
sleep 1

echo "[3/4] 拷贝到项目根目录..."
cp "$RELEASE_BIN" "$ROOT/$BIN_NAME"

echo "[4/4] 启动当前服务..."
nohup "$ROOT/$BIN_NAME" >> "$ROOT/poly_activity.log" 2>&1 &
echo $! > "$PID_FILE"
echo "  已启动 PID=$(cat "$PID_FILE"), 日志: $ROOT/poly_activity.log"
