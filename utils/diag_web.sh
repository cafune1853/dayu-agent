URL="${1:?用法: $0 <URL> [额外参数...]}"
shift
OUTPUT_DIR="./workspace/output/web_diagnostics"
STATE_DIR="$OUTPUT_DIR/storage_states"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$STATE_DIR"

python -m utils.diagnose_web_access \
  --url "$URL" \
  --headed \
  --channel chrome \
  --manual-wait-seconds 30 \
  --storage-state-dir "$STATE_DIR" \
  "$@" \
  --output "$OUTPUT_DIR/diagnose_web_access_$(date +%Y%m%d%H%M%S).json"
