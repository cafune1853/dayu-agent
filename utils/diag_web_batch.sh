#!/usr/bin/env bash
set -euo pipefail

URL_FILE="${1:?用法: $0 <URL文件> [额外参数...] }"
shift

python -m utils.diagnose_web_access \
  --url-file "$URL_FILE" \
  --headed \
  --channel chrome \
  --manual-wait-seconds 30 \
  --storage-state-dir ./workspace/output/web_diagnostics/storage_states \
  "$@"
