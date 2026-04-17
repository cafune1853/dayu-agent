TICKER="${1:?用法: $0 <TICKER>}"
shift 1

python -m dayu.cli write \
  --ticker "$TICKER" \
  --enable-tool-trace \
  "$@"