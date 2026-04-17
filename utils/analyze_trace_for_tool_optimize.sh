TICKER="${1:?用法: $0 <TICKER>}"
log_dir="./workspace/tool_optimize/new_round/log/${TICKER}"
manifest="./workspace/tool_optimize/new_round/draft/${TICKER}/manifest.json"
output="./workspace/tool_optimize/new_round/trace_analysis_${TICKER}.md"
if [ -d "$log_dir" ]; then
    python -m utils.analyze_tool_trace --input "$log_dir" --manifest "$manifest" --ticker "$TICKER" --output "$output"
    echo "Trace analysis for ${TICKER} completed. Output saved to ${output}."
else
    echo "Log directory for ${TICKER} not found. Please run write_reports.sh first."
fi
