TICKER="$1"
if [[ -n "$TICKER" ]]; then
    python -m utils.analyze_tool_trace --input ./workspace/output/tool_call_traces --ticker "$TICKER" --output "./workspace/trace_analysis_${TICKER}.md"
    echo "Trace analysis for ticker '$TICKER' has been saved to './workspace/trace_analysis_${TICKER}.md'"
else
    python -m utils.analyze_tool_trace --input ./workspace/output/tool_call_traces --output "./workspace/trace_analysis.md"
    echo "Trace analysis has been saved to './workspace/trace_analysis.md'"
fi
