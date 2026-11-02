# ADR-001: Interval-Merged Exclusive Span Duration & Multi-Window SLO Burn Rates

## Status
Accepted (2026-11-02)

## Context
Naive distributed trace analyzers compute a parent span's exclusive duration by subtracting the sum of all child span durations:
$$D_{\text{excl, naive}} = D_{\text{parent}} - \sum_{i} D_{\text{child}, i}$$
When a service issues concurrent asynchronous requests (`asyncio.gather` or gRPC fan-out), the sum of child durations often exceeds the parent duration, clamping exclusive time to `0.0ms` and obscuring parent CPU bottlenecks.

Simultaneously, single-window error rate alerting suffers from either slow reset times or high false-positive paging on transient spikes.

## Decision
1. **Interval-Union Exclusive Duration**: `TraceAnalyzer._merged_child_intervals_duration` sorts all child span `[start_time_ms, end_time_ms]` intervals and computes their disjoint union before subtracting from `parent.duration_ms`.
2. **Google SRE Workbook Multi-Window Burn Rates**: `SLOEngine` evaluates both short (`1h`, `6h`) and long (`24h`, `72h`) windows:
   $$\text{Burn Rate}_{W} = \frac{\text{Error Rate}_{W}}{1 - \text{SLO Target}}$$
   requiring `burn_1h >= 14.4` and `burn_6h >= 6.0` for `PAGE_CRITICAL` alerts.
