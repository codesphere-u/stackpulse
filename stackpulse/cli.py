"""Command-Line Interface for StackPulse: Simulation, Benchmarking, and Observability Server."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence

import uvicorn

from stackpulse.incidents.correlator import IncidentCorrelator
from stackpulse.ingest.models import TelemetryStore
from stackpulse.simulator.chaos import ChaosSimulator, ScenarioType
from stackpulse.slo.engine import SLOEngine
from stackpulse.topology.graph import TopologyGraphBuilder
from stackpulse.tracing.analyzer import TraceAnalyzer


def cmd_simulate(
    scenario: ScenarioType,
    count: int = 24,
    as_json: bool = False,
) -> int:
    """Run synthetic distributed workload through the StackPulse pipeline and print SRE diagnostics."""
    store = TelemetryStore()
    sim = ChaosSimulator(seed=42)
    stats = sim.inject_scenario(store, scenario=scenario, count=count)

    analyzer = TraceAnalyzer()
    topo_builder = TopologyGraphBuilder()
    slo_engine = SLOEngine()
    correlator = IncidentCorrelator()

    trace_analyses = [
        analyzer.analyze_trace(store.get_trace_spans(tid))
        for tid in store.trace_order
        if store.get_trace_spans(tid)
    ]
    all_spans = store.get_all_spans()
    topology = topo_builder.build(all_spans)
    slo_reports = slo_engine.evaluate_all(all_spans)
    incident = correlator.correlate(trace_analyses, slo_reports, store.logs)

    if as_json:
        payload = {
            "scenario": scenario,
            "ingestion_stats": stats,
            "topology": topology,
            "slo_reports": [r.to_dict() for r in slo_reports],
            "incident": incident.to_dict(),
        }
        print(json.dumps(payload, indent=2))
        return 0

    print("=" * 84)
    print(f" STACKPULSE v1.0 — DISTRIBUTED TRACE & MULTI-WINDOW SLO SIMULATION ({scenario.upper()})")
    print("=" * 84)
    print(
        f" Ingested: {stats['traces_added']} traces | {stats['spans_added']} W3C spans | {stats['logs_added']} correlated logs"
    )
    print("-" * 84)
    print(" [1] SERVICE TOPOLOGY & GOLDEN SIGNALS")
    for node in topology["nodes"]:
        print(
            f"   • {node['service_name']:<18} [{node['status']:<8}] "
            f"Spans={node['request_count']:<4} Err={node['error_rate_pct']:>5.1f}% "
            f"P50={node['p50_ms']:>6.1f}ms P95={node['p95_ms']:>6.1f}ms P99={node['p99_ms']:>6.1f}ms"
        )

    print("-" * 84)
    print(" [2] MULTI-WINDOW SLO & ERROR BUDGET BURN RATES")
    for slo in slo_reports:
        print(
            f"   • {slo.name:<48} [{slo.alert_severity:<14}] "
            f"SLI={slo.current_sli_pct:>6.2f}% | 1h Burn={slo.burn_rate_1h:>5.1f}x | Budget Left={slo.remaining_budget_pct:>5.1f}%"
        )

    print("-" * 84)
    print(" [3] AUTOMATED INCIDENT CORRELATION & SRE POSTMORTEM")
    print(incident.markdown_postmortem)
    print("=" * 84)
    return 0


def cmd_benchmark(trace_count: int = 500) -> int:
    """Benchmark W3C span ingestion throughput and DAG critical-path reconstruction latency."""
    store = TelemetryStore(max_traces=trace_count + 100)
    sim = ChaosSimulator(seed=99)
    analyzer = TraceAnalyzer()

    # Pre-generate spans
    batches = []
    total_spans = 0
    for i in range(trace_count):
        if i % 4 == 0:
            sp, _ = sim.generate_n_plus_one_trace()
        elif i % 4 == 1:
            sp, _ = sim.generate_db_saturation_trace()
        elif i % 4 == 2:
            sp, _ = sim.generate_retry_storm_trace()
        else:
            sp, _ = sim.generate_normal_trace()
        batches.append(sp)
        total_spans += len(sp)

    t0 = time.perf_counter()
    for sp in batches:
        store.ingest_spans(sp)
    t_ingest = time.perf_counter() - t0
    spans_per_sec = total_spans / max(t_ingest, 1e-9)

    t1 = time.perf_counter()
    for tid in store.trace_order:
        analyzer.analyze_trace(store.get_trace_spans(tid))
    t_analyze = time.perf_counter() - t1
    avg_dag_us = (t_analyze / max(trace_count, 1)) * 1_000_000.0
    traces_per_sec = trace_count / max(t_analyze, 1e-9)

    print("=" * 78)
    print(" STACKPULSE PIPELINE BENCHMARK REPORT")
    print("=" * 78)
    print(f" Total Traces Evaluated          : {trace_count}")
    print(f" Total W3C Spans Ingested        : {total_spans}")
    print(f" Span Ingestion Throughput       : {spans_per_sec:,.0f} spans/sec ({t_ingest*1000:.2f} ms)")
    print(f" Trace DAG Assembly Throughput   : {traces_per_sec:,.0f} traces/sec ({t_analyze*1000:.2f} ms)")
    print(f" Mean Critical-Path DAG Latency  : {avg_dag_us:.1f} µs/trace")
    print("=" * 78)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stackpulse",
        description="StackPulse — Cloud Observability, Distributed Tracing & Multi-Window SLO Platform",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sim_p = sub.add_parser("simulate", help="Run synthetic microservice & chaos trace simulations")
    sim_p.add_argument(
        "--scenario",
        choices=["all", "normal", "db-saturation", "n-plus-one", "retry-storm"],
        default="all",
        help="Chaos scenario to inject",
    )
    sim_p.add_argument("--count", type=int, default=24, help="Number of distributed traces to simulate")
    sim_p.add_argument("--json", action="store_true", help="Emit JSON output")

    bench_p = sub.add_parser("benchmark", help="Benchmark span ingestion and DAG critical-path analyzer")
    bench_p.add_argument("--traces", type=int, default=500, help="Number of traces to benchmark")

    serve_p = sub.add_parser("serve", help="Launch FastAPI Observability & SRE Console")
    serve_p.add_argument("--host", default="127.0.0.1", help="Bind host address")
    serve_p.add_argument("--port", type=int, default=8098, help="Bind TCP port")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "simulate":
        return cmd_simulate(scenario=args.scenario, count=args.count, as_json=args.json)
    if args.command == "benchmark":
        return cmd_benchmark(trace_count=args.traces)
    if args.command == "serve":
        uvicorn.run("stackpulse.server.app:app", host=args.host, port=args.port, reload=False)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
