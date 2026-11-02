"""Comprehensive test suite for StackPulse (38 tests covering ingestion, DAG tracing, N+1, topology, SLOs, incidents, CLI, and FastAPI)."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from stackpulse.cli import build_parser, cmd_benchmark, cmd_simulate
from stackpulse.incidents.correlator import IncidentCorrelator
from stackpulse.ingest.models import (
    LogRecord,
    MetricPoint,
    SpanRecord,
    TelemetryStore,
)
from stackpulse.server.app import create_app
from stackpulse.simulator.chaos import ChaosSimulator
from stackpulse.slo.engine import SLODefinition, SLOEngine
from stackpulse.topology.graph import TopologyGraphBuilder, _percentile
from stackpulse.tracing.analyzer import TraceAnalyzer

# ---------------------------------------------------------------------------
# 1. Ingestion & Telemetry Store Tests
# ---------------------------------------------------------------------------


def test_span_record_properties():
    span = SpanRecord(
        trace_id="trace-100001",
        span_id="span-01",
        service_name="api-gateway",
        operation="GET /v1/health",
        start_time_ms=1000.0,
        duration_ms=45.5,
        status_code="OK",
        http_status=200,
    )
    assert span.end_time_ms == 1045.5
    assert span.is_error is False


def test_span_record_error_detection_via_http_status():
    span = SpanRecord(
        trace_id="trace-100002",
        span_id="span-02",
        service_name="payment-worker",
        operation="POST /charge",
        start_time_ms=2000.0,
        duration_ms=120.0,
        status_code="OK",
        http_status=503,
    )
    assert span.is_error is True


def test_telemetry_store_bounded_trace_eviction():
    store = TelemetryStore(max_traces=3)
    for i in range(5):
        store.ingest_span(
            SpanRecord(
                trace_id=f"tr-evict-{i:04d}",
                span_id=f"sp-{i:04d}",
                service_name="svc-a",
                operation="op",
                duration_ms=10.0,
            )
        )
    assert len(store.traces) == 3
    assert "tr-evict-0000" not in store.traces
    assert "tr-evict-0004" in store.traces


def test_telemetry_store_log_correlation_and_eviction():
    store = TelemetryStore(max_logs=4)
    for i in range(6):
        store.ingest_log(
            LogRecord(
                service_name="auth-service",
                severity="ERROR",
                message=f"err {i}",
                trace_id="tr-corr-001" if i % 2 == 0 else "tr-corr-002",
            )
        )
    assert len(store.logs) == 4
    corr_logs = store.get_logs_for_trace("tr-corr-001")
    assert len(corr_logs) == 2


def test_telemetry_store_metrics_and_clear():
    store = TelemetryStore(max_metrics=3)
    for i in range(5):
        store.ingest_metric(
            MetricPoint(
                name="cpu_usage",
                metric_type="gauge",
                value=float(i),
                service_name="api-gateway",
            )
        )
    assert len(store.metrics) == 3
    store.clear()
    assert len(store.metrics) == 0
    assert len(store.traces) == 0


# ---------------------------------------------------------------------------
# 2. Distributed Trace DAG & Critical-Path Analyzer Tests
# ---------------------------------------------------------------------------


def test_trace_analyzer_empty_raises():
    analyzer = TraceAnalyzer()
    with pytest.raises(ValueError, match="empty"):
        analyzer.analyze_trace([])


def test_trace_analyzer_reconstructs_dag_and_exclusive_duration():
    spans = [
        SpanRecord(
            trace_id="tr-dag-0001",
            span_id="root-gw",
            parent_span_id=None,
            service_name="api-gateway",
            operation="POST /checkout",
            start_time_ms=1000.0,
            duration_ms=100.0,
        ),
        SpanRecord(
            trace_id="tr-dag-0001",
            span_id="child-chk",
            parent_span_id="root-gw",
            service_name="checkout-service",
            operation="process",
            start_time_ms=1010.0,
            duration_ms=80.0,
        ),
        SpanRecord(
            trace_id="tr-dag-0001",
            span_id="leaf-db",
            parent_span_id="child-chk",
            service_name="postgres-primary",
            operation="SELECT ledger",
            start_time_ms=1020.0,
            duration_ms=65.0,
        ),
    ]
    analyzer = TraceAnalyzer()
    analysis = analyzer.analyze_trace(spans)
    assert analysis.root_service == "api-gateway"
    assert analysis.span_count == 3
    assert analysis.bottleneck_span_id == "leaf-db"
    assert analysis.bottleneck_service == "postgres-primary"
    assert analysis.bottleneck_exclusive_ms == 65.0
    assert "leaf-db" in analysis.critical_path_span_ids


def test_trace_analyzer_overlapping_async_children_exclusive_time():
    """Verify overlapping parallel child spans are merged before subtracting from parent duration."""
    spans = [
        SpanRecord(
            trace_id="tr-async-01",
            span_id="parent-1",
            parent_span_id=None,
            service_name="api-gateway",
            operation="parallel_fanout",
            start_time_ms=100.0,
            duration_ms=100.0,
        ),
        SpanRecord(
            trace_id="tr-async-01",
            span_id="child-a",
            parent_span_id="parent-1",
            service_name="svc-a",
            operation="call_a",
            start_time_ms=110.0,
            duration_ms=50.0,  # 110 -> 160
        ),
        SpanRecord(
            trace_id="tr-async-01",
            span_id="child-b",
            parent_span_id="parent-1",
            service_name="svc-b",
            operation="call_b",
            start_time_ms=120.0,
            duration_ms=50.0,  # 120 -> 170 (union is 110->170 = 60ms)
        ),
    ]
    analyzer = TraceAnalyzer()
    res = analyzer.analyze_trace(spans)
    root_row = res.flat_waterfall[0]
    assert root_row["exclusive_duration_ms"] == 40.0


def test_trace_analyzer_detects_n_plus_one_pattern():
    sim = ChaosSimulator(seed=7)
    spans, _ = sim.generate_n_plus_one_trace(query_count=8)
    analyzer = TraceAnalyzer(n_plus_one_threshold=4)
    res = analyzer.analyze_trace(spans)
    assert len(res.n_plus_one_findings) == 1
    finding = res.n_plus_one_findings[0]
    assert finding.parent_service == "checkout-service"
    assert finding.child_service == "postgres-primary"
    assert finding.repeat_count == 8


def test_trace_analyzer_detects_retry_storm():
    sim = ChaosSimulator(seed=11)
    spans, _ = sim.generate_retry_storm_trace(attempts=4)
    analyzer = TraceAnalyzer()
    res = analyzer.analyze_trace(spans)
    assert res.has_error is True
    assert len(res.retry_findings) == 1
    rf = res.retry_findings[0]
    assert rf.service_name == "payment-worker"
    assert rf.attempt_count == 4
    assert rf.failed_attempts == 4


def test_trace_analyzer_orphan_span_handling():
    """Spans whose parent_span_id is missing from the trace should still attach as roots cleanly."""
    spans = [
        SpanRecord(
            trace_id="tr-orphan-01",
            span_id="sp-orphan",
            parent_span_id="missing-parent",
            service_name="checkout-service",
            operation="orphanOp",
            start_time_ms=500.0,
            duration_ms=33.0,
        )
    ]
    res = TraceAnalyzer().analyze_trace(spans)
    assert res.span_count == 1
    assert res.root_service == "checkout-service"


# ---------------------------------------------------------------------------
# 3. Service Topology Graph & Anomaly Detection Tests
# ---------------------------------------------------------------------------


def test_percentile_interpolation():
    assert _percentile([], 50.0) == 0.0
    assert _percentile([42.0], 95.0) == 42.0
    assert _percentile([10.0, 20.0, 30.0, 40.0, 50.0], 50.0) == 30.0


def test_topology_graph_builder_empty():
    topo = TopologyGraphBuilder().build([])
    assert topo == {"nodes": [], "edges": [], "anomalous_services": []}


def test_topology_graph_nodes_and_edges():
    sim = ChaosSimulator(seed=42)
    spans, _ = sim.generate_normal_trace()
    topo = TopologyGraphBuilder().build(spans)
    svc_names = {n["service_name"] for n in topo["nodes"]}
    assert "api-gateway" in svc_names
    assert "checkout-service" in svc_names
    assert "postgres-primary" in svc_names
    edge_pairs = {(e["source"], e["target"]) for e in topo["edges"]}
    assert ("api-gateway", "auth-service") in edge_pairs
    assert ("checkout-service", "postgres-primary") in edge_pairs


def test_topology_graph_flags_critical_service_on_errors():
    sim = ChaosSimulator(seed=42)
    spans, _ = sim.generate_retry_storm_trace()
    topo = TopologyGraphBuilder().build(spans)
    assert "payment-worker" in topo["anomalous_services"]
    pay_node = next(n for n in topo["nodes"] if n["service_name"] == "payment-worker")
    assert pay_node["status"] == "CRITICAL"
    assert pay_node["error_rate_pct"] == 100.0


def test_topology_graph_peer_service_fallback():
    spans = [
        SpanRecord(
            trace_id="tr-peer-01",
            span_id="sp-client-1",
            service_name="checkout-service",
            operation="EXT /stripe",
            duration_ms=40.0,
            peer_service="stripe-external",
        )
    ]
    topo = TopologyGraphBuilder().build(spans)
    assert len(topo["edges"]) == 1
    assert topo["edges"][0]["source"] == "checkout-service"
    assert topo["edges"][0]["target"] == "stripe-external"


# ---------------------------------------------------------------------------
# 4. Multi-Window SLO & Error-Budget Burn-Rate Engine Tests
# ---------------------------------------------------------------------------


def test_slo_engine_no_traffic_returns_ok_and_full_budget():
    slo = SLODefinition(
        slo_id="test-slo",
        service_name="unused-service",
        name="Unused SLO",
        target_pct=99.9,
    )
    engine = SLOEngine([slo])
    report = engine.evaluate_single(slo, [])
    assert report.alert_severity == "OK"
    assert report.remaining_budget_pct == 100.0
    assert report.burn_rate_1h == 0.0
    assert report.time_to_exhaustion_hours is None


def test_slo_engine_fast_burn_critical_page():
    """With 99.9% availability target (0.1% error budget), a 5% error rate equals a 50x burn rate -> PAGE_CRITICAL."""
    slo = SLODefinition(
        slo_id="slo-gw",
        service_name="api-gateway",
        name="Gateway 99.9%",
        slo_type="availability",
        target_pct=99.9,
    )
    spans = []
    for i in range(100):
        spans.append(
            SpanRecord(
                trace_id=f"tr-slo-{i:04d}",
                span_id=f"sp-slo-{i:04d}",
                service_name="api-gateway",
                operation="GET /",
                start_time_ms=10000.0 + i * 10,
                duration_ms=20.0,
                status_code="ERROR" if i < 5 else "OK",
            )
        )
    engine = SLOEngine([slo])
    report = engine.evaluate_single(slo, spans)
    assert pytest.approx(report.burn_rate_1h, rel=1e-2) == 50.0
    assert report.alert_severity == "PAGE_CRITICAL"
    assert report.remaining_budget_pct == 0.0


def test_slo_engine_slow_burn_ticket_warning():
    """With 99.0% availability target (1% error budget), a 2% error rate equals a 2.0x burn rate -> TICKET_WARNING."""
    slo = SLODefinition(
        slo_id="slo-slow",
        service_name="api-gateway",
        name="Gateway 99.0%",
        slo_type="availability",
        target_pct=99.0,
    )
    spans = [
        SpanRecord(
            trace_id=f"tr-sw-{i:04d}",
            span_id=f"sp-sw-{i:04d}",
            service_name="api-gateway",
            operation="GET /",
            start_time_ms=10000.0 + i * 10,
            duration_ms=20.0,
            status_code="ERROR" if i < 2 else "OK",
        )
        for i in range(100)
    ]
    report = SLOEngine([slo]).evaluate_single(slo, spans)
    assert pytest.approx(report.burn_rate_1h, rel=1e-2) == 2.0
    assert report.alert_severity == "TICKET_WARNING"


def test_slo_engine_latency_sli_evaluation():
    slo = SLODefinition(
        slo_id="slo-lat",
        service_name="postgres-primary",
        name="DB Latency < 100ms",
        slo_type="latency",
        target_pct=99.0,
        latency_threshold_ms=100.0,
    )
    spans = [
        SpanRecord(
            trace_id=f"tr-lat-{i:04d}",
            span_id=f"sp-lat-{i:04d}",
            service_name="postgres-primary",
            operation="SELECT",
            start_time_ms=1000.0 + i,
            duration_ms=250.0 if i < 10 else 15.0,
            status_code="OK",
        )
        for i in range(50)
    ]
    report = SLOEngine([slo]).evaluate_single(slo, spans)
    assert report.bad_events == 10
    assert report.current_sli_pct == 80.0
    assert report.alert_severity == "PAGE_CRITICAL"


# ---------------------------------------------------------------------------
# 5. Automated Incident Correlation & SRE Postmortem Tests
# ---------------------------------------------------------------------------


def test_incident_correlator_healthy_workload():
    store = TelemetryStore()
    ChaosSimulator(seed=1).inject_scenario(store, scenario="normal", count=10)
    analyzer = TraceAnalyzer()
    analyses = [analyzer.analyze_trace(store.get_trace_spans(tid)) for tid in store.trace_order]
    slos = SLOEngine().evaluate_all(store.get_all_spans())
    inc = IncidentCorrelator().correlate(analyses, slos, store.logs)
    assert inc.severity == "NONE"
    assert "System Healthy" in inc.title


def test_incident_correlator_identifies_n_plus_one_root_cause():
    store = TelemetryStore()
    ChaosSimulator(seed=2).inject_scenario(store, scenario="n-plus-one", count=12)
    analyzer = TraceAnalyzer()
    analyses = [analyzer.analyze_trace(store.get_trace_spans(tid)) for tid in store.trace_order]
    slos = SLOEngine().evaluate_all(store.get_all_spans())
    inc = IncidentCorrelator().correlate(analyses, slos, store.logs)
    assert "N+1" in inc.root_cause_category
    assert inc.root_cause_service == "postgres-primary"
    assert "selectinload" in " ".join(inc.action_items)


def test_incident_correlator_identifies_retry_storm_and_renders_markdown():
    store = TelemetryStore()
    ChaosSimulator(seed=3).inject_scenario(store, scenario="retry-storm", count=12)
    analyzer = TraceAnalyzer()
    analyses = [analyzer.analyze_trace(store.get_trace_spans(tid)) for tid in store.trace_order]
    slos = SLOEngine().evaluate_all(store.get_all_spans())
    inc = IncidentCorrelator().correlate(analyses, slos, store.logs)
    assert inc.severity == "SEV-1"
    assert inc.root_cause_service == "payment-worker"
    assert "# Blameless SRE Postmortem" in inc.markdown_postmortem
    assert "Multi-Window SLO & Error Budget Impact" in inc.markdown_postmortem


def test_incident_correlator_identifies_db_saturation():
    store = TelemetryStore()
    ChaosSimulator(seed=4).inject_scenario(store, scenario="db-saturation", count=12)
    analyzer = TraceAnalyzer()
    analyses = [analyzer.analyze_trace(store.get_trace_spans(tid)) for tid in store.trace_order]
    slos = SLOEngine().evaluate_all(store.get_all_spans())
    inc = IncidentCorrelator().correlate(analyses, slos, store.logs)
    assert inc.severity == "SEV-1"
    assert inc.root_cause_service == "postgres-primary"


# ---------------------------------------------------------------------------
# 6. CLI & Benchmark Tests
# ---------------------------------------------------------------------------


def test_cli_parser_simulate_args():
    parser = build_parser()
    args = parser.parse_args(["simulate", "--scenario", "n-plus-one", "--count", "8", "--json"])
    assert args.command == "simulate"
    assert args.scenario == "n-plus-one"
    assert args.count == 8
    assert args.json is True


def test_cli_cmd_simulate_json_output(capsys):
    rc = cmd_simulate(scenario="db-saturation", count=6, as_json=True)
    assert rc == 0
    captured = capsys.readouterr().out
    data = json.loads(captured)
    assert data["scenario"] == "db-saturation"
    assert data["ingestion_stats"]["traces_added"] == 6


def test_cli_cmd_simulate_text_output(capsys):
    rc = cmd_simulate(scenario="all", count=6, as_json=False)
    assert rc == 0
    captured = capsys.readouterr().out
    assert "STACKPULSE v1.0" in captured
    assert "BLAMELESS SRE POSTMORTEM" in captured.upper()


def test_cli_cmd_benchmark(capsys):
    rc = cmd_benchmark(trace_count=40)
    assert rc == 0
    captured = capsys.readouterr().out
    assert "STACKPULSE PIPELINE BENCHMARK REPORT" in captured
    assert "Span Ingestion Throughput" in captured


# ---------------------------------------------------------------------------
# 7. FastAPI Server & Interactive Console Endpoint Tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> TestClient:
    app = create_app(seed_initial_workload=True)
    return TestClient(app)


def test_fastapi_observability_console_html(client: TestClient):
    res = client.get("/")
    assert res.status_code == 200
    assert "StackPulse" in res.text
    assert "Interactive Trace Waterfall" in res.text


def test_fastapi_health_endpoint(client: TestClient):
    res = client.get("/api/v1/health")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["traces_count"] == 24


def test_fastapi_list_traces_and_anomalous_filter(client: TestClient):
    res = client.get("/api/v1/traces?limit=10&anomalous_only=true")
    assert res.status_code == 200
    body = res.json()
    assert len(body["traces"]) <= 10
    assert len(body["traces"]) >= 1


def test_fastapi_trace_detail_and_404(client: TestClient):
    traces_res = client.get("/api/v1/traces?limit=1").json()
    tid = traces_res["traces"][0]["trace_id"]
    detail_res = client.get(f"/api/v1/traces/{tid}")
    assert detail_res.status_code == 200
    assert detail_res.json()["analysis"]["trace_id"] == tid

    not_found = client.get("/api/v1/traces/non-existent-trace")
    assert not_found.status_code == 404


def test_fastapi_topology_endpoint(client: TestClient):
    res = client.get("/api/v1/topology")
    assert res.status_code == 200
    body = res.json()
    assert len(body["nodes"]) >= 5
    assert len(body["edges"]) >= 4


def test_fastapi_slo_endpoint(client: TestClient):
    res = client.get("/api/v1/slo")
    assert res.status_code == 200
    body = res.json()
    assert len(body["reports"]) == 5
    assert body["firing_count"] >= 1


def test_fastapi_incidents_postmortem_endpoint(client: TestClient):
    res = client.get("/api/v1/incidents/postmortem")
    assert res.status_code == 200
    body = res.json()
    assert body["incident_id"] == "INC-2026-089"
    assert "# Blameless SRE Postmortem" in body["markdown_postmortem"]


def test_fastapi_ingest_spans_logs_and_metrics(client: TestClient):
    sp_res = client.post(
        "/api/v1/ingest/spans",
        json={
            "spans": [
                {
                    "trace_id": "tr-custom-9999",
                    "span_id": "sp-custom-1",
                    "service_name": "api-gateway",
                    "operation": "GET /custom",
                    "duration_ms": 14.2,
                    "status_code": "OK",
                }
            ]
        },
    )
    assert sp_res.status_code == 200
    assert sp_res.json()["ingested_spans"] == 1

    lg_res = client.post(
        "/api/v1/ingest/logs",
        json={
            "logs": [
                {
                    "service_name": "api-gateway",
                    "severity": "INFO",
                    "message": "Custom request processed",
                    "trace_id": "tr-custom-9999",
                }
            ]
        },
    )
    assert lg_res.status_code == 200
    assert lg_res.json()["ingested_logs"] == 1

    mt_res = client.post(
        "/api/v1/ingest/metrics",
        json={
            "metrics": [
                {
                    "name": "custom_latency_ms",
                    "metric_type": "histogram",
                    "value": 14.2,
                    "service_name": "api-gateway",
                }
            ]
        },
    )
    assert mt_res.status_code == 200
    assert mt_res.json()["ingested_metrics"] == 1


def test_fastapi_simulate_endpoint_switches_scenarios(client: TestClient):
    for scenario in ("normal", "db-saturation", "n-plus-one", "retry-storm"):
        res = client.post("/api/v1/simulate", json={"scenario": scenario, "count": 8, "reset": True})
        assert res.status_code == 200
        assert res.json()["active_traces"] == 8


@pytest.mark.parametrize(
    "scenario,expected_min_spans",
    [
        ("normal", 6),
        ("db-saturation", 4),
        ("n-plus-one", 10),
        ("retry-storm", 6),
        ("all", 4),
    ],
)
def test_simulator_parametrized_scenarios(scenario, expected_min_spans):
    store = TelemetryStore()
    sim = ChaosSimulator(seed=101)
    stats = sim.inject_scenario(store, scenario=scenario, count=5)
    assert stats["traces_added"] == 5
    assert stats["spans_added"] >= expected_min_spans


@pytest.mark.parametrize(
    "target_pct,bad_count,total_count,expected_severity",
    [
        (99.9, 0, 100, "OK"),
        (99.9, 3, 100, "PAGE_CRITICAL"),
        (99.0, 2, 100, "TICKET_WARNING"),
        (99.5, 5, 100, "PAGE_CRITICAL"),
    ],
)
def test_slo_burn_rate_parametrized_matrix(target_pct, bad_count, total_count, expected_severity):
    slo = SLODefinition(
        slo_id="param-slo",
        service_name="checkout-service",
        name="Param SLO",
        slo_type="availability",
        target_pct=target_pct,
    )
    spans = [
        SpanRecord(
            trace_id=f"tr-p-{i:04d}",
            span_id=f"sp-p-{i:04d}",
            service_name="checkout-service",
            operation="op",
            start_time_ms=1000.0 + i,
            duration_ms=12.0,
            status_code="ERROR" if i < bad_count else "OK",
        )
        for i in range(total_count)
    ]
    report = SLOEngine([slo]).evaluate_single(slo, spans)
    assert report.alert_severity == expected_severity


def test_span_node_to_dict_hierarchy():
    spans = [
        SpanRecord(
            trace_id="tr-dict-01",
            span_id="sp-root",
            service_name="api-gateway",
            operation="GET /",
            start_time_ms=100.0,
            duration_ms=50.0,
        ),
        SpanRecord(
            trace_id="tr-dict-01",
            span_id="sp-child",
            parent_span_id="sp-root",
            service_name="auth-service",
            operation="verify",
            start_time_ms=105.0,
            duration_ms=20.0,
        ),
    ]
    res = TraceAnalyzer().analyze_trace(spans)
    root_dict = res.roots[0].to_dict(100.0, 50.0)
    assert root_dict["span_id"] == "sp-root"
    assert len(root_dict["children"]) == 1
    assert root_dict["children"][0]["span_id"] == "sp-child"
