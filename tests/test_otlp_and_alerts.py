"""Unit and integration tests for OTLP/HTTP JSON ingestion, Alert Dispatcher, and Aegis G4."""

from __future__ import annotations

import base64
import json
import pytest
from fastapi.testclient import TestClient

from stackpulse.aegis_bridge import (
    canonical_json_bytes,
    create_aegis_trajectory_leaf,
    create_aegis_trajectory_payload,
    rfc6962_leaf_hash,
)
from stackpulse.cli import main
from stackpulse.ingest.models import SpanRecord, TelemetryStore
from stackpulse.notifications import AlertDispatcher, AlertEvent
from stackpulse.otlp import (
    map_otlp_span_kind,
    map_otlp_status_code,
    normalize_id,
    parse_otlp_attributes,
    parse_otlp_resource_spans,
    parse_otlp_value,
)
from stackpulse.server.app import create_app
from stackpulse.slo.engine import SLOReport
from stackpulse.tracing.analyzer import TraceAnalyzer


SAMPLE_OTLP_PAYLOAD = {
    "resourceSpans": [
        {
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "payment-api"}},
                    {"key": "service.version", "value": {"stringValue": "1.2.0"}},
                ]
            },
            "scopeSpans": [
                {
                    "scope": {"name": "stackpulse.test", "version": "1.0"},
                    "spans": [
                        {
                            "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                            "spanId": "00f067aa0ba902b7",
                            "parentSpanId": None,
                            "name": "POST /checkout",
                            "kind": 2,  # SPAN_KIND_SERVER
                            "startTimeUnixNano": "1700000000000000000",
                            "endTimeUnixNano": "1700000000120000000",
                            "status": {"code": 1},
                            "attributes": [
                                {"key": "http.status_code", "value": {"intValue": 200}},
                                {"key": "peer.service", "value": {"stringValue": "postgres-db"}},
                            ],
                            "events": [
                                {
                                    "name": "cache_lookup",
                                    "timeUnixNano": "1700000000020000000",
                                    "attributes": [{"key": "cache.hit", "value": {"boolValue": True}}],
                                }
                            ],
                        },
                        {
                            "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                            "spanId": "5fb397be34d23b0f",
                            "parentSpanId": "00f067aa0ba902b7",
                            "name": "SELECT pg_catalog.pg_tables",
                            "kind": 3,  # SPAN_KIND_CLIENT
                            "startTimeUnixNano": "1700000000030000000",
                            "endTimeUnixNano": "1700000000090000000",
                            "status": {"code": 1},
                            "attributes": [
                                {"key": "db.system", "value": {"stringValue": "postgresql"}},
                                {"key": "db.statement", "value": {"stringValue": "SELECT * FROM orders"}},
                            ],
                        },
                    ],
                }
            ],
        }
    ]
}


def test_parse_otlp_value_types() -> None:
    assert parse_otlp_value({"stringValue": "hello"}) == "hello"
    assert parse_otlp_value({"intValue": 42}) == 42
    assert parse_otlp_value({"doubleValue": 3.14}) == 3.14
    assert parse_otlp_value({"boolValue": True}) is True
    assert parse_otlp_value({"arrayValue": {"values": [{"stringValue": "a"}, {"intValue": 1}]}}) == ["a", 1]
    assert parse_otlp_value({"kvlistValue": {"values": [{"key": "sub", "value": {"stringValue": "v"}}]}}) == {"sub": "v"}


def test_normalize_id() -> None:
    # Hex input
    assert normalize_id("4bf92f3577b34da6a3ce929d0e0e4736", 32) == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert normalize_id("00f067aa0ba902b7", 16) == "00f067aa0ba902b7"

    # Base64 input
    raw_span = b"\x00\xf0\x67\xaa\x0b\xa9\x02\xb7"
    b64_span = base64.b64encode(raw_span).decode()
    assert normalize_id(b64_span, 16) == "00f067aa0ba902b7"

    # Int input
    assert normalize_id(255, 4) == "00ff"


def test_map_span_kind_and_status() -> None:
    assert map_otlp_span_kind(1) == "INTERNAL"
    assert map_otlp_span_kind(2) == "SERVER"
    assert map_otlp_span_kind(3) == "CLIENT"
    assert map_otlp_span_kind("SPAN_KIND_PRODUCER") == "PRODUCER"

    assert map_otlp_status_code({"code": 1}) == "OK"
    assert map_otlp_status_code({"code": 2}) == "ERROR"
    assert map_otlp_status_code({"code": 0}) == "UNSET"


def test_parse_otlp_resource_spans_mapping() -> None:
    spans = parse_otlp_resource_spans(SAMPLE_OTLP_PAYLOAD)
    assert len(spans) == 2

    root = spans[0]
    assert root.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert root.span_id == "00f067aa0ba902b7"
    assert root.parent_span_id is None
    assert root.service_name == "payment-api"
    assert root.operation == "POST /checkout"
    assert root.duration_ms == 120.0
    assert root.http_status == 200
    assert root.peer_service == "postgres-db"
    assert len(root.events) == 1
    assert root.events[0].name == "cache_lookup"
    assert root.events[0].attributes["cache.hit"] is True

    child = spans[1]
    assert child.parent_span_id == "00f067aa0ba902b7"
    assert child.duration_ms == 60.0
    assert child.peer_service == "postgresql"


def test_fastapi_v1_traces_otlp_ingestion() -> None:
    app = create_app(seed_initial_workload=False)
    client = TestClient(app)

    # Standard OTLP endpoint
    resp = client.post("/v1/traces", json=SAMPLE_OTLP_PAYLOAD)
    assert resp.status_code == 200
    data = resp.json()
    assert data["ingested_spans"] == 2
    assert "partialSuccess" in data

    # Verify trace is queryable in detail
    detail = client.get(f"/api/v1/traces/4bf92f3577b34da6a3ce929d0e0e4736")
    assert detail.status_code == 200
    analysis = detail.json()["analysis"]
    assert analysis["span_count"] == 2
    assert analysis["root_service"] == "payment-api"
    assert analysis["total_duration_ms"] == 120.0


def test_fastapi_otlp_alias_route() -> None:
    app = create_app(seed_initial_workload=False)
    client = TestClient(app)

    resp = client.post("/api/v1/otlp/traces", json=SAMPLE_OTLP_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["ingested_spans"] == 2


def test_alert_dispatcher_cooldown_and_slack_payload() -> None:
    dispatcher = AlertDispatcher(cooldown_seconds=60.0)

    delivered = []
    dispatcher.register_webhook("https://hooks.slack.com/services/TEST/ABC/123")
    dispatcher.set_custom_sender(lambda url, payload: delivered.append((url, payload)) or True)

    slo_rep = SLOReport(
        slo_id="slo-checkout-p99",
        service_name="checkout-service",
        name="checkout_latency_p99",
        slo_type="latency",
        target_pct=99.9,
        current_sli_pct=97.0,
        error_budget_fraction=0.001,
        remaining_budget_pct=15.0,
        total_events=500,
        bad_events=15,
        burn_rate_1h=14.5,
        burn_rate_6h=10.0,
        burn_rate_24h=5.0,
        burn_rate_72h=2.0,
        alert_severity="PAGE_CRITICAL",
        alert_reason="Burn rate 14.5x exceeds 14.4x 1h threshold",
        time_to_exhaustion_hours=2.4,
    )

    # First evaluation fires
    evs = dispatcher.evaluate_slo_reports([slo_rep])
    assert len(evs) == 1
    assert evs[0].severity == "PAGE_CRITICAL"
    assert len(delivered) == 1

    # Second immediate evaluation is suppressed by cooldown
    evs2 = dispatcher.evaluate_slo_reports([slo_rep])
    assert len(evs2) == 0

    # Test Slack formatting
    slack = evs[0].to_slack_payload()
    assert "text" in slack
    assert "blocks" in slack
    assert len(slack["blocks"]) >= 3


def test_fastapi_alert_endpoints_lifecycle() -> None:
    app = create_app(seed_initial_workload=True)
    client = TestClient(app)

    # 1. Register webhook
    wh_resp = client.post("/api/v1/alerts/register-webhook", json={"url": "https://alert.example.com/sre"})
    assert wh_resp.status_code == 200
    assert wh_resp.json()["registered"] is True

    # 2. Evaluate and dispatch
    ev_resp = client.post("/api/v1/alerts/evaluate")
    assert ev_resp.status_code == 200
    data = ev_resp.json()
    assert "dispatched_count" in data

    # 3. Check history
    hist_resp = client.get("/api/v1/alerts/history")
    assert hist_resp.status_code == 200
    assert hist_resp.json()["count"] == data["dispatched_count"]


def test_aegis_g4_leaf_generation_and_api() -> None:
    analyzer = TraceAnalyzer()
    spans = parse_otlp_resource_spans(SAMPLE_OTLP_PAYLOAD)
    analysis = analyzer.analyze_trace(spans)

    leaf = create_aegis_trajectory_leaf(analysis)
    assert leaf["gate"] == "G4_TRAJECTORY_HEALTH"
    assert leaf["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert leaf["passed"] is True
    assert len(leaf["leaf_hash"]) == 64

    # Test API endpoint
    app = create_app(seed_initial_workload=False)
    client = TestClient(app)
    client.post("/v1/traces", json=SAMPLE_OTLP_PAYLOAD)

    resp = client.get(f"/api/v1/aegis/leaf/4bf92f3577b34da6a3ce929d0e0e4736")
    assert resp.status_code == 200
    api_leaf = resp.json()
    assert api_leaf["leaf_hash"] == leaf["leaf_hash"]
    assert api_leaf["payload"]["root_service"] == "payment-api"


def test_cli_export_aegis_and_ingest_otlp(tmp_path, capsys) -> None:
    # 1. Test CLI export-aegis
    ret = main(["export-aegis"])
    assert ret == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["gate"] == "G4_TRAJECTORY_HEALTH"
    assert "leaf_hash" in data

    # 2. Test CLI ingest-otlp with file
    otlp_file = tmp_path / "otlp.json"
    otlp_file.write_text(json.dumps(SAMPLE_OTLP_PAYLOAD))
    ret2 = main(["ingest-otlp", "-f", str(otlp_file)])
    assert ret2 == 0
    out2 = capsys.readouterr().out
    assert "Successfully ingested 2 OTLP spans" in out2


def test_otlp_int_and_string_enums_all_kinds() -> None:
    assert map_otlp_span_kind(4) == "PRODUCER"
    assert map_otlp_span_kind(5) == "CONSUMER"
    assert map_otlp_span_kind("SPAN_KIND_CONSUMER") == "CONSUMER"
    assert map_otlp_span_kind("internal") == "INTERNAL"
    assert map_otlp_span_kind(99) == "INTERNAL"


def test_otlp_kvlist_nested_and_arrays() -> None:
    val = {
        "kvlistValue": {
            "values": [
                {"key": "cluster", "value": {"stringValue": "us-east-1"}},
                {"key": "node_ids", "value": {"arrayValue": {"values": [{"intValue": 101}, {"intValue": 102}]}}},
            ]
        }
    }
    parsed = parse_otlp_value(val)
    assert parsed == {"cluster": "us-east-1", "node_ids": [101, 102]}


def test_otlp_empty_resource_spans_graceful() -> None:
    assert parse_otlp_resource_spans({}) == []
    assert parse_otlp_resource_spans({"resourceSpans": None}) == []
    assert parse_otlp_resource_spans({"resourceSpans": []}) == []


def test_otlp_nanosecond_to_millisecond_conversion_accuracy() -> None:
    payload = {
        "resourceSpans": [
            {
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "11112222333344445555666677778888",
                                "spanId": "aaaabbbbccccdddd",
                                "name": "op_test",
                                "startTimeUnixNano": "1700000000000000000",
                                "endTimeUnixNano": "1700000000055500000",
                            }
                        ]
                    }
                ]
            }
        ]
    }
    spans = parse_otlp_resource_spans(payload)
    assert len(spans) == 1
    assert spans[0].start_time_ms == 1700000000000.0
    assert spans[0].duration_ms == 55.5


def test_alert_dispatcher_custom_sender_handling() -> None:
    dispatcher = AlertDispatcher()
    called = []
    dispatcher.register_webhook("https://webhook1")
    dispatcher.set_custom_sender(lambda url, p: called.append(url) or True)

    ev = AlertEvent(
        alert_id="a1",
        alert_name="TestAlert",
        severity="INFO",
        service="auth",
        summary="Info summary",
    )
    dispatcher._dispatch_event(ev)
    assert "https://webhook1" in called


def test_aegis_rfc6962_sha256_canonical_merkle_spec() -> None:
    payload = {"z": 1, "a": 2}
    raw = canonical_json_bytes(payload)
    assert raw == b'{"a":2,"z":1}'

    h = rfc6962_leaf_hash(payload)
    assert len(h) == 64


def test_aegis_trajectory_failing_when_trace_has_errors() -> None:
    spans = [
        SpanRecord(
            trace_id="errtrace111122223333444455556666",
            span_id="errspan11112222",
            parent_span_id=None,
            service_name="api",
            operation="GET /fail",
            start_time_ms=100.0,
            duration_ms=50.0,
            status_code="ERROR",
            http_status=500,
        )
    ]
    analyzer = TraceAnalyzer()
    analysis = analyzer.analyze_trace(spans)
    leaf = create_aegis_trajectory_leaf(analysis)
    assert leaf["passed"] is False
    assert leaf["payload"]["has_error"] is True


def test_fastapi_v1_traces_malformed_json_422() -> None:
    app = create_app(seed_initial_workload=False)
    client = TestClient(app)
    # Sending invalid body
    resp = client.post("/v1/traces", content=b"invalid json", headers={"Content-Type": "application/json"})
    assert resp.status_code == 422
