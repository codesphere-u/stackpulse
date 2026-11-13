"""Production hardening, SSRF validation, probe, and metrics tests for StackPulse."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from stackpulse.server.app import create_app


def test_health_probes_head_and_security_headers() -> None:
    app = create_app(seed_initial_workload=False)
    with TestClient(app) as client:
        for path in ("/health", "/healthz", "/readyz", "/api/v1/health"):
            res = client.get(path, headers={"X-Request-ID": "sp-req-001"})
            assert res.status_code == 200
            assert res.headers["x-request-id"] == "sp-req-001"
            assert res.headers["x-content-type-options"] == "nosniff"
            assert res.headers["x-frame-options"] == "SAMEORIGIN"
            assert res.headers["referrer-policy"] == "strict-origin-when-cross-origin"
            assert res.headers["cache-control"] == "no-store"

            head = client.head(path)
            assert head.status_code == 200
            assert head.content == b""


def test_metrics_prometheus_and_json() -> None:
    app = create_app(seed_initial_workload=False)
    with TestClient(app) as client:
        client.get("/health")
        prom = client.get("/metrics")
        assert prom.status_code == 200
        assert "http_requests_total" in prom.text
        assert "stackpulse_traces_count" in prom.text

        as_json = client.get("/metrics?format=json")
        assert as_json.status_code == 200
        body = as_json.json()
        assert body["service"] == "stackpulse"
        assert body["total_requests"] >= 2


def test_body_limit_and_invalid_content_length() -> None:
    app = create_app(seed_initial_workload=False, max_body_bytes=256)
    client = TestClient(app)

    oversized = client.post(
        "/api/v1/ingest/spans",
        content=b"x" * 512,
        headers={"Content-Type": "application/json"},
    )
    assert oversized.status_code == 413
    assert oversized.headers["x-content-type-options"] == "nosniff"

    bad_cl = client.post(
        "/api/v1/ingest/spans",
        content=b"{}",
        headers={"Content-Type": "application/json", "Content-Length": "-5"},
    )
    assert bad_cl.status_code == 400


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "http://169.254.169.254/latest/meta-data/",
        "http://127.0.0.1:8080/hook",
        "http://localhost/hook",
        "http://10.0.0.5/hook",
        "http://192.168.1.10/hook",
        "http://[::1]:8000/hook",
        "http://2130706433/hook",
        "http://user:pass@example.com/hook",
        "file:///etc/passwd",
        "http://service.internal/hook",
    ],
)
def test_webhook_registration_blocks_ssrf_targets(unsafe_url: str) -> None:
    app = create_app(seed_initial_workload=False)
    client = TestClient(app)
    res = client.post("/api/v1/alerts/register-webhook", json={"url": unsafe_url})
    assert res.status_code == 400
    assert "SSRF" in res.json()["detail"] or "scheme" in res.json()["detail"] or "credentials" in res.json()["detail"]


def test_unhandled_exception_sanitized_no_traceback_leak() -> None:
    app = create_app(seed_initial_workload=False)

    @app.get("/api/v1/test-boom")
    def boom() -> None:
        raise RuntimeError("sensitive-internal-secret-value")

    client = TestClient(app, raise_server_exceptions=False)
    res = client.get("/api/v1/test-boom", headers={"X-Request-ID": "sp-boom-1"})
    assert res.status_code == 500
    assert "sensitive-internal-secret-value" not in res.text
    assert res.json()["request_id"] == "sp-boom-1"
