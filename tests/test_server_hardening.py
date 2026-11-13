"""Production hardening, SSRF validation, probe, and metrics tests for StackPulse."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from stackpulse import cli as stackpulse_cli
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
            assert head.headers["cache-control"] == "no-store"
            assert head.headers["x-content-type-options"] == "nosniff"

        ui_resp = client.get("/")
        assert ui_resp.status_code == 200
        assert ui_resp.headers["x-content-type-options"] == "nosniff"
        assert "cache-control" not in ui_resp.headers


def test_request_id_sanitization() -> None:
    client = TestClient(create_app(seed_initial_workload=False))
    for bad_id in ("bad id", "bad\nid", "<script>", "a" * 200):
        resp = client.get("/health", headers={"X-Request-ID": bad_id})
        assert resp.status_code == 200
        echoed = resp.headers["x-request-id"]
        assert echoed != bad_id
        assert len(echoed) == 32


def test_metrics_prometheus_and_json() -> None:
    app = create_app(seed_initial_workload=True)
    with TestClient(app) as client:
        client.get("/health")
        client.get("/api/v1/traces/does-not-exist")  # 404

        prom = client.get("/metrics")
        assert prom.status_code == 200
        assert prom.headers["content-type"].startswith("text/plain; version=0.0.4")
        assert 'http_requests_total{service="stackpulse"} 2' in prom.text
        assert 'http_requests_by_status_total{service="stackpulse",status="404"} 1' in prom.text
        assert "stackpulse_traces_count" in prom.text
        assert "stackpulse_spans_count" in prom.text
        assert "stackpulse_alerts_dispatched_count" in prom.text

        as_json = client.get("/metrics", headers={"Accept": "application/json"})
        assert as_json.status_code == 200
        body = as_json.json()
        assert body["service"] == "stackpulse"
        assert body["total_requests"] == 3
        assert body["error_counts"]["client_errors_4xx"] == 1
        assert body["gauges"]["traces_count"] == 24
        assert body["gauges"]["spans_count"] > 24
        assert body["uptime_seconds"] >= 0.0


def test_body_limit_and_invalid_content_length(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STACKPULSE_MAX_BODY_BYTES", "256")
    client = TestClient(create_app(seed_initial_workload=False))

    oversized = client.post(
        "/api/v1/ingest/spans",
        content=b"x" * 512,
        headers={"Content-Type": "application/json", "X-Request-ID": "sp-413"},
    )
    assert oversized.status_code == 413
    assert oversized.json()["error"] == "Payload too large"
    assert oversized.json()["request_id"] == "sp-413"
    assert oversized.headers["x-request-id"] == "sp-413"
    assert oversized.headers["x-content-type-options"] == "nosniff"
    assert oversized.headers["cache-control"] == "no-store"

    def chunked_body() -> Iterator[bytes]:
        yield b'{"spans":[{"trace_id":"'
        yield b"a" * 400
        yield b'"}]}'

    chunked_resp = client.post(
        "/api/v1/ingest/spans",
        content=chunked_body(),
        headers={"Content-Type": "application/json"},
    )
    assert chunked_resp.status_code == 413

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
        "http://[::ffff:169.254.169.254]/latest/meta-data/",
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
    detail = res.json()["detail"]
    assert "SSRF" in detail or "scheme" in detail or "credentials" in detail


def test_webhook_registration_allows_private_with_env_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STACKPULSE_ALLOW_PRIVATE_WEBHOOKS", "1")
    client = TestClient(create_app(seed_initial_workload=False))
    ok = client.post(
        "/api/v1/alerts/register-webhook",
        json={"url": "http://127.0.0.1:9000/hook"},
    )
    assert ok.status_code == 200
    assert ok.json()["registered"] is True

    bad_scheme = client.post(
        "/api/v1/alerts/register-webhook",
        json={"url": "file:///etc/passwd"},
    )
    assert bad_scheme.status_code == 400


def test_unhandled_exception_sanitized_and_http_errors_preserved() -> None:
    app = create_app(seed_initial_workload=False)

    @app.get("/api/v1/test-boom")
    def boom() -> None:
        raise RuntimeError("sensitive-internal-secret-value")

    @app.get("/api/v1/test-http")
    def http_err() -> None:
        raise HTTPException(status_code=404, detail="Custom item missing")

    client = TestClient(app, raise_server_exceptions=False)
    res = client.get("/api/v1/test-boom", headers={"X-Request-ID": "sp-boom-1"})
    assert res.status_code == 500
    assert "sensitive-internal-secret-value" not in res.text
    assert res.json() == {"error": "Internal server error", "request_id": "sp-boom-1"}
    assert res.headers["x-request-id"] == "sp-boom-1"
    assert res.headers["x-content-type-options"] == "nosniff"
    assert res.headers["cache-control"] == "no-store"

    http_res = client.get("/api/v1/test-http")
    assert http_res.status_code == 404
    assert http_res.json()["detail"] == "Custom item missing"


def test_cli_serve_invokes_uvicorn(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    def fake_run(target: object, host: str, port: int, reload: bool = False) -> None:
        called["target"] = target
        called["host"] = host
        called["port"] = port
        called["reload"] = reload

    monkeypatch.setattr(stackpulse_cli.uvicorn, "run", fake_run)
    rc = stackpulse_cli.main(["serve", "--host", "0.0.0.0", "--port", "8098"])
    assert rc == 0
    assert called == {
        "target": "stackpulse.server.app:app",
        "host": "0.0.0.0",
        "port": 8098,
        "reload": False,
    }
