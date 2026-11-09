# Changelog

All notable changes to **StackPulse** are documented in this file.

## [1.1.0] - 2026-11-09

### Added
- **OpenTelemetry Standard OTLP/HTTP JSON Mapping Adapter (`stackpulse.otlp`)**:
  - Direct ingestion compatibility for standard OpenTelemetry Collector `resourceSpans` payloads.
  - Native deserialization of OTLP `AnyValue` containers (strings, ints, doubles, bools, nested arrays, and kvlists) with 128-bit/64-bit ID hex normalization (Closes #1).
  - Production endpoints at `POST /v1/traces` (standard OTLP) and `POST /api/v1/otlp/traces`.
- **SRE Alert Dispatcher & Webhook Notification Engine (`stackpulse.notifications`)**:
  - Webhook integration supporting Slack blocks and JSON event streams for multi-window SLO burn-rate violations (`PAGE_CRITICAL`, `TICKET_WARNING`) and Correlated Incident postmortems.
  - Built-in cooling-off window and deduplication mechanism to prevent alerting storms during cascade failures.
  - API endpoints `POST /api/v1/alerts/register-webhook`, `GET /api/v1/alerts/history`, and `POST /api/v1/alerts/evaluate`.
- **Nexus Aegis G4 Trajectory Health Bridge (`stackpulse.aegis_bridge`)**:
  - Cryptographic RFC 6962 SHA-256 Merkle leaf attestation (`0x00 || canonical_json`) certifying trace health, critical paths, and anomaly counts.
  - API endpoint `GET /api/v1/aegis/leaf/{trace_id}` and CLI command `stackpulse export-aegis`.
- **CLI Enhancements (`stackpulse.cli`)**:
  - `stackpulse ingest-otlp -f <file>` for local batch trace ingestion.
  - `stackpulse export-aegis` for instant cryptographic audit attestation exports.
- **Test Suite**:
  - Expanded unit and integration test suite from 47 to 65 tests with 100% pass rate.

## [1.0.0] - 2026-11-02

### Added
- **OpenTelemetry-Compatible Telemetry Store (`stackpulse/ingest/`)**: W3C `SpanRecord`, correlated `LogRecord`, and `MetricPoint` ingestion with bounded ring-buffer eviction.
- **Distributed Trace DAG & Critical-Path Analyzer (`stackpulse/tracing/analyzer.py`)**: Parent-child tree assembly, merged child interval exclusive duration calculation, N+1 ORM query regression detection, and downstream retry storm detection.
- **Service Dependency Topology & Anomaly Detector (`stackpulse/topology/graph.py`)**: Live caller-to-callee graph with RPS, error rate %, P50/P95/P99 latency percentiles, and z-score spike detection.
- **Multi-Window SLO & Error-Budget Burn-Rate Engine (`stackpulse/slo/engine.py`)**: Google SRE Workbook 1h/6h/24h/72h burn-rate alerting (`PAGE_CRITICAL`, `TICKET_WARNING`, `OK`) and time-to-exhaustion projection.
- **Automated Incident Timeline & Blameless SRE Postmortem Generator (`stackpulse/incidents/correlator.py`)**: Root-cause ranking and Markdown postmortem synthesis.
- **Chaos Workload Simulator, CLI & FastAPI Dark-Mode Console (`stackpulse/simulator/`, `stackpulse/cli.py`, `stackpulse/server/app.py`)**.
