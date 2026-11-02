# Changelog

All notable changes to **StackPulse** are documented in this file.

## [1.0.0] - 2026-11-02

### Added
- **OpenTelemetry-Compatible Telemetry Store (`stackpulse/ingest/`)**: W3C `SpanRecord`, correlated `LogRecord`, and `MetricPoint` ingestion with bounded ring-buffer eviction.
- **Distributed Trace DAG & Critical-Path Analyzer (`stackpulse/tracing/analyzer.py`)**: Parent-child tree assembly, merged child interval exclusive duration calculation, N+1 ORM query regression detection, and downstream retry storm detection.
- **Service Dependency Topology & Anomaly Detector (`stackpulse/topology/graph.py`)**: Live caller-to-callee graph with RPS, error rate %, P50/P95/P99 latency percentiles, and z-score spike detection.
- **Multi-Window SLO & Error-Budget Burn-Rate Engine (`stackpulse/slo/engine.py`)**: Google SRE Workbook 1h/6h/24h/72h burn-rate alerting (`PAGE_CRITICAL`, `TICKET_WARNING`, `OK`) and time-to-exhaustion projection.
- **Automated Incident Timeline & Blameless SRE Postmortem Generator (`stackpulse/incidents/correlator.py`)**: Root-cause ranking and Markdown postmortem synthesis.
- **Chaos Workload Simulator, CLI & FastAPI Dark-Mode Console (`stackpulse/simulator/`, `stackpulse/cli.py`, `stackpulse/server/app.py`)**.
