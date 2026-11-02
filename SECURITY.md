# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

## Reporting a Vulnerability

Please report suspected security vulnerabilities via GitHub Security Advisories on `codesphere-u/stackpulse` rather than opening a public issue. Include:
- Affected component (`stackpulse/ingest`, `stackpulse/server`, etc.)
- Minimal reproduction payload or curl command
- Impact assessment

## Telemetry Ingestion Guardrails

- All span, log, and metric payloads are validated via strict Pydantic v2 schemas (`SpanRecord`, `LogRecord`, `MetricPoint`).
- Bounded ring-buffer retention (`max_traces`, `max_logs`, `max_metrics`) prevents unbounded memory exhaustion from high-cardinality span floods.
