# StackPulse Architecture & Telemetry Pipeline

## Overview

StackPulse processes distributed telemetry through five decoupled layers:

1. **Ingestion & Bounded Retention (`stackpulse/ingest/models.py`)**
   - Accepts W3C TraceContext spans (`trace_id`, `span_id`, `parent_span_id`, `service_name`, `operation`, `duration_ms`, `status_code`, `attributes`, `events`), structured logs, and time-series metrics.
2. **Trace DAG Reconstruction & Critical-Path Analysis (`stackpulse/tracing/analyzer.py`)**
   - Links flat spans into a directed acyclic tree (`SpanNode`), computes interval-merged exclusive durations, traces the heaviest latency path, and detects N+1 query patterns and retry storms.
3. **Service Dependency Topology (`stackpulse/topology/graph.py`)**
   - Infers caller $\rightarrow$ callee edges from parent-child span transitions and `PeerService` attributes, calculating P50/P95/P99 latency and error rates.
4. **Multi-Window SLO Engine (`stackpulse/slo/engine.py`)**
   - Evaluates availability and latency SLIs across 1h, 6h, 24h, and 72h windows using the Google SRE Workbook multi-burn-rate alerting model.
5. **Automated Incident Correlator (`stackpulse/incidents/correlator.py`)**
   - Synthesizes trace bottlenecks, error logs, and SLO alerts into a ranked root-cause determination and a Markdown SRE Postmortem.
