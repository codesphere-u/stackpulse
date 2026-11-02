"""FastAPI Observability Backend & Interactive Dark-Mode SRE Console."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from stackpulse import __version__
from stackpulse.incidents.correlator import IncidentCorrelator
from stackpulse.ingest.models import (
    LogRecord,
    MetricPoint,
    SpanRecord,
    TelemetryStore,
)
from stackpulse.simulator.chaos import ChaosSimulator, ScenarioType
from stackpulse.slo.engine import SLOEngine
from stackpulse.topology.graph import TopologyGraphBuilder
from stackpulse.tracing.analyzer import TraceAnalyzer


class SpanBatchRequest(BaseModel):
    spans: list[SpanRecord] = Field(..., min_length=1)


class LogBatchRequest(BaseModel):
    logs: list[LogRecord] = Field(..., min_length=1)


class MetricBatchRequest(BaseModel):
    metrics: list[MetricPoint] = Field(..., min_length=1)


class SimulateRequest(BaseModel):
    scenario: ScenarioType = "all"
    count: int = Field(default=24, ge=1, le=200)
    reset: bool = True


CONSOLE_HTML = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>StackPulse — Cloud Observability, Distributed Tracing & SLO Console</title>
  <style>
    :root {
      --bg: #090d16;
      --panel: #111827;
      --panel-alt: #172033;
      --border: #1f2937;
      --text: #f3f4f6;
      --muted: #9ca3af;
      --accent: #38bdf8;
      --ok: #10b981;
      --warn: #f59e0b;
      --crit: #ef4444;
      --purple: #a855f7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif;
      font-size: 13.5px;
      line-height: 1.5;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 14px 24px;
      background: #0d1322;
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
      z-index: 20;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .brand-dot {
      width: 11px;
      height: 11px;
      border-radius: 50%;
      background: var(--ok);
      box-shadow: 0 0 12px var(--ok);
    }
    .brand h1 {
      font-size: 16px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .brand span {
      font-size: 11.5px;
      color: var(--muted);
      background: var(--panel-alt);
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid var(--border);
    }
    .controls {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    button {
      background: var(--panel-alt);
      color: var(--text);
      border: 1px solid #334155;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    button:hover {
      border-color: var(--accent);
      background: #1e293b;
    }
    button.primary {
      background: #0284c7;
      border-color: #38bdf8;
    }
    button.danger {
      background: #7f1d1d;
      border-color: #ef4444;
    }
    main {
      max-width: 1560px;
      margin: 0 auto;
      padding: 18px 24px 48px;
      display: grid;
      gap: 18px;
    }
    .kpi-strip {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
    }
    .kpi-card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px 16px;
    }
    .kpi-label {
      color: var(--muted);
      font-size: 11.5px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .kpi-val {
      font-size: 24px;
      font-weight: 700;
      margin-top: 4px;
      font-variant-numeric: tabular-nums;
    }
    .grid-2 {
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 18px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
    }
    .panel-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 11px 16px;
      background: #0e1626;
      border-bottom: 1px solid var(--border);
      font-weight: 600;
      font-size: 13px;
    }
    .panel-body {
      padding: 14px 16px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-variant-numeric: tabular-nums;
    }
    th, td {
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid var(--border);
      font-size: 12.5px;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    tr.clickable:hover {
      background: var(--panel-alt);
      cursor: pointer;
    }
    .badge {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 4px;
      font-size: 10.5px;
      font-weight: 700;
      text-transform: uppercase;
    }
    .badge-ok { background: rgba(16, 185, 129, 0.16); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.35); }
    .badge-warn { background: rgba(245, 158, 11, 0.16); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.35); }
    .badge-crit { background: rgba(239, 68, 68, 0.18); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .badge-purple { background: rgba(168, 85, 247, 0.18); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.4); }
    .waterfall-row {
      display: grid;
      grid-template-columns: 270px 1fr 85px;
      align-items: center;
      gap: 10px;
      padding: 6px 8px;
      border-bottom: 1px solid rgba(31, 41, 55, 0.7);
      font-size: 12px;
    }
    .waterfall-track {
      position: relative;
      height: 18px;
      background: #0b111e;
      border-radius: 4px;
      overflow: hidden;
      border: 1px solid #1e293b;
    }
    .waterfall-bar {
      position: absolute;
      top: 2px;
      bottom: 2px;
      border-radius: 3px;
      background: #38bdf8;
    }
    .waterfall-bar.crit { background: #ef4444; }
    .waterfall-bar.nplus1 { background: #a855f7; }
    .waterfall-bar.bottleneck { box-shadow: 0 0 0 1px #fbbf24 inset; }
    pre.postmortem {
      background: #080c14;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 14px;
      color: #e2e8f0;
      font-family: "JetBrains Mono", "Fira Code", monospace;
      font-size: 12px;
      white-space: pre-wrap;
      max-height: 360px;
      overflow-y: auto;
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="brand-dot"></div>
      <h1>StackPulse</h1>
      <span>v1.0 • OpenTelemetry DAG + Multi-Window SLO Console</span>
    </div>
    <div class="controls">
      <button onclick="injectScenario('normal')">Baseline Workload</button>
      <button class="danger" onclick="injectScenario('db-saturation')">Inject DB Pool Saturation</button>
      <button class="danger" onclick="injectScenario('n-plus-one')">Inject N+1 Query Regression</button>
      <button class="danger" onclick="injectScenario('retry-storm')">Inject 503 Retry Storm</button>
      <button class="primary" onclick="injectScenario('all')">Run Full Chaos Mix</button>
    </div>
  </header>

  <main>
    <section class="kpi-strip">
      <div class="kpi-card">
        <div class="kpi-label">Active Distributed Traces</div>
        <div class="kpi-val" id="kpi-traces">0</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Topology Services Monitored</div>
        <div class="kpi-val" id="kpi-services">0</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">SLO Burn Alerts Firing</div>
        <div class="kpi-val" id="kpi-slo-alerts">0</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Correlated Incident Severity</div>
        <div class="kpi-val" id="kpi-incident-sev">NONE</div>
      </div>
    </section>

    <div class="grid-2">
      <div class="panel">
        <div class="panel-header">
          <span>Service Health & Dependency Topology Matrix</span>
          <span style="color: var(--muted); font-size: 11.5px;">Golden Signals (RPS, Error %, P50/P95/P99)</span>
        </div>
        <div class="panel-body">
          <table>
            <thead>
              <tr>
                <th>Service</th>
                <th>Status</th>
                <th>Spans</th>
                <th>Error %</th>
                <th>P50</th>
                <th>P95</th>
                <th>P99</th>
                <th>Downstream</th>
              </tr>
            </thead>
            <tbody id="topology-tbody"></tbody>
          </table>
        </div>
      </div>

      <div class="panel">
        <div class="panel-header">
          <span>Multi-Window SLO & Error-Budget Burn-Rate Monitor</span>
          <span style="color: var(--muted); font-size: 11.5px;">Google SRE Workbook (1h / 6h / 24h)</span>
        </div>
        <div class="panel-body">
          <table>
            <thead>
              <tr>
                <th>SLO Target</th>
                <th>Alert</th>
                <th>SLI %</th>
                <th>1h Burn</th>
                <th>6h Burn</th>
                <th>Budget Left</th>
              </tr>
            </thead>
            <tbody id="slo-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="grid-2">
      <div class="panel">
        <div class="panel-header">
          <span>Distributed Trace Explorer (Click Row to Inspect Waterfall)</span>
          <span id="trace-count-label" style="color: var(--muted); font-size: 11.5px;"></span>
        </div>
        <div class="panel-body" style="max-height: 410px; overflow-y: auto;">
          <table>
            <thead>
              <tr>
                <th>Trace ID</th>
                <th>Root Operation</th>
                <th>Duration</th>
                <th>Spans</th>
                <th>Critical Bottleneck</th>
                <th>Diagnostics</th>
              </tr>
            </thead>
            <tbody id="traces-tbody"></tbody>
          </table>
        </div>
      </div>

      <div class="panel">
        <div class="panel-header">
          <span>Interactive Trace Waterfall & Critical-Path Inspector</span>
          <span id="waterfall-title" style="color: var(--accent); font-size: 11.5px;">Select a trace</span>
        </div>
        <div class="panel-body" id="waterfall-container" style="max-height: 410px; overflow-y: auto;">
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-header">
        <span>Automated Incident Timeline & Blameless SRE Postmortem Generator</span>
        <button onclick="refreshAll()">Regenerate Postmortem</button>
      </div>
      <div class="panel-body">
        <pre class="postmortem" id="postmortem-box">Loading correlated incident analysis...</pre>
      </div>
    </div>
  </main>

  <script>
    async function injectScenario(scenario) {
      await fetch('/api/v1/simulate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scenario, count: 24, reset: true })
      });
      await refreshAll();
    }

    function statusBadge(status) {
      if (status === 'HEALTHY' || status === 'OK') return `<span class="badge badge-ok">${status}</span>`;
      if (status === 'DEGRADED' || status === 'TICKET_WARNING') return `<span class="badge badge-warn">${status}</span>`;
      return `<span class="badge badge-crit">${status}</span>`;
    }

    async function inspectTrace(traceId) {
      const res = await fetch(`/api/v1/traces/${traceId}`);
      const data = await res.json();
      const t = data.analysis;
      document.getElementById('waterfall-title').textContent =
        `${t.trace_id} • ${t.total_duration_ms}ms • Bottleneck: ${t.bottleneck_service} (${t.bottleneck_exclusive_ms}ms excl)`;
      const container = document.getElementById('waterfall-container');
      container.innerHTML = t.waterfall.map(row => {
        const indent = row.depth * 14;
        let barClass = 'waterfall-bar';
        if (row.status_code === 'ERROR' || row.is_retry) barClass += ' crit';
        else if (row.is_n_plus_one) barClass += ' nplus1';
        if (row.on_critical_path) barClass += ' bottleneck';

        let tags = '';
        if (row.on_critical_path) tags += `<span class="badge badge-warn" style="margin-left:4px">CRIT</span>`;
        if (row.is_n_plus_one) tags += `<span class="badge badge-purple" style="margin-left:4px">N+1</span>`;
        if (row.is_retry) tags += `<span class="badge badge-crit" style="margin-left:4px">RETRY</span>`;

        return `
          <div class="waterfall-row">
            <div style="padding-left:${indent}px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${row.service_name}: ${row.operation}">
              <strong>${row.service_name}</strong> <span style="color:var(--muted)">${row.operation}</span>${tags}
            </div>
            <div class="waterfall-track">
              <div class="${barClass}" style="left:${row.offset_pct}%; width:${row.width_pct}%;"></div>
            </div>
            <div style="text-align:right; font-variant-numeric:tabular-nums;">
              ${row.duration_ms}ms <span style="color:var(--muted); font-size:10.5px;">(${row.exclusive_duration_ms}m)</span>
            </div>
          </div>
        `;
      }).join('');
    }

    async function refreshAll() {
      const [topoRes, sloRes, tracesRes, incRes] = await Promise.all([
        fetch('/api/v1/topology'),
        fetch('/api/v1/slo'),
        fetch('/api/v1/traces?limit=25'),
        fetch('/api/v1/incidents/postmortem')
      ]);
      const topo = await topoRes.json();
      const slo = await sloRes.json();
      const tracesData = await tracesRes.json();
      const inc = await incRes.json();

      document.getElementById('kpi-traces').textContent = tracesData.total_traces;
      document.getElementById('kpi-services').textContent = topo.nodes.length;
      const firingCount = slo.reports.filter(r => r.alert_severity !== 'OK').length;
      document.getElementById('kpi-slo-alerts').textContent = firingCount;
      document.getElementById('kpi-incident-sev').textContent = inc.severity;

      document.getElementById('topology-tbody').innerHTML = topo.nodes.map(n => `
        <tr>
          <td><strong>${n.service_name}</strong></td>
          <td>${statusBadge(n.status)}</td>
          <td>${n.request_count}</td>
          <td>${n.error_rate_pct}%</td>
          <td>${n.p50_ms}ms</td>
          <td><strong>${n.p95_ms}ms</strong></td>
          <td>${n.p99_ms}ms</td>
          <td style="color:var(--muted)">${n.downstream_services.join(', ') || '—'}</td>
        </tr>
      `).join('');

      document.getElementById('slo-tbody').innerHTML = slo.reports.map(r => `
        <tr>
          <td><strong>${r.service_name}</strong><div style="color:var(--muted); font-size:11px;">${r.name}</div></td>
          <td>${statusBadge(r.alert_severity)}</td>
          <td>${r.current_sli_pct}%</td>
          <td><strong>${r.burn_rate_1h}x</strong></td>
          <td>${r.burn_rate_6h}x</td>
          <td>${r.remaining_budget_pct}%</td>
        </tr>
      `).join('');

      document.getElementById('trace-count-label').textContent = `Showing ${tracesData.traces.length} of ${tracesData.total_traces}`;
      document.getElementById('traces-tbody').innerHTML = tracesData.traces.map(t => {
        let diag = [];
        if (t.has_error) diag.push('<span class="badge badge-crit">ERROR</span>');
        if (t.n_plus_one_findings.length > 0) diag.push(`<span class="badge badge-purple">N+1 (${t.n_plus_one_findings[0].repeat_count}q)</span>`);
        if (t.retry_findings.length > 0) diag.push(`<span class="badge badge-crit">RETRY (${t.retry_findings[0].attempt_count}x)</span>`);
        if (diag.length === 0) diag.push('<span class="badge badge-ok">NOMINAL</span>');
        return `
          <tr class="clickable" onclick="inspectTrace('${t.trace_id}')">
            <td><code>${t.trace_id}</code></td>
            <td>${t.root_operation}</td>
            <td><strong>${t.total_duration_ms}ms</strong></td>
            <td>${t.span_count}</td>
            <td><code>${t.bottleneck_service}</code> (${t.bottleneck_exclusive_ms}ms)</td>
            <td>${diag.join(' ')}</td>
          </tr>
        `;
      }).join('');

      if (tracesData.traces.length > 0) {
        await inspectTrace(tracesData.traces[0].trace_id);
      }
      document.getElementById('postmortem-box').textContent = inc.markdown_postmortem;
    }

    window.addEventListener('DOMContentLoaded', refreshAll);
  </script>
</body>
</html>
"""


def create_app(seed_initial_workload: bool = True) -> FastAPI:
    """Create and configure the StackPulse FastAPI application."""
    app = FastAPI(
        title="StackPulse Observability & SRE Reliability API",
        version=__version__,
        description=(
            "OpenTelemetry-compatible distributed trace DAG reconstruction, N+1 detection, "
            "service topology anomaly detection, and multi-window SLO error-budget alerting."
        ),
    )

    store = TelemetryStore()
    analyzer = TraceAnalyzer()
    topology_builder = TopologyGraphBuilder()
    slo_engine = SLOEngine()
    correlator = IncidentCorrelator()
    simulator = ChaosSimulator(seed=42)

    if seed_initial_workload:
        simulator.inject_scenario(store, scenario="all", count=24)

    app.state.store = store
    app.state.analyzer = analyzer
    app.state.topology_builder = topology_builder
    app.state.slo_engine = slo_engine
    app.state.correlator = correlator
    app.state.simulator = simulator

    @app.get("/", response_class=HTMLResponse)
    def observability_console() -> HTMLResponse:
        return HTMLResponse(content=CONSOLE_HTML)

    @app.get("/api/v1/health")
    def health_check() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "traces_count": len(store.traces),
            "logs_count": len(store.logs),
            "metrics_count": len(store.metrics),
        }

    @app.post("/api/v1/ingest/spans")
    def ingest_spans(payload: SpanBatchRequest) -> dict[str, Any]:
        ingested = store.ingest_spans(payload.spans)
        return {"ingested_spans": ingested, "active_traces": len(store.traces)}

    @app.post("/api/v1/ingest/logs")
    def ingest_logs(payload: LogBatchRequest) -> dict[str, Any]:
        ingested = store.ingest_logs(payload.logs)
        return {"ingested_logs": ingested, "total_logs": len(store.logs)}

    @app.post("/api/v1/ingest/metrics")
    def ingest_metrics(payload: MetricBatchRequest) -> dict[str, Any]:
        for m in payload.metrics:
            store.ingest_metric(m)
        return {"ingested_metrics": len(payload.metrics), "total_metrics": len(store.metrics)}

    @app.get("/api/v1/traces")
    def list_traces(
        limit: int = Query(default=25, ge=1, le=200),
        anomalous_only: bool = Query(default=False),
    ) -> dict[str, Any]:
        analyses: list[dict[str, Any]] = []
        for tid in reversed(store.trace_order):
            spans = store.get_trace_spans(tid)
            if not spans:
                continue
            ta = analyzer.analyze_trace(spans)
            if anomalous_only and not (ta.has_error or ta.n_plus_one_findings or ta.retry_findings or ta.total_duration_ms >= 250.0):
                continue
            analyses.append(ta.to_dict())
            if len(analyses) >= limit:
                break
        return {
            "total_traces": len(store.traces),
            "traces": analyses,
        }

    @app.get("/api/v1/traces/{trace_id}")
    def get_trace_detail(trace_id: str) -> dict[str, Any]:
        spans = store.get_trace_spans(trace_id)
        if not spans:
            raise HTTPException(status_code=404, detail=f"Trace '{trace_id}' not found")
        ta = analyzer.analyze_trace(spans)
        correlated_logs = [log.model_dump() for log in store.get_logs_for_trace(trace_id)]
        return {
            "analysis": ta.to_dict(),
            "logs": correlated_logs,
        }

    @app.get("/api/v1/topology")
    def get_topology() -> dict[str, Any]:
        all_spans = store.get_all_spans()
        return topology_builder.build(all_spans)

    @app.get("/api/v1/slo")
    def get_slo_reports() -> dict[str, Any]:
        all_spans = store.get_all_spans()
        reports = slo_engine.evaluate_all(all_spans)
        return {
            "reports": [r.to_dict() for r in reports],
            "firing_count": sum(1 for r in reports if r.alert_severity != "OK"),
        }

    @app.get("/api/v1/incidents/postmortem")
    def get_incident_postmortem() -> dict[str, Any]:
        trace_analyses = [
            analyzer.analyze_trace(store.get_trace_spans(tid))
            for tid in store.trace_order
            if store.get_trace_spans(tid)
        ]
        all_spans = store.get_all_spans()
        slo_reports = slo_engine.evaluate_all(all_spans)
        report = correlator.correlate(
            trace_analyses=trace_analyses,
            slo_reports=slo_reports,
            logs=store.logs,
        )
        return report.to_dict()

    @app.post("/api/v1/simulate")
    def trigger_simulation(req: SimulateRequest) -> dict[str, Any]:
        if req.reset:
            store.clear()
        stats = simulator.inject_scenario(store, scenario=req.scenario, count=req.count)
        return {
            "scenario": req.scenario,
            "stats": stats,
            "active_traces": len(store.traces),
        }

    return app


app = create_app()
