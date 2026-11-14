# OTLP Ingestion, SRE Burn Rates & Nexus Aegis G4

## Key HTTP Endpoints
- `POST /v1/traces` — Standard OpenTelemetry OTLP/HTTP JSON `resourceSpans` ingestion
- `GET /api/v1/slo` — 1h / 6h / 24h / 72h error-budget burn-rate evaluation
- `POST /api/v1/alerts/register-webhook` — SSRF-hardened Slack/JSON webhook registration
- `GET /api/v1/incidents/postmortem` — Automated Markdown SRE postmortem synthesis
- `GET /api/v1/aegis/leaf/{trace_id}` — RFC 6962 `0x00`-prefixed SHA-256 `G4_TRAJECTORY_HEALTH` leaf

## CLI Workflows

```bash
stackpulse ingest-otlp -f sample_otlp.json
stackpulse export-aegis
```
