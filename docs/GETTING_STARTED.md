# Getting Started with StackPulse

`stackpulse` (`v1.2.0`) provides OpenTelemetry OTLP ingestion, W3C TraceContext DAG critical-path analysis, multi-window SRE SLO burn-rate alerts, and automated blameless postmortems.

## Installation & Quick Run

```bash
git clone https://github.com/codesphere-u/stackpulse.git
cd stackpulse
pip install -e ".[dev]"
stackpulse simulate --scenario all --count 24
stackpulse benchmark --traces 500
stackpulse serve --host 127.0.0.1 --port 8098
```
