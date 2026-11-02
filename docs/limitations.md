# Known Limitations & Production Roadmap

1. **In-Memory Telemetry Retention**: `TelemetryStore` maintains a bounded in-memory ring buffer (`max_traces=5000`, `max_logs=10000`, `max_metrics=20000`) designed for single-node evaluation, CI regression gates, and chaos simulation. For multi-terabyte long-term retention, pair with ClickHouse or OpenTelemetry Collector exporters.
2. **OTLP JSON vs Protobuf**: The ingestion endpoints accept structured JSON span/log/metric batches (`POST /api/v1/ingest/*`). Native gRPC OTLP protobuf ingestion is planned for v1.1.
