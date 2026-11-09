"""OpenTelemetry-compatible telemetry ingestion models and in-memory telemetry store."""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field


class SpanEvent(BaseModel):
    """Represents a structured event attached to an OpenTelemetry span."""

    name: str
    timestamp_ms: float = Field(default_factory=lambda: time.time() * 1000.0)
    attributes: dict[str, Any] = Field(default_factory=dict)


class SpanRecord(BaseModel):
    """W3C TraceContext compatible distributed trace span."""

    trace_id: str = Field(..., min_length=8, description="W3C 128-bit or compact trace identifier")
    span_id: str = Field(..., min_length=4, description="64-bit or compact span identifier")
    parent_span_id: str | None = Field(default=None, description="Parent span identifier if non-root")
    service_name: str = Field(..., min_length=1)
    operation: str = Field(..., min_length=1)
    start_time_ms: float = Field(default_factory=lambda: time.time() * 1000.0)
    duration_ms: float = Field(..., ge=0.0)
    status_code: Literal["OK", "ERROR", "UNSET"] = "OK"
    http_status: int | None = None
    span_kind: Literal["SERVER", "CLIENT", "INTERNAL", "PRODUCER", "CONSUMER"] = "INTERNAL"
    PeerService: str | None = Field(default=None, alias="peer_service")
    attributes: dict[str, Any] = Field(default_factory=dict)
    events: list[SpanEvent] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    @property
    def end_time_ms(self) -> float:
        return self.start_time_ms + self.duration_ms

    @property
    def peer_service(self) -> str | None:
        return self.PeerService

    @property
    def is_error(self) -> bool:
        return self.status_code == "ERROR" or (self.http_status is not None and self.http_status >= 500)


class LogRecord(BaseModel):
    """Structured log entry correlated with W3C trace context."""

    timestamp_ms: float = Field(default_factory=lambda: time.time() * 1000.0)
    severity: Literal["DEBUG", "INFO", "WARN", "ERROR", "FATAL"] = "INFO"
    service_name: str
    message: str
    trace_id: str | None = None
    span_id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)


class MetricPoint(BaseModel):
    """Time-series metric data point supporting counter, gauge, and histogram types."""

    name: str
    metric_type: Literal["counter", "gauge", "histogram"] = "gauge"
    value: float
    timestamp_ms: float = Field(default_factory=lambda: time.time() * 1000.0)
    service_name: str
    labels: dict[str, str] = Field(default_factory=dict)


@dataclass
class TelemetryStore:
    """High-throughput in-memory store for spans, structured logs, and time-series metrics."""

    max_traces: int = 5000
    max_logs: int = 10000
    max_metrics: int = 20000

    traces: dict[str, list[SpanRecord]] = field(default_factory=lambda: defaultdict(list))
    trace_order: list[str] = field(default_factory=list)
    logs: list[LogRecord] = field(default_factory=list)
    metrics: list[MetricPoint] = field(default_factory=list)

    def ingest_span(self, span: SpanRecord) -> None:
        """Ingest a single W3C span record and maintain bounded trace retention."""
        if span.trace_id not in self.traces:
            self.trace_order.append(span.trace_id)
            if len(self.trace_order) > self.max_traces:
                oldest = self.trace_order.pop(0)
                self.traces.pop(oldest, None)
        self.traces[span.trace_id].append(span)

    def ingest_spans(self, spans: list[SpanRecord]) -> int:
        """Batch-ingest multiple spans."""
        for span in spans:
            self.ingest_span(span)
        return len(spans)

    def ingest_log(self, log: LogRecord) -> None:
        """Ingest a structured log record."""
        self.logs.append(log)
        if len(self.logs) > self.max_logs:
            self.logs = self.logs[-self.max_logs :]

    def ingest_logs(self, logs: list[LogRecord]) -> int:
        """Batch-ingest multiple structured logs."""
        for log in logs:
            self.ingest_log(log)
        return len(logs)

    def ingest_metric(self, metric: MetricPoint) -> None:
        """Ingest a metric data point."""
        self.metrics.append(metric)
        if len(self.metrics) > self.max_metrics:
            self.metrics = self.metrics[-self.max_metrics :]

    def get_trace_spans(self, trace_id: str) -> list[SpanRecord]:
        """Return all spans for a trace ID ordered by start_time_ms."""
        spans = self.traces.get(trace_id, [])
        return sorted(spans, key=lambda s: (s.start_time_ms, s.span_id))

    def get_all_spans(self) -> list[SpanRecord]:
        """Return all currently stored spans across all traces."""
        out: list[SpanRecord] = []
        for tid in self.trace_order:
            out.extend(self.traces.get(tid, []))
        return out

    def get_logs_for_trace(self, trace_id: str) -> list[LogRecord]:
        """Return all structured logs correlated with a given trace ID."""
        return [log for log in self.logs if log.trace_id == trace_id]

    def clear(self) -> None:
        """Reset all stored telemetry."""
        self.traces.clear()
        self.trace_order.clear()
        self.logs.clear()
        self.metrics.clear()
