"""Service Dependency Graph & Statistical Anomaly Detector."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from stackpulse.ingest.models import SpanRecord


def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Compute linear interpolated percentile from a sorted list of floats."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    rank = (pct / 100.0) * (len(sorted_vals) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    weight = rank - low
    return sorted_vals[low] * (1.0 - weight) + sorted_vals[high] * weight


@dataclass
class ServiceNodeMetrics:
    """Aggregated golden-signal telemetry for a single service in the topology."""

    service_name: str
    request_count: int
    error_count: int
    error_rate_pct: float
    rps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    mean_ms: float
    std_ms: float
    max_z_score: float
    status: str  # "HEALTHY", "DEGRADED", "CRITICAL"
    upstream_services: list[str] = field(default_factory=list)
    downstream_services: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "service_name": self.service_name,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate_pct": round(self.error_rate_pct, 2),
            "rps": round(self.rps, 2),
            "p50_ms": round(self.p50_ms, 2),
            "p95_ms": round(self.p95_ms, 2),
            "p99_ms": round(self.p99_ms, 2),
            "mean_ms": round(self.mean_ms, 2),
            "std_ms": round(self.std_ms, 2),
            "max_z_score": round(self.max_z_score, 2),
            "status": self.status,
            "upstream_services": self.upstream_services,
            "downstream_services": self.downstream_services,
        }


@dataclass
class DependencyEdgeMetrics:
    """Directed caller -> callee dependency edge with golden signal statistics."""

    source: str
    target: str
    call_count: int
    error_count: int
    error_rate_pct: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    is_anomalous: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "call_count": self.call_count,
            "error_count": self.error_count,
            "error_rate_pct": round(self.error_rate_pct, 2),
            "p50_ms": round(self.p50_ms, 2),
            "p95_ms": round(self.p95_ms, 2),
            "p99_ms": round(self.p99_ms, 2),
            "is_anomalous": self.is_anomalous,
        }


class TopologyGraphBuilder:
    """Builds a live directed service dependency graph and detects latency/error anomalies."""

    def __init__(self, z_score_threshold: float = 2.5, baseline_p95_ms: float = 150.0) -> None:
        self.z_score_threshold = z_score_threshold
        self.baseline_p95_ms = baseline_p95_ms

    def build(self, spans: list[SpanRecord], window_seconds: float = 60.0) -> dict[str, Any]:
        """Construct full service nodes and directed edges from a set of spans."""
        if not spans:
            return {"nodes": [], "edges": [], "anomalous_services": []}

        spans_by_id: dict[str, SpanRecord] = {s.span_id: s for s in spans}
        service_durations: dict[str, list[float]] = defaultdict(list)
        service_errors: dict[str, int] = defaultdict(int)

        edge_durations: dict[tuple[str, str], list[float]] = defaultdict(list)
        edge_errors: dict[tuple[str, str], int] = defaultdict(int)
        upstream_map: dict[str, set[str]] = defaultdict(set)
        downstream_map: dict[str, set[str]] = defaultdict(set)

        for s in spans:
            svc = s.service_name
            service_durations[svc].append(s.duration_ms)
            if s.is_error:
                service_errors[svc] += 1

            # Determine caller -> callee relationship via parent_span_id or PeerService
            if s.parent_span_id and s.parent_span_id in spans_by_id:
                parent = spans_by_id[s.parent_span_id]
                if parent.service_name != svc:
                    edge_key = (parent.service_name, svc)
                    edge_durations[edge_key].append(s.duration_ms)
                    if s.is_error:
                        edge_errors[edge_key] += 1
                    downstream_map[parent.service_name].add(svc)
                    upstream_map[svc].add(parent.service_name)
            elif s.PeerService and s.PeerService != svc:
                edge_key = (svc, s.PeerService)
                edge_durations[edge_key].append(s.duration_ms)
                if s.is_error:
                    edge_errors[edge_key] += 1
                downstream_map[svc].add(s.PeerService)
                upstream_map[s.PeerService].add(svc)

        nodes: list[ServiceNodeMetrics] = []
        anomalous_services: list[str] = []

        eff_window = max(window_seconds, 1.0)

        for svc in sorted(service_durations.keys()):
            durs = sorted(service_durations[svc])
            count = len(durs)
            errs = service_errors[svc]
            err_rate = (errs / count * 100.0) if count > 0 else 0.0
            p50 = _percentile(durs, 50.0)
            p95 = _percentile(durs, 95.0)
            p99 = _percentile(durs, 99.0)
            mean_val = sum(durs) / count if count > 0 else 0.0
            variance = sum((x - mean_val) ** 2 for x in durs) / count if count > 1 else 0.0
            std_val = math.sqrt(variance)
            max_z = (( durs[-1] - mean_val ) / std_val) if std_val > 1e-6 else 0.0

            status = "HEALTHY"
            if err_rate >= 5.0 or p95 >= self.baseline_p95_ms * 2.5:
                status = "CRITICAL"
                anomalous_services.append(svc)
            elif err_rate > 0.5 or p95 >= self.baseline_p95_ms or max_z >= self.z_score_threshold:
                status = "DEGRADED"
                anomalous_services.append(svc)

            nodes.append(
                ServiceNodeMetrics(
                    service_name=svc,
                    request_count=count,
                    error_count=errs,
                    error_rate_pct=err_rate,
                    rps=count / eff_window,
                    p50_ms=p50,
                    p95_ms=p95,
                    p99_ms=p99,
                    mean_ms=mean_val,
                    std_ms=std_val,
                    max_z_score=max_z,
                    status=status,
                    upstream_services=sorted(upstream_map[svc]),
                    downstream_services=sorted(downstream_map[svc]),
                )
            )

        edges: list[DependencyEdgeMetrics] = []
        for (src, dst), durs in sorted(edge_durations.items()):
            sorted_durs = sorted(durs)
            c = len(sorted_durs)
            e = edge_errors[(src, dst)]
            e_rate = (e / c * 100.0) if c > 0 else 0.0
            p50 = _percentile(sorted_durs, 50.0)
            p95 = _percentile(sorted_durs, 95.0)
            p99 = _percentile(sorted_durs, 99.0)
            is_anom = e_rate >= 2.0 or p95 >= self.baseline_p95_ms
            edges.append(
                DependencyEdgeMetrics(
                    source=src,
                    target=dst,
                    call_count=c,
                    error_count=e,
                    error_rate_pct=e_rate,
                    p50_ms=p50,
                    p95_ms=p95,
                    p99_ms=p99,
                    is_anomalous=is_anom,
                )
            )

        return {
            "nodes": [n.to_dict() for n in nodes],
            "edges": [e.to_dict() for e in edges],
            "anomalous_services": anomalous_services,
        }
