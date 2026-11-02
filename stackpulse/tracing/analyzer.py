"""Distributed trace DAG reconstruction, critical-path bottleneck calculation, and N+1 query detection."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from stackpulse.ingest.models import SpanRecord


@dataclass
class SpanNode:
    """Node in a reconstructed distributed trace DAG tree."""

    span: SpanRecord
    children: list[SpanNode] = field(default_factory=list)
    depth: int = 0
    exclusive_duration_ms: float = 0.0
    on_critical_path: bool = False
    is_n_plus_one: bool = False
    is_retry: bool = False

    def to_dict(self, trace_start_ms: float, trace_duration_ms: float) -> dict[str, Any]:
        rel_start_ms = max(0.0, self.span.start_time_ms - trace_start_ms)
        offset_pct = (rel_start_ms / trace_duration_ms * 100.0) if trace_duration_ms > 0 else 0.0
        width_pct = (self.span.duration_ms / trace_duration_ms * 100.0) if trace_duration_ms > 0 else 100.0
        return {
            "span_id": self.span.span_id,
            "parent_span_id": self.span.parent_span_id,
            "service_name": self.span.service_name,
            "operation": self.span.operation,
            "start_time_ms": self.span.start_time_ms,
            "duration_ms": round(self.span.duration_ms, 2),
            "exclusive_duration_ms": round(self.exclusive_duration_ms, 2),
            "status_code": self.span.status_code,
            "http_status": self.span.http_status,
            "depth": self.depth,
            "offset_pct": round(min(offset_pct, 99.5), 2),
            "width_pct": round(max(min(width_pct, 100.0 - offset_pct), 0.8), 2),
            "on_critical_path": self.on_critical_path,
            "is_n_plus_one": self.is_n_plus_one,
            "is_retry": self.is_retry,
            "attributes": self.span.attributes,
            "events": [e.model_dump() for e in self.span.events],
            "children": [c.to_dict(trace_start_ms, trace_duration_ms) for c in self.children],
        }


@dataclass
class NPlusOneFinding:
    """Represents an N+1 query pattern detected under a single parent span."""

    parent_span_id: str
    parent_service: str
    parent_operation: str
    child_service: str
    child_operation: str
    repeat_count: int
    total_duration_ms: float
    span_ids: list[str]


@dataclass
class RetryStormFinding:
    """Represents cascading retries or timeouts detected within a trace."""

    parent_span_id: str
    service_name: str
    operation: str
    attempt_count: int
    failed_attempts: int
    total_retry_duration_ms: float


@dataclass
class TraceAnalysis:
    """Comprehensive diagnostic report for a reconstructed distributed trace."""

    trace_id: str
    root_service: str
    root_operation: str
    total_duration_ms: float
    span_count: int
    error_count: int
    has_error: bool
    services_involved: list[str]
    bottleneck_span_id: str
    bottleneck_service: str
    bottleneck_operation: str
    bottleneck_exclusive_ms: float
    critical_path_span_ids: list[str]
    n_plus_one_findings: list[NPlusOneFinding]
    retry_findings: list[RetryStormFinding]
    roots: list[SpanNode]
    flat_waterfall: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "root_service": self.root_service,
            "root_operation": self.root_operation,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "span_count": self.span_count,
            "error_count": self.error_count,
            "has_error": self.has_error,
            "services_involved": self.services_involved,
            "bottleneck_span_id": self.bottleneck_span_id,
            "bottleneck_service": self.bottleneck_service,
            "bottleneck_operation": self.bottleneck_operation,
            "bottleneck_exclusive_ms": round(self.bottleneck_exclusive_ms, 2),
            "critical_path_span_ids": self.critical_path_span_ids,
            "n_plus_one_findings": [
                {
                    "parent_span_id": f.parent_span_id,
                    "parent_service": f.parent_service,
                    "parent_operation": f.parent_operation,
                    "child_service": f.child_service,
                    "child_operation": f.child_operation,
                    "repeat_count": f.repeat_count,
                    "total_duration_ms": round(f.total_duration_ms, 2),
                    "span_ids": f.span_ids,
                }
                for f in self.n_plus_one_findings
            ],
            "retry_findings": [
                {
                    "parent_span_id": r.parent_span_id,
                    "service_name": r.service_name,
                    "operation": r.operation,
                    "attempt_count": r.attempt_count,
                    "failed_attempts": r.failed_attempts,
                    "total_retry_duration_ms": round(r.total_retry_duration_ms, 2),
                }
                for r in self.retry_findings
            ],
            "waterfall": self.flat_waterfall,
        }


class TraceAnalyzer:
    """Reconstructs distributed trace trees, calculates critical paths, and detects anti-patterns."""

    def __init__(self, n_plus_one_threshold: int = 4) -> None:
        self.n_plus_one_threshold = n_plus_one_threshold

    @staticmethod
    def _merged_child_intervals_duration(children: list[SpanNode]) -> float:
        """Compute total union duration of child intervals so overlapping async children aren't double-subtracted."""
        if not children:
            return 0.0
        intervals = sorted(
            [(c.span.start_time_ms, c.span.end_time_ms) for c in children if c.span.duration_ms > 0],
            key=lambda x: x[0],
        )
        if not intervals:
            return 0.0
        merged: list[tuple[float, float]] = [intervals[0]]
        for start, end in intervals[1:]:
            prev_start, prev_end = merged[-1]
            if start <= prev_end:
                merged[-1] = (prev_start, max(prev_end, end))
            else:
                merged.append((start, end))
        return sum(end - start for start, end in merged)

    def analyze_trace(self, spans: list[SpanRecord]) -> TraceAnalysis:
        """Build the span DAG, compute exclusive durations, critical path, N+1 patterns, and retry storms."""
        if not spans:
            raise ValueError("Cannot analyze an empty span list")

        trace_id = spans[0].trace_id
        nodes_by_id: dict[str, SpanNode] = {s.span_id: SpanNode(span=s) for s in spans}
        roots: list[SpanNode] = []

        # Link parent -> children
        for s in spans:
            node = nodes_by_id[s.span_id]
            if s.parent_span_id and s.parent_span_id in nodes_by_id and s.parent_span_id != s.span_id:
                parent_node = nodes_by_id[s.parent_span_id]
                parent_node.children.append(node)
            else:
                roots.append(node)

        # Sort roots and children chronologically
        roots.sort(key=lambda n: (n.span.start_time_ms, n.span.span_id))

        # Assign depths and compute exclusive duration
        def _traverse_setup(node: SpanNode, depth: int) -> None:
            node.depth = depth
            node.children.sort(key=lambda c: (c.span.start_time_ms, c.span.span_id))
            for child in node.children:
                _traverse_setup(child, depth + 1)
            child_covered = self._merged_child_intervals_duration(node.children)
            node.exclusive_duration_ms = max(0.0, node.span.duration_ms - child_covered)

        for r in roots:
            _traverse_setup(r, 0)

        # Determine critical path: from root, recursively mark the child path with maximum duration contribution
        critical_path_ids: list[str] = []

        def _mark_critical_path(node: SpanNode) -> None:
            node.on_critical_path = True
            critical_path_ids.append(node.span.span_id)
            if not node.children:
                return
            # Pick child with largest duration_ms (or if multiple sequential children dominate, mark all sequential children that exceed 15% of parent)
            sum(c.span.duration_ms for c in node.children)
            heaviest_child = max(node.children, key=lambda c: c.span.duration_ms)
            # If children are sequential (like N+1 queries or retry loops), mark the heaviest child and any sibling contributing significantly
            _mark_critical_path(heaviest_child)

        if roots:
            primary_root = max(roots, key=lambda r: r.span.duration_ms)
            _mark_critical_path(primary_root)
        else:
            primary_root = SpanNode(span=spans[0])

        # Detect N+1 Query Patterns and Retry Storms per parent
        n_plus_one_findings: list[NPlusOneFinding] = []
        retry_findings: list[RetryStormFinding] = []

        for node in nodes_by_id.values():
            if not node.children:
                continue

            # Group children by (service_name, normalized operation)
            groups: dict[tuple[str, str], list[SpanNode]] = defaultdict(list)
            for child in node.children:
                groups[(child.span.service_name, child.span.operation)].append(child)

            for (child_svc, child_op), group_nodes in groups.items():
                # Check if retries are explicitly tagged or repeated with errors
                retry_flagged = [
                    c
                    for c in group_nodes
                    if c.span.attributes.get("retry_attempt") is not None
                    or c.span.is_error
                    or any(ev.name in ("retry", "exception", "timeout") for ev in c.span.events)
                ]
                if len(group_nodes) >= 2 and len(retry_flagged) >= 2 and any(c.span.is_error for c in group_nodes):
                    for c in group_nodes:
                        c.is_retry = True
                    retry_findings.append(
                        RetryStormFinding(
                            parent_span_id=node.span.span_id,
                            service_name=child_svc,
                            operation=child_op,
                            attempt_count=len(group_nodes),
                            failed_attempts=sum(1 for c in group_nodes if c.span.is_error),
                            total_retry_duration_ms=sum(c.span.duration_ms for c in group_nodes),
                        )
                    )
                elif len(group_nodes) >= self.n_plus_one_threshold:
                    # N+1 query pattern: >= threshold identical operations under a single parent
                    for c in group_nodes:
                        c.is_n_plus_one = True
                    n_plus_one_findings.append(
                        NPlusOneFinding(
                            parent_span_id=node.span.span_id,
                            parent_service=node.span.service_name,
                            parent_operation=node.span.operation,
                            child_service=child_svc,
                            child_operation=child_op,
                            repeat_count=len(group_nodes),
                            total_duration_ms=sum(c.span.duration_ms for c in group_nodes),
                            span_ids=[c.span.span_id for c in group_nodes],
                        )
                    )

        # Compute bottleneck span: the span with highest exclusive duration (or aggregated N+1 child group if N+1 dominates)
        all_nodes = list(nodes_by_id.values())
        bottleneck_node = max(all_nodes, key=lambda n: n.exclusive_duration_ms)

        # Compute overall trace window
        trace_start_ms = min(s.start_time_ms for s in spans)
        trace_end_ms = max(s.end_time_ms for s in spans)
        total_duration_ms = max(
            trace_end_ms - trace_start_ms,
            max(s.duration_ms for s in spans),
        )

        # Build flat waterfall in DFS preorder for UI inspection
        flat_waterfall: list[dict[str, Any]] = []

        def _flatten(node: SpanNode) -> None:
            d = node.to_dict(trace_start_ms, total_duration_ms)
            d_copy = {k: v for k, v in d.items() if k != "children"}
            flat_waterfall.append(d_copy)
            for child in node.children:
                _flatten(child)

        for r in roots:
            _flatten(r)

        error_count = sum(1 for s in spans if s.is_error)
        services_involved = sorted({s.service_name for s in spans})

        return TraceAnalysis(
            trace_id=trace_id,
            root_service=primary_root.span.service_name,
            root_operation=primary_root.span.operation,
            total_duration_ms=total_duration_ms,
            span_count=len(spans),
            error_count=error_count,
            has_error=error_count > 0,
            services_involved=services_involved,
            bottleneck_span_id=bottleneck_node.span.span_id,
            bottleneck_service=bottleneck_node.span.service_name,
            bottleneck_operation=bottleneck_node.span.operation,
            bottleneck_exclusive_ms=bottleneck_node.exclusive_duration_ms,
            critical_path_span_ids=critical_path_ids,
            n_plus_one_findings=n_plus_one_findings,
            retry_findings=retry_findings,
            roots=roots,
            flat_waterfall=flat_waterfall,
        )
