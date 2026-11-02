"""Automated Incident Timeline Correlator & Blameless SRE Postmortem Generator."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from stackpulse.ingest.models import LogRecord
from stackpulse.slo.engine import SLOReport
from stackpulse.tracing.analyzer import TraceAnalysis


@dataclass
class TimelineEntry:
    """Single chronological event in an automated incident timeline."""

    timestamp_ms: float
    category: str  # "SLO_ALERT", "TRACE_BOTTLENECK", "N_PLUS_ONE", "RETRY_STORM", "ERROR_LOG"
    service_name: str
    summary: str
    trace_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        dt_str = datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        return {
            "timestamp_ms": self.timestamp_ms,
            "time_utc": dt_str,
            "category": self.category,
            "service_name": self.service_name,
            "summary": self.summary,
            "trace_id": self.trace_id,
        }


@dataclass
class IncidentReport:
    """Correlated incident summary and structured Markdown SRE postmortem."""

    incident_id: str
    title: str
    severity: str  # "SEV-1", "SEV-2", "SEV-3", "NONE"
    root_cause_service: str
    root_cause_category: str
    impacted_services: list[str]
    breached_slos: list[dict[str, Any]]
    timeline: list[TimelineEntry]
    action_items: list[str]
    markdown_postmortem: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "title": self.title,
            "severity": self.severity,
            "root_cause_service": self.root_cause_service,
            "root_cause_category": self.root_cause_category,
            "impacted_services": self.impacted_services,
            "breached_slos": self.breached_slos,
            "timeline": [t.to_dict() for t in self.timeline],
            "action_items": self.action_items,
            "markdown_postmortem": self.markdown_postmortem,
        }


class IncidentCorrelator:
    """Correlates trace diagnostics, error logs, and SLO alerts into an automated SRE Postmortem."""

    def correlate(
        self,
        trace_analyses: list[TraceAnalysis],
        slo_reports: list[SLOReport],
        logs: list[LogRecord],
        incident_id: str = "INC-2026-089",
    ) -> IncidentReport:
        timeline: list[TimelineEntry] = []
        bottleneck_votes: Counter[str] = Counter()
        category_votes: Counter[str] = Counter()
        impacted_services_set: set[str] = set()

        base_ts = min(
            [log.timestamp_ms for log in logs] or [1730500000000.0]
        )

        # 1. Inspect trace analyses
        for idx, ta in enumerate(trace_analyses):
            is_slow_or_err = ta.has_error or ta.total_duration_ms >= 250.0 or bool(ta.n_plus_one_findings)
            if not is_slow_or_err:
                continue

            impacted_services_set.update(ta.services_involved)
            bottleneck_votes[ta.bottleneck_service] += 2

            if ta.n_plus_one_findings:
                for f in ta.n_plus_one_findings:
                    bottleneck_votes[f.child_service] += 3
                    category_votes["N+1 ORM Query Regression"] += 3
                    timeline.append(
                        TimelineEntry(
                            timestamp_ms=base_ts + idx * 120.0,
                            category="N_PLUS_ONE",
                            service_name=f.parent_service,
                            summary=(
                                f"Detected N+1 query pattern: {f.repeat_count} sequential calls to "
                                f"{f.child_service} ({f.child_operation}) totaling {f.total_duration_ms:.1f}ms"
                            ),
                            trace_id=ta.trace_id,
                        )
                    )

            if ta.retry_findings:
                for r in ta.retry_findings:
                    bottleneck_votes[r.service_name] += 3
                    category_votes["Downstream 503 Retry Storm"] += 3
                    timeline.append(
                        TimelineEntry(
                            timestamp_ms=base_ts + idx * 120.0 + 25.0,
                            category="RETRY_STORM",
                            service_name=r.service_name,
                            summary=(
                                f"Cascading retry loop on {r.service_name}.{r.operation}: "
                                f"{r.attempt_count} attempts ({r.failed_attempts} failed, {r.total_retry_duration_ms:.1f}ms)"
                            ),
                            trace_id=ta.trace_id,
                        )
                    )

            if ta.total_duration_ms >= 300.0 or ta.has_error:
                if not ta.n_plus_one_findings and not ta.retry_findings:
                    category_votes["Database Connection Pool Saturation & Slow Query Cascade"] += 2
                timeline.append(
                    TimelineEntry(
                        timestamp_ms=base_ts + idx * 120.0 + 40.0,
                        category="TRACE_BOTTLENECK",
                        service_name=ta.bottleneck_service,
                        summary=(
                            f"Trace {ta.trace_id} critical-path bottleneck in {ta.bottleneck_service}"
                            f" ({ta.bottleneck_operation}): {ta.bottleneck_exclusive_ms:.1f}ms exclusive "
                            f"/ {ta.total_duration_ms:.1f}ms total"
                        ),
                        trace_id=ta.trace_id,
                    )
                )

        # 2. Correlate structured ERROR/WARN logs
        for log in logs:
            if log.severity in ("ERROR", "FATAL", "WARN"):
                impacted_services_set.add(log.service_name)
                bottleneck_votes[log.service_name] += 1
                timeline.append(
                    TimelineEntry(
                        timestamp_ms=log.timestamp_ms,
                        category="ERROR_LOG",
                        service_name=log.service_name,
                        summary=f"[{log.severity}] {log.message}",
                        trace_id=log.trace_id,
                    )
                )

        # 3. Correlate SLO burn alerts
        breached_slos: list[dict[str, Any]] = []
        for idx, slo in enumerate(slo_reports):
            if slo.alert_severity != "OK":
                breached_slos.append(slo.to_dict())
                impacted_services_set.add(slo.service_name)
                timeline.append(
                    TimelineEntry(
                        timestamp_ms=base_ts + 500.0 + idx * 50.0,
                        category="SLO_ALERT",
                        service_name=slo.service_name,
                        summary=(
                            f"[{slo.alert_severity}] {slo.name}: 1h burn={slo.burn_rate_1h:.1f}x, "
                            f"remaining budget={slo.remaining_budget_pct:.1f}%"
                        ),
                    )
                )

        timeline.sort(key=lambda e: e.timestamp_ms)

        # Determine root cause service, category, and severity
        root_service = bottleneck_votes.most_common(1)[0][0] if bottleneck_votes else "none"
        root_category = (
            category_votes.most_common(1)[0][0]
            if category_votes
            else "Nominal Operations (No Active Anomaly)"
        )

        if any(s["alert_severity"] == "PAGE_CRITICAL" for s in breached_slos):
            severity = "SEV-1"
        elif breached_slos or bottleneck_votes:
            severity = "SEV-2"
        else:
            severity = "NONE"

        title = (
            f"{severity}: {root_category} originating in {root_service}"
            if severity != "NONE"
            else "System Healthy — All SLO Error Budgets Nominal"
        )

        action_items = self._generate_action_items(root_service, root_category, breached_slos)
        impacted_list = sorted(impacted_services_set)
        md = self._render_markdown(
            incident_id=incident_id,
            title=title,
            severity=severity,
            root_service=root_service,
            root_category=root_category,
            impacted_services=impacted_list,
            breached_slos=breached_slos,
            timeline=timeline[:18],
            action_items=action_items,
        )

        return IncidentReport(
            incident_id=incident_id,
            title=title,
            severity=severity,
            root_cause_service=root_service,
            root_cause_category=root_category,
            impacted_services=impacted_list,
            breached_slos=breached_slos,
            timeline=timeline[:30],
            action_items=action_items,
            markdown_postmortem=md,
        )

    @staticmethod
    def _generate_action_items(
        root_service: str,
        root_category: str,
        breached_slos: list[dict[str, Any]],
    ) -> list[str]:
        items: list[str] = []
        if "N+1" in root_category:
            items.append(
                f"Replace sequential per-item queries in `{root_service}` caller with batched `WHERE id = ANY(...)` / eager `selectinload` preload."
            )
            items.append(
                "Add automated CI trace assertion failing builds when child span repeat count exceeds 4 per parent span."
            )
        if "Retry Storm" in root_category or "503" in root_category:
            items.append(
                f"Enforce exponential backoff with full jitter and a token-bucket retry budget (max 10% retry ratio) on `{root_service}`."
            )
            items.append(
                f"Configure adaptive circuit-breaking on upstream callers of `{root_service}` to fail fast during 503 brownouts."
            )
        if "Saturation" in root_category or "Slow Query" in root_category or not items:
            items.append(
                f"Enforce strict statement_timeout (150ms) andPgBouncer transaction pooling on `{root_service}`."
            )
            items.append(
                "Add missing composite index on high-cardinality checkout ledger lookup columns."
            )
        if breached_slos:
            items.append(
                "Freeze non-reliability feature deployments for 48h until 1h SLO burn rate drops below 1.0x."
            )
        return items

    @staticmethod
    def _render_markdown(
        incident_id: str,
        title: str,
        severity: str,
        root_service: str,
        root_category: str,
        impacted_services: list[str],
        breached_slos: list[dict[str, Any]],
        timeline: list[TimelineEntry],
        action_items: list[str],
    ) -> str:
        lines = [
            f"# Blameless SRE Postmortem — `{incident_id}`",
            "",
            f"**Incident Title:** {title}  ",
            f"**Severity:** `{severity}`  ",
            f"**Root-Cause Service:** `{root_service}`  ",
            f"**Failure Mode:** {root_category}  ",
            f"**Impacted Topology:** {', '.join(f'`{s}`' for s in impacted_services) or 'None'}",
            "",
            "## 1. Executive Summary & Root Cause Analysis",
            (
                f"Automated critical-path trace reconstruction and multi-window SLO correlation isolated the primary bottleneck "
                f"to **`{root_service}`** under failure pattern **{root_category}**. Upstream latency and error rates propagated "
                f"across {len(impacted_services)} dependent services."
            ),
            "",
            "## 2. Multi-Window SLO & Error Budget Impact",
        ]
        if breached_slos:
            lines.append("| SLO Name | Service | Alert | SLI % | 1h Burn | 6h Burn | Budget Left |")
            lines.append("|---|---|---|---|---|---|---|")
            for s in breached_slos:
                lines.append(
                    f"| {s['name']} | `{s['service_name']}` | **{s['alert_severity']}** | "
                    f"{s['current_sli_pct']}% | {s['burn_rate_1h']}x | {s['burn_rate_6h']}x | {s['remaining_budget_pct']}% |"
                )
        else:
            lines.append("_No SLO burn alerts triggered during the observation window._")

        lines.extend(["", "## 3. Correlated Incident Timeline (UTC)"])
        if timeline:
            for entry in timeline:
                d = entry.to_dict()
                tid_badge = f" (`trace={d['trace_id']}`)" if d["trace_id"] else ""
                lines.append(
                    f"- **`{d['time_utc']}`** [{d['category']}] **`{d['service_name']}`** — {d['summary']}{tid_badge}"
                )
        else:
            lines.append("- _No anomalous events recorded._")

        lines.extend(["", "## 4. Remediation & Prevention Action Items"])
        for idx, item in enumerate(action_items, 1):
            lines.append(f"{idx}. [ ] {item}")

        return "\n".join(lines)
