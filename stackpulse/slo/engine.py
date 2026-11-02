"""Google SRE Workbook Multi-Window, Multi-Burn-Rate SLO & Error-Budget Engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from stackpulse.ingest.models import SpanRecord


@dataclass
class SLODefinition:
    """Service Level Objective configuration following the Google SRE Workbook."""

    slo_id: str
    service_name: str
    name: str
    slo_type: Literal["availability", "latency"] = "availability"
    target_pct: float = 99.9  # e.g., 99.9%
    latency_threshold_ms: float = 250.0  # used when slo_type == "latency"
    compliance_window_hours: float = 720.0  # 30-day rolling window (720 hours)
    fast_burn_threshold: float = 14.4  # 2% budget consumed in 1 hour -> Page
    slow_burn_threshold: float = 6.0  # 5% budget consumed in 6 hours -> Page / Ticket


@dataclass
class SLOReport:
    """Computed multi-window SLO status and error budget burn-rate metrics."""

    slo_id: str
    service_name: str
    name: str
    slo_type: str
    target_pct: float
    current_sli_pct: float
    error_budget_fraction: float
    remaining_budget_pct: float
    total_events: int
    bad_events: int
    burn_rate_1h: float
    burn_rate_6h: float
    burn_rate_24h: float
    burn_rate_72h: float
    alert_severity: Literal["PAGE_CRITICAL", "TICKET_WARNING", "OK"]
    alert_reason: str
    time_to_exhaustion_hours: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "slo_id": self.slo_id,
            "service_name": self.service_name,
            "name": self.name,
            "slo_type": self.slo_type,
            "target_pct": self.target_pct,
            "current_sli_pct": round(self.current_sli_pct, 4),
            "error_budget_fraction": round(self.error_budget_fraction, 6),
            "remaining_budget_pct": round(self.remaining_budget_pct, 2),
            "total_events": self.total_events,
            "bad_events": self.bad_events,
            "burn_rate_1h": round(self.burn_rate_1h, 2),
            "burn_rate_6h": round(self.burn_rate_6h, 2),
            "burn_rate_24h": round(self.burn_rate_24h, 2),
            "burn_rate_72h": round(self.burn_rate_72h, 2),
            "alert_severity": self.alert_severity,
            "alert_reason": self.alert_reason,
            "time_to_exhaustion_hours": (
                round(self.time_to_exhaustion_hours, 2)
                if self.time_to_exhaustion_hours is not None
                else None
            ),
        }


DEFAULT_SLO_DEFINITIONS: list[SLODefinition] = [
    SLODefinition(
        slo_id="slo-api-gateway-avail",
        service_name="api-gateway",
        name="API Gateway Availability SLO (99.9%)",
        slo_type="availability",
        target_pct=99.9,
    ),
    SLODefinition(
        slo_id="slo-api-gateway-lat",
        service_name="api-gateway",
        name="API Gateway Latency SLO (99.0% < 250ms)",
        slo_type="latency",
        target_pct=99.0,
        latency_threshold_ms=250.0,
    ),
    SLODefinition(
        slo_id="slo-checkout-avail",
        service_name="checkout-service",
        name="Checkout Service Availability SLO (99.9%)",
        slo_type="availability",
        target_pct=99.9,
    ),
    SLODefinition(
        slo_id="slo-payment-avail",
        service_name="payment-worker",
        name="Payment Worker Availability SLO (99.5%)",
        slo_type="availability",
        target_pct=99.5,
    ),
    SLODefinition(
        slo_id="slo-postgres-lat",
        service_name="postgres-primary",
        name="Postgres Primary Query Latency SLO (99.0% < 120ms)",
        slo_type="latency",
        target_pct=99.0,
        latency_threshold_ms=120.0,
    ),
]


class SLOEngine:
    """Evaluates multi-window SLO error-budget burn rates across ingested spans."""

    def __init__(self, definitions: list[SLODefinition] | None = None) -> None:
        self.definitions: list[SLODefinition] = (
            list(definitions) if definitions is not None else list(DEFAULT_SLO_DEFINITIONS)
        )

    @staticmethod
    def _is_bad_event(span: SpanRecord, slo: SLODefinition) -> bool:
        if slo.slo_type == "availability":
            return span.is_error
        return span.is_error or span.duration_ms > slo.latency_threshold_ms

    @classmethod
    def compute_burn_rate(
        cls,
        spans: list[SpanRecord],
        slo: SLODefinition,
        window_ms: float | None = None,
        now_ms: float | None = None,
    ) -> float:
        """Compute burn rate = (observed error rate in window) / (1 - target_fraction)."""
        error_budget = max(1e-6, 1.0 - (slo.target_pct / 100.0))
        if not spans:
            return 0.0
        ref_now = now_ms if now_ms is not None else max(s.end_time_ms for s in spans)
        if window_ms is not None:
            cutoff = ref_now - window_ms
            window_spans = [s for s in spans if s.start_time_ms >= cutoff]
            # If synthetic simulation spans all happen within a few seconds, fall back to full set
            if not window_spans:
                window_spans = spans
        else:
            window_spans = spans

        total = len(window_spans)
        if total == 0:
            return 0.0
        bad = sum(1 for s in window_spans if cls._is_bad_event(s, slo))
        observed_error_rate = bad / total
        return observed_error_rate / error_budget

    def evaluate_single(
        self,
        slo: SLODefinition,
        spans: list[SpanRecord],
        now_ms: float | None = None,
    ) -> SLOReport:
        """Evaluate a single SLODefinition against service spans."""
        svc_spans = [s for s in spans if s.service_name == slo.service_name]
        error_budget_fraction = max(1e-6, 1.0 - (slo.target_pct / 100.0))
        total = len(svc_spans)
        bad = sum(1 for s in svc_spans if self._is_bad_event(s, slo))

        if total == 0:
            return SLOReport(
                slo_id=slo.slo_id,
                service_name=slo.service_name,
                name=slo.name,
                slo_type=slo.slo_type,
                target_pct=slo.target_pct,
                current_sli_pct=100.0,
                error_budget_fraction=error_budget_fraction,
                remaining_budget_pct=100.0,
                total_events=0,
                bad_events=0,
                burn_rate_1h=0.0,
                burn_rate_6h=0.0,
                burn_rate_24h=0.0,
                burn_rate_72h=0.0,
                alert_severity="OK",
                alert_reason="No traffic observed; 100% budget remaining",
                time_to_exhaustion_hours=None,
            )

        observed_error_rate = bad / total
        current_sli_pct = (1.0 - observed_error_rate) * 100.0

        ref_now = now_ms if now_ms is not None else max(s.end_time_ms for s in svc_spans)
        burn_1h = self.compute_burn_rate(svc_spans, slo, window_ms=3600 * 1000.0, now_ms=ref_now)
        burn_6h = self.compute_burn_rate(svc_spans, slo, window_ms=6 * 3600 * 1000.0, now_ms=ref_now)
        burn_24h = self.compute_burn_rate(svc_spans, slo, window_ms=24 * 3600 * 1000.0, now_ms=ref_now)
        burn_72h = self.compute_burn_rate(svc_spans, slo, window_ms=72 * 3600 * 1000.0, now_ms=ref_now)

        # Remaining budget % relative to allowed error rate
        consumed_fraction = observed_error_rate / error_budget_fraction
        remaining_budget_pct = max(0.0, min(100.0, (1.0 - consumed_fraction) * 100.0))

        # Multi-window alert classification (Google SRE Workbook Ch. 5)
        severity: Literal["PAGE_CRITICAL", "TICKET_WARNING", "OK"] = "OK"
        reason = "Burn rate within normal error budget envelope"

        if burn_1h >= slo.fast_burn_threshold and burn_6h >= slo.slow_burn_threshold:
            severity = "PAGE_CRITICAL"
            reason = (
                f"Fast-burn SLO breach: 1h burn={burn_1h:.1f}x (>={slo.fast_burn_threshold}x) "
                f"& 6h burn={burn_6h:.1f}x"
            )
        elif burn_6h >= slo.slow_burn_threshold:
            severity = "PAGE_CRITICAL"
            reason = f"Sustained high burn: 6h burn={burn_6h:.1f}x (>={slo.slow_burn_threshold}x)"
        elif burn_24h >= 3.0 or burn_72h >= 1.0:
            severity = "TICKET_WARNING"
            reason = f"Slow-burn budget consumption: 24h burn={burn_24h:.1f}x, 72h burn={burn_72h:.1f}x"

        # Time to exhaustion in hours at current 1h burn rate
        tte_hours: float | None = None
        if burn_1h > 0.01:
            remaining_ratio = remaining_budget_pct / 100.0
            tte_hours = (slo.compliance_window_hours * remaining_ratio) / burn_1h

        return SLOReport(
            slo_id=slo.slo_id,
            service_name=slo.service_name,
            name=slo.name,
            slo_type=slo.slo_type,
            target_pct=slo.target_pct,
            current_sli_pct=current_sli_pct,
            error_budget_fraction=error_budget_fraction,
            remaining_budget_pct=remaining_budget_pct,
            total_events=total,
            bad_events=bad,
            burn_rate_1h=burn_1h,
            burn_rate_6h=burn_6h,
            burn_rate_24h=burn_24h,
            burn_rate_72h=burn_72h,
            alert_severity=severity,
            alert_reason=reason,
            time_to_exhaustion_hours=tte_hours,
        )

    def evaluate_all(self, spans: list[SpanRecord], now_ms: float | None = None) -> list[SLOReport]:
        """Evaluate all configured SLO definitions."""
        return [self.evaluate_single(slo, spans, now_ms=now_ms) for slo in self.definitions]
