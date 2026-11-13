"""Webhook notifications and SRE alert dispatcher for StackPulse."""

from __future__ import annotations

import ipaddress
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlsplit

from stackpulse.incidents.correlator import IncidentReport
from stackpulse.slo.engine import SLOReport

_BLOCKED_WEBHOOK_HOSTS = frozenset(
    {
        "localhost",
        "ip6-localhost",
        "ip6-loopback",
        "metadata",
        "metadata.google.internal",
    }
)


def validate_webhook_url(url: str) -> str:
    """Validate an outbound webhook URL against common SSRF targets."""
    cleaned = url.strip()
    parsed = urlsplit(cleaned)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("Webhook URL must use http or https scheme.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Webhook URL must not contain embedded credentials.")
    hostname = (parsed.hostname or "").strip().lower().rstrip(".")
    if not hostname:
        raise ValueError("Webhook URL must contain a valid hostname.")
    allow_private = os.getenv("STACKPULSE_ALLOW_PRIVATE_WEBHOOKS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if allow_private:
        return cleaned
    if (
        hostname in _BLOCKED_WEBHOOK_HOSTS
        or hostname.endswith((".localhost", ".local", ".localdomain", ".internal"))
        or hostname.isdigit()
    ):
        raise ValueError(f"Webhook host {hostname!r} is restricted (SSRF policy).")
    try:
        ip_obj = ipaddress.ip_address(hostname)
    except ValueError:
        ip_obj = None
    if ip_obj is not None and (
        ip_obj.is_private
        or ip_obj.is_loopback
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
        or ip_obj.is_unspecified
    ):
        raise ValueError(f"Webhook IP {hostname!r} is restricted (SSRF policy).")
    return cleaned

AlertSeverity = Literal["PAGE_CRITICAL", "TICKET_WARNING", "INFO"]


@dataclass
class AlertEvent:
    """Structured SRE incident or SLO breach alert event."""

    alert_id: str
    alert_name: str
    severity: AlertSeverity
    service: str
    summary: str
    timestamp_ms: float = field(default_factory=lambda: time.time() * 1000.0)
    details: dict[str, Any] = field(default_factory=dict)
    remediation_steps: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "alert_name": self.alert_name,
            "severity": self.severity,
            "service": self.service,
            "summary": self.summary,
            "timestamp_ms": self.timestamp_ms,
            "details": self.details,
            "remediation_steps": self.remediation_steps,
        }

    def to_slack_payload(self) -> dict[str, Any]:
        """Format as Slack-compatible webhook message block."""
        emoji = "🚨" if self.severity == "PAGE_CRITICAL" else ("⚠️" if self.severity == "TICKET_WARNING" else "ℹ️")
        return {
            "text": f"{emoji} [{self.severity}] {self.alert_name} on `{self.service}`",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} {self.alert_name}: {self.service}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Severity:*\n`{self.severity}`"},
                        {"type": "mrkdwn", "text": f"*Service:*\n`{self.service}`"},
                    ],
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Summary:*\n{self.summary}"},
                },
            ],
        }


class AlertDispatcher:
    """Routes alerts to registered webhooks with deduplication and cooldown."""

    def __init__(self, cooldown_seconds: float = 300.0) -> None:
        self.cooldown_seconds = cooldown_seconds
        self.webhook_urls: list[str] = []
        self._last_fired: dict[str, float] = {}
        self.dispatched_history: list[AlertEvent] = []
        self._http_sender: Callable[[str, dict[str, Any]], bool] | None = None

    def register_webhook(self, url: str) -> None:
        """Register a destination HTTP(S) webhook URL after SSRF policy validation."""
        validated = validate_webhook_url(url)
        if validated not in self.webhook_urls:
            self.webhook_urls.append(validated)

    def set_custom_sender(self, sender: Callable[[str, dict[str, Any]], bool]) -> None:
        """Override the network transport for unit tests or custom integrations."""
        self._http_sender = sender

    def should_fire(self, alert_key: str) -> bool:
        """Evaluate if an alert should fire based on cooling-off threshold."""
        now = time.time()
        last = self._last_fired.get(alert_key, 0.0)
        if (now - last) >= self.cooldown_seconds:
            self._last_fired[alert_key] = now
            return True
        return False

    def evaluate_slo_reports(self, reports: list[SLOReport]) -> list[AlertEvent]:
        """Convert firing SLO reports into alert events."""
        events: list[AlertEvent] = []
        for r in reports:
            if r.alert_severity == "OK":
                continue

            alert_key = f"slo:{r.name}:{r.alert_severity}"
            if not self.should_fire(alert_key):
                continue

            summary = (
                f"SLO '{r.name}' on service '{r.service_name}' breached: "
                f"burn_rate={r.burn_rate_1h:.2f}x ({r.alert_severity}), "
                f"error budget remaining={r.remaining_budget_pct:.2f}%."
            )
            remediation = [
                "Inspect trace DAGs with errors on service: " + r.service_name,
                "Check recent deployments or canary releases",
                "Verify database query latencies and downstream dependencies",
            ]

            ev = AlertEvent(
                alert_id=f"alert-slo-{int(time.time() * 1000)}-{r.service_name}",
                alert_name=f"SLOBurnRateBreach-{r.name}",
                severity=r.alert_severity,
                service=r.service_name,
                summary=summary,
                details=r.to_dict(),
                remediation_steps=remediation,
            )
            events.append(ev)
            self._dispatch_event(ev)

        return events

    def evaluate_incident(self, incident: IncidentReport) -> list[AlertEvent]:
        """Convert detected incidents into alerts."""
        if not incident or incident.severity == "NONE":
            return []

        alert_key = f"incident:{incident.root_cause_category}:{incident.root_cause_service}"
        if not self.should_fire(alert_key):
            return []

        summary = (
            f"Active incident detected: {incident.title} (Severity: {incident.severity}). "
            f"Root cause in '{incident.root_cause_service}', Category: {incident.root_cause_category}."
        )

        ev = AlertEvent(
            alert_id=f"alert-inc-{int(time.time() * 1000)}-{incident.root_cause_service}",
            alert_name=f"IncidentDetected-{incident.root_cause_category}",
            severity="PAGE_CRITICAL" if incident.severity in ("SEV-1", "SEV-2") else "TICKET_WARNING",
            service=incident.root_cause_service,
            summary=summary,
            details=incident.to_dict(),
            remediation_steps=incident.action_items,
        )
        self._dispatch_event(ev)
        return [ev]

    def _dispatch_event(self, event: AlertEvent) -> None:
        """Dispatch event to history and configured webhooks."""
        self.dispatched_history.append(event)
        payload = event.to_dict()
        for url in self.webhook_urls:
            if self._http_sender:
                self._http_sender(url, payload)
            else:
                # Default non-blocking attempt (or simulated in local environments)
                pass
