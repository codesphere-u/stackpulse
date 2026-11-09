"""Nexus Aegis Gate 4 (G4_TRAJECTORY_HEALTH) Attestation Bridge for StackPulse."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from stackpulse.tracing.analyzer import TraceAnalysis


def canonical_json_bytes(data: Dict[str, Any]) -> bytes:
    """Serialize dictionary into deterministic RFC 8785 canonical JSON bytes."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def rfc6962_leaf_hash(payload: Dict[str, Any]) -> str:
    """Compute RFC 6962 Merkle tree leaf hash: SHA-256(0x00 || canonical_json)."""
    leaf_bytes = b"\x00" + canonical_json_bytes(payload)
    return hashlib.sha256(leaf_bytes).hexdigest()


def create_aegis_trajectory_payload(analysis: TraceAnalysis) -> Dict[str, Any]:
    """Construct canonical payload for Aegis Gate 4 verification."""
    passed = not analysis.has_error and len(analysis.n_plus_one_findings) == 0 and len(analysis.retry_findings) == 0

    return {
        "gate": "G4_TRAJECTORY_HEALTH",
        "trace_id": analysis.trace_id,
        "root_service": analysis.root_service,
        "root_operation": analysis.root_operation,
        "span_count": analysis.span_count,
        "total_duration_ms": round(analysis.total_duration_ms, 3),
        "critical_path_span_ids": analysis.critical_path_span_ids,
        "critical_path_length": len(analysis.critical_path_span_ids),
        "bottleneck_service": analysis.bottleneck_service,
        "bottleneck_exclusive_ms": round(analysis.bottleneck_exclusive_ms, 3),
        "has_error": analysis.has_error,
        "n_plus_one_findings_count": len(analysis.n_plus_one_findings),
        "retry_findings_count": len(analysis.retry_findings),
        "passed": passed,
    }


def create_aegis_trajectory_leaf(analysis: TraceAnalysis) -> Dict[str, Any]:
    """Generate complete Aegis G4 attestation leaf with RFC 6962 cryptographic digest."""
    payload = create_aegis_trajectory_payload(analysis)
    leaf_hash = rfc6962_leaf_hash(payload)

    return {
        "gate": "G4_TRAJECTORY_HEALTH",
        "trace_id": analysis.trace_id,
        "passed": payload["passed"],
        "leaf_hash": leaf_hash,
        "payload": payload,
    }
