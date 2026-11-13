"""OpenTelemetry (OTLP/HTTP JSON) standard mapping adapter for StackPulse."""

from __future__ import annotations

import base64
import binascii
from typing import Any

from stackpulse.ingest.models import SpanEvent, SpanRecord


def parse_otlp_value(val: Any) -> Any:
    """Extract native Python value from an OTLP AnyValue container."""
    if not isinstance(val, dict):
        return val

    if "stringValue" in val:
        return val["stringValue"]
    if "string_value" in val:
        return val["string_value"]

    if "intValue" in val:
        return int(val["intValue"])
    if "int_value" in val:
        return int(val["int_value"])

    if "doubleValue" in val:
        return float(val["doubleValue"])
    if "double_value" in val:
        return float(val["double_value"])

    if "boolValue" in val:
        return bool(val["boolValue"])
    if "bool_value" in val:
        return bool(val["bool_value"])

    if "arrayValue" in val or "array_value" in val:
        arr = val.get("arrayValue") or val.get("array_value") or {}
        items = arr.get("values", [])
        return [parse_otlp_value(item) for item in items]

    if "kvlistValue" in val or "kvlist_value" in val:
        kv_list = val.get("kvlistValue") or val.get("kvlist_value") or {}
        items = kv_list.get("values", [])
        res: dict[str, Any] = {}
        for item in items:
            k = item.get("key")
            v = parse_otlp_value(item.get("value"))
            if k is not None:
                res[k] = v
        return res

    if "bytesValue" in val or "bytes_value" in val:
        b = val.get("bytesValue") or val.get("bytes_value")
        return str(b)

    # If it is a generic dictionary without standard OTLP value wrappers
    return {k: parse_otlp_value(v) for k, v in val.items()}


def parse_otlp_attributes(attrs: list[dict[str, Any]] | dict[str, Any] | None) -> dict[str, Any]:
    """Normalize OTLP attributes into a flat dictionary."""
    if not attrs:
        return {}

    if isinstance(attrs, dict):
        return {k: parse_otlp_value(v) for k, v in attrs.items()}

    result: dict[str, Any] = {}
    if isinstance(attrs, list):
        for item in attrs:
            if isinstance(item, dict) and "key" in item:
                k = item["key"]
                raw_v = item.get("value", {})
                result[k] = parse_otlp_value(raw_v)
    return result


def normalize_id(id_val: Any, default_hex_len: int = 32) -> str:
    """Normalize trace_id (128-bit) or span_id (64-bit) from hex, base64, or int."""
    if id_val is None:
        return ""

    if isinstance(id_val, int):
        return f"{id_val:0{default_hex_len}x}"

    s = str(id_val).strip()
    if not s:
        return ""

    # If purely hex characters and matches or exceeds expected length
    if len(s) == default_hex_len:
        try:
            int(s, 16)
            return s.lower()
        except ValueError:
            pass

    # Try base64 decoding (standard OTLP JSON serialization of bytes fields)
    try:
        decoded_bytes = base64.b64decode(s, validate=True)
        if len(decoded_bytes) in (8, 16):  # 64-bit span or 128-bit trace
            return decoded_bytes.hex().lower()
    except (binascii.Error, ValueError):
        pass

    # Clean non-alphanumeric chars and pad/slice
    cleaned = "".join(c for c in s if c.isalnum()).lower()
    if len(cleaned) < 4:
        cleaned = cleaned.ljust(default_hex_len, "0")
    return cleaned[:default_hex_len]


def map_otlp_span_kind(kind: Any) -> str:
    """Map OTLP span kind (enum int or string) to StackPulse SpanRecord span_kind."""
    if isinstance(kind, int):
        mapping = {
            1: "INTERNAL",
            2: "SERVER",
            3: "CLIENT",
            4: "PRODUCER",
            5: "CONSUMER",
        }
        return mapping.get(kind, "INTERNAL")

    if isinstance(kind, str):
        upper = kind.upper().replace("SPAN_KIND_", "")
        if upper in {"SERVER", "CLIENT", "INTERNAL", "PRODUCER", "CONSUMER"}:
            return upper
    return "INTERNAL"


def map_otlp_status_code(status_dict: dict[str, Any] | None) -> str:
    """Extract W3C status code from OTLP status object."""
    if not status_dict:
        return "OK"

    code = status_dict.get("code")
    if code in (1, "STATUS_CODE_OK", "OK"):
        return "OK"
    if code in (2, "STATUS_CODE_ERROR", "ERROR"):
        return "ERROR"
    if code in (0, "STATUS_CODE_UNSET", "UNSET"):
        return "UNSET"

    return "OK"


def parse_otlp_resource_spans(payload: dict[str, Any]) -> list[SpanRecord]:
    """Parse standard OpenTelemetry OTLP/HTTP JSON payload into SpanRecords."""
    resource_spans = payload.get("resourceSpans") or payload.get("resource_spans") or []
    if not isinstance(resource_spans, list):
        return []

    spans_out: list[SpanRecord] = []

    for rs in resource_spans:
        resource = rs.get("resource") or {}
        resource_attrs = parse_otlp_attributes(resource.get("attributes"))
        service_name = (
            resource_attrs.get("service.name")
            or resource_attrs.get("service_name")
            or "unknown-service"
        )

        scope_spans = rs.get("scopeSpans") or rs.get("scope_spans") or []
        for ss in scope_spans:
            raw_spans = ss.get("spans") or []
            for s in raw_spans:
                trace_id = normalize_id(s.get("traceId") or s.get("trace_id"), default_hex_len=32)
                span_id = normalize_id(s.get("spanId") or s.get("span_id"), default_hex_len=16)

                raw_parent = s.get("parentSpanId") or s.get("parent_span_id")
                parent_span_id = normalize_id(raw_parent, default_hex_len=16) if raw_parent else None

                operation = s.get("name") or s.get("operation") or "unnamed_operation"

                # Parse timestamps (OTLP transmits epoch nanoseconds as integers or strings)
                start_ns = s.get("startTimeUnixNano") or s.get("start_time_unix_nano") or 0
                end_ns = s.get("endTimeUnixNano") or s.get("end_time_unix_nano") or 0

                try:
                    start_ns_val = int(start_ns)
                except (ValueError, TypeError):
                    start_ns_val = 0

                try:
                    end_ns_val = int(end_ns)
                except (ValueError, TypeError):
                    end_ns_val = 0

                if start_ns_val > 0:
                    start_time_ms = start_ns_val / 1_000_000.0
                else:
                    start_time_ms = float(s.get("start_time_ms", 0.0))

                if end_ns_val >= start_ns_val > 0:
                    duration_ms = (end_ns_val - start_ns_val) / 1_000_000.0
                else:
                    duration_ms = float(s.get("duration_ms", 0.0))

                span_kind = map_otlp_span_kind(s.get("kind") or s.get("span_kind"))
                status_code = map_otlp_status_code(s.get("status"))

                span_attrs = parse_otlp_attributes(s.get("attributes"))
                # Merge resource attrs if not overridden
                merged_attrs = dict(resource_attrs)
                merged_attrs.update(span_attrs)

                # HTTP Status
                http_status = None
                for hk in ("http.status_code", "http.response.status_code", "http_status"):
                    if hk in span_attrs:
                        try:
                            http_status = int(span_attrs[hk])
                            break
                        except (ValueError, TypeError):
                            pass

                # Peer service
                peer_service = None
                for pk in ("peer.service", "net.peer.name", "server.address", "db.system", "peer_service"):
                    if pk in span_attrs:
                        peer_service = str(span_attrs[pk])
                        break

                # Events
                events_out: list[SpanEvent] = []
                raw_events = s.get("events") or []
                for ev in raw_events:
                    ev_name = ev.get("name", "event")
                    ev_time_ns = ev.get("timeUnixNano") or ev.get("time_unix_nano") or 0
                    try:
                        ev_time_ms = int(ev_time_ns) / 1_000_000.0 if ev_time_ns else start_time_ms
                    except (ValueError, TypeError):
                        ev_time_ms = start_time_ms
                    ev_attrs = parse_otlp_attributes(ev.get("attributes"))
                    events_out.append(
                        SpanEvent(
                            name=ev_name,
                            timestamp_ms=ev_time_ms,
                            attributes=ev_attrs,
                        )
                    )

                spans_out.append(
                    SpanRecord(
                        trace_id=trace_id,
                        span_id=span_id,
                        parent_span_id=parent_span_id,
                        service_name=service_name,
                        operation=operation,
                        start_time_ms=start_time_ms,
                        duration_ms=max(0.0, duration_ms),
                        status_code=status_code,
                        http_status=http_status,
                        span_kind=span_kind,
                        peer_service=peer_service,
                        attributes=merged_attrs,
                        events=events_out,
                    )
                )

    return spans_out
