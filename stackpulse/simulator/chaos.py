"""Reference Microservice Workload Simulator & Deterministic Chaos Scenario Injector."""

from __future__ import annotations

import random
import time
from typing import Literal

from stackpulse.ingest.models import (
    LogRecord,
    MetricPoint,
    SpanEvent,
    SpanRecord,
    TelemetryStore,
)

ScenarioType = Literal["normal", "db-saturation", "n-plus-one", "retry-storm", "all"]


class ChaosSimulator:
    """Generates realistic multi-service OpenTelemetry trace trees, logs, and metrics."""

    def __init__(self, seed: int = 42) -> None:
        self.rng = random.Random(seed)
        self._trace_seq = 1000

    def _next_ids(self, prefix: str = "tr") -> tuple[str, int]:
        self._trace_seq += 1
        return f"{prefix}-{self._trace_seq:06d}a8f4", self._trace_seq

    def generate_normal_trace(self, base_ts_ms: float | None = None) -> tuple[list[SpanRecord], list[LogRecord]]:
        """Generate a healthy low-latency distributed checkout trace."""
        t0 = base_ts_ms if base_ts_ms is not None else time.time() * 1000.0
        trace_id, seq = self._next_ids("tr-norm")

        auth_dur = round(self.rng.uniform(4.5, 11.0), 2)
        cache_dur = round(self.rng.uniform(1.2, 3.5), 2)
        db_dur = round(self.rng.uniform(8.0, 22.0), 2)
        pay_dur = round(self.rng.uniform(18.0, 38.0), 2)
        checkout_dur = round(cache_dur + db_dur + pay_dur + self.rng.uniform(6.0, 14.0), 2)
        gw_dur = round(auth_dur + checkout_dur + self.rng.uniform(4.0, 9.0), 2)

        s_gw = f"sp-{seq}-gw"
        s_auth = f"sp-{seq}-auth"
        s_chk = f"sp-{seq}-chk"
        s_red = f"sp-{seq}-red"
        s_pg = f"sp-{seq}-pg"
        s_pay = f"sp-{seq}-pay"

        spans = [
            SpanRecord(
                trace_id=trace_id,
                span_id=s_gw,
                parent_span_id=None,
                service_name="api-gateway",
                operation="POST /v1/orders/checkout",
                start_time_ms=t0,
                duration_ms=gw_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
                attributes={"http.method": "POST", "http.route": "/v1/orders/checkout", "region": "us-east-1"},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_auth,
                parent_span_id=s_gw,
                service_name="auth-service",
                operation="gRPC AuthService/VerifyJWT",
                start_time_ms=t0 + 2.0,
                duration_ms=auth_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
                attributes={"auth.scheme": "ed25519-jwt"},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_chk,
                parent_span_id=s_gw,
                service_name="checkout-service",
                operation="CheckoutOrchestrator.processOrder",
                start_time_ms=t0 + 3.0 + auth_dur,
                duration_ms=checkout_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
                attributes={"cart.items": 3},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_red,
                parent_span_id=s_chk,
                service_name="redis-cache",
                operation="GET session:cart:profile",
                start_time_ms=t0 + 5.0 + auth_dur,
                duration_ms=cache_dur,
                status_code="OK",
                span_kind="CLIENT",
                attributes={"db.system": "redis", "cache.hit": True},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_pg,
                parent_span_id=s_chk,
                service_name="postgres-primary",
                operation="SELECT inventory_ledger FOR UPDATE",
                start_time_ms=t0 + 6.0 + auth_dur + cache_dur,
                duration_ms=db_dur,
                status_code="OK",
                span_kind="CLIENT",
                attributes={"db.system": "postgresql", "db.rows_examined": 3},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_pay,
                parent_span_id=s_chk,
                service_name="payment-worker",
                operation="POST /internal/payments/authorize",
                start_time_ms=t0 + 8.0 + auth_dur + cache_dur + db_dur,
                duration_ms=pay_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
                attributes={"payment.provider": "stripe-live"},
            ),
        ]
        return spans, []

    def generate_db_saturation_trace(self, base_ts_ms: float | None = None) -> tuple[list[SpanRecord], list[LogRecord]]:
        """Scenario 2: Database Connection Pool Saturation & Slow Query Cascade."""
        t0 = base_ts_ms if base_ts_ms is not None else time.time() * 1000.0
        trace_id, seq = self._next_ids("tr-dbsat")

        auth_dur = round(self.rng.uniform(6.0, 12.0), 2)
        pool_wait_ms = round(self.rng.uniform(310.0, 480.0), 2)
        slow_sql_ms = round(self.rng.uniform(520.0, 790.0), 2)
        db_total = round(pool_wait_ms + slow_sql_ms, 2)
        chk_dur = round(db_total + 28.0, 2)
        gw_dur = round(auth_dur + chk_dur + 12.0, 2)

        s_gw = f"sp-{seq}-gw"
        s_auth = f"sp-{seq}-auth"
        s_chk = f"sp-{seq}-chk"
        s_pg = f"sp-{seq}-pg-slow"

        is_timeout = self.rng.random() < 0.45
        status_code = "ERROR" if is_timeout else "OK"
        http_code = 504 if is_timeout else 200

        spans = [
            SpanRecord(
                trace_id=trace_id,
                span_id=s_gw,
                parent_span_id=None,
                service_name="api-gateway",
                operation="POST /v1/orders/checkout",
                start_time_ms=t0,
                duration_ms=gw_dur,
                status_code=status_code,
                http_status=http_code,
                span_kind="SERVER",
                attributes={"scenario": "db-saturation", "http.route": "/v1/orders/checkout"},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_auth,
                parent_span_id=s_gw,
                service_name="auth-service",
                operation="gRPC AuthService/VerifyJWT",
                start_time_ms=t0 + 2.0,
                duration_ms=auth_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_chk,
                parent_span_id=s_gw,
                service_name="checkout-service",
                operation="CheckoutOrchestrator.processOrder",
                start_time_ms=t0 + 4.0 + auth_dur,
                duration_ms=chk_dur,
                status_code=status_code,
                http_status=http_code,
                span_kind="SERVER",
                attributes={"db.pool.active": 100, "db.pool.max": 100, "db.pool.waiting_threads": 42},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_pg,
                parent_span_id=s_chk,
                service_name="postgres-primary",
                operation="SELECT * FROM order_ledger WHERE customer_org_id = $1 FOR UPDATE",
                start_time_ms=t0 + 14.0 + auth_dur,
                duration_ms=db_total,
                status_code=status_code,
                span_kind="CLIENT",
                attributes={
                    "db.system": "postgresql",
                    "db.pool_wait_ms": pool_wait_ms,
                    "db.exec_ms": slow_sql_ms,
                    "db.lock_wait": "RowExclusiveLock",
                },
                events=[
                    SpanEvent(
                        name="pool_exhaustion_wait",
                        timestamp_ms=t0 + 16.0 + auth_dur,
                        attributes={"pool_wait_ms": pool_wait_ms, "active_connections": 100},
                    )
                ],
            ),
        ]

        logs = [
            LogRecord(
                timestamp_ms=t0 + 25.0 + auth_dur,
                severity="ERROR" if is_timeout else "WARN",
                service_name="postgres-primary",
                message=(
                    f"Connection pool saturation: waited {pool_wait_ms:.1f}ms for slot; "
                    f"sequential scan took {slow_sql_ms:.1f}ms on order_ledger"
                ),
                trace_id=trace_id,
                span_id=s_pg,
                attributes={"active_connections": 100, "max_connections": 100},
            )
        ]
        return spans, logs

    def generate_n_plus_one_trace(
        self,
        base_ts_ms: float | None = None,
        query_count: int = 9,
    ) -> tuple[list[SpanRecord], list[LogRecord]]:
        """Scenario 3: N+1 ORM Query Regression under checkout-service."""
        t0 = base_ts_ms if base_ts_ms is not None else time.time() * 1000.0
        trace_id, seq = self._next_ids("tr-nplus1")

        auth_dur = round(self.rng.uniform(5.0, 9.0), 2)
        s_gw = f"sp-{seq}-gw"
        s_auth = f"sp-{seq}-auth"
        s_chk = f"sp-{seq}-chk"

        child_spans: list[SpanRecord] = []
        cursor_ms = t0 + 10.0 + auth_dur
        total_sql_ms = 0.0

        for idx in range(query_count):
            q_dur = round(self.rng.uniform(24.0, 38.0), 2)
            child_spans.append(
                SpanRecord(
                    trace_id=trace_id,
                    span_id=f"sp-{seq}-sql-{idx + 1}",
                    parent_span_id=s_chk,
                    service_name="postgres-primary",
                    operation="SELECT sku_metadata FROM catalog_items WHERE item_id = $1",
                    start_time_ms=cursor_ms,
                    duration_ms=q_dur,
                    status_code="OK",
                    span_kind="CLIENT",
                    attributes={"db.system": "postgresql", "orm.lazy_load": True, "item_index": idx + 1},
                )
            )
            cursor_ms += q_dur + 1.5
            total_sql_ms += q_dur

        chk_dur = round((cursor_ms - (t0 + 6.0 + auth_dur)) + 15.0, 2)
        gw_dur = round(auth_dur + chk_dur + 8.0, 2)

        spans = [
            SpanRecord(
                trace_id=trace_id,
                span_id=s_gw,
                parent_span_id=None,
                service_name="api-gateway",
                operation="GET /v1/orders/cart-hydrated",
                start_time_ms=t0,
                duration_ms=gw_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
                attributes={"scenario": "n-plus-one"},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_auth,
                parent_span_id=s_gw,
                service_name="auth-service",
                operation="gRPC AuthService/VerifyJWT",
                start_time_ms=t0 + 2.0,
                duration_ms=auth_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_chk,
                parent_span_id=s_gw,
                service_name="checkout-service",
                operation="CartHydrator.fetchLineItems",
                start_time_ms=t0 + 6.0 + auth_dur,
                duration_ms=chk_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
                attributes={"orm.n_plus_one_detected": True, "child_query_count": query_count},
            ),
            *child_spans,
        ]

        logs = [
            LogRecord(
                timestamp_ms=cursor_ms,
                severity="WARN",
                service_name="checkout-service",
                message=(
                    f"N+1 ORM query regression detected in CartHydrator.fetchLineItems: "
                    f"{query_count} sequential SELECT queries took {total_sql_ms:.1f}ms"
                ),
                trace_id=trace_id,
                span_id=s_chk,
                attributes={"query_count": query_count},
            )
        ]
        return spans, logs

    def generate_retry_storm_trace(
        self,
        base_ts_ms: float | None = None,
        attempts: int = 4,
    ) -> tuple[list[SpanRecord], list[LogRecord]]:
        """Scenario 4: Downstream Payment Provider 503 Retry Storm."""
        t0 = base_ts_ms if base_ts_ms is not None else time.time() * 1000.0
        trace_id, seq = self._next_ids("tr-retry")

        auth_dur = round(self.rng.uniform(5.0, 9.0), 2)
        s_gw = f"sp-{seq}-gw"
        s_auth = f"sp-{seq}-auth"
        s_chk = f"sp-{seq}-chk"

        retry_spans: list[SpanRecord] = []
        cursor_ms = t0 + 12.0 + auth_dur

        for attempt in range(1, attempts + 1):
            att_dur = round(self.rng.uniform(95.0, 145.0), 2)
            retry_spans.append(
                SpanRecord(
                    trace_id=trace_id,
                    span_id=f"sp-{seq}-pay-att{attempt}",
                    parent_span_id=s_chk,
                    service_name="payment-worker",
                    operation="POST /internal/payments/authorize",
                    start_time_ms=cursor_ms,
                    duration_ms=att_dur,
                    status_code="ERROR",
                    http_status=503,
                    span_kind="SERVER",
                    attributes={
                        "retry_attempt": attempt,
                        "http.status_code": 503,
                        "upstream.error": "gateway_503_service_unavailable",
                    },
                    events=[
                        SpanEvent(
                            name="retry",
                            timestamp_ms=cursor_ms + att_dur,
                            attributes={"attempt": attempt, "backoff_ms": 20 * attempt},
                        )
                    ],
                )
            )
            cursor_ms += att_dur + (15.0 * attempt)

        chk_dur = round((cursor_ms - (t0 + 6.0 + auth_dur)) + 10.0, 2)
        gw_dur = round(auth_dur + chk_dur + 8.0, 2)

        spans = [
            SpanRecord(
                trace_id=trace_id,
                span_id=s_gw,
                parent_span_id=None,
                service_name="api-gateway",
                operation="POST /v1/orders/checkout",
                start_time_ms=t0,
                duration_ms=gw_dur,
                status_code="ERROR",
                http_status=503,
                span_kind="SERVER",
                attributes={"scenario": "retry-storm"},
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_auth,
                parent_span_id=s_gw,
                service_name="auth-service",
                operation="gRPC AuthService/VerifyJWT",
                start_time_ms=t0 + 2.0,
                duration_ms=auth_dur,
                status_code="OK",
                http_status=200,
                span_kind="SERVER",
            ),
            SpanRecord(
                trace_id=trace_id,
                span_id=s_chk,
                parent_span_id=s_gw,
                service_name="checkout-service",
                operation="CheckoutOrchestrator.processOrder",
                start_time_ms=t0 + 6.0 + auth_dur,
                duration_ms=chk_dur,
                status_code="ERROR",
                http_status=503,
                span_kind="SERVER",
                attributes={"retry.exhausted": True, "attempts": attempts},
            ),
            *retry_spans,
        ]

        logs = [
            LogRecord(
                timestamp_ms=cursor_ms,
                severity="ERROR",
                service_name="payment-worker",
                message=(
                    f"Downstream payment gateway returned HTTP 503 across {attempts} consecutive retry attempts; "
                    f"circuit breaker open"
                ),
                trace_id=trace_id,
                span_id=retry_spans[-1].span_id,
                attributes={"attempts": attempts, "http_status": 503},
            )
        ]
        return spans, logs

    def inject_scenario(
        self,
        store: TelemetryStore,
        scenario: ScenarioType = "all",
        count: int = 24,
    ) -> dict[str, int]:
        """Populate a TelemetryStore with traces, logs, and metrics for the requested scenario."""
        now_ms = time.time() * 1000.0
        traces_added = 0
        spans_added = 0
        logs_added = 0

        for i in range(count):
            ts = now_ms - (count - i) * 450.0
            if scenario == "normal":
                sp, lg = self.generate_normal_trace(ts)
            elif scenario == "db-saturation":
                sp, lg = (
                    self.generate_db_saturation_trace(ts)
                    if i % 3 != 0
                    else self.generate_normal_trace(ts)
                )
            elif scenario == "n-plus-one":
                sp, lg = (
                    self.generate_n_plus_one_trace(ts)
                    if i % 2 == 0
                    else self.generate_normal_trace(ts)
                )
            elif scenario == "retry-storm":
                sp, lg = (
                    self.generate_retry_storm_trace(ts)
                    if i % 3 != 0
                    else self.generate_normal_trace(ts)
                )
            else:
                # "all" mixed chaos workload
                mod = i % 6
                if mod in (0, 1):
                    sp, lg = self.generate_normal_trace(ts)
                elif mod == 2:
                    sp, lg = self.generate_db_saturation_trace(ts)
                elif mod in (3, 4):
                    sp, lg = self.generate_n_plus_one_trace(ts)
                else:
                    sp, lg = self.generate_retry_storm_trace(ts)

            store.ingest_spans(sp)
            store.ingest_logs(lg)
            traces_added += 1
            spans_added += len(sp)
            logs_added += len(lg)

        # Emit synthetic infrastructure gauge metrics
        store.ingest_metric(
            MetricPoint(
                name="db_connection_pool_utilization_ratio",
                metric_type="gauge",
                value=0.98 if scenario in ("db-saturation", "all") else 0.34,
                timestamp_ms=now_ms,
                service_name="postgres-primary",
                labels={"cluster": "prod-us-east-1"},
            )
        )
        store.ingest_metric(
            MetricPoint(
                name="http_requests_total",
                metric_type="counter",
                value=float(traces_added),
                timestamp_ms=now_ms,
                service_name="api-gateway",
                labels={"scenario": scenario},
            )
        )

        return {
            "traces_added": traces_added,
            "spans_added": spans_added,
            "logs_added": logs_added,
        }
