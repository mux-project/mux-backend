"""Prometheus metrics for the Alert Engine.

Exposes counters, gauges, and histograms for monitoring
the alert engine's health and throughput.
"""

from prometheus_client import Counter, Gauge, Histogram

# Stream metrics
alert_messages_consumed = Counter(
    "alert_messages_consumed_total",
    "Total messages consumed from the ingestion stream",
    ["status"],  # ack, skip, error
)
alert_stream_lag = Gauge(
    "alert_stream_lag_seconds",
    "Estimated lag between ingestion and consumption",
)
alert_dlq_size = Gauge(
    "alert_dlq_size",
    "Number of messages in the dead-letter queue",
)
alert_pending_messages = Gauge(
    "alert_pending_messages",
    "Number of pending (unacknowledged) messages in the consumer group",
)

# Evaluation metrics
alert_evaluations = Counter(
    "alert_evaluations_total",
    "Total rule evaluations performed",
    ["result"],  # fire, resolve, breach_start, breach_continue, breach_cleared, none
)
alert_evaluation_duration = Histogram(
    "alert_evaluation_duration_seconds",
    "Time spent evaluating a single (rule, metric) pair",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# Alert lifecycle metrics
alert_fired = Counter(
    "alert_fired_total",
    "Total alerts fired",
    ["rule_name", "tenant"],
)
alert_resolved = Counter(
    "alert_resolved_total",
    "Total alerts resolved",
    ["rule_name", "tenant"],
)
alert_renotified = Counter(
    "alert_renotified_total",
    "Total re-notifications sent",
    ["rule_name", "channel", "escalation_level"],
)

# Notification metrics
alert_notifications = Counter(
    "alert_notifications_total",
    "Total notifications dispatched",
    ["channel", "result"],  # email, slack; ok, fail
)
alert_notification_duration = Histogram(
    "alert_notification_duration_seconds",
    "Time spent dispatching a single notification",
    ["channel"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0],
)

# DB metrics
alert_db_errors = Counter(
    "alert_db_errors_total",
    "Total database errors",
    ["operation"],  # insert, update, select, integrity
)
alert_redis_errors = Counter(
    "alert_redis_errors_total",
    "Total Redis errors",
    ["operation"],  # read, write, lock
)

# State metrics
alert_active_count = Gauge(
    "alert_active_count",
    "Number of currently firing alerts (from Redis state)",
)
alert_breach_window_count = Gauge(
    "alert_breach_window_count",
    "Number of active breach windows (duration in progress)",
)
alert_cooldown_count = Gauge(
    "alert_cooldown_count",
    "Number of rules in cooldown",
)

# Retry / DLQ metrics
alert_retries = Counter(
    "alert_retries_total",
    "Total message retries attempted",
)
alert_dlq_entries = Counter(
    "alert_dlq_entries_total",
    "Total messages moved to dead-letter queue",
)
