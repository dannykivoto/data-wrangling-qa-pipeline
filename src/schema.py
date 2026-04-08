EXPECTED_COLUMNS = [
    "batch_id",
    "user_id",
    "account_created",
    "action_timestamp",
    "event_type",
    "session_id",
    "session_start",
    "session_end",
    "bug_type",
    "severity",
    "api_status",
    "country",
]

CRITICAL_RULES = {
    "timestamp_violation_rate_threshold": 0.10
}
