from dataclasses import asdict, dataclass

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

SCHEMA_VERSION = "2026.04"

COLUMN_DICTIONARY = [
    ("batch_id", "string", "Source batch identifier", "raw", "None"),
    ("user_id", "string", "Unique user identifier", "raw", "trimmed"),
    ("account_created", "datetime", "Account creation timestamp", "raw", "parsed to datetime"),
    ("action_timestamp", "datetime", "Action event timestamp", "raw", "parsed to datetime"),
    ("event_type", "string", "Type of user action", "raw", "lowercased"),
    ("session_id", "string", "Session identifier", "raw", "None"),
    ("session_start", "datetime", "Session start time", "raw", "parsed to datetime"),
    ("session_end", "datetime", "Session end time", "raw", "parsed to datetime"),
    ("bug_type", "string", "Bug classification if present", "raw", "trimmed"),
    ("severity", "string", "Bug severity label", "raw", "lowercased"),
    ("api_status", "int", "API response status", "raw", "None"),
    ("country", "string", "Country code", "raw", "uppercased"),
]

PROFILE_CATEGORICAL_COLUMNS = ["event_type", "bug_type", "severity", "country"]
PROFILE_NUMERIC_COLUMNS = ["api_status", "account_to_action_minutes", "session_duration_minutes"]


@dataclass(frozen=True)
class ValidationPolicy:
    timestamp_violation_rate_threshold: float = 0.10
    modified_zscore_threshold: float = 3.5


DEFAULT_VALIDATION_POLICY = ValidationPolicy()


def policy_as_dict(policy: ValidationPolicy = DEFAULT_VALIDATION_POLICY) -> dict[str, float]:
    return asdict(policy)
