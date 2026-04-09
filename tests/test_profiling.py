import pandas as pd

from src.profiling import (
    add_derived_metrics,
    build_column_quality_profile,
    build_dataset_profile,
)
from src.schema import DEFAULT_VALIDATION_POLICY


def test_build_column_quality_profile_captures_null_rate():
    df = pd.DataFrame(
        [
            {"user_id": "u1", "event_type": "login"},
            {"user_id": None, "event_type": "login"},
        ]
    )

    profile = build_column_quality_profile(df, dataset_label="raw_input")
    user_row = profile.loc[profile["column_name"] == "user_id"].iloc[0]

    assert user_row["dataset_label"] == "raw_input"
    assert user_row["null_count"] == 1
    assert user_row["null_rate"] == 0.5


def test_build_dataset_profile_reports_advanced_findings():
    clean_df = pd.DataFrame(
        [
            {
                "event_type": "login",
                "bug_type": "",
                "severity": "",
                "country": "KE",
                "api_status": 200,
                "account_created": pd.Timestamp("2026-01-01 10:00:00"),
                "action_timestamp": pd.Timestamp("2026-01-01 10:05:00"),
                "session_start": pd.Timestamp("2026-01-01 10:01:00"),
                "session_end": pd.Timestamp("2026-01-01 10:15:00"),
            },
            {
                "event_type": "login",
                "bug_type": "",
                "severity": "",
                "country": "UG",
                "api_status": 500,
                "account_created": pd.Timestamp("2026-01-01 11:00:00"),
                "action_timestamp": pd.Timestamp("2026-01-01 11:20:00"),
                "session_start": pd.Timestamp("2026-01-01 11:01:00"),
                "session_end": pd.Timestamp("2026-01-01 11:41:00"),
            },
        ]
    )
    rejected_df = clean_df.iloc[:0].copy()
    batch_records = [
        {"batch_id": "batch_001", "status": "accepted"},
        {"batch_id": "batch_002", "status": "rejected"},
    ]

    profile = build_dataset_profile(
        clean_df=clean_df,
        rejected_df=rejected_df,
        batch_records=batch_records,
        validation_policy=DEFAULT_VALIDATION_POLICY,
        source_file_manifests=[{"file_name": "synthetic_logs_batch_001.csv"}],
        generated_at_utc="2026-04-09T00:00:00+00:00",
        schema_version="2026.04",
    )

    assert profile["dataset_counts"]["accepted_rows"] == 2
    assert profile["dataset_counts"]["rejected_batches"] == 1
    assert profile["advanced_findings"]["dominant_event_type"] == "login"
    assert "api_status" in profile["accepted_numeric_profiles"]


def test_add_derived_metrics_adds_duration_columns():
    df = pd.DataFrame(
        [
            {
                "account_created": pd.Timestamp("2026-01-01 10:00:00"),
                "action_timestamp": pd.Timestamp("2026-01-01 10:05:00"),
                "session_start": pd.Timestamp("2026-01-01 10:01:00"),
                "session_end": pd.Timestamp("2026-01-01 10:11:00"),
            }
        ]
    )

    profiled = add_derived_metrics(df)

    assert profiled.loc[0, "account_to_action_minutes"] == 5.0
    assert profiled.loc[0, "session_duration_minutes"] == 10.0
