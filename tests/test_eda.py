import pandas as pd

from src.eda import compute_surface_stats


def test_compute_surface_stats_returns_expected_measures():
    df = pd.DataFrame(
        [
            {
                "account_created": pd.Timestamp("2026-01-01 10:00:00"),
                "action_timestamp": pd.Timestamp("2026-01-01 10:05:00"),
                "session_start": pd.Timestamp("2026-01-01 10:01:00"),
                "session_end": pd.Timestamp("2026-01-01 10:11:00"),
                "api_status": 200,
            },
            {
                "account_created": pd.Timestamp("2026-01-01 11:00:00"),
                "action_timestamp": pd.Timestamp("2026-01-01 11:10:00"),
                "session_start": pd.Timestamp("2026-01-01 11:02:00"),
                "session_end": pd.Timestamp("2026-01-01 11:22:00"),
                "api_status": 500,
            },
        ]
    )

    stats = compute_surface_stats(df)

    assert stats["api_status_mean"] == 350.0
    assert stats["account_to_action_minutes_mean"] == 7.5
    assert stats["session_duration_minutes_mean"] == 15.0
    assert stats["api_status_variance"] > 0.0
