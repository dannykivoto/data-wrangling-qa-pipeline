import pandas as pd
from src.validate import add_validation_flags

def test_timestamp_violation_flagged():
    df = pd.DataFrame([{
        "batch_id":"b1",
        "user_id":"u1",
        "account_created":"2026-01-02 10:00:00",
        "action_timestamp":"2026-01-01 10:00:00",
        "event_type":"login",
        "session_id":"s1",
        "session_start":"2026-01-02 10:01:00",
        "session_end":"2026-01-02 10:05:00",
        "bug_type":"",
        "severity":"",
        "api_status":200,
        "country":"KE",
    }])
    out = add_validation_flags(df)
    assert bool(out.loc[0, "flag_time_violation"]) is True


def test_missing_user_id_flagged_for_null_values():
    df = pd.DataFrame([{
        "batch_id":"b1",
        "user_id":None,
        "account_created":"2026-01-02 10:00:00",
        "action_timestamp":"2026-01-02 10:03:00",
        "event_type":"login",
        "session_id":"s1",
        "session_start":"2026-01-02 10:01:00",
        "session_end":"2026-01-02 10:05:00",
        "bug_type":"",
        "severity":"",
        "api_status":200,
        "country":"KE",
    }])
    out = add_validation_flags(df)
    assert bool(out.loc[0, "flag_missing_user_id"]) is True
