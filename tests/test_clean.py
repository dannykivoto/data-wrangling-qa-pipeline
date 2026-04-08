import pandas as pd
from src.validate import add_validation_flags
from src.clean import split_clean_and_rejected

def test_duplicate_rejected():
    rows = [{
        "batch_id":"b1","user_id":"u1","account_created":"2026-01-01 10:00:00","action_timestamp":"2026-01-01 10:05:00",
        "event_type":"login","session_id":"s1","session_start":"2026-01-01 10:01:00","session_end":"2026-01-01 10:10:00",
        "bug_type":"","severity":"","api_status":200,"country":"KE"
    }] * 2
    df = add_validation_flags(pd.DataFrame(rows))
    clean, rejected = split_clean_and_rejected(df)
    assert len(clean) == 1
    assert len(rejected) == 1
