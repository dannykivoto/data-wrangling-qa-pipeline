import pandas as pd

DATETIME_COLS = ["account_created", "action_timestamp", "session_start", "session_end"]

def cast_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in DATETIME_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def flag_timestamp_violations(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["flag_time_violation"] = df["action_timestamp"] < df["account_created"]
    return df

def flag_session_order_violations(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["flag_session_order"] = df["session_end"] < df["session_start"]
    return df

def flag_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["flag_duplicate"] = df.duplicated(
        subset=["user_id", "action_timestamp", "event_type", "session_id"],
        keep="first"
    )
    return df

def flag_missing_user_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["flag_missing_user_id"] = df["user_id"].astype(str).str.strip().eq("")
    return df

def add_validation_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = cast_datetimes(df)
    df = flag_timestamp_violations(df)
    df = flag_session_order_violations(df)
    df = flag_duplicates(df)
    df = flag_missing_user_id(df)
    return df

def summarize_flags(df: pd.DataFrame) -> dict:
    total = max(len(df), 1)
    return {
        "rows_total": int(len(df)),
        "timestamp_violations": int(df["flag_time_violation"].sum()),
        "session_order_violations": int(df["flag_session_order"].sum()),
        "duplicates": int(df["flag_duplicate"].sum()),
        "missing_user_id": int(df["flag_missing_user_id"].sum()),
        "timestamp_violation_rate": float(df["flag_time_violation"].sum() / total),
    }
