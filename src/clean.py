import pandas as pd

from .schema import DEFAULT_VALIDATION_POLICY
from .validate import summarize_flags

def standardize_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()
    df["bug_type"] = df["bug_type"].fillna("").astype(str).str.strip()
    df["severity"] = df["severity"].fillna("").astype(str).str.strip().str.lower()
    df["country"] = df["country"].astype(str).str.upper().str.strip()
    return df

def split_clean_and_rejected(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = standardize_text(df)

    reject_mask = (
        df["flag_duplicate"]
        | df["flag_missing_user_id"]
        | df["flag_time_violation"]
        | df["flag_session_order"]
    )
    rejected = df[reject_mask].copy()
    clean = df[~reject_mask].copy()
    return clean, rejected

def batch_status(
    df: pd.DataFrame,
    threshold: float = DEFAULT_VALIDATION_POLICY.timestamp_violation_rate_threshold,
) -> tuple[str, str]:
    summary = summarize_flags(df)
    if summary["timestamp_violation_rate"] > threshold:
        return "rejected", f"Rejected because timestamp violation rate exceeded {int(threshold * 100)}%"
    return "accepted", f"Accepted because critical timestamp violation rate remained at or below {int(threshold * 100)}%"
