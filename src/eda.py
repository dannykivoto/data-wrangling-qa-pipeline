from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _safe_mean_variance(series: pd.Series) -> tuple[float, float]:
    values = series.dropna().astype(float)
    if values.empty:
        return 0.0, 0.0
    array = values.to_numpy(dtype=float)
    return float(np.mean(array)), float(np.var(array))


def compute_surface_stats(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {
            "api_status_mean": 0.0,
            "api_status_variance": 0.0,
            "account_to_action_minutes_mean": 0.0,
            "account_to_action_minutes_variance": 0.0,
            "session_duration_minutes_mean": 0.0,
            "session_duration_minutes_variance": 0.0,
        }

    account_to_action_minutes = (
        df["action_timestamp"] - df["account_created"]
    ).dt.total_seconds() / 60
    session_duration_minutes = (
        df["session_end"] - df["session_start"]
    ).dt.total_seconds() / 60

    api_status_mean, api_status_variance = _safe_mean_variance(df["api_status"])
    account_gap_mean, account_gap_variance = _safe_mean_variance(account_to_action_minutes)
    session_mean, session_variance = _safe_mean_variance(session_duration_minutes)

    return {
        "api_status_mean": api_status_mean,
        "api_status_variance": api_status_variance,
        "account_to_action_minutes_mean": account_gap_mean,
        "account_to_action_minutes_variance": account_gap_variance,
        "session_duration_minutes_mean": session_mean,
        "session_duration_minutes_variance": session_variance,
    }


def write_summary_report(
    path: str | Path,
    accepted_rows: int,
    rejected_rows: int,
    batch_log_path: str,
    batches_accepted: int,
    batches_rejected: int,
    total_rows: int,
    issue_totals: dict[str, int],
    generated_outputs: list[str],
    scorecard_path: str,
    surface_stats: dict[str, float],
) -> None:
    content = f"""# Validation Summary Report

## Result
- Total input rows: {total_rows}
- Accepted rows: {accepted_rows}
- Rejected rows: {rejected_rows}
- Accepted batches: {batches_accepted}
- Rejected batches: {batches_rejected}

## Quality Findings
- Duplicate rows flagged: {issue_totals["duplicates"]}
- Missing user IDs flagged: {issue_totals["missing_user_id"]}
- Timestamp violations flagged: {issue_totals["timestamp_violations"]}
- Session-order violations flagged: {issue_totals["session_order_violations"]}

## Initial Analysis (EDA)
- Mean API status: {surface_stats["api_status_mean"]:.2f}
- Variance of API status: {surface_stats["api_status_variance"]:.2f}
- Mean minutes from account creation to action: {surface_stats["account_to_action_minutes_mean"]:.2f}
- Variance of account-to-action minutes: {surface_stats["account_to_action_minutes_variance"]:.2f}
- Mean session duration (minutes): {surface_stats["session_duration_minutes_mean"]:.2f}
- Variance of session duration (minutes): {surface_stats["session_duration_minutes_variance"]:.2f}

## Notes
- Rows can be rejected for duplicates, missing IDs, or impossible time relationships.
- Entire batches can be quarantined when critical timestamp leakage exceeds policy thresholds.
- Batch decision details are stored in `{batch_log_path}`.
- Spreadsheet-friendly reviewer output is stored in `{scorecard_path}`.

## Generated Outputs
""" + "\n".join(f"- `{output}`" for output in generated_outputs) + "\n"
    Path(path).write_text(content, encoding="utf-8")

def plot_null_rates(df: pd.DataFrame, path: str | Path) -> None:
    null_rates = df.isna().mean().sort_values(ascending=False)
    plt.figure(figsize=(8, 4.5))
    null_rates.plot(kind="bar")
    plt.title("Null rate by column")
    plt.ylabel("Fraction null")
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()

def plot_timestamp_gap(df: pd.DataFrame, path: str | Path) -> None:
    gap_minutes = (df["action_timestamp"] - df["account_created"]).dt.total_seconds() / 60
    plt.figure(figsize=(8, 4.5))
    gap_minutes.plot(kind="hist", bins=15)
    plt.title("Action timestamp minus account creation (minutes)")
    plt.xlabel("Minutes")
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()

def plot_event_distribution(df: pd.DataFrame, path: str | Path) -> None:
    counts = df["event_type"].value_counts()
    plt.figure(figsize=(8, 4.5))
    counts.plot(kind="bar")
    plt.title("Event type distribution")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()
