from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


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

## Notes
- Rows can be rejected for duplicates, missing IDs, or impossible time relationships.
- Entire batches can be quarantined when critical timestamp leakage exceeds policy thresholds.
- Batch decision details are stored in `{batch_log_path}`.

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
