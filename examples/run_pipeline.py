from pathlib import Path
import shutil
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clean import batch_status, split_clean_and_rejected
from src.eda import (
    plot_event_distribution,
    plot_null_rates,
    plot_timestamp_gap,
    write_summary_report,
)
from src.ingest import load_all_batches
from src.lineage import append_lineage_log, reset_lineage_log, update_data_dictionary
from src.validate import add_validation_flags, summarize_flags

RAW_DIR = ROOT / "data/raw"
PROCESSED_DIR = ROOT / "data/processed"
LINEAGE_DIR = ROOT / "data/lineage"
QUARANTINE_DIR = ROOT / "data/quarantine"
REPORTS_DIR = ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
LINEAGE_LOG_PATH = LINEAGE_DIR / "batch_lineage.json"


def clear_previous_outputs() -> None:
    files_to_remove = [
        PROCESSED_DIR / "clean_logs.csv",
        PROCESSED_DIR / "clean_logs.parquet",
        PROCESSED_DIR / "rejected_rows.csv",
        PROCESSED_DIR / "rejected_rows.parquet",
        LINEAGE_DIR / "data_dictionary.csv",
        LINEAGE_DIR / "validation_summary.csv",
        REPORTS_DIR / "summary_report.md",
        FIGURES_DIR / "null_rates.png",
        FIGURES_DIR / "timestamp_diff_hist.png",
        FIGURES_DIR / "category_distribution.png",
    ]

    for path in files_to_remove:
        if path.exists():
            path.unlink()

    if QUARANTINE_DIR.exists():
        for batch_dir in QUARANTINE_DIR.iterdir():
            if batch_dir.is_dir():
                shutil.rmtree(batch_dir)


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    LINEAGE_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    clear_previous_outputs()

    all_clean = []
    all_rejected = []
    accepted_batches = 0
    rejected_batches = 0
    issue_totals = {
        "timestamp_violations": 0,
        "session_order_violations": 0,
        "duplicates": 0,
        "missing_user_id": 0,
    }

    update_data_dictionary(LINEAGE_DIR / "data_dictionary.csv", pd.DataFrame())
    reset_lineage_log(LINEAGE_LOG_PATH)

    for batch_id, df in load_all_batches(RAW_DIR):
        validated = add_validation_flags(df)
        summary = summarize_flags(validated)
        status, decision = batch_status(validated)

        for key in issue_totals:
            issue_totals[key] += int(summary[key])

        if status == "rejected":
            quarantine_path = QUARANTINE_DIR / batch_id
            quarantine_path.mkdir(parents=True, exist_ok=True)
            validated.to_csv(quarantine_path / f"{batch_id}.csv", index=False)

            append_lineage_log(
                path=LINEAGE_LOG_PATH,
                batch_id=batch_id,
                status=status,
                rows_total=len(validated),
                rows_preserved=0,
                rows_removed=len(validated),
                issues_found=summary,
                decision_rule=decision,
                quarantine_path=quarantine_path.relative_to(ROOT).as_posix(),
            )
            all_rejected.append(validated)
            rejected_batches += 1
            continue

        clean, rejected = split_clean_and_rejected(validated)
        all_clean.append(clean)
        all_rejected.append(rejected)
        accepted_batches += 1

        append_lineage_log(
            path=LINEAGE_LOG_PATH,
            batch_id=batch_id,
            status=status,
            rows_total=len(validated),
            rows_preserved=len(clean),
            rows_removed=len(rejected),
            issues_found=summary,
            decision_rule=decision,
        )

    clean_df = pd.concat(all_clean, ignore_index=True) if all_clean else pd.DataFrame()
    rejected_df = pd.concat(all_rejected, ignore_index=True) if all_rejected else pd.DataFrame()
    total_rows = int(len(clean_df) + len(rejected_df))

    if not clean_df.empty:
        clean_df.to_csv(PROCESSED_DIR / "clean_logs.csv", index=False)
        clean_df.to_parquet(PROCESSED_DIR / "clean_logs.parquet", index=False)
        plot_null_rates(clean_df, FIGURES_DIR / "null_rates.png")
        plot_timestamp_gap(clean_df, FIGURES_DIR / "timestamp_diff_hist.png")
        plot_event_distribution(clean_df, FIGURES_DIR / "category_distribution.png")

    if not rejected_df.empty:
        rejected_df.to_csv(PROCESSED_DIR / "rejected_rows.csv", index=False)
        rejected_df.to_parquet(PROCESSED_DIR / "rejected_rows.parquet", index=False)

    validation_summary = pd.DataFrame(
        [
            {
                "input_rows": total_rows,
                "accepted_rows": int(len(clean_df)),
                "rejected_rows": int(len(rejected_df)),
                "accepted_batches": accepted_batches,
                "rejected_batches": rejected_batches,
            }
        ]
    )
    validation_summary.to_csv(LINEAGE_DIR / "validation_summary.csv", index=False)

    write_summary_report(
        REPORTS_DIR / "summary_report.md",
        accepted_rows=int(len(clean_df)),
        rejected_rows=int(len(rejected_df)),
        batch_log_path="data/lineage/batch_lineage.json",
        batches_accepted=accepted_batches,
        batches_rejected=rejected_batches,
        total_rows=total_rows,
        issue_totals=issue_totals,
        generated_outputs=[
            "data/processed/clean_logs.csv",
            "data/processed/clean_logs.parquet",
            "data/processed/rejected_rows.csv",
            "data/processed/rejected_rows.parquet",
            "data/lineage/batch_lineage.json",
            "data/lineage/data_dictionary.csv",
            "data/lineage/validation_summary.csv",
            "reports/summary_report.md",
            "reports/figures/null_rates.png",
            "reports/figures/timestamp_diff_hist.png",
            "reports/figures/category_distribution.png",
        ],
    )

    print(
        "Pipeline complete: "
        f"{len(clean_df)} accepted rows, "
        f"{len(rejected_df)} rejected rows, "
        f"{accepted_batches} accepted batch(es), "
        f"{rejected_batches} rejected batch(es)."
    )


if __name__ == "__main__":
    main()
