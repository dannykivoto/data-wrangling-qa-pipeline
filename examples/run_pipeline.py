from pathlib import Path
import pandas as pd
from src.ingest import load_all_batches
from src.validate import add_validation_flags, summarize_flags
from src.clean import split_clean_and_rejected, batch_status
from src.lineage import update_data_dictionary, append_lineage_log
from src.eda import write_summary_report, plot_null_rates, plot_timestamp_gap, plot_event_distribution

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data/raw"
PROCESSED_DIR = ROOT / "data/processed"
LINEAGE_DIR = ROOT / "data/lineage"
QUARANTINE_DIR = ROOT / "data/quarantine"
REPORTS_DIR = ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    LINEAGE_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    all_clean = []
    all_rejected = []

    update_data_dictionary(LINEAGE_DIR / "data_dictionary.csv", pd.DataFrame())

    for batch_id, df in load_all_batches(RAW_DIR):
        validated = add_validation_flags(df)
        summary = summarize_flags(validated)
        status, decision = batch_status(validated)

        if status == "rejected":
            quarantine_path = QUARANTINE_DIR / batch_id
            quarantine_path.mkdir(parents=True, exist_ok=True)
            validated.to_csv(quarantine_path / f"{batch_id}.csv", index=False)

            append_lineage_log(
                path=LINEAGE_DIR / "batch_lineage.json",
                batch_id=batch_id,
                status=status,
                rows_total=len(validated),
                rows_preserved=0,
                rows_removed=len(validated),
                issues_found=summary,
                decision_rule=decision,
                quarantine_path=str(quarantine_path.relative_to(ROOT))
            )
            all_rejected.append(validated)
            continue

        clean, rejected = split_clean_and_rejected(validated)
        all_clean.append(clean)
        all_rejected.append(rejected)

        append_lineage_log(
            path=LINEAGE_DIR / "batch_lineage.json",
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

    if not clean_df.empty:
        clean_df.to_parquet(PROCESSED_DIR / "clean_logs.parquet", index=False)
        plot_null_rates(clean_df, FIGURES_DIR / "null_rates.png")
        plot_timestamp_gap(clean_df, FIGURES_DIR / "timestamp_diff_hist.png")
        plot_event_distribution(clean_df, FIGURES_DIR / "category_distribution.png")

    if not rejected_df.empty:
        rejected_df.to_parquet(PROCESSED_DIR / "rejected_rows.parquet", index=False)

    validation_summary = pd.DataFrame([{
        "accepted_rows": int(len(clean_df)),
        "rejected_rows": int(len(rejected_df)),
    }])
    validation_summary.to_csv(LINEAGE_DIR / "validation_summary.csv", index=False)

    write_summary_report(
        REPORTS_DIR / "summary_report.md",
        accepted_rows=int(len(clean_df)),
        rejected_rows=int(len(rejected_df)),
        batch_log_path="data/lineage/batch_lineage.json"
    )

if __name__ == "__main__":
    main()
