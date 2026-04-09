from pathlib import Path
import shutil
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clean import batch_status, split_clean_and_rejected
from src.eda import (
    compute_surface_stats,
    plot_event_distribution,
    plot_null_rates,
    plot_timestamp_gap,
    write_summary_report,
)
from src.ingest import load_all_batches
from src.lineage import (
    append_lineage_log,
    reset_lineage_log,
    update_data_dictionary,
    write_run_metadata,
)
from src.profiling import (
    build_column_quality_profile,
    build_dataset_profile,
    write_dataset_profile,
)
from src.schema import (
    DEFAULT_VALIDATION_POLICY,
    EXPECTED_COLUMNS,
    SCHEMA_VERSION,
    policy_as_dict,
)
from src.utils import build_source_file_manifests, utc_now_iso
from src.validate import add_validation_flags, summarize_flags

RAW_DIR = ROOT / "data/raw"
PROCESSED_DIR = ROOT / "data/processed"
LINEAGE_DIR = ROOT / "data/lineage"
QUARANTINE_DIR = ROOT / "data/quarantine"
REPORTS_DIR = ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
LINEAGE_LOG_PATH = LINEAGE_DIR / "batch_lineage.json"
RUN_METADATA_PATH = LINEAGE_DIR / "run_metadata.json"
SCORECARD_PATH = REPORTS_DIR / "batch_quality_scorecard.csv"
DATASET_PROFILE_PATH = REPORTS_DIR / "dataset_profile.json"
COLUMN_PROFILE_PATH = REPORTS_DIR / "column_quality_profile.csv"
BATCH_SCORECARD_COLUMNS = [
    "batch_id",
    "status",
    "rows_total",
    "rows_preserved",
    "rows_removed",
    "duplicates",
    "missing_user_id",
    "timestamp_violations",
    "session_order_violations",
    "timestamp_violation_rate",
    "session_order_violation_rate",
    "duplicate_rate",
    "missing_user_id_rate",
    "decision_rule",
    "quarantine_path",
]


def clear_previous_outputs() -> None:
    files_to_remove = [
        PROCESSED_DIR / "clean_logs.csv",
        PROCESSED_DIR / "clean_logs.parquet",
        PROCESSED_DIR / "rejected_rows.csv",
        PROCESSED_DIR / "rejected_rows.parquet",
        LINEAGE_DIR / "data_dictionary.csv",
        LINEAGE_DIR / "validation_summary.csv",
        RUN_METADATA_PATH,
        REPORTS_DIR / "summary_report.md",
        SCORECARD_PATH,
        DATASET_PROFILE_PATH,
        COLUMN_PROFILE_PATH,
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

    run_started_at = utc_now_iso()
    raw_file_paths = sorted(RAW_DIR.glob("synthetic_logs_batch_*.csv"))
    source_file_manifests = build_source_file_manifests(raw_file_paths, root=ROOT)

    raw_frames = []
    all_clean = []
    all_rejected = []
    accepted_batches = 0
    rejected_batches = 0
    batch_records = []
    issue_totals = {
        "timestamp_violations": 0,
        "session_order_violations": 0,
        "duplicates": 0,
        "missing_user_id": 0,
    }

    update_data_dictionary(LINEAGE_DIR / "data_dictionary.csv", pd.DataFrame())
    reset_lineage_log(LINEAGE_LOG_PATH)

    for batch_id, df in load_all_batches(RAW_DIR):
        raw_frames.append(df.copy())
        validated = add_validation_flags(df)
        summary = summarize_flags(validated)
        status, decision = batch_status(
            validated,
            threshold=DEFAULT_VALIDATION_POLICY.timestamp_violation_rate_threshold,
        )

        for key in issue_totals:
            issue_totals[key] += int(summary[key])

        record = {
            "batch_id": batch_id,
            "status": status,
            "rows_total": len(validated),
            "duplicates": summary["duplicates"],
            "missing_user_id": summary["missing_user_id"],
            "timestamp_violations": summary["timestamp_violations"],
            "session_order_violations": summary["session_order_violations"],
            "timestamp_violation_rate": summary["timestamp_violation_rate"],
            "session_order_violation_rate": summary["session_order_violation_rate"],
            "duplicate_rate": summary["duplicate_rate"],
            "missing_user_id_rate": summary["missing_user_id_rate"],
            "decision_rule": decision,
        }

        if status == "rejected":
            quarantine_path = QUARANTINE_DIR / batch_id
            quarantine_path.mkdir(parents=True, exist_ok=True)
            validated.to_csv(quarantine_path / f"{batch_id}.csv", index=False)
            quarantine_path_str = quarantine_path.relative_to(ROOT).as_posix()

            append_lineage_log(
                path=LINEAGE_LOG_PATH,
                batch_id=batch_id,
                status=status,
                rows_total=len(validated),
                rows_preserved=0,
                rows_removed=len(validated),
                issues_found=summary,
                decision_rule=decision,
                quarantine_path=quarantine_path_str,
            )
            batch_records.append(
                {
                    **record,
                    "rows_preserved": 0,
                    "rows_removed": len(validated),
                    "quarantine_path": quarantine_path_str,
                }
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
        batch_records.append(
            {
                **record,
                "rows_preserved": len(clean),
                "rows_removed": len(rejected),
                "quarantine_path": "",
            }
        )

    raw_df = (
        pd.concat(raw_frames, ignore_index=True)
        if raw_frames
        else pd.DataFrame(columns=EXPECTED_COLUMNS)
    )
    clean_df = pd.concat(all_clean, ignore_index=True) if all_clean else pd.DataFrame(columns=raw_df.columns)
    rejected_df = pd.concat(all_rejected, ignore_index=True) if all_rejected else pd.DataFrame(columns=raw_df.columns)
    total_rows = int(len(clean_df) + len(rejected_df))
    surface_stats = compute_surface_stats(clean_df)

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
                "schema_version": SCHEMA_VERSION,
                "input_rows": total_rows,
                "accepted_rows": int(len(clean_df)),
                "rejected_rows": int(len(rejected_df)),
                "accepted_batches": accepted_batches,
                "rejected_batches": rejected_batches,
                "timestamp_violation_rate_threshold": DEFAULT_VALIDATION_POLICY.timestamp_violation_rate_threshold,
                "modified_zscore_threshold": DEFAULT_VALIDATION_POLICY.modified_zscore_threshold,
            }
        ]
    )
    validation_summary.to_csv(LINEAGE_DIR / "validation_summary.csv", index=False)

    batch_scorecard = pd.DataFrame(batch_records, columns=BATCH_SCORECARD_COLUMNS)
    batch_scorecard.to_csv(SCORECARD_PATH, index=False)

    column_quality_profile = pd.concat(
        [
            build_column_quality_profile(raw_df, dataset_label="raw_input"),
            build_column_quality_profile(clean_df, dataset_label="clean_output"),
            build_column_quality_profile(rejected_df, dataset_label="rejected_output"),
        ],
        ignore_index=True,
    )
    column_quality_profile.to_csv(COLUMN_PROFILE_PATH, index=False)

    dataset_profile = build_dataset_profile(
        clean_df=clean_df,
        rejected_df=rejected_df,
        batch_records=batch_records,
        validation_policy=DEFAULT_VALIDATION_POLICY,
        source_file_manifests=source_file_manifests,
        generated_at_utc=run_started_at,
        schema_version=SCHEMA_VERSION,
    )
    write_dataset_profile(DATASET_PROFILE_PATH, dataset_profile)

    generated_outputs = [
        "data/processed/clean_logs.csv",
        "data/processed/clean_logs.parquet",
        "data/processed/rejected_rows.csv",
        "data/processed/rejected_rows.parquet",
        "data/lineage/batch_lineage.json",
        "data/lineage/data_dictionary.csv",
        "data/lineage/validation_summary.csv",
        "data/lineage/run_metadata.json",
        "reports/batch_quality_scorecard.csv",
        "reports/column_quality_profile.csv",
        "reports/dataset_profile.json",
        "reports/summary_report.md",
        "reports/figures/null_rates.png",
        "reports/figures/timestamp_diff_hist.png",
        "reports/figures/category_distribution.png",
    ]

    write_run_metadata(
        RUN_METADATA_PATH,
        {
            "generated_at_utc": run_started_at,
            "schema_version": SCHEMA_VERSION,
            "validation_policy": policy_as_dict(DEFAULT_VALIDATION_POLICY),
            "input_rows": int(len(raw_df)),
            "accepted_rows": int(len(clean_df)),
            "rejected_rows": int(len(rejected_df)),
            "accepted_batches": accepted_batches,
            "rejected_batches": rejected_batches,
            "source_files": source_file_manifests,
            "generated_outputs": generated_outputs,
        },
    )

    write_summary_report(
        REPORTS_DIR / "summary_report.md",
        accepted_rows=int(len(clean_df)),
        rejected_rows=int(len(rejected_df)),
        batch_log_path="data/lineage/batch_lineage.json",
        batches_accepted=accepted_batches,
        batches_rejected=rejected_batches,
        total_rows=total_rows,
        issue_totals=issue_totals,
        generated_outputs=generated_outputs,
        scorecard_path="reports/batch_quality_scorecard.csv",
        surface_stats=surface_stats,
        dataset_profile_path="reports/dataset_profile.json",
        column_profile_path="reports/column_quality_profile.csv",
        run_metadata_path="data/lineage/run_metadata.json",
        advanced_findings=dataset_profile["advanced_findings"],
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
