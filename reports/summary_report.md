# Validation Summary Report

## Result
- Total input rows: 13
- Accepted rows: 6
- Rejected rows: 7
- Accepted batches: 1
- Rejected batches: 1

## Quality Findings
- Duplicate rows flagged: 1
- Missing user IDs flagged: 1
- Timestamp violations flagged: 5
- Session-order violations flagged: 0

## Initial Analysis (EDA)
- Mean API status: 300.00
- Variance of API status: 20000.00
- Mean minutes from account creation to action: 8.17
- Variance of account-to-action minutes: 98.47
- Mean session duration (minutes): 16.08
- Variance of session duration (minutes): 83.53

## Advanced Diagnostics
- Dominant clean event type: login
- Event-type entropy (bits): 1.92
- Robust outliers in account-to-action minutes: 1
- Robust outliers in session duration minutes: 0

## Notes
- Rows can be rejected for duplicates, missing IDs, or impossible time relationships.
- Entire batches can be quarantined when critical timestamp leakage exceeds policy thresholds.
- Batch decision details are stored in `data/lineage/batch_lineage.json`.
- Spreadsheet-friendly reviewer output is stored in `reports/batch_quality_scorecard.csv`.
- Full dataset profile is stored in `reports/dataset_profile.json`.
- Column-quality profile is stored in `reports/column_quality_profile.csv`.
- Run metadata and provenance are stored in `data/lineage/run_metadata.json`.

## Generated Outputs
- `data/processed/clean_logs.csv`
- `data/processed/clean_logs.parquet`
- `data/processed/rejected_rows.csv`
- `data/processed/rejected_rows.parquet`
- `data/lineage/batch_lineage.json`
- `data/lineage/data_dictionary.csv`
- `data/lineage/validation_summary.csv`
- `data/lineage/run_metadata.json`
- `reports/batch_quality_scorecard.csv`
- `reports/column_quality_profile.csv`
- `reports/dataset_profile.json`
- `reports/summary_report.md`
- `reports/figures/null_rates.png`
- `reports/figures/timestamp_diff_hist.png`
- `reports/figures/category_distribution.png`
