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

## Notes
- Rows can be rejected for duplicates, missing IDs, or impossible time relationships.
- Entire batches can be quarantined when critical timestamp leakage exceeds policy thresholds.
- Batch decision details are stored in `data/lineage/batch_lineage.json`.

## Generated Outputs
- `data/processed/clean_logs.csv`
- `data/processed/clean_logs.parquet`
- `data/processed/rejected_rows.csv`
- `data/processed/rejected_rows.parquet`
- `data/lineage/batch_lineage.json`
- `data/lineage/data_dictionary.csv`
- `data/lineage/validation_summary.csv`
- `reports/summary_report.md`
- `reports/figures/null_rates.png`
- `reports/figures/timestamp_diff_hist.png`
- `reports/figures/category_distribution.png`
