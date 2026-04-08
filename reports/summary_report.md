# Validation Summary Report

## Result
- Accepted rows: 6
- Rejected rows: 7

## Batch decisions
- `batch_001`: accepted, with row-level removals for duplicates and missing IDs
- `batch_002`: fully rejected due to temporal leakage (`action_timestamp < account_created`)

## Outputs
- `data/processed/clean_logs.csv`
- `data/processed/rejected_rows.csv`

- `data/lineage/batch_lineage.json`
- `data/lineage/data_dictionary.csv`
