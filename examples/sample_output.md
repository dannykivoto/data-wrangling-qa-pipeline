# Expected Output

After running:

```bash
python examples/run_pipeline.py
```

You should see a summary like:

```text
Pipeline complete: 6 accepted rows, 7 rejected rows, 1 accepted batch(es), 1 rejected batch(es).
```

And the repository should contain:
- `data/processed/clean_logs.csv`
- `data/processed/rejected_rows.csv`
- local Parquet versions of both processed datasets
- `data/quarantine/batch_002/batch_002.csv`
- `data/lineage/batch_lineage.json`
- `data/lineage/data_dictionary.csv`
- `data/lineage/validation_summary.csv`
- `reports/summary_report.md`
- `reports/figures/null_rates.png`
- `reports/figures/timestamp_diff_hist.png`
- `reports/figures/category_distribution.png`

The sample data is designed so that:
- `batch_001` is accepted with row-level removals
- `batch_002` is fully quarantined because of timestamp leakage
