# Submission Notes

This repository was prepared as a targeted portfolio project for a Data Wrangling / QA Engineering role.

## What it demonstrates
- Cleaning messy software-event data
- Enforcing schema and timestamp logic
- Rejecting invalid batches using documented rules
- Preserving data lineage and a machine-readable audit trail
- Performing lightweight EDA before deeper analysis
- Using both Python and SQL for validation workflows

## Recommended files to review first
1. `README.md`
2. `examples/run_pipeline.py`
3. `src/validate.py`
4. `src/lineage.py`
5. `data/lineage/batch_lineage.json`
6. `reports/summary_report.md`

## Best talking points in an interview
- Why temporal validation is the first high-priority check
- How row-level rejection differs from full-batch quarantine
- Why lineage logging matters for analysts and AI systems
- How this design scales to large CSVs using Polars / DuckDB
