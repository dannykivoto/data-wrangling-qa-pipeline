# Data Wrangling & QA Pipeline

A portfolio project designed to match a **Data Wrangling / QA Engineering** role.  
It demonstrates how to turn messy software logs, bug reports, and API records into analysis-ready datasets with clear validation, batch-level lineage, and lightweight EDA.

## Why this project stands out
This repo is built around the exact tasks most data quality roles care about:

- transforming raw logs into clean structured tables
- detecting impossible patterns such as `action_timestamp < account_created`
- documenting what was preserved, removed, or fully rejected
- producing analysis-ready outputs in CSV / Parquet
- showing both **Python** and **SQL** approaches for initial surface analysis

## Stack
- Python
- Pandas
- Polars
- NumPy
- DuckDB / SQL
- PyArrow / Parquet
- Matplotlib
- Pytest

## Example quality rules
- reject duplicate events
- flag missing user IDs
- reject rows with temporal leakage
- reject an entire batch when critical violation rate exceeds 10%

## Repository layout
- `src/` core ingestion, cleaning, validation, EDA, and lineage modules
- `sql/` SQL checks for surface analysis and QA
- `data/raw/` messy sample input batches
- `data/processed/` cleaned outputs
- `data/quarantine/` fully rejected batches
- `data/lineage/` data dictionary and batch decision log
- `reports/` figures and markdown summaries
- `tests/` validation and lineage tests

## Quick start
```bash
pip install -r requirements.txt
python examples/run_pipeline.py
```

## What gets generated
- `clean_logs.parquet`
- `rejected_rows.parquet`
- `validation_summary.csv`
- `batch_lineage.json`
- EDA charts for null rates, timestamp gaps, and event distribution

## Portfolio talking points
This project is designed to show that I can act as the bridge between raw operational data and downstream AI / analytics teams by:
1. cleaning messy input safely
2. explaining rejection decisions clearly
3. preserving auditability and data lineage
4. handling “surface analysis” before deeper modeling begins
