# Data Wrangling & QA Pipeline

A submission-ready portfolio project tailored to an **Associate Data Scientist (Data Wrangling & QA Engineering)** role.

This repository shows how I approach the gap between raw software data and trustworthy downstream analysis: ingesting messy logs, validating logical constraints, documenting lineage, rejecting bad batches, and producing clean analysis-ready outputs.

## Role alignment
This project is built to mirror the workflow described in the job posting:

- **Data wrangling:** transform raw software logs into structured outputs
- **QA intuition:** detect impossible patterns, duplicates, missing IDs, and temporal leakage
- **Initial EDA:** run surface-level checks and lightweight visualizations before deeper analysis
- **Metadata management:** preserve a clear data dictionary and batch-level lineage log
- **Communication:** make every reject / keep decision auditable and easy to explain

## Project scenario
The example data simulates weekly batches of software usage logs, bug reports, and API records.

The pipeline must:
- accept valid batches
- remove low-quality rows from partially valid batches
- fully quarantine batches with critical violations
- document exactly what was preserved vs. removed

## Core checks implemented
- duplicate event detection
- missing user ID detection
- temporal validation: `action_timestamp >= account_created`
- session order validation: `session_end >= session_start`
- batch rejection policy when critical timestamp leakage exceeds 10%

## Tech stack
- Python
- Pandas
- Polars
- NumPy
- DuckDB / SQL
- PyArrow / Parquet
- Matplotlib
- Pytest

## Repository structure
```text
src/                 core ingestion, validation, cleaning, EDA, lineage
sql/                 SQL checks for quick surface analysis
data/raw/            messy sample input batches
data/processed/      cleaned outputs
data/lineage/        data dictionary + batch decision log
reports/             summary report and figures
tests/               validation and lineage tests
examples/            runnable pipeline entry point
```

## Quick start
```bash
pip install -r requirements.txt
python examples/run_pipeline.py
```

## Key outputs
- `data/processed/clean_logs.csv`
- `data/processed/rejected_rows.csv`
- `data/lineage/data_dictionary.csv`
- `data/lineage/batch_lineage.json`
- `data/lineage/validation_summary.csv`
- `reports/summary_report.md`

## Example batch decision logic
- `batch_001` is **accepted** with row-level removals for duplicates and missing IDs
- `batch_002` is **rejected** because the timestamp violation rate exceeds policy threshold

## Why this is relevant to AI / analytics teams
A model or analyst is only as reliable as the dataset it receives. This project focuses on the work that happens *before* modeling:

1. making raw operational data usable
2. enforcing logic and temporal consistency
3. documenting what was removed and why
4. preserving reproducibility and auditability

## Submission notes
This repo is intentionally small, clear, and recruiter-friendly. It is designed to show immediate fit for roles involving:
- data cleaning
- ETL / ELT preparation
- QA engineering for datasets
- surface-level EDA
- metadata and lineage tracking

## Author
**Danny Kivoto**  
GitHub: `github.com/dannykivoto`  
LinkedIn: `linkedin.com/in/danny-kivoto-315b5a13b`
