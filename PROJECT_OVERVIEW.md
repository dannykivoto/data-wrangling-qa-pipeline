# Project Overview

## Summary
This project demonstrates a compact data wrangling and QA pipeline for messy operational event logs. It ingests multiple raw batches, applies validation rules, separates clean and rejected records, records batch-level lineage, and generates lightweight exploratory analysis outputs for downstream review.

## Problem Framing
Teams that rely on logs, bug reports, and API event streams often need a repeatable way to:
- standardize inconsistent raw data
- detect invalid or impossible records early
- quarantine bad batches when quality thresholds are exceeded
- preserve an audit trail of what was accepted, rejected, and why

This repository is structured to show that workflow end to end.

## Pipeline Components
- `src/ingest.py`: loads raw batches from `data/raw/`
- `src/validate.py`: applies quality rules and produces flag summaries
- `src/clean.py`: splits accepted and rejected rows and decides batch status
- `src/lineage.py`: updates metadata outputs and lineage logs
- `src/eda.py`: generates figures and a markdown summary report
- `sql/`: provides SQL-based checks for nulls, surface analysis, and temporal validation

## Inputs And Outputs
Inputs:
- raw CSV batches under `data/raw/`

Outputs:
- cleaned parquet output in `data/processed/`
- rejected or quarantined data in `data/processed/` and `data/quarantine/`
- lineage artifacts in `data/lineage/`
- summary report and figures in `reports/`

## Representative QA Rules
- missing identifiers are flagged
- duplicate events are rejected
- temporal leakage such as `action_timestamp < account_created` is rejected
- a batch can be fully quarantined when critical issue rates exceed threshold

## How To Run
```bash
pip install -r requirements.txt
python examples/run_pipeline.py
pytest
```
