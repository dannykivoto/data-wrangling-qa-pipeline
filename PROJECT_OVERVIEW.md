# Project Overview

## Goal
This project is designed to show the kind of work expected from an Associate Data Scientist or Data Engineer focused on **data wrangling and QA engineering**: making raw software data trustworthy before higher-level analysis begins.

## Role Alignment
This repository directly maps to the responsibilities in the target role:
- **Transform raw logs, API records, and bug-report fields** into structured CSV and Parquet outputs.
- **Identify missing values, duplicates, and temporal outliers** with explicit validation flags.
- **Perform initial analysis** with charts plus mean and variance calculations.
- **Document metadata and lineage** through a data dictionary, batch decision log, validation summary, run metadata, and file-level provenance.
- **Explain change and discard decisions** through reviewer-friendly markdown and spreadsheet-ready CSV artifacts.

## Problem Framing
Operational software data is often messy, mixed, and difficult to trust. Common issues include:
- duplicate or partial records
- invalid timestamps
- mixed product, API, and bug-report attributes
- low-confidence batches that should not be passed downstream
- weak documentation around what was changed and why

This project addresses those problems with a compact, transparent pipeline that also leaves behind reproducible evidence about policy, source files, and quality diagnostics.

## Workflow
1. Load raw batches from `data/raw/`.
2. Enforce expected schema during ingestion.
3. Parse timestamps and apply QA validation rules.
4. Split accepted and rejected rows.
5. Quarantine whole batches when leakage thresholds are exceeded.
6. Write structured outputs for downstream analysis.
7. Generate lineage, scorecard, EDA, and statistical profiling artifacts for reviewer communication.

## Engineering Choices
- Validation flags are explicit so the QA logic is easy to audit.
- Batch-level rejection is separate from row-level rejection to make policy decisions visible.
- Outputs include both analytics-friendly formats and spreadsheet-friendly review artifacts.
- Policy settings and schema versioning are surfaced in run metadata rather than buried in code.
- Statistical profiling uses entropy, IQR, MAD, and modified z-score style diagnostics to move beyond a purely descriptive summary.
- The project intentionally includes tests and SQL checks to show breadth beyond a single script.

## Evidence Of Fit
This repo demonstrates:
- practical ETL-style thinking
- QA-style curiosity about edge cases in software datasets
- comfort with explaining why data was removed or retained
- an ability to prepare trustworthy inputs for downstream AI or analytics workflows
- stronger analytical maturity through provenance tracking and robust profiling artifacts

## Review Path
1. Read [README.md](README.md).
2. Inspect [reports/summary_report.md](reports/summary_report.md), [reports/batch_quality_scorecard.csv](reports/batch_quality_scorecard.csv), and [reports/dataset_profile.json](reports/dataset_profile.json).
3. Review [data/lineage/batch_lineage.json](data/lineage/batch_lineage.json) and [data/lineage/run_metadata.json](data/lineage/run_metadata.json).
4. Run `python examples/run_pipeline.py`.
5. Run `python -m pytest -q`.
