# Application Summary

## Brief Role-Tailored Summary
I build data-cleaning and ETL-style workflows that turn messy operational data into trustworthy, analysis-ready datasets. In this portfolio project, I transform raw software logs, API-style records, and bug-report fields into structured CSV and Parquet outputs, apply explicit QA rules for duplicates, missing identifiers, and temporal anomalies, document lineage, and generate lightweight EDA plus reproducibility artifacts before downstream analysis begins.

## Why This Repo Fits The Role
- It demonstrates hands-on **data wrangling** on messy software data.
- It shows **QA intuition** through rule-based checks, rejection logic, and quarantine handling.
- It includes **metadata management** via lineage logs, validation summaries, and a data dictionary.
- It provides **initial analysis** through charts, mean and variance reporting, robust profiling, and SQL surface checks.
- It supports **communication** with markdown summaries and spreadsheet-ready CSV scorecards.
- It demonstrates stronger rigor through schema versioning, policy metadata, and source-file fingerprinting.

## Paste-Ready Experience Summary
I have experience building small ETL and QA workflows that take inconsistent raw data and turn it into structured, reviewer-friendly outputs. My approach focuses on schema enforcement, missing-value handling, duplicate detection, temporal validation, batch-level rejection rules, clear lineage tracking, and reproducible profiling so downstream analysts can trust the final dataset. I also document why data was removed or quarantined and generate summary artifacts that are easy to review in markdown, CSV, Excel, or Google Sheets.

## Best Supporting Files In This Repo
- [README.md](README.md)
- [reports/summary_report.md](reports/summary_report.md)
- [reports/batch_quality_scorecard.csv](reports/batch_quality_scorecard.csv)
- [reports/dataset_profile.json](reports/dataset_profile.json)
- [data/lineage/batch_lineage.json](data/lineage/batch_lineage.json)
- [data/lineage/run_metadata.json](data/lineage/run_metadata.json)
- [examples/run_pipeline.py](examples/run_pipeline.py)
