# Project Overview

## Goal
This project is designed to showcase the type of work expected in data wrangling, analytics engineering, QA engineering, and junior data platform roles: transforming noisy operational data into trustworthy, analysis-ready outputs with explicit quality checks and clear lineage.

## Problem Framing
Operational logs are rarely ready for downstream use. Teams commonly face:
- inconsistent batch quality
- duplicate or incomplete records
- invalid temporal relationships
- missing audit context when rows or entire batches are rejected

This repository addresses those issues with a small but complete pipeline that is easy to review, run, and test.

## Design Choices
- Use explicit validation flags rather than hiding logic in one large transform step.
- Separate row-level rejection from batch-level quarantine to make decision boundaries visible.
- Produce reviewer-friendly CSV, JSON, and markdown outputs in addition to analytics-friendly Parquet.
- Include SQL checks to show that quality analysis is not limited to Python-only workflows.
- Keep the project intentionally compact so the full flow is understandable in one sitting.

## End-To-End Workflow
1. Load raw batches from `data/raw/`.
2. Enforce expected columns during ingestion.
3. Parse timestamps and apply validation rules.
4. Split accepted and rejected rows.
5. Quarantine fully invalid batches when threshold rules are exceeded.
6. Write processed outputs, lineage artifacts, and summary reporting.
7. Generate charts that help a reviewer understand the resulting dataset quality quickly.

## Validation Rules
- duplicate event detection
- missing `user_id` detection
- `action_timestamp < account_created` leakage detection
- `session_end < session_start` ordering checks
- full-batch rejection when timestamp leakage exceeds policy threshold

## Portfolio Evidence
This repo is meant to give a hiring reviewer evidence that I can:
- turn messy input into reproducible, documented outputs
- think in terms of quality rules and edge cases
- communicate tradeoffs clearly through summaries and lineage artifacts
- support both programmatic consumers and human reviewers
- write tests around the most important validation behaviors

## Outputs Produced
- processed clean and rejected datasets in `data/processed/`
- quarantined batches in `data/quarantine/`
- lineage artifacts in `data/lineage/`
- SQL checks in `sql/`
- a markdown report and charts in `reports/`

## How To Review
For the fastest evaluation:
1. Read the top-level [README.md](README.md).
2. Inspect [examples/run_pipeline.py](examples/run_pipeline.py) for the orchestration flow.
3. Open [reports/summary_report.md](reports/summary_report.md) and [data/lineage/batch_lineage.json](data/lineage/batch_lineage.json).
4. Run `python examples/run_pipeline.py`.
5. Run `python -m pytest -q`.
