# Submission Notes

## Reviewer Intent
This repository is organized to be easy for both hiring managers and technical reviewers to evaluate quickly. It combines a runnable pipeline, explicit QA logic, generated artifacts, and tests in one place.

## Best 3-Minute Review Path
1. Start with [README.md](README.md) for the project story and outcomes.
2. Open [reports/summary_report.md](reports/summary_report.md) for the latest run summary.
3. Inspect [data/lineage/batch_lineage.json](data/lineage/batch_lineage.json) to see accepted versus quarantined batch decisions.
4. Review [examples/run_pipeline.py](examples/run_pipeline.py) to understand the orchestration flow.
5. Check [tests/](tests/) for the core validation coverage.

## What Was Improved For Portfolio Use
- aligned the repository structure with a clean, submission-ready layout
- fixed the demo run command so `python examples/run_pipeline.py` works directly from the repo root
- made the pipeline rerunnable without duplicating lineage entries or leaving stale outputs behind
- kept browser-friendly sample outputs in CSV while also generating Parquet for local runs
- upgraded the README and overview docs to highlight business value, engineering choices, and reviewer guidance
- expanded test coverage for ingest and missing-ID validation behavior

## Validation
- `python examples/run_pipeline.py`
- `python -m pytest -q`

## Notes
- The sample data is synthetic and intentionally messy so the quality rules are visible.
- Figures under `reports/figures/` are included as showcase artifacts for GitHub review.
- Parquet outputs are generated locally and ignored in git to keep the repository reviewer-friendly.
