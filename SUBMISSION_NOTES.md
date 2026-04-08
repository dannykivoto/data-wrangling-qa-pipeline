# Submission Notes

## Reviewer Intent
This repository is organized to support a fast hiring review for a data wrangling and QA-focused role. It combines runnable code, validation logic, metadata artifacts, visual summaries, and tests in one place.

## Strongest Signals For This Application
- the project works on raw software-style data rather than toy tabular examples
- QA rules are explicit and tied to realistic failure modes
- generated outputs are easy to inspect in code, markdown, JSON, CSV, Excel, or Google Sheets
- the repository demonstrates both technical execution and communication discipline

## Best 3-Minute Review Path
1. Read [APPLICATION_SUMMARY.md](APPLICATION_SUMMARY.md).
2. Open [reports/summary_report.md](reports/summary_report.md).
3. Review [reports/batch_quality_scorecard.csv](reports/batch_quality_scorecard.csv).
4. Inspect [data/lineage/batch_lineage.json](data/lineage/batch_lineage.json).
5. Check [tests/](tests/) and [examples/run_pipeline.py](examples/run_pipeline.py).

## Validation
- `python examples/run_pipeline.py`
- `python -m pytest -q`

## Notes
- The sample data is synthetic and intentionally messy so the QA logic is visible.
- Figures under `reports/figures/` are included as GitHub-friendly showcase artifacts.
- CSV outputs are designed to be easy to open in Excel or Google Sheets for wider stakeholder review.
