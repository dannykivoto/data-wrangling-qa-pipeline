# Submission Notes

## Included Structure
This submission now includes the expected portfolio-ready repository shape at the project root:
- `README.md`
- `PROJECT_OVERVIEW.md`
- `SUBMISSION_NOTES.md`
- `requirements.txt`
- `.gitignore`
- `LICENSE`
- `data/`
- `src/`
- `sql/`
- `examples/`
- `tests/`
- `reports/`

## What Was Corrected
- flattened the project so the actual repo contents live at the workspace root instead of inside a nested folder
- confirmed `null_checks.sql` is inside `sql/`
- confirmed `src/` includes the full pipeline modules: `ingest.py`, `validate.py`, `clean.py`, `lineage.py`, `eda.py`, `schema.py`, and `__init__.py`
- retained example, report, and test directories at the root
- removed generated `__pycache__/` artifacts from `src/`

## Suggested Reviewer Flow
1. Read `README.md` for the quick project summary.
2. Review `PROJECT_OVERVIEW.md` for the pipeline design and QA framing.
3. Run `python examples/run_pipeline.py` to generate outputs.
4. Run `pytest` to validate the core cleaning, lineage, and validation logic.

## Notes
The repository also includes a `notebooks/` directory for exploratory work. It is not required for the main pipeline run, but it can help illustrate the analysis process behind the final outputs.
