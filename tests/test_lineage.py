import json
from pathlib import Path

import pandas as pd

from src.lineage import append_lineage_log, update_data_dictionary, write_run_metadata

def test_lineage_written(tmp_path: Path):
    path = tmp_path / "lineage.json"
    append_lineage_log(
        path=path,
        batch_id="batch_001",
        status="accepted",
        rows_total=10,
        rows_preserved=9,
        rows_removed=1,
        issues_found={"duplicates": 1},
        decision_rule="Accepted because threshold not exceeded",
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data[0]["batch_id"] == "batch_001"
    assert data[0]["rows_removed"] == 1


def test_run_metadata_written(tmp_path: Path):
    path = tmp_path / "run_metadata.json"
    payload = {"schema_version": "2026.04", "input_rows": 13}

    write_run_metadata(path, payload)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema_version"] == "2026.04"
    assert data["input_rows"] == 13


def test_data_dictionary_contains_expected_columns(tmp_path: Path):
    path = tmp_path / "dictionary.csv"
    update_data_dictionary(path, pd.DataFrame())

    df = pd.read_csv(path)
    assert "column_name" in df.columns
    assert "batch_id" in df["column_name"].tolist()
