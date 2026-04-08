import json
from pathlib import Path
from src.lineage import append_lineage_log

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
