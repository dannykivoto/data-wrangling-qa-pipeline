from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .schema import COLUMN_DICTIONARY


def update_data_dictionary(path: str | Path, df: pd.DataFrame) -> None:
    out = pd.DataFrame(
        COLUMN_DICTIONARY,
        columns=["column_name", "data_type", "description", "source", "transformation"],
    )
    out.to_csv(path, index=False)


def append_lineage_log(
    path: str | Path,
    batch_id: str,
    status: str,
    rows_total: int,
    rows_preserved: int,
    rows_removed: int,
    issues_found: dict,
    decision_rule: str,
    quarantine_path: str | None = None,
) -> None:
    path = Path(path)
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        data = []

    payload = {
        "batch_id": batch_id,
        "status": status,
        "rows_total": rows_total,
        "rows_preserved": rows_preserved,
        "rows_removed": rows_removed,
        "issues_found": issues_found,
        "decision_rule": decision_rule,
    }
    if quarantine_path:
        payload["quarantine_path"] = quarantine_path

    data.append(payload)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def reset_lineage_log(path: str | Path) -> None:
    Path(path).write_text("[]\n", encoding="utf-8")


def write_run_metadata(path: str | Path, payload: dict[str, object]) -> None:
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
