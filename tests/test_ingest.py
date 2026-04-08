from pathlib import Path

import pytest

from src.ingest import load_batch


def test_load_batch_requires_expected_columns(tmp_path: Path):
    csv_path = tmp_path / "bad_batch.csv"
    csv_path.write_text("batch_id,user_id\nb1,u1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required columns"):
        load_batch(csv_path)
