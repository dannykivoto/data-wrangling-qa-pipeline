from pathlib import Path
import pandas as pd
from .schema import EXPECTED_COLUMNS

def load_batch(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df[EXPECTED_COLUMNS].copy()

def load_all_batches(raw_dir: str | Path) -> list[tuple[str, pd.DataFrame]]:
    raw_dir = Path(raw_dir)
    batches = []
    for path in sorted(raw_dir.glob("synthetic_logs_batch_*.csv")):
        batches.append((path.stem.replace("synthetic_logs_", ""), load_batch(path)))
    return batches
