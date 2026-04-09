from __future__ import annotations

import json
from collections import Counter
from math import log2
from pathlib import Path

import numpy as np
import pandas as pd

from .schema import PROFILE_CATEGORICAL_COLUMNS, PROFILE_NUMERIC_COLUMNS, ValidationPolicy


def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        df["account_to_action_minutes"] = pd.Series(dtype="float64")
        df["session_duration_minutes"] = pd.Series(dtype="float64")
        return df

    df["account_to_action_minutes"] = (
        df["action_timestamp"] - df["account_created"]
    ).dt.total_seconds() / 60
    df["session_duration_minutes"] = (
        df["session_end"] - df["session_start"]
    ).dt.total_seconds() / 60
    return df


def _clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").dropna().astype(float)


def _median_absolute_deviation(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    median = float(series.median())
    return float(np.median(np.abs(series.to_numpy(dtype=float) - median)))


def _modified_zscore_outlier_count(series: pd.Series, threshold: float) -> int:
    if series.empty:
        return 0
    median = float(series.median())
    mad = _median_absolute_deviation(series)
    if mad == 0.0:
        return 0
    modified_zscores = 0.6745 * (series.to_numpy(dtype=float) - median) / mad
    return int(np.sum(np.abs(modified_zscores) > threshold))


def _shannon_entropy(series: pd.Series) -> float:
    values = series.astype("string").dropna()
    values = values[values.str.strip() != ""]
    if values.empty:
        return 0.0
    counts = Counter(values.tolist())
    total = sum(counts.values())
    probabilities = [count / total for count in counts.values()]
    return float(-sum(probability * log2(probability) for probability in probabilities))


def compute_numeric_profile(series: pd.Series, modified_zscore_threshold: float) -> dict[str, float]:
    values = _clean_numeric(series)
    if values.empty:
        return {
            "count_non_null": 0,
            "mean": 0.0,
            "variance": 0.0,
            "std_dev": 0.0,
            "median": 0.0,
            "iqr": 0.0,
            "mad": 0.0,
            "min": 0.0,
            "max": 0.0,
            "modified_zscore_outliers": 0,
        }

    q1 = float(values.quantile(0.25))
    q3 = float(values.quantile(0.75))
    return {
        "count_non_null": int(values.shape[0]),
        "mean": float(values.mean()),
        "variance": float(values.var(ddof=0)),
        "std_dev": float(values.std(ddof=0)),
        "median": float(values.median()),
        "iqr": float(q3 - q1),
        "mad": _median_absolute_deviation(values),
        "min": float(values.min()),
        "max": float(values.max()),
        "modified_zscore_outliers": _modified_zscore_outlier_count(values, modified_zscore_threshold),
    }


def compute_categorical_profile(series: pd.Series) -> dict[str, float | int | str]:
    values = series.astype("string").dropna()
    values = values[values.str.strip() != ""]
    if values.empty:
        return {
            "count_non_null": 0,
            "unique_count": 0,
            "mode": "",
            "mode_share": 0.0,
            "entropy_bits": 0.0,
        }

    counts = values.value_counts()
    mode = str(counts.index[0])
    mode_share = float(counts.iloc[0] / counts.sum())
    return {
        "count_non_null": int(values.shape[0]),
        "unique_count": int(values.nunique(dropna=True)),
        "mode": mode,
        "mode_share": mode_share,
        "entropy_bits": _shannon_entropy(values),
    }


def build_column_quality_profile(df: pd.DataFrame, dataset_label: str) -> pd.DataFrame:
    records = []
    row_count = max(len(df), 1)

    for column in df.columns:
        series = df[column]
        as_string = series.astype("string")
        blank_count = int(as_string.fillna("").str.strip().eq("").sum())
        null_count = int(series.isna().sum())
        unique_count = int(series.nunique(dropna=True))
        non_null_count = int(series.notna().sum())
        dominant_value = ""
        dominant_share = 0.0

        if non_null_count > 0:
            counts = as_string.dropna()
            counts = counts[counts.str.strip() != ""].value_counts()
            if not counts.empty:
                dominant_value = str(counts.index[0])
                dominant_share = float(counts.iloc[0] / counts.sum())

        records.append(
            {
                "dataset_label": dataset_label,
                "column_name": column,
                "dtype": str(series.dtype),
                "rows_total": int(len(df)),
                "non_null_count": non_null_count,
                "null_count": null_count,
                "blank_like_count": blank_count,
                "null_rate": float(null_count / row_count),
                "unique_count": unique_count,
                "dominant_value": dominant_value,
                "dominant_value_share": dominant_share,
            }
        )

    return pd.DataFrame(records)


def build_dataset_profile(
    clean_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    batch_records: list[dict[str, object]],
    validation_policy: ValidationPolicy,
    source_file_manifests: list[dict[str, object]],
    generated_at_utc: str,
    schema_version: str,
) -> dict[str, object]:
    clean_profile_df = add_derived_metrics(clean_df)
    rejected_profile_df = add_derived_metrics(rejected_df)

    numeric_profiles = {
        column: compute_numeric_profile(
            clean_profile_df[column],
            modified_zscore_threshold=validation_policy.modified_zscore_threshold,
        )
        for column in PROFILE_NUMERIC_COLUMNS
        if column in clean_profile_df.columns
    }
    categorical_profiles = {
        column: compute_categorical_profile(clean_profile_df[column])
        for column in PROFILE_CATEGORICAL_COLUMNS
        if column in clean_profile_df.columns
    }

    dominant_event_type = categorical_profiles.get("event_type", {}).get("mode", "")
    event_entropy = float(categorical_profiles.get("event_type", {}).get("entropy_bits", 0.0))

    return {
        "generated_at_utc": generated_at_utc,
        "schema_version": schema_version,
        "validation_policy": {
            "timestamp_violation_rate_threshold": validation_policy.timestamp_violation_rate_threshold,
            "modified_zscore_threshold": validation_policy.modified_zscore_threshold,
        },
        "dataset_counts": {
            "accepted_rows": int(len(clean_df)),
            "rejected_rows": int(len(rejected_df)),
            "accepted_batches": int(sum(record["status"] == "accepted" for record in batch_records)),
            "rejected_batches": int(sum(record["status"] == "rejected" for record in batch_records)),
        },
        "advanced_findings": {
            "dominant_event_type": dominant_event_type,
            "event_type_entropy_bits": event_entropy,
            "account_to_action_modified_zscore_outliers": int(
                numeric_profiles.get("account_to_action_minutes", {}).get("modified_zscore_outliers", 0)
            ),
            "session_duration_modified_zscore_outliers": int(
                numeric_profiles.get("session_duration_minutes", {}).get("modified_zscore_outliers", 0)
            ),
        },
        "accepted_numeric_profiles": numeric_profiles,
        "accepted_categorical_profiles": categorical_profiles,
        "rejected_dataset_counts": {
            "rows": int(len(rejected_df)),
            "batches": int(sum(record["status"] == "rejected" for record in batch_records)),
        },
        "batch_quality_records": batch_records,
        "source_files": source_file_manifests,
        "rejected_numeric_profiles": {
            column: compute_numeric_profile(
                rejected_profile_df[column],
                modified_zscore_threshold=validation_policy.modified_zscore_threshold,
            )
            for column in PROFILE_NUMERIC_COLUMNS
            if column in rejected_profile_df.columns
        },
    }


def write_dataset_profile(path: str | Path, payload: dict[str, object]) -> None:
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
