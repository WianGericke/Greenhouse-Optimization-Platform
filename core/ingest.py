from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, cast

import pandas as pd

# ----------------------
# Public config / mapping styles
# ----------------------

@dataclass(frozen=True)
class ColumnMapping:
    """
    Maps user provided CSV column names to canonical internal names

    List of internal names used throughout the platform:
        - timestamp (datetime64)
        - temperature_c (float)
        - relative_humidity (float)
    """
    timestamp: str
    temperature: str
    humidity: str

@dataclass(frozen=True)
class IngestOptions:
    """
    Options that control ingestion without altering algorithms
    """
    timezone: Optional[str] = None
    allow_na_rows: bool = False

# ----------------------
# Core Ingestion Utilities
# ----------------------

def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def ensure_coloumns(df: pd.DataFrame, required: Sequence[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
def standardize_columns(df: pd.DataFrame, mapping: ColumnMapping) -> pd.DataFrame:
    # Renames user columns -> internal canonical column names
    ensure_coloumns(df, [mapping.timestamp, mapping.temperature, mapping.humidity])
    out = df.rename(
        columns = {
            mapping.timestamp: "timestamp",
            mapping.temperature: "temperature",
            mapping.humidity: "relative_humidity",
        }
    ).copy()
    return out

def parse_timestamp_column(df: pd.DataFrame, opts: IngestOptions) -> pd.DataFrame:
    
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    if out["timestamp"].isna().any():
        bad = out[out["timestamp"].isna()].head(8)
        raise ValueError(
            "Some timestamps could not be parsed. Example bad rows: \n"
            f"{bad}"
        )
    
    if opts.timezone:
        if out["timestamp"].dt.tz is None:
            out["timestamp"] = out["timestamp"].dt.tz_localize(opts.timezone)
        else:
            out["timestamp"] = out["timestamp"].dt.tz_convert(opts.timezone)
    return out

def coerce_required_numeric(df: pd.DataFrame, opts: IngestOptions) -> pd.DataFrame:
    out = df.copy()

    out["temperature_c"] = pd.to_numeric(out["temperature_c"], errors="coerce")
    out["relative_humidity"] = pd.to_numeric(out["relative_humidity"], errors="coerce")

    if not opts.allow_na_rows:
        required = out[["timestamp", "temperature_c", "relative_humidity"]]
        if required.isna().any().any():
            bad = out[required.isna().any(axis=1)].head(8)
            raise ValueError(
                "Some required values are missing or non-numeric. Example bad rows:\n"
                f"{bad}"
            )
    return out

def add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["hour"] = out["timestamp"].dt.hour
    out["date"] = out["timestamp"].dt.date
    return out

def filter_nighttime(df: pd.DataFrame, start_hour: int = 0, end_hour: int = 6) -> pd.DataFrame:
    if start_hour > end_hour:
        raise ValueError("start_hour must be <= end_hour for v1 nighttime filter.")
    
    mask = df["hour"].between(start_hour, end_hour)
    result = cast(pd.DataFrame, df.loc[mask].copy())
    return result

def ingest_csv(
    path: str,
    mapping: ColumnMapping,
    opts: Optional[IngestOptions] = None,
    sort: bool = True,
) -> pd.DataFrame:

    opts = opts or IngestOptions()

    df = load_csv(path)
    df = standardize_columns(df, mapping)
    df = parse_timestamp_column(df, opts)
    df = coerce_required_numeric(df, opts)
    df = add_time_columns(df)

    if sort:
        df = df.sort_values("timestamp").reset_index(drop=True)

    return df