"""
Data Utilities for Hubverse Dashboard

Helper functions for date conversion, JSON encoding, and data type handling.
"""

import json
import pandas as pd
import numpy as np


def to_utc_iso_string(date_value) -> str:
    """
    Convert a date value to full UTC ISO string format: YYYY-MM-DDTHH:mm:ssZ

    This ensures consistent date key formatting that JavaScript will interpret as UTC,
    avoiding local timezone interpretation issues when parsing date-only strings.

    Args:
        date_value: A date-like value (string, datetime, Timestamp, etc.)

    Returns:
        str: Full ISO UTC string like "2023-04-01T00:00:00Z"
    """
    dt = pd.to_datetime(date_value)
    return dt.strftime("%Y-%m-%dT00:00:00Z")


class NpEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy and Pandas types.

    Handles special float values (NaN, Infinity) that are not valid JSON
    by converting them to None (which becomes null in JSON).
    """

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            # Handle NaN and Infinity - convert to None for valid JSON
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, np.ndarray):
            # Convert array, handling NaN/Inf values
            return [None if (isinstance(x, float) and (np.isnan(x) or np.isinf(x))) else x for x in obj.tolist()]
        if isinstance(obj, pd.Timestamp):
            # Ensure timestamp includes UTC timezone info (adds 'Z' suffix)
            if obj.tz is None:
                obj = obj.tz_localize("UTC")
            return obj.isoformat()
        # Handle regular Python float NaN/Inf
        if isinstance(obj, float):
            if np.isnan(obj) or np.isinf(obj):
                return None
        return super(NpEncoder, self).default(obj)


def ensure_string_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Ensure a DataFrame column is consistently string type.
    
    This is important for parquet serialization which doesn't handle mixed types well.
    
    Args:
        df: DataFrame to modify
        column_name: Name of column to convert
    
    Returns:
        DataFrame with column converted to string
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str)
    return df