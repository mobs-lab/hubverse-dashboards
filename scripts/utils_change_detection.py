"""
Change Detection Utilities for Hubverse Dashboard

Helper functions for detecting changes in target data and extracting prediction keys
for incremental update processing.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def identify_target_data_changes(old_df: pd.DataFrame, new_df: pd.DataFrame) -> set:
    """
    Identify which keys (location, date, target) have changed between old and new target data.

    Tracks both new observations and revisions to existing observations.

    NOTE ON DATA COMPARISON STRATEGY:
    - Manifest Manager: Compares RAW file checksums to detect IF source files changed
    - This method: Compares PROCESSED data (after column renaming, as_of shifting, etc.)
    - old_df: Previously processed data (loaded from intermediates/target_data.parquet)
    - new_df: Newly processed data (loaded from raw files and transformed)
    - Both are in same format (standard column names, shifted as_of dates, etc.)
    - This ensures comparison of both processed target-data while detecting byte-level source changes

    Args:
        old_df: Previous target data (PROCESSED)
        new_df: New target data (PROCESSED)

    Returns:
        set of tuples: {(location, date, target), ...}
    """
    if old_df is None or old_df.empty:
        return set()  # Empty set means all data is new (handled differently)

    # Ensure consistency in comparison columns
    old_comp = old_df.copy()
    new_comp = new_df.copy()

    # Build merge keys - include target if it exists (for multi-target scenarios)
    keys = ["location", "date"]
    if "target" in old_df.columns and "target" in new_df.columns:
        keys.append("target")

    # Convert date to datetime if not already
    old_comp["date"] = pd.to_datetime(old_comp["date"])
    new_comp["date"] = pd.to_datetime(new_comp["date"])

    # Convert observation to float for comparison stability
    old_comp["observation"] = old_comp["observation"].astype(float)
    new_comp["observation"] = new_comp["observation"].astype(float)

    # Merge on keys to compare observations
    merged = pd.merge(new_comp, old_comp, on=keys, suffixes=("_new", "_old"), how="outer", indicator=True)

    # 1. New rows (left_only) - new observations
    new_rows = merged[merged["_merge"] == "left_only"]

    # 2. Changed rows (both, but observation differs) - revisions
    changed_mask = (merged["_merge"] == "both") & (
        (merged["observation_new"] != merged["observation_old"]) & ~(merged["observation_new"].isna() & merged["observation_old"].isna())
    )
    changed_rows = merged[changed_mask]

    affected_keys = set()

    for df in [new_rows, changed_rows]:
        if not df.empty:
            for _, row in df.iterrows():
                # If 'target' was in merge keys, it won't have suffix
                # Otherwise it will be 'target_new' after merge
                if "target" in row.index:
                    target_val = str(row["target"])
                elif "target_new" in row.index:
                    target_val = str(row["target_new"])
                else:
                    # No target column in data - use empty string as placeholder
                    target_val = ""
                
                affected_keys.add((str(row["location"]), row["date"], target_val))

    return affected_keys


def extract_prediction_keys(model_df: pd.DataFrame) -> set:
    """
    Extract unique prediction keys from model output dataframe.

    Used to track which predictions are new and need evaluation.
    Each key includes the model name to track predictions separately per model.

    Args:
        model_df: Model output dataframe (must include 'model' column)

    Returns:
        set of tuples: {(location, reference_date, target_end_date, target, model), ...}
    """
    if model_df is None or model_df.empty:
        return set()

    keys = set()
    required_cols = ["model", "location", "reference_date", "target_end_date", "target", "horizon", "output_type", "output_type_id"]

    # Check if all required columns exist
    if not all(col in model_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in model_df.columns]
        logger.warning(f"Missing required columns for prediction key extraction: {missing}")
        return keys

    # Extract unique combinations including model name
    # Use only the key columns for drop_duplicates to improve performance
    key_cols = ["model", "location", "reference_date", "target_end_date", "target"]
    
    for _, row in model_df[key_cols].drop_duplicates().iterrows():
        keys.add(
            (
                str(row["location"]),
                pd.to_datetime(row["reference_date"]),
                pd.to_datetime(row["target_end_date"]),
                str(row["target"]),
                str(row["model"]),
            )
        )

    return keys
