"""
Model Output Validation Utilities for Hubverse Dashboard

Helper functions for validating and transforming model output data,
including schema validation and quantile pivoting.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def validate_model_output_schema(df: pd.DataFrame) -> None:
    """
    Validate model output data schema and detect potential issues.

    Checks for required column presence and expected data types. Logs
    warnings for type mismatches but only raises on missing columns.

    Args:
        df: Model output :class:`~pandas.DataFrame` to validate.

    Raises:
        ValueError: If one or more required columns are missing from *df*.
    """
    required_cols = ["reference_date", "target_end_date", "location", "target", 
                    "output_type", "output_type_id", "value", "model"]
    
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in model output: {missing}")
    
    # Check data types
    expected_types = {
        "reference_date": "datetime64[ns]",
        "target_end_date": "datetime64[ns]",
        "location": "object",
        "output_type_id": "object",  # Should be string after normalization
        "value": "float64"
    }
    
    for col, expected_dtype in expected_types.items():
        if col in df.columns:
            actual_dtype = str(df[col].dtype)
            if not actual_dtype.startswith(expected_dtype.split('[')[0]):
                logger.warning(f"  Column '{col}' has type '{actual_dtype}', expected '{expected_dtype}'")
    
    logger.info("  [OK] Model output schema validation passed")


def validate_pivot_quantiles(pivoted_df: pd.DataFrame) -> None:
    """
    Validate pivoted quantile data for duplicate columns.

    Detects cases where mixed types in source data (e.g., both ``0.5`` and
    ``"0.5"``) create duplicate quantile columns after pivoting. Called
    automatically by :func:`pivot_quantiles`.

    Args:
        pivoted_df: Pivoted :class:`~pandas.DataFrame` with quantile
            levels as column names.
    """
    # Get quantile columns (numeric-looking column names)
    q_cols = [c for c in pivoted_df.columns 
             if isinstance(c, (float, int, str)) and 
             str(c).replace(".", "", 1).replace("-", "").replace("e", "").isdigit()]
    
    if not q_cols:
        return
    
    # Convert to strings and check for duplicates
    q_strings = [str(c) for c in q_cols]
    unique_q_strings = set(q_strings)
    
    if len(q_strings) != len(unique_q_strings):
        # Find which quantiles are duplicated
        duplicates = [q for q in unique_q_strings if q_strings.count(q) > 1]
        logger.warning(
            f"Duplicate quantile columns detected: {duplicates}. "
            f"This indicates mixed data types (float vs string) in source data. "
            f"Data normalization should have prevented this."
        )
        # Log the actual column types for debugging
        dup_cols = [c for c in q_cols if str(c) in duplicates]
        logger.warning(f"      Duplicate column details: {[(c, type(c).__name__) for c in dup_cols]}")


def pivot_quantiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot long-format quantile data into wide format.

    Each unique ``output_type_id`` value becomes its own column, making it
    straightforward for the frontend to map prediction intervals. Non-quantile
    rows are preserved and concatenated back after pivoting.

    Validation of the result is performed by
    :func:`validate_pivot_quantiles`.

    Args:
        df: Long-format model output :class:`~pandas.DataFrame` containing
            an ``output_type`` column with ``"quantile"`` rows.

    Returns:
        pandas.DataFrame: Wide-format DataFrame with quantile levels as
        columns. If no quantile rows exist, the original *df* is returned
        unchanged.
    """

    quantile_rows = df[df["output_type"] == "quantile"].copy()
    other_rows = df[df["output_type"] != "quantile"]

    if quantile_rows.empty:
        return df

    # Define index for pivoting
    index_cols = [
        "reference_date",
        "target_end_date",
        "location",
        "target",
        "horizon",
        "model",
    ]
    # Ensure all index columns exist in the dataframe
    index_cols = [col for col in index_cols if col in quantile_rows.columns]

    # Pivot the table
    pivoted = quantile_rows.pivot_table(index=index_cols, columns="output_type_id", values="value").reset_index()

    # Validate pivot results for duplicate quantile columns (indicates mixed types in source)
    validate_pivot_quantiles(pivoted)

    # Merge back with non-quantile rows if any
    if not other_rows.empty:
        final_df = pd.concat([pivoted, other_rows], ignore_index=True)
    else:
        final_df = pivoted

    return final_df
