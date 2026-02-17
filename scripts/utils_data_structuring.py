"""
Data Structuring Utilities for Hubverse Dashboard

Helper functions for transforming DataFrames into nested dictionary structures
optimized for frontend consumption.
"""

import logging
import pandas as pd
import numpy as np
from utils_data import to_utc_iso_string

logger = logging.getLogger(__name__)


def structure_raw_scores(df: pd.DataFrame, val_col: str) -> dict:
    """
    Structure raw evaluation scores into a nested dictionary for JSON export.

    Filters out records with ``NaN`` or ``Infinity`` scores to ensure the
    resulting dictionary is JSON-serializable. Date fields are converted to
    UTC ISO-8601 strings via :func:`~utils_data.to_utc_iso_string`.

    Args:
        df: :class:`~pandas.DataFrame` containing raw scores. Must include
            columns ``model``, ``location``, ``horizon``, ``reference_date``,
            ``target_end_date``, and the column named by *val_col*.
        val_col: Name of the column containing the numeric score values
            (e.g., ``"wis"``, ``"mape"``, ``"wis_ratio"``).

    Returns:
        dict: Nested dictionary structured as
            ``model -> location -> horizon -> [score records]``, where each
            score record contains ``referenceDate``, ``targetEndDate``, and
            ``score``.
    """
    structured = {}
    for model_name in df["model"].unique():
        model_df = df[df["model"] == model_name]
        structured[model_name] = {}

        for location in model_df["location"].unique():
            loc_key = str(location).zfill(2)
            loc_df = model_df[model_df["location"] == location]
            structured[model_name][loc_key] = {}

            if "horizon" in loc_df.columns:
                for horizon in loc_df["horizon"].unique():
                    h_df = loc_df[loc_df["horizon"] == horizon]
                    records = []
                    for _, row in h_df.iterrows():
                        score_val = row[val_col]
                        # Skip records with NaN or Infinity scores
                        if pd.isna(score_val) or not np.isfinite(score_val):
                            continue
                        records.append(
                            {
                                "referenceDate": to_utc_iso_string(row["reference_date"]),
                                "targetEndDate": to_utc_iso_string(row["target_end_date"]),
                                "score": float(score_val),
                            }
                        )
                    # Only add horizon key if there are valid records
                    if records:
                        structured[model_name][loc_key][int(horizon)] = records
    return structured


def process_target_data(
    target_data_df: pd.DataFrame,
    target_key_to_id_map: dict,
    target_id_to_dvp_config: dict,
    config_targets: list
) -> dict:
    """
    Transform ground-truth data into a nested dictionary for frontend lookup.

    Produces the structure ``Map<location, Map<date, Map<target, data>>>``
    where dates are UTC ISO-8601 strings and location codes are zero-padded
    to two digits. Configured scaling factors from
    :class:`DataValueProcessingConfig` are applied to observation values.

    Args:
        target_data_df: Standardized target-data :class:`~pandas.DataFrame`
            with columns ``date``, ``location``, ``observation``, and
            optionally ``target`` and ``location_name``.
        target_key_to_id_map: Mapping from raw target keys (as they appear
            in the data) to canonical target IDs defined in
            :class:`TargetConfig`.
        target_id_to_dvp_config: Mapping from target IDs to their
            :class:`DataValueProcessingConfig` (scaling and rounding).
        config_targets: List of :class:`TargetConfig` instances from the
            dashboard configuration.

    Returns:
        dict: Nested dictionary
            ``{location: {date_iso: {target_id: {observation, ...}}}}``.
    """
    processed_data = {}

    for _, row in target_data_df.iterrows():
        location_key = str(row.get("location", "US")).zfill(2) if "location" in row else "US"
        date_iso = to_utc_iso_string(row["date"])

        data_entry = {
            "observation": float(row["observation"]) if pd.notna(row["observation"]) and row["observation"] >= -1 else None,
        }

        if "location_name" in row and pd.notna(row["location_name"]):
            data_entry["location_name"] = str(row["location_name"])

        raw_target_key = str(row.get("target", config_targets[0].target_key_in_data if config_targets else "default"))
        target_id = target_key_to_id_map.get(raw_target_key, raw_target_key)

        # Apply scaling
        dvp_config = target_id_to_dvp_config.get(target_id)
        if dvp_config and data_entry["observation"] is not None and data_entry["observation"] != -1:
            scaling_factor = dvp_config.scaling_factor.target_data
            data_entry["observation"] *= scaling_factor

        # Create nested dictionaries
        if location_key not in processed_data:
            processed_data[location_key] = {}
        if date_iso not in processed_data[location_key]:
            processed_data[location_key][date_iso] = {}

        processed_data[location_key][date_iso][target_id] = data_entry

    logger.info(f"Processed {len(target_data_df)} rows of target data.")
    return processed_data


def process_model_output_data(
    model_output_df: pd.DataFrame,
    available_models: list,
    target_key_to_id_map: dict,
    target_id_to_dvp_config: dict,
    prediction_intervals: list
) -> dict:
    """
    Transform model predictions into a nested dictionary for the frontend.

    Produces the structure::

        model -> location -> reference_date -> target_date -> target_id -> prediction

    Each prediction entry contains ``horizon``, ``targetId``,
    ``value_median``, and nested ``prediction_intervals`` keyed by level.

    **Baseline model exclusion:** Only models listed in *available_models*
    are included in the output. The baseline model (used solely for
    evaluation WIS ratio calculations) is silently skipped if it does not
    appear in *available_models*, ensuring it never reaches the frontend
    display while still being available for :class:`EvaluationProcessor`.

    Configured scaling factors from :class:`DataValueProcessingConfig` are
    applied to both median and prediction interval values.

    Args:
        model_output_df: Standardized model-output :class:`~pandas.DataFrame`
            with columns ``model``, ``location``, ``reference_date``,
            ``target_end_date``, ``horizon``, ``target``, and quantile
            columns.
        available_models: List of :class:`ModelConfig` instances representing
            models to include in the frontend output.
        target_key_to_id_map: Mapping from raw target keys (as they appear
            in the data) to canonical target IDs.
        target_id_to_dvp_config: Mapping from target IDs to their
            :class:`DataValueProcessingConfig` (scaling and rounding).
        prediction_intervals: List of :class:`PredictionIntervalConfig`
            instances defining which quantile pairs to extract.

    Returns:
        dict: Nested dictionary
            ``{model: {location: {ref_date: {predictions: {target_date: {target_id: entry}}}}}}``.
    """
    processed_data = {}

    # Get list of models to include in frontend output
    available_model_names = [m.model_name for m in available_models] if available_models else []

    for model_name in model_output_df["model"].unique():
        # Skip baseline if not in available_models (it's only for evaluation)
        if model_name not in available_model_names:
            logger.debug(f"Skipping model '{model_name}' from frontend output (not in available_models)")
            continue
        model_data = model_output_df[model_output_df["model"] == model_name]
        model_dict = {}

        for location in model_data["location"].unique():
            location_key = str(location).zfill(2)
            location_data = model_data[model_data["location"] == location]
            location_dict = {}

            for ref_date in location_data["reference_date"].unique():
                ref_date_iso = to_utc_iso_string(ref_date)
                ref_date_data = location_data[location_data["reference_date"] == ref_date]
                predictions_dict = {}

                for _, row in ref_date_data.iterrows():
                    target_date_iso = to_utc_iso_string(row["target_end_date"])

                    # Get target ID
                    if "target" in row and pd.notna(row["target"]):
                        raw_target_key = str(row["target"])
                        target_id = target_key_to_id_map.get(raw_target_key, raw_target_key)
                    else:
                        # Fallback if target missing
                        target_id = "unknown"

                    pred_entry = {
                        "horizon": int(row["horizon"]) if pd.notna(row["horizon"]) else None,
                        "targetId": target_id,
                    }

                    dvp_config = target_id_to_dvp_config.get(target_id)
                    scaling_factor = dvp_config.scaling_factor.model_output if dvp_config else 1.0

                    quantile_cols = [col for col in row.index if isinstance(col, (float, str))]

                    for qc in quantile_cols:
                        if str(qc) == "0.5" and pd.notna(row[qc]):
                            pred_entry["value_median"] = row[qc] * scaling_factor

                    pred_intervals = {}
                    for desired_PI in prediction_intervals:
                        single_interval_info = {}
                        for target_quantile in desired_PI.uses_output_type_ids:
                            for qc in quantile_cols:
                                if str(qc) == target_quantile and pd.notna(row[qc]):
                                    value = row[qc] * scaling_factor
                                    if target_quantile == desired_PI.uses_output_type_ids[0]:
                                        single_interval_info["pi_value_low"] = value
                                    else:
                                        single_interval_info["pi_value_high"] = value
                        pred_intervals[str(desired_PI.level)] = single_interval_info

                    pred_entry["prediction_intervals"] = pred_intervals

                    # Nest by target_date THEN targetId to support multiple targets per date
                    if target_date_iso not in predictions_dict:
                        predictions_dict[target_date_iso] = {}
                    predictions_dict[target_date_iso][target_id] = pred_entry

                location_dict[ref_date_iso] = {"predictions": predictions_dict}

            model_dict[location_key] = location_dict

        processed_data[model_name] = model_dict

    logger.info(f"Processed predictions for {len(processed_data)} models.")
    return processed_data


def process_historical_target_data(
    df: pd.DataFrame,
    target_key_to_id_map: dict,
    target_id_to_dvp_config: dict
) -> dict:
    """
    Process historical target data into a nested dictionary of snapshots.

    Produces the structure ``Map<as_of_date, Map<date, Map<location, data>>>``
    so the frontend can look up "what we knew" at any given point in time.
    Each snapshot represents the ground-truth data that was available as of
    a specific reporting date.

    Configured scaling factors from :class:`DataValueProcessingConfig` are
    applied to observation values within each snapshot.

    Args:
        df: Raw :class:`~pandas.DataFrame` containing an ``as_of`` column
            alongside ``date``, ``location``, ``observation``, and
            optionally ``target`` and ``location_name``.
        target_key_to_id_map: Mapping from raw target keys to canonical
            target IDs defined in :class:`TargetConfig`.
        target_id_to_dvp_config: Mapping from target IDs to their
            :class:`DataValueProcessingConfig` (scaling and rounding).

    Returns:
        dict: Nested dictionary
            ``{as_of_iso: {date_iso: {location: {target_id: data_entry}}}}``.
    """
    historical_data = {}

    # Get all unique as_of dates
    unique_as_of_dates = df["as_of"].unique()

    for as_of_date in unique_as_of_dates:
        as_of_iso = to_utc_iso_string(as_of_date)
        snapshot_df = df[df["as_of"] == as_of_date]

        # Group all date values
        date_map = {}
        for date in snapshot_df["date"].unique():
            date_iso = to_utc_iso_string(date)
            date_records = snapshot_df[snapshot_df["date"] == date]

            # Group by location
            location_map = {}
            for _, row in date_records.iterrows():
                location_key = str(row.get("location", "US")).zfill(2) if "location" in row else "US"

                # Build data record
                data_entry = {
                    "observation": float(row["observation"]) if pd.notna(row["observation"]) else None,
                }

                # Add location_name if available
                if "location_name" in row and pd.notna(row["location_name"]):
                    data_entry["location_name"] = str(row["location_name"])

                # IMPORTANT: Add target field - required for multi-target scenarios
                # Map raw target key to target_id for consistency with config
                target_id = "default"
                if "target" in row and pd.notna(row["target"]):
                    raw_target_key = str(row["target"])
                    target_id = target_key_to_id_map.get(raw_target_key, raw_target_key)

                    # Apply scaling for target data
                    dvp_config = target_id_to_dvp_config.get(target_id)
                    if dvp_config and "observation" in data_entry and data_entry["observation"] is not None and data_entry["observation"] != -1:
                        scaling_factor = dvp_config.scaling_factor.target_data
                        data_entry["observation"] *= scaling_factor

                    data_entry["target"] = target_id

                # Use nested structure: location -> target -> data
                if location_key not in location_map:
                    location_map[location_key] = {}

                location_map[location_key][target_id] = data_entry

            date_map[date_iso] = location_map

        historical_data[as_of_iso] = date_map

    return historical_data


def organize_metric_all_data(
    df: pd.DataFrame,
    metric_name: str,
    val_col: str,
    raw_scores_dict: dict,
    target_key_to_id_map: dict
):
    """
    Organize a metric's raw scores for ALL data (no period filtering).

    Groups scores by target, then delegates to :func:`structure_raw_scores`
    to build the nested structure
    ``target -> metric -> model -> location -> horizon -> [scores]``.

    This function mutates *raw_scores_dict* in place.

    Args:
        df: :class:`~pandas.DataFrame` containing metric scores. Must include
            ``target_end_date`` and optionally ``target`` columns.
        metric_name: Display name of the metric (e.g., ``"WIS/Baseline"``,
            ``"MAPE"``), used as a key in *raw_scores_dict*.
        val_col: Name of the column containing the numeric score values.
        raw_scores_dict: Dictionary to populate with organized scores.
            Modified in place; keyed by target ID at the top level.
        target_key_to_id_map: Mapping from raw target keys to canonical
            target IDs defined in :class:`TargetConfig`.

    Returns:
        None. The *raw_scores_dict* argument is mutated in place.
    """
    if not pd.api.types.is_datetime64_any_dtype(df["target_end_date"]):
        df["target_end_date"] = pd.to_datetime(df["target_end_date"])

    # Group by target
    unique_targets = df["target"].unique() if "target" in df.columns else ["default"]

    for target in unique_targets:
        target_id = target_key_to_id_map.get(target, target) if target != "default" else "default"
        target_df = df if target == "default" else df[df["target"] == target]

        if target_id not in raw_scores_dict:
            raw_scores_dict[target_id] = {}

        raw_scores_dict[target_id][metric_name] = structure_raw_scores(target_df, val_col)
