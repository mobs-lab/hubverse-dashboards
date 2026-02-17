"""
Evaluation Aggregation Utilities for Hubverse Dashboard

Helper functions for aggregating and processing evaluation metrics,
including IQR statistics, location map aggregates, and coverage aggregates.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def calculate_boxplot_stats(location_averages: list) -> dict:
    """
    Calculate boxplot statistics from a list of per-location score averages.

    These statistics represent the distribution of model performance across
    locations. The percentiles (q05, q25, median, q75, q95) are computed
    from the list of location averages. Used by :func:`process_iqr_stats`
    to produce Season Overview boxplot data.

    Args:
        location_averages: List of average scores, one per location.
            ``NaN`` and ``Inf`` values are filtered out before computation.

    Returns:
        dict or None: BoxplotStats dictionary with keys ``q05``, ``q25``,
        ``median``, ``q75``, ``q95``, ``min``, ``max``, ``mean``, and
        ``count``. Returns ``None`` if no valid (finite) data is available.
    """
    if not location_averages or len(location_averages) == 0:
        return None

    scores = np.array(location_averages, dtype=float)

    # Filter out NaN and Infinity values
    valid_mask = np.isfinite(scores)
    scores = scores[valid_mask]

    if len(scores) == 0:
        return None

    # Calculate percentiles from the distribution of location averages
    percentiles = np.percentile(scores, [5, 25, 50, 75, 95])

    # Verify all results are finite before returning
    min_val = float(np.min(scores))
    max_val = float(np.max(scores))
    mean_val = float(np.mean(scores))

    # Double-check that our computed values are valid
    if not all(np.isfinite([min_val, max_val, mean_val, *percentiles])):
        logger.warning("Boxplot stats computation produced non-finite values, skipping")
        return None

    stats = {
        "q05": float(percentiles[0]),
        "q25": float(percentiles[1]),
        "median": float(percentiles[2]),
        "q75": float(percentiles[3]),
        "q95": float(percentiles[4]),
        "min": min_val,
        "max": max_val,
        "mean": mean_val,
        "count": int(len(scores)),
    }

    return stats


def process_iqr_stats(period_id: str, precalculated: dict):
    """
    Calculate IQR statistics for Season Overview boxplot charts.

    **Must** be called *after* :func:`process_location_map_aggregates` as it
    reads from ``precalculated["locationMap_aggregates"]`` to compute
    per-location averages.

    Logic:

    1. For each metric (WIS/Baseline, MAPE), target, model, and horizon
    2. Compute the average score for each location: ``sum / count``
    3. Collect all location averages into a list
    4. Pass the list to :func:`calculate_boxplot_stats` to obtain percentiles

    Only pre-calculates single-horizon IQR. The frontend computes
    multi-horizon combinations using ``locationMap_aggregates`` data.

    Args:
        period_id: The forecast period identifier string.
        precalculated: Mutable dictionary containing aggregated evaluation
            data. Must already contain ``"locationMap_aggregates"`` and
            ``"iqr"`` keys with nested dicts for *period_id*.
    """
    state_map_data = precalculated.get("locationMap_aggregates", {}).get(period_id, {})

    if not state_map_data:
        return

    # Iterate through the state_map_aggregates structure
    for target_id, target_data in state_map_data.items():
        if target_id not in precalculated["iqr"][period_id]:
            precalculated["iqr"][period_id][target_id] = {}

        for metric_name, metric_data in target_data.items():
            # Skip Coverage metric - IQR is only for WIS/Baseline and MAPE
            if metric_name == "Coverage":
                continue

            if metric_name not in precalculated["iqr"][period_id][target_id]:
                precalculated["iqr"][period_id][target_id][metric_name] = {}

            for model_name, model_data in metric_data.items():
                if model_name not in precalculated["iqr"][period_id][target_id][metric_name]:
                    precalculated["iqr"][period_id][target_id][metric_name][model_name] = {}

                # Get all available horizons for this model
                available_horizons = set()
                for loc_data in model_data.values():
                    available_horizons.update(loc_data.keys())

                # Calculate IQR for each individual horizon only
                # Frontend will compute multi-horizon combinations using locationMap_aggregates
                for horizon in available_horizons:
                    horizon_str = str(horizon)

                    # Compute location averages for this horizon
                    location_averages = []
                    for loc, loc_data in model_data.items():
                        if horizon_str in loc_data:
                            agg = loc_data[horizon_str]
                            if agg["count"] > 0:
                                avg = agg["sum"] / agg["count"]
                                location_averages.append(avg)

                    # Calculate IQR stats from location averages
                    if len(location_averages) >= 1:
                        stats = calculate_boxplot_stats(location_averages)
                        if stats:
                            precalculated["iqr"][period_id][target_id][metric_name][model_name][horizon_str] = stats


def process_location_map_aggregates(
    raw_evaluations: dict,
    period_id: str,
    start,
    end,
    precalculated: dict,
    target_key_to_id_map: dict,
    location_map_coverage_level: int = 95
):
    """
    Process location map aggregates for geographic visualization and IQR calculation.

    Aggregates ``sum`` / ``count`` per location per horizon for WIS over
    Baseline, MAPE, and Coverage metrics. These aggregates are consumed by:

    1. Location map visualisation (computing location averages for map colouring)
    2. :func:`process_iqr_stats` (computing percentiles across location averages)

    Note: WIS/Baseline and MAPE each have a single score per forecast instance.
    Coverage uses a configurable prediction interval level (default 95 %).

    Args:
        raw_evaluations: Dictionary of raw evaluation DataFrames keyed by
            metric name (``"wis_ratio"``, ``"mape"``, ``"coverage"``).
        period_id: The forecast period identifier string.
        start: Start date (inclusive) for filtering ``target_end_date``.
        end: End date (inclusive) for filtering ``target_end_date``.
        precalculated: Mutable dictionary to store aggregated results.
            Must already contain a ``"locationMap_aggregates"`` key with
            a nested dict for *period_id*.
        target_key_to_id_map: Mapping from target column values to target
            ID strings used in the output structure.
        location_map_coverage_level: Coverage level percentage used when
            aggregating the Coverage metric for the location map
            (default ``95``).
    """
    # WIS/Baseline: wis_ratio column (single value per forecast)
    # MAPE: mape column (single value per forecast)
    # Coverage: use configured coverage level column (binary 0/1 per forecast, averaged to percentage)
    metrics_to_process = []
    if "wis_ratio" in raw_evaluations and not raw_evaluations["wis_ratio"].empty:
        metrics_to_process.append(("WIS/Baseline", raw_evaluations["wis_ratio"], "wis_ratio"))
    if "mape" in raw_evaluations and not raw_evaluations["mape"].empty:
        metrics_to_process.append(("MAPE", raw_evaluations["mape"], "mape"))
    if "coverage" in raw_evaluations and not raw_evaluations["coverage"].empty:
        # Use configured coverage level for location map aggregates
        cov_df = raw_evaluations["coverage"]
        coverage_col_name = f"{location_map_coverage_level}_coverage"
        if coverage_col_name in cov_df.columns:
            metrics_to_process.append(("Coverage", cov_df, coverage_col_name))
        else:
            logger.warning(f"{coverage_col_name} column not found in coverage data, skipping Coverage metric for location map. "
                         f"Available coverage columns: {[col for col in cov_df.columns if col.endswith('_coverage')]}")

    for metric_name, df, val_col in metrics_to_process:
        # Filter by date range
        if not pd.api.types.is_datetime64_any_dtype(df["target_end_date"]):
            df = df.copy()
            df["target_end_date"] = pd.to_datetime(df["target_end_date"])

        period_df = df[(df["target_end_date"] >= start) & (df["target_end_date"] <= end)]

        if period_df.empty:
            continue

        unique_targets = period_df["target"].unique() if "target" in period_df.columns else ["default"]

        for target in unique_targets:
            target_id = target_key_to_id_map.get(target, target) if target != "default" else "default"
            target_df = period_df if target == "default" else period_df[period_df["target"] == target]

            if target_id not in precalculated["locationMap_aggregates"][period_id]:
                precalculated["locationMap_aggregates"][period_id][target_id] = {}

            if metric_name not in precalculated["locationMap_aggregates"][period_id][target_id]:
                precalculated["locationMap_aggregates"][period_id][target_id][metric_name] = {}

            for model_name in target_df["model"].unique():
                model_df = target_df[target_df["model"] == model_name].copy()
                precalculated["locationMap_aggregates"][period_id][target_id][metric_name][model_name] = {}

                if "location" in model_df.columns and "horizon" in model_df.columns:
                    # Filter out NaN and Infinity values before aggregation
                    model_df = model_df[np.isfinite(model_df[val_col])]

                    if model_df.empty:
                        continue

                    grouped = model_df.groupby(["location", "horizon"])[val_col].agg(["sum", "count"]).reset_index()

                    for _, row in grouped.iterrows():
                        # Skip if sum is not finite (shouldn't happen after filtering, but extra safety)
                        if not np.isfinite(row["sum"]):
                            continue

                        loc = str(row["location"]).zfill(2)
                        horizon = str(int(row["horizon"]))

                        if loc not in precalculated["locationMap_aggregates"][period_id][target_id][metric_name][model_name]:
                            precalculated["locationMap_aggregates"][period_id][target_id][metric_name][model_name][loc] = {}

                        precalculated["locationMap_aggregates"][period_id][target_id][metric_name][model_name][loc][horizon] = {
                            "sum": float(row["sum"]),
                            "count": int(row["count"]),
                        }

def process_coverage_aggregates(
    raw_evaluations: dict,
    period_id: str,
    start,
    end,
    precalculated: dict,
    cov_levels: list,
    target_key_to_id_map: dict
):
    """
    Process coverage aggregates for the Season Overview coverage chart.

    For each configured coverage level (e.g., 50, 80, 95), computes
    per-horizon ``sum`` / ``count`` aggregates that the frontend uses to
    derive average coverage rates. Results are stored in
    ``precalculated["detailedCoverage_aggregates"]``, keyed by period,
    target, model, horizon, and coverage level.

    Args:
        raw_evaluations: Dictionary of raw evaluation DataFrames. Must
            contain a ``"coverage"`` key whose DataFrame has columns for
            each requested level (e.g., ``"95_coverage"``).
        period_id: The forecast period identifier string.
        start: Start date (inclusive) for filtering ``target_end_date``.
        end: End date (inclusive) for filtering ``target_end_date``.
        precalculated: Mutable dictionary to store aggregated results.
            Must already contain a ``"detailedCoverage_aggregates"`` key
            with a nested dict for *period_id*.
        cov_levels: List of integer coverage level percentages to process
            (e.g., ``[50, 80, 95]``).
        target_key_to_id_map: Mapping from target column values to target
            ID strings used in the output structure.
    """
    if "coverage" not in raw_evaluations or raw_evaluations["coverage"].empty:
        return

    df = raw_evaluations["coverage"]
    if not pd.api.types.is_datetime64_any_dtype(df["target_end_date"]):
        df["target_end_date"] = pd.to_datetime(df["target_end_date"])
    period_df = df[(df["target_end_date"] >= start) & (df["target_end_date"] <= end)]

    if period_df.empty:
        return

    unique_targets = period_df["target"].unique() if "target" in period_df.columns else ["default"]

    for target in unique_targets:
        target_id = target_key_to_id_map.get(target, target) if target != "default" else "default"
        target_df = period_df if target == "default" else period_df[period_df["target"] == target]

        if target_id not in precalculated["detailedCoverage_aggregates"][period_id]:
            precalculated["detailedCoverage_aggregates"][period_id][target_id] = {}

        for model_name in target_df["model"].unique():
            model_df = target_df[target_df["model"] == model_name]
            precalculated["detailedCoverage_aggregates"][period_id][target_id][model_name] = {}

            for level in cov_levels:
                col_name = f"{level}_coverage"
                if col_name not in model_df.columns:
                    continue

                if "horizon" in model_df.columns:
                    grouped = model_df.groupby("horizon")[col_name].agg(["sum", "count"]).reset_index()

                    for _, row in grouped.iterrows():
                        horizon = int(row["horizon"])
                        if horizon not in precalculated["detailedCoverage_aggregates"][period_id][target_id][model_name]:
                            precalculated["detailedCoverage_aggregates"][period_id][target_id][model_name][horizon] = {}

                        precalculated["detailedCoverage_aggregates"][period_id][target_id][model_name][horizon][str(level)] = {
                            # Coverage values are already in percentage format (0-100) from evaluation_processor
                            "sum": float(row["sum"]),
                            "count": int(row["count"]),
                        }