"""
Evaluation Processor for Hubverse Dashboard

Calculates evaluation metrics (WIS, MAPE, Coverage) for forecasting models.
Based on the original epistorm_evaluations.ipynb logic but generalized for multi-target scenarios.
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class EvaluationProcessor:
    """
    Calculate evaluation metrics for forecasting models.

    Compares model predictions against ground-truth target data to produce
    the following metrics:

    - **WIS** (Weighted Interval Score) — measures the quality of
      probabilistic (quantile) forecasts.
    - **MAPE** (Mean Absolute Percentage Error) — measures the accuracy of
      median point forecasts as a percentage of the observed value.
    - **Coverage** — fraction of observations that fall within each
      configured prediction interval level.
    - **WIS Ratio** — ratio of a model's WIS to the baseline model's WIS,
      providing a relative performance measure.

    Attributes:
        config: Validated :class:`DashboardConfig` instance.
        baseline_model: Name of the model used as the baseline for
            WIS ratio calculations.
    """

    def __init__(self, config, baseline_model: str):
        """
        Initialize the evaluation processor.

        Args:
            config: Validated :class:`DashboardConfig` instance containing
                evaluation settings such as coverage levels and prediction
                intervals.
            baseline_model: Model name to use as the baseline for WIS ratio
                calculations. Must be present in the model-output data.
        """
        self.config = config
        self.baseline_model = baseline_model

    def evaluate_predictions(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> dict:
        """
        Calculate evaluation scores for all predictions against target data.

        Merges predictions with ground truth on ``target_end_date``,
        ``location``, and (optionally) ``target``, then delegates to
        :meth:`_calculate_wis`, :meth:`_calculate_mape`, and
        :meth:`_calculate_coverage`.

        Placeholder observations (value ``-1``) are excluded before scoring.

        Args:
            target_data_df: Ground-truth DataFrame with columns
                ``['date', 'location', 'target', 'observation']``.
            model_output_df: Model predictions DataFrame with columns
                ``['reference_date', 'target_end_date', 'location', 'target',
                'horizon', 'model', 'output_type', 'output_type_id', 'value']``.

        Returns:
            dict: Dictionary with keys ``'wis'``, ``'mape'``, and ``'coverage'``,
                each mapping to a :class:`~pandas.DataFrame` of per-instance
                scores. DataFrames may be empty if no overlapping data exists.
        """
        logger.info("Evaluating predictions...")

        # Prepare data for evaluation
        truth_df = target_data_df.copy()
        pred_df = model_output_df.copy()

        # Ensure date columns are datetime
        if not pd.api.types.is_datetime64_any_dtype(truth_df["date"]):
            truth_df["date"] = pd.to_datetime(truth_df["date"])
        if not pd.api.types.is_datetime64_any_dtype(pred_df["target_end_date"]):
            pred_df["target_end_date"] = pd.to_datetime(pred_df["target_end_date"])

        # Filter out placeholder observations (-1) from target data
        # These represent missing observation and should not be evaluated against
        truth_df = truth_df[truth_df["observation"] != -1].copy()

        # Rename for merging
        truth_df = truth_df.rename(columns={"date": "target_end_date", "observation": "truth_value"})

        # Ensure location types match
        truth_df["location"] = truth_df["location"].astype(str)
        pred_df["location"] = pred_df["location"].astype(str)

        # Determine merge keys
        merge_keys = ["target_end_date", "location"]
        if "target" in truth_df.columns and "target" in pred_df.columns:
            merge_keys.append("target")

        # Merge predictions with ground truth
        merged_df = pd.merge(pred_df, truth_df, on=merge_keys, how="inner")

        if merged_df.empty:
            logger.warning("No overlapping data found between predictions and ground truth.")
            return {"wis": pd.DataFrame(), "mape": pd.DataFrame(), "coverage": pd.DataFrame()}

        logger.info(f"Found {len(merged_df)} prediction-truth pairs for evaluation")

        # Calculate metrics
        wis_df = self._calculate_wis(merged_df)
        mape_df = self._calculate_mape(merged_df)
        coverage_df = self._calculate_coverage(merged_df)

        return {"wis": wis_df, "mape": mape_df, "coverage": coverage_df}

    def calculate_wis_ratio(self, wis_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate WIS Ratio (Model WIS / Baseline WIS).

        Separates baseline model scores, merges them with each non-baseline
        model on matching dimensions (location, horizon, date, target), and
        computes the ratio. The baseline model itself is excluded from the
        returned output.

        Args:
            wis_df: DataFrame containing per-instance WIS scores for all
                models, including the baseline specified in :attr:`baseline_model`.

        Returns:
            pd.DataFrame: DataFrame with a ``wis_ratio`` column added for each
                non-baseline model. Returns an empty DataFrame if the baseline
                model is missing or there are no non-baseline models.
        """
        if wis_df.empty:
            return pd.DataFrame()

        # Separate baseline and other models
        baseline_df = wis_df[wis_df["model"] == self.baseline_model].copy()
        other_models_df = wis_df[wis_df["model"] != self.baseline_model].copy()

        if baseline_df.empty:
            logger.warning(f"Baseline model '{self.baseline_model}' not found in WIS results. Cannot calculate ratios.")
            return pd.DataFrame()

        if other_models_df.empty:
            logger.warning("No non-baseline models found for WIS ratio calculation.")
            return pd.DataFrame()

        # Prepare baseline for merge
        baseline_df = baseline_df.rename(columns={"wis": "baseline_wis"}).drop(columns=["model"])

        # Merge keys (all columns except 'wis' and 'model')
        merge_keys = [col for col in wis_df.columns if col not in ["wis", "model"]]

        # Merge
        ratio_df = pd.merge(other_models_df, baseline_df, on=merge_keys, how="left")

        # Calculate ratio
        ratio_df["wis_ratio"] = ratio_df["wis"] / ratio_df["baseline_wis"]

        # # Detect and log NaN/Infinity values
        # invalid_mask = ~np.isfinite(ratio_df['wis_ratio'])
        # if invalid_mask.any():
        #     invalid_count = invalid_mask.sum()
        #     logger.warning(f"[NaN/Inf Detection] Found {invalid_count} invalid WIS ratio values")

        #     # Log details about invalid values
        #     invalid_rows = ratio_df[invalid_mask]
        #     for idx, row in invalid_rows.iterrows():
        #         logger.warning(
        #             f"  Invalid WIS Ratio: model={row['model']}, location={row['location']}, "
        #             f"horizon={row.get('horizon', 'N/A')}, target_end_date={row['target_end_date']}, "
        #             f"wis={row['wis']}, baseline_wis={row['baseline_wis']}, ratio={row['wis_ratio']}"
        #         )

        # Drop baseline_wis column to keep output clean
        ratio_df = ratio_df.drop(columns=["baseline_wis"])

        logger.info(f"Calculated WIS ratios for {len(ratio_df['model'].unique())} models against baseline '{self.baseline_model}'")

        return ratio_df

    def _calculate_wis(self, merged_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the Weighted Interval Score (WIS) for each prediction instance.

        Uses the formula::

            WIS = 1/(K+0.5) * (0.5*|y - median| + sum_k [alpha_k/2 * IS_k])

        where *K* is the number of symmetric prediction intervals and *IS_k*
        is the interval score for interval *k*.

        Only ``quantile`` output-type rows are used. The method pivots
        quantiles into columns and performs a fully vectorized computation.

        Args:
            merged_df: DataFrame with merged predictions and ground-truth
                values (must contain a ``truth_value`` column from the merge).

        Returns:
            pd.DataFrame: DataFrame with group columns plus a ``wis`` column.
                Returns an empty DataFrame if no quantile predictions or no
                median (0.5) quantile is found.
        """
        logger.info("Calculating WIS scores...")

        # Filter for quantile predictions only
        quantile_df = merged_df[merged_df["output_type"] == "quantile"].copy()
        if quantile_df.empty:
            logger.warning("No quantile predictions found for WIS calculation")
            return pd.DataFrame()

        # Group by prediction instance
        group_cols = ["model", "location", "reference_date", "target_end_date", "horizon"]
        if "target" in quantile_df.columns:
            group_cols.append("target")
        group_cols.append("truth_value")

        # Pivot to get quantiles as columns
        pivot_df = quantile_df.pivot_table(index=group_cols, columns="output_type_id", values="value").reset_index()

        # Validate pivot results
        self._validate_pivot_quantiles(pivot_df, "WIS")

        # Get available quantiles and sort them
        q_cols = [c for c in pivot_df.columns if isinstance(c, (float, str)) and str(c).replace(".", "", 1).isdigit()]
        quantiles = sorted([float(q) for q in q_cols])

        # Check for median (required) - handle both string and float column names
        median_col = None
        if 0.5 in quantiles:
            # Try to find the median column (could be stored as "0.5" string or 0.5 float)
            if "0.5" in pivot_df.columns:
                median_col = "0.5"
            elif 0.5 in pivot_df.columns:
                median_col = 0.5
        
        if median_col is None:
            logger.error("Median (0.5) quantile missing. Cannot calculate WIS.")
            logger.error(f"  Available quantiles: {quantiles}")
            logger.error(f"  Available columns (first 20): {list(pivot_df.columns[:20])}")
            return pd.DataFrame()

        logger.info(f"Using {len(quantiles)} quantiles for WIS: {quantiles}")

        # Vectorized WIS calculation
        # Start with median component: 0.5 * |truth - median|
        wis_scores = 0.5 * np.abs(pivot_df["truth_value"] - pivot_df[median_col])

        # Add interval components
        K = 0  # Number of intervals
        for i in range(len(quantiles) // 2):
            q_lower = quantiles[i]
            q_upper = quantiles[-(i + 1)]

            # Check if this forms a symmetric interval
            alpha = 2 * q_lower
            expected_upper = 1 - q_lower
            if abs(q_upper - expected_upper) > 0.001:  # Allow small floating point errors
                continue

            # Get interval bounds - try both numeric and string column names for robustness
            lower_vals = self._get_quantile_column(pivot_df, q_lower)
            upper_vals = self._get_quantile_column(pivot_df, q_upper)
            
            if lower_vals is None or upper_vals is None:
                logger.warning(f"  Skipping interval [{q_lower}, {q_upper}] - columns not found")
                continue
            
            truth_vals = pivot_df["truth_value"]

            # Interval Score components
            width = upper_vals - lower_vals
            under_penalty = (2 / alpha) * (lower_vals - truth_vals) * (truth_vals < lower_vals)
            over_penalty = (2 / alpha) * (truth_vals - upper_vals) * (truth_vals > upper_vals)

            interval_scores = width + under_penalty + over_penalty

            # Add weighted interval score
            wis_scores += (alpha / 2) * interval_scores
            K += 1

        # Normalize by number of intervals + 0.5
        wis_scores = wis_scores / (K + 0.5)

        # Add WIS scores to result
        pivot_df["wis"] = wis_scores

        # # Detect and log NaN/Infinity values
        # invalid_mask = ~np.isfinite(wis_scores)
        # if invalid_mask.any():
        #     invalid_count = invalid_mask.sum()
        #     logger.warning(f"[NaN/Inf Detection] Found {invalid_count} invalid WIS values")

        #     # Log details about invalid values, especially for negative horizons
        #     invalid_rows = pivot_df[invalid_mask]
        #     for idx, row in invalid_rows.head(10).iterrows():  # Limit to first 10 to avoid spam
        #         logger.warning(
        #             f"  Invalid WIS: model={row['model']}, location={row['location']}, "
        #             f"horizon={row.get('horizon', 'N/A')}, target_end_date={row['target_end_date']}, "
        #             f"truth={row['truth_value']}, wis={row['wis']}"
        #         )

        # Return only the group columns + wis
        result_cols = [c for c in group_cols if c in pivot_df.columns] + ["wis"]
        result_df = pivot_df[result_cols].copy()

        logger.info(f"Calculated WIS for {len(result_df)} prediction instances")
        return result_df

    def _calculate_mape(self, merged_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate MAPE (Mean Absolute Percentage Error) using median predictions.

        Uses the formula::

            MAPE = |truth - median| / |truth| * 100

        Rows where the truth value is zero are excluded because MAPE is
        undefined in that case. The result is stored as a percentage
        (e.g., ``3.0`` means 3%).

        Args:
            merged_df: DataFrame with merged predictions and ground-truth
                values (must contain ``truth_value`` and ``output_type_id``
                columns).

        Returns:
            pd.DataFrame: DataFrame with group columns plus a ``mape`` column.
                Returns an empty DataFrame if no median (0.5) predictions or
                no non-zero truth values are found.
        """
        logger.info("Calculating MAPE scores...")

        # Filter for median predictions (0.5 quantile)
        # Use string comparison to handle both float and string types robustly
        median_df = merged_df[(merged_df["output_type"] == "quantile") & 
                             (merged_df["output_type_id"].astype(str) == "0.5")].copy()

        if median_df.empty:
            logger.warning("No median (0.5) predictions found for MAPE calculation")
            return pd.DataFrame()

        # Filter out zero truth values (MAPE undefined)
        median_df = median_df[median_df["truth_value"] != 0].copy()

        if median_df.empty:
            logger.warning("No non-zero truth values found for MAPE calculation")
            return pd.DataFrame()

        # Calculate MAPE as percentage (multiply by 100)
        # MAPE = |truth - median| / |truth| * 100
        # This way, a 3% error is stored as 3.0, not 0.03
        median_df["mape"] = (np.abs(median_df["truth_value"] - median_df["value"]) / np.abs(median_df["truth_value"])) * 100

        # Select output columns
        output_cols = ["model", "location", "reference_date", "target_end_date", "horizon"]
        if "target" in median_df.columns:
            output_cols.append("target")
        output_cols.append("mape")

        result_df = median_df[output_cols].copy()

        logger.info(f"Calculated MAPE for {len(result_df)} prediction instances")
        return result_df

    def _calculate_coverage(self, merged_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate coverage for configured prediction interval levels.

        For each configured level (e.g., 50%, 95%), determines whether the
        truth value falls within the corresponding quantile bounds::

            coverage = 100.0  if  q_lower <= truth <= q_upper
                       0.0    otherwise

        The coverage level specified in
        :attr:`config.evaluation_coverage_level_for_location_map` is
        automatically included even if not listed in
        :attr:`config.evaluation_coverage_levels`.

        Args:
            merged_df: DataFrame with merged predictions and ground-truth
                values (must contain ``truth_value`` and quantile columns).

        Returns:
            pd.DataFrame: DataFrame with group columns plus one
                ``<level>_coverage`` column per configured level, where values
                are 0.0 or 100.0. Returns an empty DataFrame if no quantile
                predictions are found.
        """
        logger.info("Calculating Coverage scores...")

        # Get coverage levels from config
        levels = self.config.evaluation_coverage_levels or [50, 95]
        
        # Ensure location map coverage level is included (even if not in main list)
        # Convert to list of integers for processing
        levels_to_calculate = set([int(x) for x in levels])
        if hasattr(self.config, 'evaluation_coverage_level_for_location_map'):
            location_map_level = self.config.evaluation_coverage_level_for_location_map
            if location_map_level not in levels_to_calculate:
                logger.info(f"Adding location map coverage level {location_map_level}% to calculation (not in main coverage levels)")
            levels_to_calculate.add(location_map_level)
        
        # Sort for consistent processing
        levels = sorted(list(levels_to_calculate))

        # Filter for quantile predictions
        quantile_df = merged_df[merged_df["output_type"] == "quantile"].copy()
        if quantile_df.empty:
            logger.warning("No quantile predictions found for coverage calculation")
            return pd.DataFrame()

        # Group columns for pivot
        group_cols = ["model", "location", "reference_date", "target_end_date", "horizon"]
        if "target" in quantile_df.columns:
            group_cols.append("target")
        group_cols.append("truth_value")

        # Pivot to get quantiles as columns
        pivot_df = quantile_df.pivot_table(index=group_cols, columns="output_type_id", values="value").reset_index()

        # Validate pivot results
        self._validate_pivot_quantiles(pivot_df, "Coverage")

        # Calculate coverage for each level
        result_cols = [c for c in group_cols if c != "truth_value"]

        for level in levels:
            # Calculate quantiles for this coverage level
            alpha = 1.0 - (float(level) / 100.0)
            q_lower = round(alpha / 2.0, 3)
            q_upper = round(1.0 - (alpha / 2.0), 3)

            # Check if required quantiles exist - use robust column access
            lower_vals = self._get_quantile_column(pivot_df, q_lower)
            upper_vals = self._get_quantile_column(pivot_df, q_upper)
            
            if lower_vals is None or upper_vals is None:
                logger.warning(f"Quantiles {q_lower}, {q_upper} required for {level}% coverage not found in data")
                continue

            # Calculate coverage (boolean: 1 if covered, 0 if not)
            truth_vals = pivot_df["truth_value"]

            coverage_col = f"{level}_coverage"
            # Convert coverage from binary (0/1) to percentage (0-100)
            # This ensures all downstream aggregations and visualizations display coverage as a percentage
            pivot_df[coverage_col] = ((truth_vals >= lower_vals) & (truth_vals <= upper_vals)).astype(float) * 100.0
            result_cols.append(coverage_col)

        result_df = pivot_df[result_cols].copy()

        logger.info(f"Calculated coverage for {len(result_df)} prediction instances across {len(levels)} levels")
        return result_df

    def _get_quantile_column(self, df: pd.DataFrame, quantile_value: float) -> pd.Series:
        """
        Robustly retrieve a quantile column from a pivoted DataFrame.

        After pivoting, column names may be stored as either numeric floats
        or strings depending on the source data types. This helper tries both
        representations to ensure a match.

        Args:
            df: Pivoted DataFrame whose columns include quantile identifiers.
            quantile_value: The quantile level to retrieve (e.g., ``0.5``
                for the median, ``0.025`` for the lower 95% bound).

        Returns:
            pd.Series: The column data if found, or ``None`` if the quantile
                is not present under either numeric or string column names.
        """
        # Try numeric column name first
        if quantile_value in df.columns:
            return df[quantile_value]
        
        # Try string column name
        str_quantile = str(quantile_value)
        if str_quantile in df.columns:
            return df[str_quantile]
        
        # Not found
        return None

    def _validate_pivot_quantiles(self, pivoted_df: pd.DataFrame, metric_name: str = "metric") -> None:
        """
        Validate pivoted quantile data for duplicate-column issues.

        Detects cases where mixed types in the source data (e.g., both
        ``0.5`` as a float and ``"0.5"`` as a string) produce duplicate
        quantile columns after pivoting. Logs a warning when duplicates are
        found but does not raise an exception.

        Args:
            pivoted_df: Pivoted DataFrame whose columns include quantile
                identifiers (numeric or string).
            metric_name: Human-readable name of the metric being calculated,
                used in log messages for context (e.g., ``"WIS"``,
                ``"Coverage"``).
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
                f"[{metric_name}] Duplicate quantile columns detected: {duplicates}. "
                f"This indicates mixed data types (float vs string) in source data. "
                f"Data normalization should have prevented this."
            )
            # Log the actual column types for debugging
            dup_cols = [c for c in q_cols if str(c) in duplicates]
            logger.warning(f"      Duplicate column details: {[(c, type(c).__name__) for c in dup_cols]}")
