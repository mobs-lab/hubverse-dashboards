"""
Evaluation Processor for Hubverse Dashboard
Calculates model performance metrics: WIS, WIS Ratio, Coverage, and MAPE
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class EvaluationProcessor:
    """
    Processes model evaluations against target data.
    Calculates WIS, Relative WIS (if specified), Coverage, MAPE, and other scoring metrics.
    """

    def __init__(self, config, baseline_model: str):
        """
        Initialize the evaluation processor.

        Parameters:
            config: DashboardConfig object with evaluation settings
            baseline_model: Name of baseline model for relative WIS calculations
        """
        self.config = config
        self.baseline_model = baseline_model
        self.evaluation_intervals = self._get_evaluation_intervals()

    def _get_evaluation_intervals(self) -> List[int]:
        """Extract evaluation interval levels from config."""
        return [interval.level for interval in self.config.evaluation_intervals]

    def interval_score(self, observation: np.ndarray, lower: np.ndarray, upper: np.ndarray, interval_range: float) -> Dict[str, np.ndarray]:
        """
        (Copied from `epistorm-evaluations`)
        Calculate interval score components.

        Parameters:
        -----------
        observation : array_like
            Vector of observations
        lower : array_like
            Prediction for the lower quantile
        upper : array_like
            Prediction for the upper quantile
        interval_range : float
            Percentage covered by the interval (e.g., 90 for 90% interval)

        Returns:
        --------
        out : Dictionary containing interval_score, dispersion, underprediction, overprediction

        Raises
        ------
        ValueError:
            If the observation, the lower and upper vectors are not the same length or if interval_range is not
            between 0 and 100
        """
        if len(lower) != len(upper) or len(lower) != len(observation):
            raise ValueError("vector shape mismatch")
        if interval_range > 100 or interval_range < 0:
            raise ValueError("interval_range should be between 0 and 100")

        obs = np.array(observation, dtype=float)
        l = np.array(lower, dtype=float)
        u = np.array(upper, dtype=float)

        alpha = 1 - interval_range / 100  # Probability outside the interval
        dispersion = u - l
        underprediction = (2 / alpha) * (l - obs) * (obs < l)
        overprediction = (2 / alpha) * (obs - u) * (obs > u)
        score = dispersion + underprediction + overprediction

        out = {"interval_score": score, "dispersion": dispersion, "underprediction": underprediction, "overprediction": overprediction}
        return out

    def calculate_wis(self, observations: pd.Series, predictions_df: pd.DataFrame, quantile_col: str = "output_type_id") -> float:
        """
        Calculate Weighted Interval Score (WIS).

        Parameters:
        -----------
        observations : pd.Series
            Series of observed values
        predictions_df : pd.DataFrame
            DataFrame with quantile predictions (must have quantile_col and 'value' columns)
        quantile_col : str
            Name of column containing quantile levels

        Returns:
        --------
        float : WIS score
        """
        if observations.empty or predictions_df.empty:
            return np.nan

        # Ensure quantile_col exists
        if quantile_col not in predictions_df.columns:
            logger.debug(f"Column '{quantile_col}' not found in predictions dataframe")
            return np.nan

        # Get sorted quantiles (convert to float for consistency)
        quantiles = sorted([float(q) for q in predictions_df[quantile_col].unique()])

        # Extract quantile values aligned with observations
        Q = []
        for q in quantiles:
            q_vals = predictions_df[predictions_df[quantile_col] == q]["value"].values
            Q.append(q_vals)
        Q = np.array(Q)

        y = observations.values

        # Ensure dimensions match
        if Q.shape[1] != len(y):
            logger.warning(f"Shape mismatch: predictions {Q.shape[1]} vs observations {len(y)}")
            return np.nan

        # Calculate WIS
        WIS = np.zeros(len(y))
        n_quantiles = len(quantiles)

        # Sum over all interval ranges
        for i in range(n_quantiles // 2):
            interval_range = 100 * (quantiles[-i - 1] - quantiles[i])
            alpha = 1 - (quantiles[-i - 1] - quantiles[i])
            IS = self.interval_score(y, Q[i], Q[-i - 1], interval_range)
            WIS += IS["interval_score"] * alpha / 2

        # Add median absolute error (if median exists)
        median_idx = None
        for idx, q in enumerate(quantiles):
            if abs(q - 0.5) < 0.01:  # Close to 0.5
                median_idx = idx
                break

        if median_idx is not None:
            WIS += 0.5 * np.abs(Q[median_idx] - y)

        # Normalize by number of components
        K = len(self.evaluation_intervals) if self.evaluation_intervals else (n_quantiles // 2)
        WIS = WIS / (K + 0.5)

        return float(WIS[0]) if len(WIS) == 1 else float(np.mean(WIS))

    def calculate_coverage(self, observations: pd.Series, predictions_df: pd.DataFrame, interval_level: int, quantile_col: str = "output_type_id") -> float:
        """
        Calculate coverage for a specific prediction interval.

        Parameters:
        -----------
        observations : pd.Series
            Series of observed values
        predictions_df : pd.DataFrame
            DataFrame with quantile predictions
        interval_level : int
            Interval level (e.g., 50 for 50% interval)
        quantile_col : str
            Name of column containing quantile levels

        Returns:
        --------
        float : Coverage fraction (0 to 1)
        """
        if observations.empty or predictions_df.empty:
            return np.nan

        # Ensure quantile_col exists
        if quantile_col not in predictions_df.columns:
            logger.debug(f"Column '{quantile_col}' not found in predictions dataframe")
            return np.nan

        # Calculate quantile bounds for this interval
        q_low = round(0.5 - interval_level / 200, 3)
        q_upp = round(0.5 + interval_level / 200, 3)

        # Get lower and upper quantile predictions (convert quantile values to float for comparison)
        predictions_df_copy = predictions_df.copy()
        predictions_df_copy[quantile_col] = predictions_df_copy[quantile_col].astype(float)

        lower_preds = predictions_df_copy[predictions_df_copy[quantile_col] == q_low]["value"].values
        upper_preds = predictions_df_copy[predictions_df_copy[quantile_col] == q_upp]["value"].values
        obs_vals = observations.values

        if len(lower_preds) != len(obs_vals) or len(upper_preds) != len(obs_vals):
            logger.warning("Coverage calculation: length mismatch")
            return np.nan

        # Calculate fraction of observations within bounds
        within_bounds = np.logical_and(obs_vals >= lower_preds, obs_vals <= upper_preds)
        coverage = np.mean(within_bounds)

        return float(coverage)

    def calculate_mape(self, observations: pd.Series, predictions_df: pd.DataFrame, quantile_col: str = "output_type_id") -> float:
        """
        Calculate Mean Absolute Percentage Error using median prediction.

        Parameters:
        -----------
        observations : pd.Series
            Series of observed values
        predictions_df : pd.DataFrame
            DataFrame with quantile predictions
        quantile_col : str
            Name of column containing quantile levels

        Returns:
        --------
        float : MAPE value
        """
        if observations.empty or predictions_df.empty:
            return np.nan

        # Ensure quantile_col exists
        if quantile_col not in predictions_df.columns:
            logger.debug(f"Column '{quantile_col}' not found in predictions dataframe")
            return np.nan

        # Get median (0.5 quantile) predictions (convert to float for comparison)
        predictions_df_copy = predictions_df.copy()
        predictions_df_copy[quantile_col] = predictions_df_copy[quantile_col].astype(float)
        median_preds = predictions_df_copy[predictions_df_copy[quantile_col] == 0.5]["value"].values
        obs_vals = observations.values

        if len(median_preds) != len(obs_vals):
            return np.nan

        # Filter out zero observations (MAPE undefined)
        non_zero_mask = obs_vals != 0
        if not non_zero_mask.any():
            return np.nan

        obs_vals = obs_vals[non_zero_mask]
        median_preds = median_preds[non_zero_mask]

        # Calculate MAPE
        mape = np.mean(np.abs((obs_vals - median_preds) / obs_vals))

        return float(mape)

    def evaluate_predictions(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame, period_id: str) -> Dict[str, pd.DataFrame]:
        """
        Evaluate model predictions against target data for a specific period.

        Parameters:
        -----------
        target_data_df : pd.DataFrame
            Ground truth data with 'date', 'location', 'observation' columns
        model_output_df : pd.DataFrame
            Model predictions with quantiles
        period_id : str
            Forecast period identifier

        Returns:
        --------
        dict : Dictionary with 'wis', 'coverage', 'mape' DataFrames
        """
        logger.info(f"Evaluating predictions for period: {period_id}")

        wis_results = []
        coverage_results = []
        mape_results = []

        # Get unique combinations to evaluate
        if "model" not in model_output_df.columns:
            logger.warning("No 'model' column in model output data")
            return {}

        models = model_output_df["model"].unique()

        for model in models:
            model_data = model_output_df[model_output_df["model"] == model]

            # Get unique reference dates for this model
            ref_dates = model_data["reference_date"].unique()

            for ref_date in ref_dates:
                ref_date_data = model_data[model_data["reference_date"] == ref_date]

                # Get unique locations
                locations = ref_date_data["location"].unique()

                for location in locations:
                    location_data = ref_date_data[ref_date_data["location"] == location]

                    # Get unique horizons
                    horizons = location_data["horizon"].unique() if "horizon" in location_data.columns else [None]

                    for horizon in horizons:
                        if horizon is not None:
                            horizon_data = location_data[location_data["horizon"] == horizon]
                        else:
                            horizon_data = location_data

                        # Get target end date for this prediction
                        if "target_end_date" in horizon_data.columns:
                            target_dates = horizon_data["target_end_date"].unique()

                            for target_date in target_dates:
                                target_date_data = horizon_data[horizon_data["target_end_date"] == target_date]

                                # Get corresponding observation
                                obs_data = target_data_df[(target_data_df["date"] == target_date) & (target_data_df["location"] == location)]

                                if obs_data.empty or "observation" not in obs_data.columns:
                                    continue

                                observation = obs_data["observation"].iloc[0]

                                # Skip if observation is negative (missing data)
                                if observation < 0:
                                    continue

                                obs_series = pd.Series([observation])

                                # Calculate WIS
                                try:
                                    wis = self.calculate_wis(obs_series, target_date_data)
                                    if not np.isnan(wis):
                                        wis_results.append(
                                            {
                                                "model": model,
                                                "location": location,
                                                "horizon": horizon,
                                                "reference_date": ref_date,
                                                "target_end_date": target_date,
                                                "wis": wis,
                                                "period_id": period_id,
                                            }
                                        )
                                except Exception as e:
                                    logger.debug(f"WIS calculation failed for {model}/{location}/{horizon}: {e}")

                                # Calculate Coverage for each interval
                                for interval in self.evaluation_intervals:
                                    try:
                                        coverage = self.calculate_coverage(obs_series, target_date_data, interval)
                                        if not np.isnan(coverage):
                                            coverage_results.append(
                                                {
                                                    "model": model,
                                                    "location": location,
                                                    "horizon": horizon,
                                                    "reference_date": ref_date,
                                                    "target_end_date": target_date,
                                                    f"{interval}_coverage": coverage,
                                                    "period_id": period_id,
                                                }
                                            )
                                    except Exception as e:
                                        logger.debug(f"Coverage calculation failed: {e}")

                                # Calculate MAPE
                                try:
                                    mape = self.calculate_mape(obs_series, target_date_data)
                                    if not np.isnan(mape):
                                        mape_results.append(
                                            {
                                                "model": model,
                                                "location": location,
                                                "horizon": horizon,
                                                "reference_date": ref_date,
                                                "target_end_date": target_date,
                                                "mape": mape,
                                                "period_id": period_id,
                                            }
                                        )
                                except Exception as e:
                                    logger.debug(f"MAPE calculation failed: {e}")

        # Convert to DataFrames
        results = {}
        if wis_results:
            results["wis"] = pd.DataFrame(wis_results)
            logger.info(f"  ✓ Calculated {len(wis_results)} WIS scores")
        if coverage_results:
            results["coverage"] = pd.DataFrame(coverage_results)
            logger.info(f"  ✓ Calculated {len(coverage_results)} coverage values")
        if mape_results:
            results["mape"] = pd.DataFrame(mape_results)
            logger.info(f"  ✓ Calculated {len(mape_results)} MAPE values")

        return results

    def calculate_wis_ratio(self, wis_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate relative WIS (WIS ratio) compared to baseline model.

        Parameters:
        -----------
        wis_df : pd.DataFrame
            DataFrame with WIS scores

        Returns:
        --------
        pd.DataFrame : DataFrame with WIS ratio added
        """
        if wis_df.empty or "model" not in wis_df.columns:
            logger.warning("Cannot calculate WIS ratio: empty or invalid WIS dataframe")
            return pd.DataFrame()

        # Extract baseline model scores
        baseline_df = wis_df[wis_df["model"] == self.baseline_model].copy()
        if baseline_df.empty:
            logger.warning(f"Baseline model '{self.baseline_model}' not found in WIS results")
            return pd.DataFrame()

        baseline_df = baseline_df.rename(columns={"wis": "wis_baseline", "model": "baseline"})

        # Get non-baseline models
        other_models_df = wis_df[wis_df["model"] != self.baseline_model].copy()

        # Merge on common keys
        merge_keys = ["location", "target_end_date", "horizon", "reference_date"]
        merge_keys = [k for k in merge_keys if k in wis_df.columns]

        if "period_id" in wis_df.columns:
            merge_keys.append("period_id")

        wis_ratio_df = pd.merge(other_models_df, baseline_df[merge_keys + ["wis_baseline", "baseline"]], on=merge_keys, how="inner")

        # Calculate ratio
        wis_ratio_df["wis_ratio"] = wis_ratio_df["wis"] / wis_ratio_df["wis_baseline"]

        logger.info(f"  ✓ Calculated WIS ratios for {len(wis_ratio_df)} predictions")

        return wis_ratio_df
