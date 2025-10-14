"""
Generalized Data Processor for the Hubverse Dashboard

This script contains the core logic for ingesting, processing, and structuring
target data and model outputs based on a user-defined configuration.
"""

import pandas as pd
from pathlib import Path
import logging

# Assuming yaml_config_processor is in the same directory or accessible via sys.path
from yaml_config_processor import DashboardConfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, config: DashboardConfig, dev_mode: bool = False):
        self.config = config
        self.project_root = Path(__file__).parent.parent
        self.dev_mode = dev_mode
        self.historical_target_data = None
        # TODO: Handle GitHub data sources
        if self.dev_mode:
            logger.info("Running in development mode. Using 'test-data-input/' for data sources.")
            self.target_data_path = self.project_root / "test-data-input" / "target-data"
            self.model_output_path = self.project_root / "test-data-input" / "model-output"
        else:
            self.target_data_path = self.project_root / "target-data"
            self.model_output_path = self.project_root / "model-output"

    def run(self):
        """Main entry point to run the data processing pipeline."""
        logger.info("Starting data processing...")

        # 2: Data Ingestion
        target_data_df = self._load_target_data()
        model_output_df = self._load_model_output_data()

        # New step for location detection
        locations = self._detect_locations(target_data_df, model_output_df)

        # 3: Main Processing and Structuring
        processed_data = self._process_and_structure_data(target_data_df, model_output_df)

        # 4: Generate Metadata
        metadata = self._generate_metadata(locations, model_output_df, target_data_df)

        logger.info("Data processing completed.")
        # For now, we will just log a summary of the processed data.
        for period_id, dataframes in processed_data.items():
            logger.info(f"Period '{period_id}':")
            if "target_data" in dataframes:
                logger.info(f"  - Target Data: {len(dataframes['target_data'])} rows")
            if "model_output" in dataframes:
                logger.info(f"  - Model Output: {len(dataframes['model_output'])} rows")

        return True  # Indicate success

    def _load_target_data(self) -> pd.DataFrame:
        """Loads and prepares the target data."""
        logger.info("Loading target data...")
        file_format = self.config.target_data_file_format

        # Single CSV file mode for target-data
        if file_format == "csv":
            try:
                csv_file = next(self.target_data_path.glob("*.csv"))
                df = pd.read_csv(csv_file)
                logger.info(f"Loaded target data from {csv_file}")
            except StopIteration:
                raise FileNotFoundError(f"No CSV file found in {self.target_data_path}")
        elif file_format == "parquet":
            raise NotImplementedError("Parquet file format for target data is WIP.")
        else:
            raise ValueError(f"Unsupported target_data_file_format: {file_format}")

        # Rename csv file column headers from users' specifications to Hubverse standard
        mapping = self.config.column_mapping
        rename_dict = {
            mapping.date_col: "date",
            mapping.observation_col: "observation",
        }
        if mapping.location_col:
            rename_dict[mapping.location_col] = "location"
        if mapping.location_name_col:
            rename_dict[mapping.location_name_col] = "location_name"
        if mapping.target_col:
            rename_dict[mapping.target_col] = "target"
        if mapping.as_of_col:
            rename_dict[mapping.as_of_col] = "as_of"

        df.rename(columns=rename_dict, inplace=True)

        # Critical error if no date is present
        if "date" not in df.columns:
            raise ValueError(f"Date column '{mapping.date_col}' not found in target data.")

        df["date"] = pd.to_datetime(df["date"])

        # Handle historical target-data if 'as_of' column is present
        if "as_of" in df.columns:
            logger.info("Found 'as_of' column, processing historical data.")
            df["as_of"] = pd.to_datetime(df["as_of"])

            # The actual most recent "as_of" date will be used as "Ground Truth",
            # Used for the default rendered target-data lines/values for all visualizations.
            # Other historical snapshots will be shown only when "Historical Target-Data Mode" toggle is on.
            latest_as_of = df["as_of"].max()
            logger.info(f"Latest 'as_of' date is {latest_as_of.date()}. Using this for current ground truth.")
            current_df = df[df["as_of"] == latest_as_of].copy()

            # TODO: for each unique "as_of" datetime, there should exist a mapping of:
            # `historical as_of date` ---> {"date in history snapshot": `row of data`}

            return current_df

        return df

    def _load_model_output_data(self) -> pd.DataFrame:
        """Loads and prepares all model output data."""
        logger.info("Loading model output data...")
        all_model_dfs = []
        mapping = self.config.column_mapping
        rename_dict = {
            mapping.reference_date_col: "reference_date",
            mapping.target_end_date_col: "target_end_date",
            mapping.model_target_col: "target",
            mapping.horizon_col: "horizon",
            mapping.location_col: "location",
            mapping.output_type_col: "output_type",
            mapping.output_type_id_col: "output_type_id",
            mapping.value_col: "value",
        }
        # Filter out None keys from rename_dict that may result from optional config fields
        valid_rename_dict = {k: v for k, v in rename_dict.items() if k is not None}

        for model in self.config.models:
            # Checkout model's output in their designated folder
            model_dir = self.model_output_path / model.model_name
            if not model_dir.is_dir():
                logger.warning(f"Directory not found for model '{model.model_name}', skipping.")
                continue

            model_files = list(model_dir.glob("*.csv"))
            if not model_files:
                logger.warning(f"No CSV files found for model '{model.model_name}', skipping.")
                continue

            df_list = [pd.read_csv(f, low_memory=False) for f in model_files]
            model_df = pd.concat(df_list, ignore_index=True)
            model_df["model"] = model.model_name
            all_model_dfs.append(model_df)

        if not all_model_dfs:
            raise FileNotFoundError("No model output data could be loaded.")

        df = pd.concat(all_model_dfs, ignore_index=True)
        df.rename(columns=valid_rename_dict, inplace=True)

        for col in ["reference_date", "target_end_date"]:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in model output data after mapping.")

        df["reference_date"] = pd.to_datetime(df["reference_date"])
        df["target_end_date"] = pd.to_datetime(df["target_end_date"])

        time_unit = self.config.time_unit
        if "horizon" not in df.columns:
            logger.info("Calculating 'horizon' column from date differences.")
            if time_unit > 0:
                df["horizon"] = ((df["target_end_date"] - df["reference_date"]).dt.days / time_unit).astype(int)
            else:
                raise ValueError("time_unit must be greater than 0 to calculate horizon.")
        else:
            logger.info("'horizon' column already exists, using it.")

        # Pivot quantile data to wide format
        if "output_type" in df.columns and "quantile" in df["output_type"].unique():
            logger.info("Pivoting quantile data to wide format...")
            quantile_df = self._pivot_quantiles(df)
            return quantile_df
        else:
            logger.warning("No 'quantile' output_type found. Skipping quantile pivot.")

        return df

    def _pivot_quantiles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pivots the long-format quantile data into a wide format."""

        quantile_rows = df[df["output_type"] == "quantile"].copy()
        other_rows = df[df["output_type"] != "quantile"]

        if quantile_rows.empty:
            return df

        # Define index for pivoting
        index_cols = ["reference_date", "target_end_date", "location", "target", "horizon", "model"]
        # Ensure all index columns exist in the dataframe
        index_cols = [col for col in index_cols if col in quantile_rows.columns]

        # Pivot the table
        pivoted = quantile_rows.pivot_table(index=index_cols, columns="output_type_id", values="value").reset_index()

        # Rename columns to be valid identifiers (e.g., q0.5 -> q0_5)
        # Later, these will be used against user-specified "predictions interval" configs.
        pivoted.columns = [
            f"q{str(c).replace('.', '_')}" if isinstance(c, (float, str)) and str(c).replace(".", "").isnumeric() else c for c in pivoted.columns
        ]

        # Merge back with non-quantile rows if any
        if not other_rows.empty:
            final_df = pd.concat([pivoted, other_rows], ignore_index=True)
        else:
            final_df = pivoted

        return final_df

    def _detect_locations(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> list:
        """Detects all unique locations from the data."""
        logger.info("Detecting locations from data...")

        target_locations = pd.DataFrame()
        if "location" in target_data_df.columns and "location_name" in target_data_df.columns:
            target_locations = target_data_df[["location", "location_name"]].drop_duplicates()

        model_locations = pd.DataFrame()
        if "location" in model_output_df.columns:
            # In case user makes an error, we always use the FIPS mapping
            model_loc_ids = model_output_df["location"].unique()

            # Create a dataframe to merge with target locations
            model_locations_list = []
            for loc_id in model_loc_ids:
                loc_name = self.config.us_state_fips_mapping.get(str(loc_id).zfill(2), "Unknown")
                model_locations_list.append({"location": loc_id, "location_name": loc_name})
            model_locations = pd.DataFrame(model_locations_list)

        # Combine and deduplicate
        all_locations_df = pd.concat([target_locations, model_locations], ignore_index=True)
        all_locations_df.drop_duplicates(subset=["location"], keep="first", inplace=True)
        all_locations_df.sort_values(by="location", inplace=True)

        locations_list = all_locations_df.to_dict("records")
        logger.info(f"Detected {len(locations_list)} unique locations.")

        return locations_list

    def _generate_metadata(self, locations: list, model_output_df: pd.DataFrame, target_data_df: pd.DataFrame) -> dict:
        """Generates metadata for the frontend."""
        logger.info("Generating metadata...")

        # Get latest dates for special period calculations
        latest_model_ref_date = model_output_df["reference_date"].max()
        latest_target_date = target_data_df["date"].max()

        full_range_seasons_info = []
        for period in self.config.forecast_periods:
            full_range_seasons_info.append(
                {
                    "seasonId": period.period_id,
                    "displayString": period.display_string,
                    "startDate": period.start_date.isoformat(),
                    "endDate": period.end_date.isoformat(),
                }
            )

        dynamic_periods_info = []
        for period in self.config.dynamic_periods:
            anchor_config = period.time_anchor
            if not anchor_config:
                continue

            anchor_mode = anchor_config.get("anchor_mode")
            range_calc = anchor_config.get("range_calculation")
            time_unit = self.config.time_unit

            # Different date to serve as base of calculation: "latestModelOutputRefDate" or "latestTargetDataDate"
            anchor_date = latest_model_ref_date if anchor_mode == "model-output" else latest_target_date

            if pd.notna(anchor_date):
                # Calculation based on user's config
                end_date = anchor_date
                start_date = end_date + pd.Timedelta(days=range_calc * time_unit)

                dynamic_periods_info.append(
                    {
                        "label": period.period_id,
                        "displayString": period.display_string,
                        "isDynamic": True,
                        "startDate": start_date.isoformat(),
                        "endDate": end_date.isoformat(),
                    }
                )

        metadata = {
            "locations": locations,
            "fullRangeSeasons": full_range_seasons_info,
            "dynamicTimePeriod": dynamic_periods_info,
            "modelNames": [model.model_name for model in self.config.models],
            "defaultSelectedDate": latest_model_ref_date.isoformat() if pd.notna(latest_model_ref_date) else None,
        }

        logger.info("Metadata generated.")
        # Only produce log for now for metadata.
        logger.info(f"Default selected date for frontend: {metadata['defaultSelectedDate']}")
        return metadata

    def _process_and_structure_data(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> dict:
        """Filters and structures data for each forecast period."""
        logger.info("Processing and structuring data by forecast period...")
        processed_data = {}
        all_periods = self.config.forecast_periods + self.config.dynamic_periods

        for period in all_periods:
            if period.is_special_period:
                anchor_config = period.time_anchor
                if not anchor_config:
                    logger.warning(f"Special period '{period.period_id}' is missing time_anchor config. Skipping.")
                    continue

                anchor_mode = anchor_config.get("anchor_mode")
                range_calc = anchor_config.get("range_calculation")
                time_unit = self.config.time_unit

                if anchor_mode == "model-output":
                    anchor_date = model_output_df["reference_date"].max()
                elif anchor_mode == "target-data":
                    anchor_date = target_data_df["date"].max()
                else:
                    logger.warning(f"Invalid anchor_mode '{anchor_mode}' for special period '{period.period_id}'. Skipping.")
                    continue

                if pd.isna(anchor_date):
                    logger.warning(f"Could not determine anchor date for special period '{period.period_id}'. Skipping.")
                    continue

                end = anchor_date
                start = end + pd.Timedelta(days=range_calc * time_unit)
            else:
                start = period.start_date
                end = period.end_date

            logger.info(f"Processing data for period: '{period.period_id}' ({start.date()} to {end.date()})")

            # Filter out target-data and model-output that fall in the current processing forecast period
            period_target_data = target_data_df[(target_data_df["date"] >= start) & (target_data_df["date"] <= end)].copy()
            period_model_output = model_output_df[(model_output_df["reference_date"] >= start) & (model_output_df["reference_date"] <= end)].copy()

            # Multiple target
            valid_model_targets = []
            for target in self.config.targets:
                if period.period_id in target.forecast_periods:
                    valid_model_targets.append(target.corresponding_key_in_model_output)

            if not self.config.is_single_target and "target" in period_model_output.columns:
                # Instead of just filtering, we group the output by target
                grouped_by_target = dict(tuple(period_model_output.groupby("target")))

                # Filter to only include valid targets for the period
                period_model_output_by_target = {target: df for target, df in grouped_by_target.items() if target in valid_model_targets}

                processed_data[period.period_id] = {"target_data": period_target_data, "model_output_by_target": period_model_output_by_target}
            else:
                processed_data[period.period_id] = {"target_data": period_target_data, "model_output": period_model_output}

        return processed_data


def process_data(config: DashboardConfig, dev_mode: bool = False):
    """
    Main function to instantiate and run the data processor.
    This will be called by the main workflow orchestrator.
    """
    try:
        processor = DataProcessor(config, dev_mode=dev_mode)
        success = processor.run()
        if not success:
            raise RuntimeError("Data processing failed.")
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        raise
