"""
Generalized Data Processor for the Hubverse Dashboard

This script contains the core logic for ingesting, processing, and structuring
target data and model outputs based on a user-defined configuration.
"""

import pandas as pd
from pathlib import Path
import logging
import json
import numpy as np

# Assuming yaml_config_processor is in the same directory or accessible via sys.path
from yaml_config_processor import DashboardConfig
from evaluation_processor import EvaluationProcessor

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class NpEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy and Pandas types."""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super(NpEncoder, self).default(obj)


class DataProcessor:
    def __init__(
        self,
        config: DashboardConfig,
        dev_mode: bool = False,
        skip_evaluations: bool = False,
    ):
        self.config = config
        self.project_root = Path(__file__).parent.parent
        self.dev_mode = dev_mode
        self.skip_evaluations = skip_evaluations
        self.historical_target_data = None  # Map<as_of_date, Map<date, data>>
        self.current_target_data = None
        self.fixed_target_data = None
        self.model_output_unpivoted = None  # Keep unpivoted data for evaluations

        # Create a mapping from the raw target key in data to the corresponding targetId
        self.target_key_to_id_map = {
            t.target_key_in_data: t.target_id for t in self.config.targets
        }

        # Initialize evaluation processor (only if evaluations are enabled)
        if not skip_evaluations:
            self.evaluation_processor = EvaluationProcessor(config=config, baseline_model=config.baseline_model_for_relative_wis)
        else:
            self.evaluation_processor = None

        # For tracking processing statistics
        self.processing_stats = {
            "target_data_rows": 0,
            "model_output_rows": 0,
            "locations_detected": 0,
            "models_processed": 0,
            "forecast_periods": 0,
            "files_written": 0,
            "output_files": [],
            "evaluations_calculated": 0,
        }

        # TODO: Handle GitHub data sources
        if self.dev_mode:
            logger.info("=" * 60)
            logger.info("   DEVELOPMENT MODE ENABLED")
            logger.info("=" * 60)
            logger.info("Reading data from: test-data-input/")
            logger.info("Writing output to: test-data-output/")
            self.target_data_path = self.project_root / "test-data-input" / "target-data"
            self.model_output_path = self.project_root / "test-data-input" / "model-output"
            self.output_base_path = self.project_root / "public" / "test-data-output"
        else:
            self.target_data_path = self.project_root / "target-data"
            self.model_output_path = self.project_root / "model-output"
            self.output_base_path = self.project_root / "public" / "data"

    def run(self):
        """Main entry point to run the data processing pipeline."""
        logger.info("Starting data processing...")

        # 2: Data Ingestion
        target_data_df = self._load_target_data()
        self.processing_stats["target_data_rows"] = len(target_data_df)

        model_output_df = self._load_model_output_data()
        self.processing_stats["model_output_rows"] = len(model_output_df)

        # 2b: Fix missing time intervals
        fixed_target_data_df = self._fix_missing_time_intervals(target_data_df, model_output_df)
        self.fixed_target_data = fixed_target_data_df

        # 3: Location detection
        locations = self._detect_locations(target_data_df, model_output_df)
        self.processing_stats["locations_detected"] = len(locations)

        # 4: Process target data by forecast periods
        target_data_by_period = self._process_target_data_by_forecast_periods(fixed_target_data_df, model_output_df)

        # 5: Process model output data by forecast periods
        model_output_by_period = self._process_model_output_by_periods(model_output_df, fixed_target_data_df)
        self.processing_stats["forecast_periods"] = len(model_output_by_period)

        # 6: Calculate evaluations (use unpivoted data for evaluations)
        if not self.skip_evaluations:
            evaluations_by_period = self._process_evaluations_by_periods(fixed_target_data_df, self.model_output_unpivoted)
        else:
            logger.info("Skipping evaluation calculations (disabled by user)")
            evaluations_by_period = {}

        # 7: Generate Metadata
        metadata = self._generate_metadata(locations, model_output_df, target_data_df)

        # 8: Write output files
        self._write_output_files(
            target_data_by_period,
            model_output_by_period,
            metadata,
            evaluations_by_period,
        )

        # 8: Print summary
        # self._print_processing_summary(target_data_df, model_output_df, metadata)

        logger.info("Data processing completed successfully.")
        return True  # Indicate success

    def _load_target_data(self) -> pd.DataFrame:
        """Loads and prepares the target data."""
        logger.info("Loading target data...")
        logger.info(f"  → Looking in: {self.target_data_path}")
        file_format = self.config.target_data_file_format

        # Single CSV file mode for target-data
        if file_format == "csv":
            try:
                file_name = self.config.single_target_data_file_name

                csv_file = self.target_data_path / f"{file_name}.csv"
                if not csv_file.exists():
                    raise FileNotFoundError(f"Target data file not found: {file_name}.csv in {self.target_data_path}")

                df = pd.read_csv(csv_file)
                logger.info(f"  ✓ Loaded target data from {csv_file.name}")
                logger.info(f"  ✓ Shape: {df.shape[0]} rows × {df.shape[1]} columns")
            except ValueError as e:
                raise e
            except FileNotFoundError as e:
                raise e
            except Exception as e:
                raise RuntimeError(f"Error loading CSV file: {e}")
        elif file_format == "parquet":
            # Check if using partitioned parquet format
            if self.config.is_partitioned_parquet:
                # Partitioned mode: each subdirectory represents an as_of date
                logger.info("  → Using partitioned parquet format")
                df = self._load_partitioned_parquet()
            else:
                # Single file mode - use the configured file name
                try:
                    file_name = self.config.single_target_data_file_name
                    if not file_name:
                        raise ValueError("single_target_data_file_name is required in config for non-partitioned parquet")

                    # Try .parquet extension first
                    parquet_file = self.target_data_path / f"{file_name}.parquet"
                    if not parquet_file.exists():
                        # Try .pq extension
                        parquet_file = self.target_data_path / f"{file_name}.pq"

                    if not parquet_file.exists():
                        raise FileNotFoundError(f"Target data file not found: {file_name}.parquet or {file_name}.pq in {self.target_data_path}")

                    df = pd.read_parquet(parquet_file)
                    logger.info(f"  ✓ Loaded target data from {parquet_file.name}")
                    logger.info(f"  ✓ Shape: {df.shape[0]} rows × {df.shape[1]} columns")
                except Exception as e:
                    raise RuntimeError(f"Error loading parquet file: {e}")
        else:
            raise ValueError(f"Unsupported target_data_file_format: {file_format}")

        # Rename csv file column headers from users' specifications to Hubverse standard
        mapping = self.config.column_mapping

        # Log available columns for debugging
        logger.info(f"  → Available columns in target data: {df.columns.tolist()}")

        # Build rename dict and validate that required columns exist
        rename_dict = {}

        # Required columns
        if mapping.date_col not in df.columns:
            raise ValueError(f"Date column '{mapping.date_col}' not found in target data. Available columns: {df.columns.tolist()}")
        rename_dict[mapping.date_col] = "date"

        if mapping.observation_col not in df.columns:
            raise ValueError(f"Observation column '{mapping.observation_col}' not found in target data. Available columns: {df.columns.tolist()}")
        rename_dict[mapping.observation_col] = "observation"

        # Optional columns
        if mapping.location_col and mapping.location_col in df.columns:
            rename_dict[mapping.location_col] = "location"
        if mapping.location_name_col and mapping.location_name_col in df.columns:
            rename_dict[mapping.location_name_col] = "location_name"
        if mapping.target_col and mapping.target_col in df.columns:
            rename_dict[mapping.target_col] = "target"
        if mapping.as_of_col and mapping.as_of_col in df.columns:
            rename_dict[mapping.as_of_col] = "as_of"

        df.rename(columns=rename_dict, inplace=True)
        logger.info(f"  ✓ Renamed columns: {list(rename_dict.keys())} → {list(rename_dict.values())}")

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

            # Process historical snapshots: Map<as_of_date, Map<date, Map<location, data>>>
            logger.info("Processing historical target data snapshots...")
            self.historical_target_data = self._process_historical_target_data(df)
            logger.info(f"Processed {len(self.historical_target_data)} historical snapshots.")

            return current_df

        return df

    def _load_partitioned_parquet(self) -> pd.DataFrame:
        """
        Load partitioned parquet files where each subdirectory represents an as_of date.
        Returns:
            Combined DataFrame with as_of column added
        """
        import re
        from datetime import datetime

        all_partitions = []

        # Find all subdirectories
        subdirs = [d for d in self.target_data_path.iterdir() if d.is_dir()]

        if not subdirs:
            raise FileNotFoundError(f"No subdirectories found in {self.target_data_path} for partitioned parquet mode")

        logger.info(f"  → Found {len(subdirs)} partition directories")

        for subdir in sorted(subdirs):
            # Extract as_of date from directory name
            dir_name = subdir.name

            # Try different date formats
            # Format 1: "as_of=2024-01-01" (Hive-style partitioning)

            as_of_date = None

            # Try Hive-style: as_of=YYYY-MM-DD
            match = re.match(r"as_of[=_](\d{4}-\d{2}-\d{2})", dir_name)
            if match:
                as_of_date = match.group(1)

            if not as_of_date:
                logger.warning(f"  ⚠ Could not parse as_of date from directory: {dir_name}, skipping")
                continue

            # Validate date format
            try:
                pd.to_datetime(as_of_date)
            except:
                logger.warning(f"  ⚠ Invalid date format in directory: {dir_name}, skipping")
                continue

            # Look for parquet files in this subdirectory
            parquet_files = list(subdir.glob("*.parquet")) + list(subdir.glob("*.pq"))

            if not parquet_files:
                logger.warning(f"  ⚠ No parquet files found in {dir_name}, skipping")
                continue

            # Read all parquet files in this partition
            partition_dfs = []
            for pq_file in parquet_files:
                try:
                    partition_df = pd.read_parquet(pq_file)
                    partition_dfs.append(partition_df)
                except Exception as e:
                    logger.warning(f"  ⚠ Error reading {pq_file}: {e}, skipping")
                    continue

            if partition_dfs:
                # Combine files from this partition
                partition_combined = pd.concat(partition_dfs, ignore_index=True)

                # Add as_of column
                partition_combined["as_of"] = as_of_date

                all_partitions.append(partition_combined)
                logger.info(f"  ✓ Loaded partition {dir_name}: {len(partition_combined)} rows")

        if not all_partitions:
            raise RuntimeError("No valid partitions could be loaded from partitioned parquet format")

        # Combine all partitions
        combined_df = pd.concat(all_partitions, ignore_index=True)
        logger.info(f"  ✓ Combined all partitions: {combined_df.shape[0]} rows × {combined_df.shape[1]} columns")
        logger.info(f"  ✓ Found {len(all_partitions)} as_of snapshots")

        return combined_df

    def _load_model_output_data(self) -> pd.DataFrame:
        """Loads and prepares all model output data."""
        logger.info("Loading model output data...")
        logger.info(f"  → Looking in: {self.model_output_path}")
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
                logger.warning(f"  ✗ Directory not found for model '{model.model_name}', skipping.")
                continue

            model_files = list(model_dir.glob("*.csv"))
            if not model_files:
                logger.warning(f"  ✗ No CSV files found for model '{model.model_name}', skipping.")
                continue

            logger.info(f"  ✓ Loading model '{model.model_name}': {len(model_files)} files")
            df_list = [pd.read_csv(f, low_memory=False) for f in model_files]
            model_df = pd.concat(df_list, ignore_index=True)
            model_df["model"] = model.model_name
            all_model_dfs.append(model_df)
            self.processing_stats["models_processed"] += 1

        if not all_model_dfs:
            raise FileNotFoundError("No model output data could be loaded.")

        df = pd.concat(all_model_dfs, ignore_index=True)
        logger.info(f"  ✓ Combined shape: {df.shape[0]} rows × {df.shape[1]} columns")
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

        # Store unpivoted data for evaluations (before pivoting)
        self.model_output_unpivoted = df.copy()
        logger.info("Stored unpivoted model output data for evaluations")

        # Pivot quantile data to wide format for predictions output
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

    def _process_historical_target_data(self, df: pd.DataFrame) -> dict:
        """
        Process historical target data into nested dictionary structure.
        Returns: Map<as_of_date, Map<date, Map<location, data>>>
        """
        historical_data = {}

        # Get all unique as_of dates
        unique_as_of_dates = df["as_of"].unique()

        for as_of_date in unique_as_of_dates:
            as_of_iso = pd.to_datetime(as_of_date).strftime("%Y-%m-%d")
            snapshot_df = df[df["as_of"] == as_of_date]

            # Group all date values
            date_map = {}
            for date in snapshot_df["date"].unique():
                date_iso = pd.to_datetime(date).strftime("%Y-%m-%d")
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

                    # Add target if available, mapping to target_id for consistency
                    if "target" in row and pd.notna(row["target"]):
                        raw_target_key = str(row["target"])
                        target_id = self.target_key_to_id_map.get(raw_target_key, raw_target_key)
                        data_entry["target"] = target_id

                    location_map[location_key] = data_entry

                date_map[date_iso] = location_map

            historical_data[as_of_iso] = date_map

        return historical_data

    def _fix_missing_time_intervals(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills in missing time intervals in target data based on time_unit.
        Instead of hard-coding Saturdays, uses the configured time_unit to generate appropriate intervals.
        """
        logger.info("Fixing missing time intervals in target data...")

        # Determine the overall date range
        earliest_target_date = target_data_df["date"].min()
        latest_target_date = target_data_df["date"].max()
        latest_model_date = model_output_df["target_end_date"].max()

        earliest_date = earliest_target_date
        latest_date = max(latest_target_date, latest_model_date) if pd.notna(latest_model_date) else latest_target_date

        logger.info(f"Date range: {earliest_date.date()} to {latest_date.date()}")

        # Generate complete date range based on time_unit
        time_unit_days = self.config.time_unit
        date_range = pd.date_range(start=earliest_date, end=latest_date, freq=f"{time_unit_days}D")

        # Get all unique locations
        if "location" in target_data_df.columns:
            all_locations = target_data_df["location"].unique()
        else:
            all_locations = ["US"]  # Default single location

        # Create complete grid of dates x locations
        complete_grid = pd.MultiIndex.from_product([date_range, all_locations], names=["date", "location"])
        complete_df = pd.DataFrame(index=complete_grid).reset_index()

        # Merge with existing target data
        merge_cols = ["date", "location"] if "location" in target_data_df.columns else ["date"]
        if "location" not in target_data_df.columns:
            complete_df = pd.DataFrame({"date": date_range})
            merge_cols = ["date"]

        fixed_df = pd.merge(complete_df, target_data_df, on=merge_cols, how="left")

        # Fill missing observation values with -1 (indicating no data)
        fixed_df["observation"] = fixed_df["observation"].fillna(-1)

        logger.info(f"Fixed target data shape: {fixed_df.shape}")

        return fixed_df

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
                loc_name = self.config.location_mapping.get(str(loc_id), "Unknown")
                model_locations_list.append({"location": loc_id, "location_name": loc_name})
            model_locations = pd.DataFrame(model_locations_list)

        # Combine and deduplicate
        all_locations_df = pd.concat([target_locations, model_locations], ignore_index=True)
        all_locations_df.drop_duplicates(subset=["location"], keep="first", inplace=True)
        all_locations_df.sort_values(by="location", inplace=True)

        locations_list = all_locations_df.to_dict("records")
        logger.info(f"Detected {len(locations_list)} unique locations.")

        return locations_list

    def _generate_metadata(
        self,
        locations_info: list,
        model_output_df: pd.DataFrame,
        target_data_df: pd.DataFrame,
    ) -> dict:
        """Generates metadata for the frontend."""
        logger.info("Generating metadata...")

        # Get the date range
        all_dates = pd.concat([target_data_df["date"], model_output_df["target_end_date"]]).dropna()
        earliest_date = all_dates.min()
        latest_date = all_dates.max()

        # Get the latest reference date across all models, for default selection
        # Since all the models are toggled on by default, visualization guaranteed has prediction line thus
        latest_model_ref_date = model_output_df["reference_date"].max()

        # Build forecast period info
        forecast_periods_info = []
        for idx, period in enumerate(self.config.forecast_periods):
            forecast_periods_info.append(
                {
                    "forecastPeriodId": period.period_id,
                    "displayString": period.display_string,
                    "timeValue": f"{period.start_date.date()}/{period.end_date.date()}",
                    "startDate": period.start_date.isoformat(),
                    "endDate": period.end_date.isoformat(),
                    "isDefaultSelected": getattr(period, "is_default_selected", False),
                }
            )

        # Dynamic/special forecast periods
        for period in self.config.dynamic_periods:
            period_meta = {
                "forecastPeriodId": period.period_id,
                "displayString": period.display_string,
                "timeValue": f"{period.start_date.date()}/{period.end_date.date()}",
                "startDate": period.start_date.isoformat(),
                "endDate": period.end_date.isoformat(),
                "isDefaultSelected": False,
                "isDynamic": True,
                "isSpecial": True,
            }
            forecast_periods_info.append(period_meta)

        # Build targets info
        targets_info = []
        for target in self.config.targets:
            targets_info.append(
                {
                    "targetId": target.target_id,
                    "targetKeyInData": target.target_key_in_data,
                    "displayString": target.task_display_string,
                    "forecastPeriods": target.forecast_periods,
                }
            )

        prediction_intervals_info = []
        for interval in self.config.prediction_intervals:
            prediction_intervals_info.append(
                {
                    "level": interval.level,
                    "quantiles": interval.output_type_ids,
                }
            )

        model_configs = []
        model_colors = {}
        for model in self.config.models:
            model_configs.append({"modelName": model.model_name, "color": model.color_hex})
            model_colors[model.model_name] = model.color_hex

        # NOTE: Metadata design here
        metadata = {
            # === FEATURE FLAGS FOR FRONTEND ===
            "features": {
                "evaluationsEnabled": not self.skip_evaluations,
                "historicalTargetDataEnabled": self.historical_target_data is not None,
            },
            # === SPATIAL CONFIGURATION ===
            "spatial": {
                "isSingleLocation": self.config.is_single_location,
                "singleLocationCode": self.config.single_location_mapping if self.config.is_single_location else None,
                "disableMapInDashboard": self.config.spatial_config.disable_map,
                "customShapeFileName": self.config.spatial_config.custom_shape_file_name,
                "locationCodeHeader": self.config.spatial_config.location_code_col_header,
                "locationNameHeader": self.config.spatial_config.location_name_col_header,
                "locationMappingList": locations_info,
            },
            # === TEMPORAL CONFIGURATION ===
            "temporal": {
                "timeUnit": self.config.time_unit,
                "horizons": self.config.horizons,
                "earliestDate": earliest_date.isoformat() if pd.notna(earliest_date) else None,
                "latestDate": latest_date.isoformat() if pd.notna(latest_date) else None,
                "defaultSelectedDate": latest_model_ref_date.isoformat() if pd.notna(latest_model_ref_date) else None,
            },
            # === FORECAST PERIODS ===
            "forecastPeriods": forecast_periods_info,
            # === MODELS ===
            "models": {
                "list": model_configs,
                "colors": model_colors,
                "baselineModel": self.config.baseline_model_for_relative_wis if not self.skip_evaluations else None,
            },
            # === TARGETS ===
            "targets": {
                "list": targets_info,
                "isSingleTarget": self.config.is_single_target,
                "defaultTargetId": targets_info[0]["targetId"] if targets_info else None,
            },
            # === PREDICTION INTERVALS ===
            "predictionIntervals": {
                "available": prediction_intervals_info,
                "defaults": ["90"],  # Could make this configurable in the future
            },
            # === DATA FILES MANIFEST ===
            "dataManifest": {
                "forecastPeriods": [p["forecastPeriodId"] for p in forecast_periods_info],
                "hasHistoricalData": self.historical_target_data is not None,
                "hasEvaluations": not self.skip_evaluations,
            },
            # === COLUMN MAPPINGS (for debugging/reference) ===
            "columnMappings": {
                "targetData": {
                    "date": self.config.column_mapping.date_col,
                    "observation": self.config.column_mapping.observation_col,
                    "location": self.config.column_mapping.location_col,
                    "locationName": self.config.column_mapping.location_name_col,
                    "target": self.config.column_mapping.target_col,
                    "asOf": self.config.column_mapping.as_of_col,
                },
                "modelOutput": {
                    "referenceDate": self.config.column_mapping.reference_date_col,
                    "targetEndDate": self.config.column_mapping.target_end_date_col,
                    "target": self.config.column_mapping.model_target_col,
                    "horizon": self.config.column_mapping.horizon_col,
                    "location": self.config.column_mapping.location_col,
                    "outputType": self.config.column_mapping.output_type_col,
                    "outputTypeId": self.config.column_mapping.output_type_id_col,
                    "value": self.config.column_mapping.value_col,
                },
            },
            # === METADATA INFO ===
            "_meta": {
                "generatedAt": pd.Timestamp.now().isoformat(),
                "dataProcessor": {
                    "skipEvaluations": self.skip_evaluations,
                    "devMode": self.dev_mode,
                },
            },
        }

        logger.info("Metadata generated.")
        # Only produce log for now for metadata.
        logger.info(f"Default selected date for frontend: {metadata['temporal']['defaultSelectedDate']}")
        logger.info(f"Evaluations enabled: {metadata['features']['evaluationsEnabled']}")
        return metadata

    def _process_target_data_by_forecast_periods(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> dict:
        """
        Structure ground truth (target) data by forecast periods.
        Returns: Map<period_id, Map<location, Map<date, Map<target, data>>>>
        """
        logger.info("Processing ground truth data by forecast periods...")
        ground_truth_by_period = {}
        all_periods = self.config.forecast_periods + self.config.dynamic_periods

        for period in all_periods:
            date_range = self._get_period_date_range(period, target_data_df, model_output_df)
            if not date_range:
                continue
            start, end = date_range

            logger.info(f"Processing ground truth for period: '{period.period_id}' ({start.date()} to {end.date()})")

            # Filter target data for this period
            period_target_data = target_data_df[(target_data_df["date"] >= start) & (target_data_df["date"] <= end)].copy()

            # Structure as Map<location, Map<date, Map<target, data>>>
            period_dict = {}

            group_cols = []
            if "location" in period_target_data.columns:
                group_cols.append("location")
            if "date" in period_target_data.columns:
                group_cols.append("date")
            if "target" in period_target_data.columns:
                group_cols.append("target")

            # Efficiently group and structure data
            for _, row in period_target_data.iterrows():
                location_key = str(row.get("location", "US")).zfill(2) if "location" in row else "US"
                date_iso = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
                target_key = str(row.get("target", self.config.targets[0].target_id if self.config.targets else "default"))

                data_entry = {
                    "observation": float(row["observation"]) if pd.notna(row["observation"]) and row["observation"] >= 0 else None,
                }

                if "location_name" in row and pd.notna(row["location_name"]):
                    data_entry["location_name"] = str(row["location_name"])

                # Create nested dictionaries
                if location_key not in period_dict:
                    period_dict[location_key] = {}
                if date_iso not in period_dict[location_key]:
                    period_dict[location_key][date_iso] = {}

                # Use the canonical targetId as the key
                raw_target_key = str(
                    row.get(
                        'target',
                        self.config.targets[0].target_key_in_data
                        if self.config.targets
                        else 'default',
                    )
                )
                target_id = self.target_key_to_id_map.get(raw_target_key, raw_target_key)

                period_dict[location_key][date_iso][target_id] = data_entry

            ground_truth_by_period[period.period_id] = period_dict

        logger.info(f"Processed ground truth for {len(ground_truth_by_period)} periods.")
        return ground_truth_by_period

    def _process_model_output_by_periods(self, model_output_df: pd.DataFrame, target_data_df: pd.DataFrame) -> dict:
        """
        Structure model predictions output by forecast periods.
        Returns: Map<period_id, Map<model, Map<location, Map<reference_date, Map<target_date, predictions>>>>>
        """
        logger.info("Processing predictions data by forecast periods...")
        model_output_by_period = {}
        all_periods = self.config.forecast_periods + self.config.dynamic_periods

        for period in all_periods:
            date_range = self._get_period_date_range(period, target_data_df, model_output_df)
            if not date_range:
                continue
            start, end = date_range

            logger.info(f"Processing predictions for period: '{period.period_id}' ({start.date()} to {end.date()})")

            # Filter model output for this period
            period_model_output = model_output_df[(model_output_df["reference_date"] >= start) & (model_output_df["reference_date"] <= end)].copy()

            # Get valid targets for this period
            valid_model_targets = []
            for target in self.config.targets:
                if period.period_id in target.forecast_periods:
                    valid_model_targets.append(target.target_key_in_data)

            # Filter by valid targets if not single target mode
            if not self.config.is_single_target and "target" in period_model_output.columns and valid_model_targets:
                period_model_output = period_model_output[period_model_output["target"].isin(valid_model_targets)]

            # Structure as Map<model, Map<location, Map<reference_date, Map<target_date, prediction>>>>
            period_dict = {}

            for model_name in period_model_output["model"].unique():
                model_data = period_model_output[period_model_output["model"] == model_name]
                model_dict = {}

                for location in model_data["location"].unique():
                    location_key = str(location).zfill(2)
                    location_data = model_data[model_data["location"] == location]
                    location_dict = {}

                    for ref_date in location_data["reference_date"].unique():
                        ref_date_iso = pd.to_datetime(ref_date).strftime("%Y-%m-%d")
                        ref_date_data = location_data[location_data["reference_date"] == ref_date]
                        predictions_dict = {}

                        for _, row in ref_date_data.iterrows():
                            target_date_iso = pd.to_datetime(row["target_end_date"]).strftime("%Y-%m-%d")

                            pred_entry = {
                                "horizon": int(row["horizon"]) if pd.notna(row["horizon"]) else None,
                            }

                            quantile_cols = [col for col in row.index if col.startswith("q")]
                            for q_col in quantile_cols:
                                if pd.notna(row[q_col]):
                                    pred_entry[q_col] = float(row[q_col])

                            if "target" in row and pd.notna(row["target"]):
                                raw_target_key = str(row["target"])
                                target_id = self.target_key_to_id_map.get(
                                    raw_target_key, raw_target_key
                                )
                                pred_entry["targetId"] = target_id

                            predictions_dict[target_date_iso] = pred_entry

                        location_dict[ref_date_iso] = {"predictions": predictions_dict}

                    model_dict[location_key] = location_dict

                period_dict[model_name] = model_dict

            model_output_by_period[period.period_id] = period_dict

        logger.info(f"Processed predictions for {len(model_output_by_period)} periods.")
        return model_output_by_period

    def _process_evaluations_by_periods(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> dict:
        """
        Calculate evaluation metrics for each forecast period.
        Returns: Map<period_id, Map<metric_type, DataFrame>>
        """
        logger.info("Processing evaluations by forecast periods...")
        evaluations_by_period = {}
        all_periods = self.config.forecast_periods + self.config.dynamic_periods

        for period in all_periods:
            date_range = self._get_period_date_range(period, target_data_df, model_output_df)
            if not date_range:
                continue
            start, end = date_range

            logger.info(f"Calculating evaluations for period: '{period.period_id}' ({start.date()} to {end.date()})")

            # Filter data for this period
            period_target_data = target_data_df[(target_data_df["date"] >= start) & (target_data_df["date"] <= end)].copy()

            period_model_output = model_output_df[(model_output_df["reference_date"] >= start) & (model_output_df["reference_date"] <= end)].copy()

            # Get valid targets for this period
            valid_model_targets = []
            for target in self.config.targets:
                if period.period_id in target.forecast_periods:
                    valid_model_targets.append(target.target_key_in_data)

            # Filter by valid targets if not single target mode
            if not self.config.is_single_target and "target" in period_model_output.columns and valid_model_targets:
                period_model_output = period_model_output[period_model_output["target"].isin(valid_model_targets)]

            if period_target_data.empty or period_model_output.empty:
                logger.warning(f"No data available for evaluations in period '{period.period_id}'")
                continue

            # Calculate evaluation metrics
            evaluation_results = self.evaluation_processor.evaluate_predictions(
                target_data_df=period_target_data,
                model_output_df=period_model_output,
                period_id=period.period_id,
            )

            # Calculate WIS ratio if WIS results exist
            if "wis" in evaluation_results and not evaluation_results["wis"].empty:
                wis_ratio_df = self.evaluation_processor.calculate_wis_ratio(evaluation_results["wis"])
                if not wis_ratio_df.empty:
                    evaluation_results["wis_ratio"] = wis_ratio_df

            evaluations_by_period[period.period_id] = evaluation_results

            # Update statistics
            total_evals = sum(len(df) for df in evaluation_results.values() if isinstance(df, pd.DataFrame))
            self.processing_stats["evaluations_calculated"] += total_evals

        logger.info(f"Processed evaluations for {len(evaluations_by_period)} periods.")
        logger.info(f"Total evaluations calculated: {self.processing_stats['evaluations_calculated']}")
        return evaluations_by_period

    def _write_output_files(
        self,
        target_data_by_period: dict,
        model_output_by_period: dict,
        metadata: dict,
        evaluations_by_period: dict = None,
    ):
        """Write all processed data to JSON files."""
        logger.info("Writing output files...")
        logger.info(f"  → Output directory: {self.output_base_path}")

        # Create directory structure
        auxiliary_dir = self.output_base_path / "auxiliary"
        auxiliary_dir.mkdir(exist_ok=True, parents=True)

        dynamic_dir = self.output_base_path / "dynamic-time-periods"
        dynamic_dir.mkdir(exist_ok=True, parents=True)

        historical_dir = self.output_base_path / "historical-target-data"
        historical_dir.mkdir(exist_ok=True, parents=True)

        # Write auxiliary data
        logger.info("Writing auxiliary data files...")

        # Write metadata data into one file
        metadata_file = auxiliary_dir / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, separators=(",", ":"))
        self._track_file_written(metadata_file)

        logger.info("  ✓ Written auxiliary data: metadata")

        # Write historical ground truth data if available
        if self.historical_target_data:
            logger.info("Writing historical ground truth data...")
            historical_file = historical_dir / "historical-target-data.json"
            with open(historical_file, "w") as f:
                json.dump(self.historical_target_data, f, cls=NpEncoder, separators=(",", ":"))
            self._track_file_written(historical_file)
            logger.info(f"  ✓ Written historical data: {len(self.historical_target_data)} snapshots")

        # Write data for each forecast period
        logger.info("Writing forecast period data...")

        # Separate full range periods from dynamic periods
        full_range_periods = [p for p in self.config.forecast_periods if not p.is_special_period]
        dynamic_periods = self.config.dynamic_periods

        # Write full range season data
        for period in full_range_periods:
            period_id = period.period_id
            period_dir = self.output_base_path / period_id
            period_dir.mkdir(exist_ok=True, parents=True)

            logger.info(f"Writing data for period: {period_id}")

            # Write ground truth data
            if period_id in target_data_by_period:
                gt_file = period_dir / "targetData.json"
                with open(gt_file, "w") as f:
                    json.dump(
                        target_data_by_period[period_id],
                        f,
                        cls=NpEncoder,
                        separators=(",", ":"),
                    )
                self._track_file_written(gt_file)

            # Write predictions data
            if period_id in model_output_by_period:
                pred_file = period_dir / "modelOutputData.json"
                with open(pred_file, "w") as f:
                    json.dump(
                        model_output_by_period[period_id],
                        f,
                        cls=NpEncoder,
                        separators=(",", ":"),
                    )
                self._track_file_written(pred_file)

            logger.info(f"  ✓ Written data files for {period_id}")

        # Write dynamic period data
        for period in dynamic_periods:
            period_id = period.period_id

            logger.info(f"Writing data for dynamic period: {period_id}")

            # Dynamic periods get a single combined JSON file
            dynamic_data = {}

            if period_id in target_data_by_period:
                dynamic_data["groundTruth"] = target_data_by_period[period_id]

            if period_id in model_output_by_period:
                dynamic_data["predictions"] = model_output_by_period[period_id]

            dynamic_file = dynamic_dir / f"{period_id}.json"
            with open(dynamic_file, "w") as f:
                json.dump(dynamic_data, f, cls=NpEncoder, separators=(",", ":"))
            self._track_file_written(dynamic_file)

            logger.info(f"  ✓ Written {period_id}.json")

        # Write evaluation data if available
        if evaluations_by_period:
            logger.info("Writing evaluation data...")

            for period in full_range_periods:
                period_id = period.period_id

                if period_id not in evaluations_by_period:
                    continue

                period_evaluations = evaluations_by_period[period_id]
                period_dir = self.output_base_path / period_id
                period_dir.mkdir(exist_ok=True, parents=True)

                # Write WIS scores
                if "wis" in period_evaluations and not period_evaluations["wis"].empty:
                    wis_file = period_dir / "evaluationsRawScoresData.json"
                    wis_data = period_evaluations["wis"].to_dict(orient="records")
                    with open(wis_file, "w") as f:
                        json.dump(wis_data, f, cls=NpEncoder, separators=(",", ":"))
                    self._track_file_written(wis_file)

                # Write WIS ratio (precalculated evaluations)
                if "wis_ratio" in period_evaluations and not period_evaluations["wis_ratio"].empty:
                    wis_ratio_file = period_dir / "evaluationsPrecalculatedData.json"
                    wis_ratio_data = period_evaluations["wis_ratio"].to_dict(orient="records")
                    with open(wis_ratio_file, "w") as f:
                        json.dump(wis_ratio_data, f, cls=NpEncoder, separators=(",", ":"))
                    self._track_file_written(wis_ratio_file)

                # Write Coverage data
                if "coverage" in period_evaluations and not period_evaluations["coverage"].empty:
                    coverage_file = period_dir / "coverageData.json"
                    coverage_data = period_evaluations["coverage"].to_dict(orient="records")
                    with open(coverage_file, "w") as f:
                        json.dump(coverage_data, f, cls=NpEncoder, separators=(",", ":"))
                    self._track_file_written(coverage_file)

                # Write MAPE data
                if "mape" in period_evaluations and not period_evaluations["mape"].empty:
                    mape_file = period_dir / "mapeData.json"
                    mape_data = period_evaluations["mape"].to_dict(orient="records")
                    with open(mape_file, "w") as f:
                        json.dump(mape_data, f, cls=NpEncoder, separators=(",", ":"))
                    self._track_file_written(mape_file)

                logger.info(f"  ✓ Written evaluation data for {period_id}")

            # Write dynamic period evaluations
            for period in dynamic_periods:
                period_id = period.period_id

                if period_id not in evaluations_by_period:
                    continue

                period_evaluations = evaluations_by_period[period_id]

                # Append evaluation data to dynamic period JSON
                dynamic_file = dynamic_dir / f"{period_id}.json"

                # Read existing file if it exists
                if dynamic_file.exists():
                    with open(dynamic_file, "r") as f:
                        dynamic_data = json.load(f)
                else:
                    dynamic_data = {}

                # Add evaluation results
                if "wis" in period_evaluations and not period_evaluations["wis"].empty:
                    dynamic_data["evaluationsRawScores"] = period_evaluations["wis"].to_dict(orient="records")

                if "wis_ratio" in period_evaluations and not period_evaluations["wis_ratio"].empty:
                    dynamic_data["evaluationsPrecalculated"] = period_evaluations["wis_ratio"].to_dict(orient="records")

                if "coverage" in period_evaluations and not period_evaluations["coverage"].empty:
                    dynamic_data["coverage"] = period_evaluations["coverage"].to_dict(orient="records")

                if "mape" in period_evaluations and not period_evaluations["mape"].empty:
                    dynamic_data["mape"] = period_evaluations["mape"].to_dict(orient="records")

                # Write updated file
                with open(dynamic_file, "w") as f:
                    json.dump(dynamic_data, f, cls=NpEncoder, separators=(",", ":"))

                logger.info(f"  ✓ Updated {period_id}.json with evaluation data")

        logger.info("All output files written successfully!")

    def _track_file_written(self, file_path: Path):
        """Track files written for summary reporting."""
        self.processing_stats["files_written"] += 1
        self.processing_stats["output_files"].append(str(file_path.relative_to(self.project_root)))

    def _print_processing_summary(
        self,
        target_data_df: pd.DataFrame,
        model_output_df: pd.DataFrame,
        metadata: dict,
    ):
        """Print a comprehensive summary of the data processing."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("📊 DATA PROCESSING SUMMARY")
        logger.info("=" * 60)

        # Input data summary
        logger.info("")
        logger.info("INPUT DATA:")
        logger.info(f"  • Target data rows: {self.processing_stats['target_data_rows']:,}")
        if "date" in target_data_df.columns:
            date_range = f"{target_data_df['date'].min().date()} to {target_data_df['date'].max().date()}"
            logger.info(f"  • Target date range: {date_range}")

        logger.info(f"  • Model output rows: {self.processing_stats['model_output_rows']:,}")
        if "reference_date" in model_output_df.columns:
            ref_date_range = f"{model_output_df['reference_date'].min().date()} to {model_output_df['reference_date'].max().date()}"
            logger.info(f"  • Model reference date range: {ref_date_range}")

        # Processing results
        logger.info("")
        logger.info("PROCESSING RESULTS:")
        logger.info(f"  • Models processed: {self.processing_stats['models_processed']}")
        logger.info(f"  • Locations detected: {self.processing_stats['locations_detected']}")
        logger.info(f"  • Forecast periods: {self.processing_stats['forecast_periods']}")

        if self.skip_evaluations:
            logger.info(f"  • Evaluations: DISABLED (skipped by user)")
        else:
            logger.info(f"  • Evaluations calculated: {self.processing_stats['evaluations_calculated']}")

        if self.historical_target_data:
            logger.info(f"  • Historical snapshots: {len(self.historical_target_data)}")

        # Output files
        logger.info("")
        logger.info("OUTPUT:")
        logger.info(f"  • Files written: {self.processing_stats['files_written']}")
        logger.info(f"  • Output directory: {self.output_base_path.relative_to(self.project_root)}")

        if self.dev_mode:
            logger.info("")
            logger.info("DEV MODE - Files written:")
            for file_path in self.processing_stats["output_files"]:
                logger.info(f"  ✓ {file_path}")

        # Data validation checks
        logger.info("")
        logger.info("VALIDATION CHECKS:")

        # Check 1: Do we have output from all models?
        all_models_have_data = all(model_name in model_output_df["model"].unique() for model_name in metadata["modelNames"])
        logger.info(f"  • All configured models have data: {'✓ Yes' if all_models_have_data else '✗ No'}")

        # Check 2: Default selected date
        if metadata["temporal"].get("defaultSelectedDate"):
            logger.info(f"  • Default selected date: {metadata['defaultSelectedDate']}")
        else:
            logger.info("  • Default selected date: ⚠ Not set")

        # Check 3: Date coverage
        if "date" in target_data_df.columns and "target_end_date" in model_output_df.columns:
            target_latest = target_data_df["date"].max()
            model_latest = model_output_df["target_end_date"].max()
            if target_latest < model_latest:
                logger.info("  • Date coverage: ⚠ Model predictions extend beyond target data")
                logger.info(f"    (Target: {target_latest.date()}, Model: {model_latest.date()})")
            else:
                logger.info("  • Date coverage: ✓ Good")

        logger.info("")
        logger.info("=" * 60)

    def _get_period_date_range(
        self,
        period,
        target_data_df: pd.DataFrame,
        model_output_df: pd.DataFrame,
    ) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        """Determines the start and end date for a given forecast period."""
        if period.is_special_period:
            anchor_config = period.time_anchor
            if not anchor_config:
                logger.warning(f"Special period '{period.period_id}' is missing time_anchor config. Skipping.")
                return None

            anchor_mode = anchor_config.get("anchor_mode")
            range_calc = anchor_config.get("range_calculation")
            time_unit = self.config.time_unit

            if anchor_mode == "model-output":
                anchor_date = model_output_df["reference_date"].max()
            elif anchor_mode == "target-data":
                anchor_date = target_data_df["date"].max()
            else:
                logger.warning(f"Invalid anchor_mode '{anchor_mode}' for special period '{period.period_id}'. Skipping.")
                return None

            if pd.isna(anchor_date):
                logger.warning(f"Could not determine anchor date for special period '{period.period_id}'. Skipping.")
                return None

            end_date = anchor_date
            start_date = end_date + pd.Timedelta(days=range_calc * time_unit)
            return start_date, end_date
        else:
            return period.start_date, period.end_date


def process_data(config: DashboardConfig, dev_mode: bool = False, skip_evaluations: bool = False):
    """
    Main function to instantiate and run the data processor.
    This will be called by the main workflow orchestrator.

    Args:
        config: DashboardConfig object with all settings
        dev_mode: If True, use test-data-input/ directory
        skip_evaluations: If True, skip evaluation metrics calculation
    """
    try:
        processor = DataProcessor(config, dev_mode=dev_mode, skip_evaluations=skip_evaluations)
        success = processor.run()
        if not success:
            raise RuntimeError("Data processing failed.")
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        raise
