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

# Assuming yaml_config_processor_pydantic is in the same directory or accessible via sys.path
from yaml_config_processor_pydantic import DashboardConfig
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
    """
    Main data processing class handling the entire pipeline from ingestion to export.

    This class coordinates the loading, validation, transformation, and export of:
    1.  Target data (ground truth)
    2.  Model output data (forecasts)
    3.  Evaluation metrics (if enabled)
    4.  Metadata generation for the frontend

    Attributes:
        config (DashboardConfig): The validated configuration object.
        dev_mode (bool): Whether to use test data input/output directories.
        skip_evaluations (bool): Whether to skip the evaluation metrics calculation step.
        processing_stats (dict): Dictionary tracking statistics of the current run.
    """

    def __init__(
        self,
        config: DashboardConfig,
        dev_mode: bool = False,
        skip_evaluations: bool = False,
    ):
        """
        Initialize the DataProcessor.

        Args:
            config (DashboardConfig): The validated Pydantic configuration object.
            dev_mode (bool, optional): If True, reads from ``test-data-input/``. Defaults to False.
            skip_evaluations (bool, optional): If True, skips calculating WIS/Coverage. Defaults to False.
        """
        self.config = config
        self.project_root = Path(__file__).parent.parent
        self.dev_mode = dev_mode
        self.skip_evaluations = skip_evaluations
        self.historical_target_data = None  # Map<as_of_date, Map<date, data>>
        self.current_target_data = None
        self.fixed_target_data = None
        self.model_output_unpivoted = None  # Keep unpivoted data for evaluations

        # Create a mapping from the raw target key in data to the corresponding targetId
        targets = self.config.targets if self.config.targets else []
        self.target_key_to_id_map = {t.target_key_in_data: t.target_id for t in targets}
        self.target_id_to_dvp_config = {t.target_id: t.data_value_processing for t in targets}

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
        """
        Executes the full data processing pipeline.

        Steps:
        1.  **Ingestion**: Loads target data and model output data.
        2.  **Preprocessing**: Fixes missing time intervals in target data.
        3.  **Discovery**: Detects locations present in the data.
        4.  **Processing**: Transforms data into the nested JSON structure required by the frontend.
        5.  **Evaluations**: Calculates metrics (WIS, coverage) if enabled.
        6.  **Export**: Writes all processed data to JSON files in the public directory.

        Returns:
            bool: True if the pipeline completes successfully.
        """
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

        # 4: Process all target data (periods are now frontend presets)
        processed_target_data = self._process_target_data(fixed_target_data_df)

        # 5: Process all model output data
        processed_model_output = self._process_model_output_data(model_output_df)
        self.processing_stats["forecast_periods"] = len(self.config.forecast_periods)

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
            processed_target_data,
            processed_model_output,
            metadata,
            evaluations_by_period,
        )

        # 8: Print summary
        # self._print_processing_summary(target_data_df, model_output_df, metadata)

        logger.info("Data processing completed successfully.")
        return True  # Indicate success

    def _load_target_data(self) -> pd.DataFrame:
        """
        Loads and standardizes the ground truth (target) data.

        Handles both CSV and Parquet formats (partitioned or single file) as specified
        in :attr:`~yaml_config_processor_pydantic.DashboardConfig.target_data_file_format`.

        Applies column renaming based on :attr:`~yaml_config_processor_pydantic.TargetDataHeaderMapping`.

        Returns:
            pd.DataFrame: A standardized DataFrame with columns: ``date``, ``observation``,
            ``location``, ``target``, and optionally ``as_of``.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            ValueError: If required columns are missing.
        """
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
                logger.info(f"  [OK] Loaded target data from {csv_file.name}")
                logger.info(f"  [OK] Shape: {df.shape[0]} rows x {df.shape[1]} columns")
            except ValueError as e:
                raise e
            except FileNotFoundError as e:
                raise e
            except Exception as e:
                raise RuntimeError(f"Error loading CSV file: {e}")
        elif file_format == "parquet":
            # Check if using partitioned parquet format
            if self.config.parquet_partitioned_by_as_of:
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
                    logger.info(f"  [OK] Loaded target data from {parquet_file.name}")
                    logger.info(f"  [OK] Shape: {df.shape[0]} rows x {df.shape[1]} columns")
                except Exception as e:
                    raise RuntimeError(f"Error loading parquet file: {e}")
        else:
            raise ValueError(f"Unsupported target_data_file_format: {file_format}")

        # Rename csv file column headers from users' specifications to Hubverse standard
        mapping = self.config.target_data_header_mapping

        # Log available columns for debugging
        logger.info(f"  → Available columns in target data: {df.columns.tolist()}")

        # Build rename dict and validate that required columns exist
        rename_dict = {}

        # Required columns
        if mapping.date_col_name not in df.columns:
            raise ValueError(f"Date column '{mapping.date_col_name}' not found in target data. Available columns: {df.columns.tolist()}")
        rename_dict[mapping.date_col_name] = "date"

        if mapping.observation_col_name not in df.columns:
            raise ValueError(f"Observation column '{mapping.observation_col_name}' not found in target data. Available columns: {df.columns.tolist()}")
        rename_dict[mapping.observation_col_name] = "observation"

        # Optional columns
        if mapping.location_col_name and mapping.location_col_name in df.columns:
            rename_dict[mapping.location_col_name] = "location"
        if mapping.location_name_col_name and mapping.location_name_col_name in df.columns:
            rename_dict[mapping.location_name_col_name] = "location_name"
        if mapping.target_col_name and mapping.target_col_name in df.columns:
            rename_dict[mapping.target_col_name] = "target"
        if mapping.as_of_col_name and mapping.as_of_col_name in df.columns:
            rename_dict[mapping.as_of_col_name] = "as_of"

        df.rename(columns=rename_dict, inplace=True)
        logger.info(f"  [OK] Renamed columns: {list(rename_dict.keys())} -> {list(rename_dict.values())}")

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
        Loads partitioned parquet files where each subdirectory represents an ``as_of`` date.

        This is typically used for historical data versioning, allowing the dashboard
        to show what the data looked like at a specific point in time.

        Returns:
            pd.DataFrame: Combined DataFrame with an added ``as_of`` column extracted from the directory name.
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
                logger.warning(f"  [!] Could not parse as_of date from directory: {dir_name}, skipping")
                continue

            # Validate date format
            try:
                pd.to_datetime(as_of_date)
            except:
                logger.warning(f"  [!] Invalid date format in directory: {dir_name}, skipping")
                continue

            # Look for parquet files in this subdirectory
            parquet_files = list(subdir.glob("*.parquet")) + list(subdir.glob("*.pq"))

            if not parquet_files:
                logger.warning(f"  [!] No parquet files found in {dir_name}, skipping")
                continue

            # Read all parquet files in this partition
            partition_dfs = []
            for pq_file in parquet_files:
                try:
                    partition_df = pd.read_parquet(pq_file)
                    partition_dfs.append(partition_df)
                except Exception as e:
                    logger.warning(f"  [!] Error reading {pq_file}: {e}, skipping")
                    continue

            if partition_dfs:
                # Combine files from this partition
                partition_combined = pd.concat(partition_dfs, ignore_index=True)

                # Add as_of column
                partition_combined["as_of"] = as_of_date

                all_partitions.append(partition_combined)
                logger.info(f"  [OK] Loaded partition {dir_name}: {len(partition_combined)} rows")

        if not all_partitions:
            raise RuntimeError("No valid partitions could be loaded from partitioned parquet format")

        # Combine all partitions
        combined_df = pd.concat(all_partitions, ignore_index=True)
        logger.info(f"  [OK] Combined all partitions: {combined_df.shape[0]} rows x {combined_df.shape[1]} columns")
        logger.info(f"  [OK] Found {len(all_partitions)} as_of snapshots")

        return combined_df

    def _load_model_output_data(self) -> pd.DataFrame:
        """
        Loads and prepares all model output data from the ``model-output`` directory.

        It iterates through each model subdirectory specified in :attr:`~yaml_config_processor_pydantic.DashboardConfig.available_models`.
        
        The data is:
        1.  Loaded from CSVs.
        2.  Renamed according to :attr:`~yaml_config_processor_pydantic.ModelOutputHeaderMapping`.
        3.  Calculated for ``horizon`` if missing.
        4.  Pivoted if it contains quantile data (converting long format to wide).

        Returns:
            pd.DataFrame: A unified DataFrame containing predictions from all models.
        """
        logger.info("Loading model output data...")
        logger.info(f"  → Looking in: {self.model_output_path}")
        all_model_dfs = []
        mapping = self.config.model_output_data_header_mapping
        rename_dict = {
            mapping.reference_date_col_name: "reference_date",
            mapping.target_end_date_col_name: "target_end_date",
            mapping.target_col_name: "target",
            mapping.horizon_col_name: "horizon",
            mapping.location_col_name: "location",
            mapping.output_type_col_name: "output_type",
            mapping.output_type_id_col_name: "output_type_id",
            mapping.value_col_name: "value",
        }
        # Filter out None keys from rename_dict that may result from optional config fields
        valid_rename_dict = {k: v for k, v in rename_dict.items() if k is not None}

        for model in self.config.available_models:
            # Checkout model's output in their designated folder
            model_dir = self.model_output_path / model.model_name
            if not model_dir.is_dir():
                logger.warning(f"  [!] Directory not found for model '{model.model_name}', skipping.")
                continue

            model_files = list(model_dir.glob("*.csv"))
            if not model_files:
                logger.warning(f"  [!] No CSV files found for model '{model.model_name}', skipping.")
                continue

            logger.info(f"  [OK] Loading model '{model.model_name}': {len(model_files)} files")
            df_list = [pd.read_csv(f, low_memory=False) for f in model_files]
            model_df = pd.concat(df_list, ignore_index=True)
            model_df["model"] = model.model_name
            all_model_dfs.append(model_df)
            self.processing_stats["models_processed"] += 1

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
            df["horizon"] = ((df["target_end_date"] - df["reference_date"]).dt.days / time_unit).astype(int)
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
        """
        Pivots the long-format quantile data into a wide format where each quantile level
        becomes its own column.
        
        This is necessary for the frontend to easily map prediction intervals.

        Args:
            df (pd.DataFrame): The raw long-format model output DataFrame.

        Returns:
            pd.DataFrame: Wide-format DataFrame.
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

        # NOTE: Keep the quantile column header to be 0.XX format
        # Since user configurations already specifies needed PIs in that format
        """ pivoted.columns = [
            f"q{str(c).replace('.', '_')}" if isinstance(c, (float, str)) and str(c).replace(".", "").isnumeric() else c for c in pivoted.columns
        ] """

        # Merge back with non-quantile rows if any
        if not other_rows.empty:
            final_df = pd.concat([pivoted, other_rows], ignore_index=True)
        else:
            final_df = pivoted

        return final_df

    def _process_historical_target_data(self, df: pd.DataFrame) -> dict:
        """
        Process historical target data into a nested dictionary structure.
        
        Structure: ``Map<as_of_date, Map<date, Map<location, data>>>``

        This allows the frontend to quickly look up "what we knew" at any given point in time.

        Args:
            df (pd.DataFrame): The raw DataFrame with 'as_of' column.

        Returns:
            dict: The nested dictionary structure.
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

                    # IMPORTANT: Add target field - required for multi-target scenarios
                    # Map raw target key to target_id for consistency with config

                    target_id = "default"
                    if "target" in row and pd.notna(row["target"]):
                        raw_target_key = str(row["target"])
                        target_id = self.target_key_to_id_map.get(raw_target_key, raw_target_key)

                        # Apply scaling for target data
                        dvp_config = self.target_id_to_dvp_config.get(target_id)
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

    def _fix_missing_time_intervals(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills in missing time intervals in target data to ensure a complete time series.
        
        This prevents gaps in the chart visualizations. It generates a complete grid of
        dates (based on :attr:`~yaml_config_processor_pydantic.DashboardConfig.time_unit`)
        for all locations and targets, filling missing observations with -1.

        Args:
            target_data_df (pd.DataFrame): The raw target data.
            model_output_df (pd.DataFrame): The model output data (used to determine the full date range).

        Returns:
            pd.DataFrame: A DataFrame with continuous date ranges for every location/target.
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

        # Handle targets for grid generation to ensure placeholders have correct target IDs
        targets = self.config.targets or []
        target_keys = [t.target_key_in_data for t in targets] if targets else []
        
        complete_df = None
        merge_cols = []

        # Create complete grid
        # If the dataframe has a 'target' column, we must include it in the grid
        # to ensure we generate placeholders for all targets
        if "target" in target_data_df.columns and target_keys:
             # Create complete grid of dates x locations x targets
             complete_grid = pd.MultiIndex.from_product(
                [date_range, all_locations, target_keys], 
                names=["date", "location", "target"]
            )
             complete_df = pd.DataFrame(index=complete_grid).reset_index()
             merge_cols = ["date", "location", "target"]
        elif "location" in target_data_df.columns:
             # Existing logic for date x location
             complete_grid = pd.MultiIndex.from_product([date_range, all_locations], names=["date", "location"])
             complete_df = pd.DataFrame(index=complete_grid).reset_index()
             merge_cols = ["date", "location"]
        else:
             # Just date
             complete_df = pd.DataFrame({"date": date_range})
             merge_cols = ["date"]

        # Merge with existing target data
        fixed_df = pd.merge(complete_df, target_data_df, on=merge_cols, how="left")

        # Fill missing observation values with -1 (indicating no data)
        fixed_df["observation"] = fixed_df["observation"].fillna(-1)

        logger.info(f"Fixed target data shape: {fixed_df.shape}")

        return fixed_df

    def _detect_locations(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> list:
        """
        Detects all unique locations from the provided data and configuration.

        This method implements a specific precedence logic to ensure the most accurate
        location names are displayed.

        The precedence order is:
        1.  **Custom Mapping File**: If ``custom_location_mapping_file_name`` is provided in config,
            ONLY these locations are used.
        2.  **Target Data**: Locations found in ``target_data_df``. If a ``location_name`` column exists,
            those names are used.
        3.  **Model Output**: Additional locations found in ``model_output_df`` that weren't in target data.
        4.  **Default Fallback**: Uses the built-in US FIPS code mapping (e.g., "01" -> "Alabama").

        Args:
            target_data_df (pd.DataFrame): The loaded ground truth data.
            model_output_df (pd.DataFrame): The loaded model predictions data.

        Returns:
            list: A list of dictionaries, where each dictionary contains:
                - ``location`` (str): The location code (e.g., "US", "06").
                - ``location_name`` (str): The human-readable name.
        """
        logger.info("Detecting locations from data...")

        # Get the location mapping (custom or default FIPS)
        location_mapping = self.config.get_location_mapping()
        has_custom_mapping = self.config.spatial_config.custom_location_mapping_file_name is not None

        # Priority 1: If custom mapping file exists, use ALL locations from it
        if has_custom_mapping and location_mapping:
            logger.info(f"  [OK] Using locations from custom mapping file: {len(location_mapping)} locations")
            locations_list = [{"location": loc_code, "location_name": loc_name} for loc_code, loc_name in location_mapping.items()]
            # Sort by location code
            locations_list.sort(key=lambda x: x["location"])
            return locations_list

        # Priority 2 & 3: Detect from data files (target-data first, then model-output)
        logger.info("  [OK] Auto-detecting locations from data files...")

        detected_locations = {}  # Map of code -> name

        # Check target-data first
        if "location" in target_data_df.columns and not target_data_df.empty:
            target_loc_codes = target_data_df["location"].unique()
            logger.info(f"  [OK] Found {len(target_loc_codes)} unique locations in target-data")

            # Check if target-data has location_name column
            if "location_name" in target_data_df.columns:
                # Use names from target-data
                for loc_code in target_loc_codes:
                    loc_code_str = str(loc_code)
                    loc_data = target_data_df[target_data_df["location"] == loc_code]
                    if not loc_data.empty:
                        loc_name = str(loc_data["location_name"].iloc[0])
                        detected_locations[loc_code_str] = loc_name
            else:
                # Use names from default mapping
                for loc_code in target_loc_codes:
                    loc_code_str = str(loc_code)
                    loc_name = location_mapping.get(loc_code_str, f"Location {loc_code_str}")
                    detected_locations[loc_code_str] = loc_name

        # Check model-output (fills in any missing locations)
        if "location" in model_output_df.columns and not model_output_df.empty:
            model_loc_codes = model_output_df["location"].unique()
            new_locs = [loc for loc in model_loc_codes if str(loc) not in detected_locations]

            if new_locs:
                logger.info(f"  [OK] Found {len(new_locs)} additional locations in model-output")

            for loc_code in model_loc_codes:
                loc_code_str = str(loc_code)
                if loc_code_str not in detected_locations:
                    # Use name from default mapping
                    loc_name = location_mapping.get(loc_code_str, f"Location {loc_code_str}")
                    detected_locations[loc_code_str] = loc_name

        # Convert to list format
        locations_list = [{"location": loc_code, "location_name": loc_name} for loc_code, loc_name in detected_locations.items()]

        # Sort by location code
        locations_list.sort(key=lambda x: x["location"])

        logger.info(f"  [OK] Total detected locations: {len(locations_list)}")

        return locations_list

    def _extract_and_validate_default_location(self, locations_info: list, configured_default: any) -> str:
        """
        Extract and validate default location with fallback chain:
        1. User specified in config (if valid)
        2. First location from locations_info
        3. "US" as final fallback
        """
        available_location_codes = {loc["location"] for loc in locations_info}

        # Extract location code from config (handle dict or string format)
        default_code = None
        if configured_default:
            if isinstance(configured_default, dict):
                # Extract first key from dict (e.g., {"US": "US"} -> "US")
                default_code = list(configured_default.keys())[0] if configured_default else None
            elif isinstance(configured_default, str):
                default_code = configured_default

        # Validate the configured default exists in available locations
        if default_code and default_code in available_location_codes:
            logger.info(f"  [OK] Using configured default location: {default_code}")
            return default_code
        elif default_code:
            logger.warning(f"  [!] Configured default location '{default_code}' not found in data")
            logger.warning(f"  [!] Available locations: {sorted(list(available_location_codes))[:5]}...")

        # Fallback: Use first available location
        if locations_info:
            fallback_code = locations_info[0]["location"]
            logger.info(f"  [OK] Using first available location as default: {fallback_code}")
            return fallback_code

        # Final fallback
        logger.warning(f"  [!] No locations detected, defaulting to 'US'")
        return "US"

    def _generate_metadata(
        self,
        locations_info: list,
        model_output_df: pd.DataFrame,
        target_data_df: pd.DataFrame,
    ) -> dict:
        """
        Generates the metadata dictionary required by the frontend application.

        This dictionary serves as the "handshake" between Python and React, containing:
        -   All configuration options (UI, spatial, temporal).
        -   Detected data ranges (min/max dates).
        -   Available models and targets.
        -   Feature flags.

        Args:
            locations_info (list): The list of detected locations.
            model_output_df (pd.DataFrame): The full model output data.
            target_data_df (pd.DataFrame): The full target data.

        Returns:
            dict: The complete metadata object serialized to JSON later.
        """
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
                    "forecastPeriodId": period.forecast_period_id,
                    "displayString": period.display_string,
                    "timeValue": f"{period.start_date.date()}/{period.end_date.date()}",
                    "startDate": period.start_date.isoformat(),
                    "endDate": period.end_date.isoformat(),
                    "isDefaultSelected": period.is_default_selected,
                }
            )

        # Dynamic/special forecast periods
        special_periods = self.config.special_forecast_periods or []
        for period in special_periods:
            # Note: special periods have placeholder dates, computed at runtime
            period_meta = {
                "forecastPeriodId": period.special_period_id,
                "displayString": period.display_string,
                "timeValue": "dynamic",  # Computed at runtime
                "startDate": None,  # Computed at runtime
                "endDate": None,  # Computed at runtime
                "isDefaultSelected": False,
                "isDynamic": True,
                "isSpecial": True,
            }
            forecast_periods_info.append(period_meta)

        # Build targets info
        targets_info = []
        default_target_id = None
        targets = self.config.targets or []
        for target in targets:
            targets_info.append(
                {
                    "targetId": target.target_id,
                    "targetKeyInData": target.target_key_in_data,
                    "displayString": target.task_display_string,
                    "forecastPeriods": target.for_forecast_periods or [],
                    "isDefaultSelected": target.is_default_selected,
                    "dataValueProcessing": target.data_value_processing.model_dump() if target.data_value_processing else None,
                }
            )
            # Track default target
            if target.is_default_selected:
                default_target_id = target.target_id

        # If no default target specified, use the first one
        if not default_target_id and targets_info:
            default_target_id = targets_info[0]["targetId"]

        prediction_intervals_info = []
        for interval in self.config.prediction_intervals:
            prediction_intervals_info.append(
                {
                    "level": str(interval.level),  # Convert to string for frontend consistency
                    "quantiles": interval.uses_output_type_ids,
                }
            )

        model_configs = []
        for model in self.config.available_models:
            model_configs.append({"modelName": model.model_name, "color": model.color_hex})

        # NOTE: Metadata design here
        metadata = {
            # === FEATURE FLAGS FOR FRONTEND ===
            "features": {
                "evaluationsEnabled": not self.skip_evaluations,
                "historicalTargetDataEnabled": self.historical_target_data is not None,
            },
            # === SPATIAL CONFIGURATION ===
            "spatial": {
                "isSingleLocation": self.config.is_single_location_forecast,
                "singleLocationCode": self.config.single_location_mapping if self.config.is_single_location_forecast else None,
                "disableMapInDashboard": self.config.spatial_config.disable_map_in_dashboard,
                "customShapeFileName": self.config.spatial_config.custom_shape_file_name,
                "locationCodeHeader": self.config.spatial_config.location_code_col_header_name,
                "locationNameHeader": self.config.spatial_config.location_name_col_header_name,
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
                "baselineModel": self.config.baseline_model_for_relative_wis if not self.skip_evaluations else None,
            },
            # === TARGETS ===
            "targets": {
                "list": targets_info,
                "defaultTargetId": default_target_id,
            },
            # === PREDICTION INTERVALS ===
            "predictionIntervals": {
                "available": prediction_intervals_info,
                "defaults": self.config.default_selected_prediction_intervals
                if self.config.default_selected_prediction_intervals
                else [str(pi.level) for pi in self.config.prediction_intervals],
            },
            # === DEFAULT SELECTIONS ===
            "defaults": {
                "location": self.config.default_selected_location,
                "horizon": self.config.default_selected_horizon
                if self.config.default_selected_horizon is not None
                else (self.config.horizons[-1] if self.config.horizons else None),
                "predictionIntervals": self.config.default_selected_prediction_intervals
                if self.config.default_selected_prediction_intervals
                else [str(pi.level) for pi in self.config.prediction_intervals],
                "predictionIntervalsForEvaluations": self.config.default_selected_prediction_intervals_for_evaluations
                if self.config.default_selected_prediction_intervals_for_evaluations
                else [str(pi.level) for pi in (self.config.evaluations_prediction_intervals or [])],
            },
            # === DATA FILES MANIFEST ===
            "dataManifest": {
                "hasTargetData": True,
                "hasModelOutputData": True,
                "hasHistoricalData": self.historical_target_data is not None,
                "hasEvaluations": not self.skip_evaluations,
            },
            # === COLUMN MAPPINGS (for debugging/reference) ===
            "columnMappings": {
                "targetData": {
                    "date": self.config.target_data_header_mapping.date_col_name,
                    "observation": self.config.target_data_header_mapping.observation_col_name,
                    "location": self.config.target_data_header_mapping.location_col_name,
                    "locationName": self.config.target_data_header_mapping.location_name_col_name,
                    "target": self.config.target_data_header_mapping.target_col_name,
                    "asOf": self.config.target_data_header_mapping.as_of_col_name,
                },
                "modelOutput": {
                    "referenceDate": self.config.model_output_data_header_mapping.reference_date_col_name,
                    "targetEndDate": self.config.model_output_data_header_mapping.target_end_date_col_name,
                    "target": self.config.model_output_data_header_mapping.target_col_name,
                    "horizon": self.config.model_output_data_header_mapping.horizon_col_name,
                    "location": self.config.model_output_data_header_mapping.location_col_name,
                    "outputType": self.config.model_output_data_header_mapping.output_type_col_name,
                    "outputTypeId": self.config.model_output_data_header_mapping.output_type_id_col_name,
                    "value": self.config.model_output_data_header_mapping.value_col_name,
                },
            },
            # === UI CUSTOMIZATION ===
            "uiCustomization": {
                "header": {
                    "titleName": self.config.ui_customization.ui_header_title_name,
                    "navButtons": [
                        {
                            "text": btn.button_text,
                            "navToPage": btn.nav_to_page,
                            "navToExternal": btn.nav_to_external,
                            "navToLink": btn.nav_to_link,
                        }
                        for btn in (self.config.ui_customization.ui_header_nav_btn or [])
                    ],
                },
                "forecastPage": {
                    "chartHeaderName": self.config.ui_customization.ui_forecast_header_chart_name,
                    "histTdToggleText": self.config.ui_customization.ui_forecast_header_hist_td_toggle_text,
                    "disableLocationInfo": self.config.ui_customization.disable_location_info_display,
                    "infoButtons": {
                        "headerInfo": (
                            {
                                "title": self.config.ui_customization.ui_forecast_header_infobutton_content.title,
                                "content": self.config.ui_customization.ui_forecast_header_infobutton_content.content,
                            }
                            if self.config.ui_customization.ui_forecast_header_infobutton_content
                            else None
                        ),
                        "horizonInfo": (
                            {
                                "title": self.config.ui_customization.ui_forecast_settings_horizon_infobutton_content.title,
                                "content": self.config.ui_customization.ui_forecast_settings_horizon_infobutton_content.content,
                            }
                            if self.config.ui_customization.ui_forecast_settings_horizon_infobutton_content
                            else None
                        ),
                    },
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

        # Validate and extract default location
        default_location_code = self._extract_and_validate_default_location(locations_info, metadata["defaults"]["location"])
        metadata["defaults"]["location"] = default_location_code

        logger.info("Metadata generated.")
        # Only produce log for now for metadata.
        logger.info(f"Default selected date for frontend: {metadata['temporal']['defaultSelectedDate']}")
        logger.info(f"Default selected location for frontend: {default_location_code}")
        logger.info(f"Evaluations enabled: {metadata['features']['evaluationsEnabled']}")
        logger.info(f"UI customization: Header title = '{metadata['uiCustomization']['header']['titleName']}'")
        return metadata

    def _process_target_data(self, target_data_df: pd.DataFrame) -> dict:
        """
        Transforms ground truth data into a nested dictionary structure optimized for frontend lookup.

        Structure: ``Map<location, Map<date, Map<target, data>>>``

        Applies configured scaling factors to the values.

        Args:
            target_data_df (pd.DataFrame): The standardized target data DataFrame.

        Returns:
            dict: The processed nested dictionary.
        """
        logger.info("Processing all ground truth data...")
        processed_data = {}
        targets = self.config.targets or []

        for _, row in target_data_df.iterrows():
            location_key = str(row.get("location", "US")).zfill(2) if "location" in row else "US"
            date_iso = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")

            data_entry = {
                "observation": float(row["observation"]) if pd.notna(row["observation"]) and row["observation"] >= -1 else None,
            }

            if "location_name" in row and pd.notna(row["location_name"]):
                data_entry["location_name"] = str(row["location_name"])

            raw_target_key = str(row.get("target", targets[0].target_key_in_data if targets else "default"))
            target_id = self.target_key_to_id_map.get(raw_target_key, raw_target_key)

            # Apply scaling
            dvp_config = self.target_id_to_dvp_config.get(target_id)
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

    def _process_model_output_data(self, model_output_df: pd.DataFrame) -> dict:
        """
        Transforms model predictions into a highly nested dictionary structure for the frontend.

        Structure: ``Map<model, Map<location, Map<reference_date, Map<target_date, predictions>>>>``

        Each prediction entry includes:
        -   ``horizon``
        -   ``targetId``
        -   ``value_median``
        -   ``prediction_intervals`` (nested by level)

        Args:
            model_output_df (pd.DataFrame): The standardized model output DataFrame (long or wide).

        Returns:
            dict: The nested dictionary structure.
        """
        logger.info("Processing all predictions data...")
        processed_data = {}

        for model_name in model_output_df["model"].unique():
            model_data = model_output_df[model_output_df["model"] == model_name]
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

                        pred_entry = {"horizon": int(row["horizon"]) if pd.notna(row["horizon"]) else None}

                        if "target" in row and pd.notna(row["target"]):
                            raw_target_key = str(row["target"])
                            target_id = self.target_key_to_id_map.get(raw_target_key, raw_target_key)
                            pred_entry["targetId"] = target_id

                        dvp_config = self.target_id_to_dvp_config.get(target_id)
                        scaling_factor = dvp_config.scaling_factor.model_output if dvp_config else 1.0

                        quantile_cols = [col for col in row.index if isinstance(col, float)]

                        for qc in quantile_cols:
                            if str(qc) == "0.5" and pd.notna(row[qc]):
                                pred_entry["value_median"] = row[qc] * scaling_factor

                        pred_intervals = {}
                        for desired_PI in self.config.prediction_intervals:
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
                        predictions_dict[target_date_iso] = pred_entry

                    location_dict[ref_date_iso] = {"predictions": predictions_dict}

                model_dict[location_key] = location_dict

            processed_data[model_name] = model_dict

        logger.info(f"Processed predictions for {len(processed_data)} models.")
        return processed_data

    def _process_evaluations_by_periods(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> dict:
        """
        Calculates evaluation metrics (WIS, Coverage) for each defined forecast period.

        It filters the data for the specific time range of each period and invokes
        the :class:`EvaluationProcessor` to compute the metrics.

        Args:
            target_data_df (pd.DataFrame): The full target data.
            model_output_df (pd.DataFrame): The full model output data.

        Returns:
            dict: A map of ``period_id`` to evaluation result DataFrames/dictionaries.
        """
        logger.info("Processing evaluations by forecast periods...")
        evaluations_by_period = {}
        special_periods = self.config.special_forecast_periods or []
        all_periods = list(self.config.forecast_periods) + list(special_periods)

        for period in all_periods:
            date_range = self._get_period_date_range(period, target_data_df, model_output_df)
            if not date_range:
                continue
            start, end = date_range

            period_id = period.forecast_period_id if hasattr(period, "forecast_period_id") else period.special_period_id
            logger.info(f"Calculating evaluations for period: '{period_id}' ({start.date()} to {end.date()})")

            # Filter data for this period
            period_target_data = target_data_df[(target_data_df["date"] >= start) & (target_data_df["date"] <= end)].copy()

            period_model_output = model_output_df[(model_output_df["reference_date"] >= start) & (model_output_df["reference_date"] <= end)].copy()

            # Get valid targets for this period
            valid_model_targets = []
            targets = self.config.targets or []
            for target in targets:
                target_periods = target.for_forecast_periods or []
                if period_id in target_periods:
                    valid_model_targets.append(target.target_key_in_data)

            # Filter by valid targets if not single target mode
            if not self.config.is_single_forecast_target and "target" in period_model_output.columns and valid_model_targets:
                period_model_output = period_model_output[period_model_output["target"].isin(valid_model_targets)]

            if period_target_data.empty or period_model_output.empty:
                logger.warning(f"No data available for evaluations in period '{period_id}'")
                continue

            # Calculate evaluation metrics
            evaluation_results = self.evaluation_processor.evaluate_predictions(
                target_data_df=period_target_data,
                model_output_df=period_model_output,
                period_id=period_id,
            )

            # Calculate WIS ratio if WIS results exist
            if "wis" in evaluation_results and not evaluation_results["wis"].empty:
                wis_ratio_df = self.evaluation_processor.calculate_wis_ratio(evaluation_results["wis"])
                if not wis_ratio_df.empty:
                    evaluation_results["wis_ratio"] = wis_ratio_df

            evaluations_by_period[period_id] = evaluation_results

            # Update statistics
            total_evals = sum(len(df) for df in evaluation_results.values() if isinstance(df, pd.DataFrame))
            self.processing_stats["evaluations_calculated"] += total_evals

        logger.info(f"Processed evaluations for {len(evaluations_by_period)} periods.")
        logger.info(f"Total evaluations calculated: {self.processing_stats['evaluations_calculated']}")
        return evaluations_by_period

    def _write_output_files(
        self,
        target_data: dict,
        model_output_data: dict,
        metadata: dict,
        evaluations_by_period: dict = None,
    ):
        """
        Writes all processed data to JSON files in the public output directory.

        Files generated:
        -   ``metadata.json``
        -   ``targetData.json``
        -   ``modelOutputData.json``
        -   ``historical-target-data.json`` (optional)

        Args:
            target_data (dict): Processed target data.
            model_output_data (dict): Processed model output data.
            metadata (dict): Metadata object.
            evaluations_by_period (dict, optional): Evaluation results. Defaults to None.
        """
        logger.info("Writing unified output files...")
        logger.info(f"  → Output directory: {self.output_base_path}")

        self.output_base_path.mkdir(exist_ok=True, parents=True)

        # Write metadata
        metadata_file = self.output_base_path / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, separators=(",", ":"))
        self._track_file_written(metadata_file)

        # Write target data
        gt_file = self.output_base_path / "targetData.json"
        with open(gt_file, "w") as f:
            json.dump(target_data, f, cls=NpEncoder, separators=(",", ":"))
        self._track_file_written(gt_file)

        # Write model output data
        pred_file = self.output_base_path / "modelOutputData.json"
        with open(pred_file, "w") as f:
            json.dump(model_output_data, f, cls=NpEncoder, separators=(",", ":"))
        self._track_file_written(pred_file)

        # Write historical data if available
        if self.historical_target_data:
            historical_file = self.output_base_path / "historical-target-data.json"
            with open(historical_file, "w") as f:
                json.dump(self.historical_target_data, f, cls=NpEncoder, separators=(",", ":"))
            self._track_file_written(historical_file)

        """ # Write evaluation data
        if evaluations:
            eval_dir = self.output_base_path / "evaluations"
            eval_dir.mkdir(exist_ok=True, parents=True)
            
            if "wis" in evaluations and not evaluations["wis"].empty:
                wis_file = eval_dir / "evaluationsRawScoresData.json"
                evaluations["wis"].to_json(wis_file, orient="records")
                self._track_file_written(wis_file)
            
            if "wis_ratio" in evaluations and not evaluations["wis_ratio"].empty:
                wis_ratio_file = eval_dir / "evaluationsPrecalculatedData.json"
                evaluations["wis_ratio"].to_json(wis_ratio_file, orient="records")
                self._track_file_written(wis_ratio_file)

            if "coverage" in evaluations and not evaluations["coverage"].empty:
                coverage_file = eval_dir / "coverageData.json"
                evaluations["coverage"].to_json(coverage_file, orient="records")
                self._track_file_written(coverage_file) """

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
        logger.info("DATA PROCESSING SUMMARY")
        logger.info("=" * 60)

        # Input data summary
        logger.info("")
        logger.info("INPUT DATA:")
        logger.info(f"  - Target data rows: {self.processing_stats['target_data_rows']:,}")
        if "date" in target_data_df.columns:
            date_range = f"{target_data_df['date'].min().date()} to {target_data_df['date'].max().date()}"
            logger.info(f"  - Target date range: {date_range}")

        logger.info(f"  - Model output rows: {self.processing_stats['model_output_rows']:,}")
        if "reference_date" in model_output_df.columns:
            ref_date_range = f"{model_output_df['reference_date'].min().date()} to {model_output_df['reference_date'].max().date()}"
            logger.info(f"  - Model reference date range: {ref_date_range}")

        # Processing results
        logger.info("")
        logger.info("PROCESSING RESULTS:")
        logger.info(f"  - Models processed: {self.processing_stats['models_processed']}")
        logger.info(f"  - Locations detected: {self.processing_stats['locations_detected']}")
        logger.info(f"  - Forecast periods: {self.processing_stats['forecast_periods']}")

        if self.skip_evaluations:
            logger.info(f"  - Evaluations: DISABLED (skipped by user)")
        else:
            logger.info(f"  - Evaluations calculated: {self.processing_stats['evaluations_calculated']}")

        if self.historical_target_data:
            logger.info(f"  - Historical snapshots: {len(self.historical_target_data)}")

        # Output files
        logger.info("")
        logger.info("OUTPUT:")
        logger.info(f"  - Files written: {self.processing_stats['files_written']}")
        logger.info(f"  - Output directory: {self.output_base_path.relative_to(self.project_root)}")

        if self.dev_mode:
            logger.info("")
            logger.info("DEV MODE - Files written:")
            for file_path in self.processing_stats["output_files"]:
                logger.info(f"  [OK] {file_path}")

        logger.info("")
        logger.info("=" * 60)

    def _get_period_date_range(
        self,
        period,
        target_data_df: pd.DataFrame,
        model_output_df: pd.DataFrame,
    ) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        """Determines the start and end date for a given forecast period."""
        # Check if this is a special period (has special_period_id attribute)
        is_special = hasattr(period, "special_period_id")
        period_id = period.special_period_id if is_special else period.forecast_period_id
        if is_special:
            anchor_config = period.time_anchor
            if not anchor_config:
                logger.warning(f"Special period '{period_id}' is missing time_anchor config. Skipping.")
                return None

            anchor_mode = anchor_config.anchor_mode
            range_calc = anchor_config.range_calculation
            time_unit = self.config.time_unit

            if anchor_mode == "model-output":
                anchor_date = model_output_df["reference_date"].max()
            elif anchor_mode == "target-data":
                anchor_date = target_data_df["date"].max()
            else:
                logger.warning(f"Invalid anchor_mode '{anchor_mode}' for special period '{period_id}'. Skipping.")
                return None

            if pd.isna(anchor_date):
                logger.warning(f"Could not determine anchor date for special period '{period_id}'. Skipping.")
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
        config (DashboardConfig): DashboardConfig object with all settings
        dev_mode (bool): If True, use test-data-input/ directory
        skip_evaluations (bool): If True, skip evaluation metrics calculation
    """
    try:
        processor = DataProcessor(config, dev_mode=dev_mode, skip_evaluations=skip_evaluations)
        success = processor.run()
        if not success:
            raise RuntimeError("Data processing failed.")
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        raise
