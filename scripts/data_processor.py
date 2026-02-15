"""
Generalized Data Processor for the Hubverse Dashboard

This script contains the core logic for ingesting, processing, and structuring
target data and model outputs based on a user-defined configuration.
"""

from yaml_config_processor_pydantic import ForecastPeriodConfig, SpecialForecastPeriodConfig, DashboardConfig
import pandas as pd
from pathlib import Path
import logging
import json
import sys

from evaluation_processor import EvaluationProcessor
from manifest_manager import ManifestManager
from utils_data import to_utc_iso_string, NpEncoder, ensure_string_column
from utils_forecast_period import compute_ongoing_period_metadata, compute_special_period_date_range
from utils_change_detection import identify_target_data_changes, extract_prediction_keys
from utils_model_output_validation import validate_model_output_schema, pivot_quantiles
from utils_evaluation_aggregation import process_iqr_stats, process_location_map_aggregates, process_coverage_aggregates
from utils_data_structuring import process_target_data, process_model_output_data, process_historical_target_data, organize_metric_all_data

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


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
            dev_mode (bool, optional): If True, reads from ``development-mode-root/``. Defaults to False.
            skip_evaluations (bool, optional): If True, skips evaluation generation, calculating WIS/Baseline, MAPE Coverage and all evaluation related logic. Defaults to False.
        """
        self.config = config
        self.project_root = Path(__file__).parent.parent
        self.dev_mode = dev_mode
        self.skip_evaluations = skip_evaluations
        self.historical_target_data = None  # Map<as_of_date, Map<date, data>>
        self.current_target_data = None
        self.fixed_target_data = None
        self.target_data_non_filled = None  # Version without placeholder dates, used for anchor calculations

        # IMPORTANT: We maintain TWO versions of model output:
        # 1. model_output_unpivoted: Long format with output_type & output_type_id columns
        #    - Required by EvaluationProcessor for WIS, MAPE, Coverage calculations
        #    - Allows filtering by output_type="quantile" and specific output_type_id values
        # 2. model_output_pivoted: Wide format with quantiles as separate columns (q_0.25, q_0.5, etc.)
        #    - Used for frontend JSON export (easier for React to map prediction intervals)
        self.model_output_unpivoted = None
        self.raw_evaluations = None  # DataFrame for raw evaluations
        self.aggregated_evaluations = {}  # Cached aggregated evaluations

        # Create a mapping from the raw target key in data to the corresponding targetId
        targets = self.config.targets if self.config.targets else []
        self.target_key_to_id_map = {t.target_key_in_data: t.target_id for t in targets}
        self.target_id_to_dvp_config = {t.target_id: t.data_value_processing for t in targets}

        # Initialize evaluation processor (only if evaluations are enabled)
        if not skip_evaluations:
            self.evaluation_processor = EvaluationProcessor(config=config, baseline_model=config.baseline_model_for_relative_WIS)
        else:
            self.evaluation_processor = None

        # Intermediates directory (separate for dev/prod)
        if self.dev_mode:
            self.intermediates_dir = self.project_root / "development-mode-root" / "intermediates"
        else:
            self.intermediates_dir = self.project_root / "intermediates"
        self.intermediates_dir.mkdir(exist_ok=True, parents=True)

        # Manifest Manager
        manifest_path = self.intermediates_dir / "manifest.json"
        self.manifest_manager = ManifestManager(self.project_root, manifest_path=manifest_path)

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

        # Track date ranges per target for frontend date pickers
        self.date_ranges_per_target = {}

        # Track model availability per period for frontend UI (for graying out unavailable models)
        self.model_availability_per_period = {}

        if self.dev_mode:
            logger.info("=" * 60)
            logger.info("   DEVELOPMENT MODE ENABLED")
            logger.info("=" * 60)
            logger.info("Reading data from: development-mode-root/")
            logger.info("Writing output to: public/test-data-output/")
            logger.info("Intermediates: development-mode-root/intermediates/")
            self.target_data_path = self.project_root / "development-mode-root" / "target-data"
            self.model_output_path = self.project_root / "development-mode-root" / "model-output"
            self.output_base_path = self.project_root / "public" / "test-data-output"
        else:
            self.target_data_path = self.project_root / "target-data"
            self.model_output_path = self.project_root / "model-output"
            self.output_base_path = self.project_root / "public" / "data"

    def run(self, is_data_update_run: bool = False):
        """
        Executes the full data processing pipeline with support for two distinct modes:

        FROM-SCRATCH MODE (is_data_update_run=False):
        - Processes all data from source without checking intermediates
        - Overwrites any existing manifest and intermediate files
        - Use for initial builds or complete rebuilds

        DATA-UPDATE MODE (is_data_update_run=True):
        - Requires existing intermediates from a previous from-scratch run
        - Scans for data changes and processes only modified data
        - Updates manifest and intermediate files incrementally
        - Use for routine data updates after initial build

        Args:
            is_data_update_run: If True, runs in data-update mode; if False, runs from-scratch
        """
        logger.info("Starting data processing...")

        # Initialize variables that will be set differently based on mode
        target_changed = False
        model_changed = False
        model_details = {}
        full_model_load = False

        if is_data_update_run:
            # ========================================
            # DATA-UPDATE RUN
            # ========================================
            logger.info("=" * 60)
            logger.info("DATA UPDATE MODE")
            logger.info("=" * 60)

            # Step 1: Check for prerequisite artifacts before proceeding
            metadata_path = self.output_base_path / "metadata.json"
            manifest_path = self.intermediates_dir / "manifest.json"

            prerequisite_missing = []
            if not manifest_path.exists():
                prerequisite_missing.append("manifest.json")
            # if not (self.intermediates_dir / "target_data.parquet").exists():
            #     prerequisite_missing.append("target_data.parquet")
            # if not (self.intermediates_dir / "model_output_unpivoted.parquet").exists():
            #     prerequisite_missing.append("model_output_unpivoted.parquet")
            if not metadata_path.exists():
                prerequisite_missing.append("metadata.json")

            if prerequisite_missing:
                logger.error("=" * 60)
                logger.error("ERROR: Data update run requires existing artifacts")
                logger.error("=" * 60)
                logger.error("Missing files:")
                for item in prerequisite_missing:
                    logger.error(f"  - {item}")
                logger.error("")
                logger.error("This can mean:")
                logger.error("  1. This is your first time running the dashboard builder")
                logger.error("  2. Intermediates were deleted or lost")
                logger.error("  3. A previous from-scratch build did not complete successfully")
                logger.error("")
                logger.error("Please run a FROM-SCRATCH build first (options 1-4 in build menu)")
                logger.error("=" * 60)
                sys.exit(1)

            logger.info("Prerequisites verified: manifest and intermediates found")

            # Step 2: Scan for data changes
            auxiliary_data_path = self.project_root / ("development-mode-root" if self.dev_mode else "") / "auxiliary-data"
            # Include baseline model in scanning even if not in available_models
            configured_models = self._get_models_to_load()

            changes = self.manifest_manager.check_changes(
                self.target_data_path,
                self.model_output_path,
                auxiliary_data_path if auxiliary_data_path.exists() else None,
                configured_models=configured_models,
            )

            target_changed = changes["target_data_changed"]
            model_changed = changes["model_output_changed"]
            model_details = changes["model_output_details"]

            # Step 3: Load intermediates from previous run
            has_intermediates = self._load_intermediates()

            if not has_intermediates:
                # This shouldn't happen since we checked prerequisites above, but handle it anyway
                logger.error("ERROR: Failed to load intermediates despite prerequisite check passing")
                sys.exit(1)

            logger.info("  [OK] Loaded intermediates successfully")

            if not target_changed and not model_changed:
                logger.info("")
                logger.info("No data changes detected. Using cached data.")

            # Data-update runs use incremental loading
            full_model_load = False

        else:
            # ========================================
            # FROM-SCRATCH RUN PATH
            # ========================================
            logger.info("=" * 60)
            logger.info("FROM-SCRATCH BUILD MODE")
            logger.info("=" * 60)
            logger.info("Processing all data from source (ignoring any existing intermediates)")

            # Skip manifest scanning or loading intermediates

            # Set flags to process all data
            target_changed = True
            model_changed = True
            full_model_load = True

            # Note: Any existing manifest/intermediates will be overwritten at the end

        # 1: Data Ingestion & Processing

        affected_target_keys = set()  # Set of (location, date, target) tuples
        new_model_predictions = set()  # Set of (location, reference_date, target_end_date, target, model) tuples

        # ======================
        # TARGET DATA PROCESSING
        # ======================
        # Strategy: If ANY change is detected in target data, fully reprocess it.
        # We need to find the latest as_of date and extract the current ground truth.
        # Then identify what changed compared to previous run.

        if target_changed or not isinstance(self.fixed_target_data, pd.DataFrame):
            logger.info("")
            logger.info("=" * 60)
            logger.info("PROCESSING TARGET DATA (Full Reprocess)")
            logger.info("=" * 60)
            target_data_df = self._load_target_data()
            self.processing_stats["target_data_rows"] = len(target_data_df)

            # Extract latest ground truth and historical snapshots
            # This method updates self.historical_target_data
            current_target_df = self._extract_latest_ground_truth_and_history(target_data_df)

            # Identify changes if we have previous data
            if self.current_target_data is not None and not self.current_target_data.empty:
                logger.info("")
                logger.info("Identifying target data changes (new/revised observations)...")
                affected_target_keys = identify_target_data_changes(self.current_target_data, current_target_df)
                if affected_target_keys:
                    logger.info(f"  [!] Found {len(affected_target_keys)} changed target data points")
                    logger.info("      (new observations or revisions to existing dates)")
            else:
                logger.info("  [!] From-scratch run - all target data treated as new")

            # Ensure location is string type for consistency
            current_target_df = ensure_string_column(current_target_df, "location")

            # Update state
            self.current_target_data = current_target_df
            target_data_df = current_target_df

        else:
            logger.info("=" * 60)
            logger.info("TARGET DATA: Using cached data (no changes detected)")
            logger.info("=" * 60)
            target_data_df = self.current_target_data

        # ==========================
        # MODEL OUTPUT PROCESSING
        # ==========================
        # Strategy:
        # - Full load: Load all model output files
        # - Incremental: Load only new/modified files and merge with existing data

        if full_model_load:
            logger.info("")
            logger.info("=" * 60)
            logger.info("PROCESSING MODEL OUTPUT (Full Load)")
            logger.info("=" * 60)
            # Load returns unpivoted data, sets self.model_output_unpivoted
            self._load_model_output_data()
        else:
            # Incremental load based on detected changes
            if model_changed:
                logger.info("")
                logger.info("=" * 60)
                logger.info("PROCESSING MODEL OUTPUT (Incremental Update)")
                logger.info("=" * 60)

                # Collect all new and modified files
                files_to_load = []
                changes_by_model = model_details["changes_by_model"]

                for model_name, model_changes in changes_by_model.items():
                    new_files = model_changes.get("new_files", [])
                    modified_files = model_changes.get("modified_files", [])

                    if new_files:
                        logger.info(f"  Model '{model_name}': {len(new_files)} new file(s)")
                        files_to_load.extend(new_files)
                    if modified_files:
                        logger.info(f"  Model '{model_name}': {len(modified_files)} modified file(s)")
                        files_to_load.extend(modified_files)

                if files_to_load:
                    logger.info(f"Loading {len(files_to_load)} new/modified model files...")
                    delta_model_df = self._load_specific_model_files(files_to_load)

                    if not delta_model_df.empty:
                        # Track new predictions for evaluation
                        new_model_predictions = extract_prediction_keys(delta_model_df)

                        # Update unpivoted store
                        if self.model_output_unpivoted is None or self.model_output_unpivoted.empty:
                            self.model_output_unpivoted = delta_model_df
                        else:
                            # Merge and deduplicate (keep newer data)
                            self.model_output_unpivoted = pd.concat([self.model_output_unpivoted, delta_model_df], ignore_index=True)

                            # Deduplicate: prefer newer data
                            keys_for_dupe_detection = ["model", "location", "reference_date", "target_end_date", "target", "output_type", "output_type_id"]
                            valid_keys = [k for k in keys_for_dupe_detection if k in self.model_output_unpivoted.columns]

                            initial_len = len(self.model_output_unpivoted)
                            self.model_output_unpivoted.drop_duplicates(subset=valid_keys, keep="last", inplace=True)
                            final_len = len(self.model_output_unpivoted)

                            if initial_len > final_len:
                                logger.info(f"  [OK] Deduplicated model output: removed {initial_len - final_len} old rows")

                        logger.info(f"  [OK] Loaded {len(delta_model_df)} new prediction rows")
                        logger.info(f"  [OK] Tracked {len(new_model_predictions)} unique prediction keys")

        # 2: Location detection (on full datasets)
        logger.info("")
        logger.info("Detecting locations...")
        locations = self._detect_locations(target_data_df)
        self.processing_stats["locations_detected"] = len(locations)
        logger.info(f"  [OK] Found {len(locations)} location(s)")

        # 3: Filter data by config specifications
        # IMPORTANT: Filter BEFORE pivoting to ensure consistency
        logger.info("")
        target_data_df, self.model_output_unpivoted = self._filter_data_by_config_specs(target_data_df, self.model_output_unpivoted, locations)

        # 3b: Pivot quantiles AFTER filtering
        # This ensures both unpivoted and pivoted versions have consistent data
        logger.info("Pivoting long-format quantile data to wide-format for frontend...")
        if (
            self.model_output_unpivoted is not None
            and not self.model_output_unpivoted.empty
            and "output_type" in self.model_output_unpivoted.columns
            and "quantile" in self.model_output_unpivoted["output_type"].unique()
        ):
            model_output_pivoted = pivot_quantiles(self.model_output_unpivoted)
        else:
            model_output_pivoted = self.model_output_unpivoted

        # 3c: Calculate date ranges per target
        self.date_ranges_per_target = self._calculate_overall_date_ranges_per_target(target_data_df, self.model_output_unpivoted)

        # 4: Store non-filled version for anchor date calculations
        # This version excludes placeholder dates and represents actual ground truth
        self.target_data_non_filled = target_data_df.copy()

        # 4b: Fix missing time intervals (for frontend target data)
        logger.info("Filling missing time intervals in target data...")
        fixed_target_data_df = self._fix_missing_time_intervals(target_data_df, self.model_output_unpivoted)
        self.fixed_target_data = fixed_target_data_df

        # 5: Process all target data (for frontend JSON)
        logger.info("Processing target data for frontend...")
        processed_target_data = process_target_data(fixed_target_data_df, self.target_key_to_id_map, self.target_id_to_dvp_config, self.config.targets or [])

        # 6: Process all model output data (for frontend JSON)
        logger.info("Processing model output data for frontend...")
        processed_model_output = process_model_output_data(
            model_output_pivoted, self.config.available_models or [], self.target_key_to_id_map, self.target_id_to_dvp_config, self.config.prediction_intervals
        )

        # 6b: Track model availability per period
        self._track_model_availability_per_period(model_output_pivoted)

        # ========================
        # EVALUATION PROCESSING
        # ========================
        # Strategy:
        # - Full run: Calculate all evaluations
        # - Incremental: Recalculate only affected rows based on:
        #   A. New/modified model predictions
        #   B. New/revised target data
        # - Smart aggregation: Re-aggregate only affected forecast periods

        aggregated_evaluations = None
        raw_scores_by_period = {}

        if not self.skip_evaluations:
            logger.info("")
            logger.info("=" * 60)
            logger.info("EVALUATION PROCESSING")
            logger.info("=" * 60)

            # Scenario 1: Full evaluation (initial run or no previous evaluations)
            if full_model_load or not self.raw_evaluations:
                logger.info("Running FULL evaluation calculation...")
                logger.info("  This will calculate WIS, MAPE, and Coverage for all predictions")

                eval_results = self._generate_raw_evaluation_collection(fixed_target_data_df, self.model_output_unpivoted)
                self.raw_evaluations = eval_results

                # All periods need aggregation
                affected_date_range = None
                logger.info(f"  [OK] Calculated evaluations for {len(self.model_output_unpivoted)} prediction rows")

            else:
                # Scenario 2: Incremental evaluation update
                logger.info("Running INCREMENTAL evaluation update...")

                rows_to_evaluate = pd.DataFrame()
                evaluation_reasons = []

                # A. New/modified model predictions
                if new_model_predictions:
                    logger.info(f"  Trigger A: {len(new_model_predictions)} new model predictions")
                    evaluation_reasons.append(f"{len(new_model_predictions)} new predictions")

                    # Extract rows matching new prediction keys
                    # Convert set to dataframe for efficient merging
                    pred_keys_list = list(new_model_predictions)
                    if pred_keys_list:
                        pred_keys_df = pd.DataFrame(pred_keys_list, columns=["location", "reference_date", "target_end_date", "target", "model"])

                        # Merge to get full rows
                        new_pred_rows = pd.merge(
                            self.model_output_unpivoted, pred_keys_df, on=["location", "reference_date", "target_end_date", "target", "model"], how="inner"
                        )

                        if not new_pred_rows.empty:
                            rows_to_evaluate = pd.concat([rows_to_evaluate, new_pred_rows])

                # B. Revised target data affecting existing predictions
                if affected_target_keys:
                    logger.info(f"  Trigger B: {len(affected_target_keys)} revised target data points")
                    evaluation_reasons.append(f"{len(affected_target_keys)} revised observations")

                    # Convert affected keys to DataFrame
                    keys_df = pd.DataFrame(list(affected_target_keys), columns=["location", "target_end_date", "target"])

                    # Ensure types match
                    keys_df["target_end_date"] = pd.to_datetime(keys_df["target_end_date"])
                    keys_df["location"] = keys_df["location"].astype(str)

                    # Find all predictions for these target keys
                    affected_pred_rows = pd.merge(self.model_output_unpivoted, keys_df, on=["location", "target_end_date", "target"], how="inner")

                    if not affected_pred_rows.empty:
                        rows_to_evaluate = pd.concat([rows_to_evaluate, affected_pred_rows])

                # Deduplicate rows to evaluate
                if not rows_to_evaluate.empty:
                    rows_to_evaluate.drop_duplicates(inplace=True)

                    logger.info(f"  [!] Recalculating evaluations for {len(rows_to_evaluate)} prediction rows")
                    logger.info(f"      ({', '.join(evaluation_reasons)})")

                    # Calculate new evaluations
                    new_eval_results = self._generate_raw_evaluation_collection(fixed_target_data_df, rows_to_evaluate)

                    # Merge with existing evaluations (update/append)
                    for metric, new_df in new_eval_results.items():
                        if metric not in self.raw_evaluations or self.raw_evaluations[metric] is None or self.raw_evaluations[metric].empty:
                            self.raw_evaluations[metric] = new_df
                            continue

                        if new_df.empty:
                            continue

                        old_df = self.raw_evaluations[metric]

                        # Define evaluation keys for deduplication
                        eval_keys = ["model", "location", "target_end_date", "target", "horizon", "reference_date"]
                        valid_keys = [k for k in eval_keys if k in old_df.columns and k in new_df.columns]

                        # Concatenate and keep last (newest)
                        combined = pd.concat([old_df, new_df], ignore_index=True)
                        combined.drop_duplicates(subset=valid_keys, keep="last", inplace=True)
                        self.raw_evaluations[metric] = combined

                        logger.info(f"      Updated {metric}: {len(new_df)} new/revised scores")

                    # Determine affected date range for smart aggregation
                    min_date = rows_to_evaluate["target_end_date"].min()
                    max_date = rows_to_evaluate["target_end_date"].max()
                    affected_date_range = (min_date, max_date) if pd.notna(min_date) and pd.notna(max_date) else None

                    if affected_date_range:
                        logger.info(f"  [!] Date range affected: {min_date.date()} to {max_date.date()}")
                else:
                    logger.info("  [OK] No evaluation changes needed")
                    affected_date_range = None

            # Step 7b: Aggregate evaluation metrics (selective aggregation)
            logger.info("")
            logger.info("Aggregating evaluations by forecast period...")
            if affected_date_range:
                logger.info(f"  Selective aggregation: only periods overlapping {affected_date_range[0].date()} - {affected_date_range[1].date()}")
            else:
                logger.info("  Full aggregation: all forecast periods")

            aggregated_evaluations = self._generate_aggregated_evaluation_collection(self.raw_evaluations, affected_date_range)
            self.aggregated_evaluations = aggregated_evaluations

            # Step 7c: Organize raw scores for Single Model view
            logger.info("Organizing raw scores for Single Model view...")
            raw_scores_by_period = self._generate_raw_scores_by_period(self.raw_evaluations)

            logger.info("  [OK] Evaluation processing complete")
        else:
            logger.info("")
            logger.info("=" * 60)
            logger.info("EVALUATION SKIPPED (disabled by user)")
            logger.info("=" * 60)

        # 8: Generate Metadata
        metadata = self._generate_metadata(locations, model_output_pivoted, target_data_df)

        # 9: Write output files
        self._write_output_files(
            processed_target_data,
            processed_model_output,
            metadata,
            raw_scores_by_period,
            aggregated_evaluations,
        )

        # 10: Save Intermediates & Manifest
        self._save_intermediates()
        
        # For from-scratch runs, populate manifest state from current directories
        # (In data-update runs, state was already populated during check_changes())
        if not is_data_update_run:
            auxiliary_data_path = self.project_root / ("development-mode-root" if self.dev_mode else "") / "auxiliary-data"
            configured_models = self._get_models_to_load()
            self.manifest_manager.update_state_from_directories(
                self.target_data_path,
                self.model_output_path,
                auxiliary_data_path if auxiliary_data_path.exists() else None,
                configured_models=configured_models
            )
        
        self.manifest_manager.save()

        logger.info("Data processing completed successfully.")
        return True

    def _load_intermediates(self) -> bool:
        """
        Load intermediate data from previous runs.

        Loads:
        - Current target data (parquet)
        - Model output unpivoted data (parquet)
        - Raw evaluations per metric (parquet files: WIS, MAPE, Coverage)
        - Aggregated evaluations (JSON)

        Returns:
            bool: True if all core intermediates loaded successfully, False otherwise
        """
        try:
            target_path = self.intermediates_dir / "target_data.parquet"
            model_path = self.intermediates_dir / "model_output_unpivoted.parquet"

            if not target_path.exists() or not model_path.exists():
                logger.info("Core intermediates not found (first run or clean slate)")
                return False

            logger.info("Loading intermediates from previous run...")

            # Load target and model data
            self.current_target_data = pd.read_parquet(target_path)
            logger.info(f"  Loaded target data: {len(self.current_target_data)} rows")

            self.model_output_unpivoted = pd.read_parquet(model_path)
            logger.info(f"  Loaded model output: {len(self.model_output_unpivoted)} rows")

            # Load raw evaluations (stored as separate parquet files per metric)
            self.raw_evaluations = {}
            metrics_loaded = []

            for metric in ["wis", "wis_ratio", "mape", "coverage"]:
                eval_path = self.intermediates_dir / f"raw_evaluations_{metric}.parquet"
                if eval_path.exists():
                    try:
                        self.raw_evaluations[metric] = pd.read_parquet(eval_path)
                        metrics_loaded.append(f"{metric} ({len(self.raw_evaluations[metric])} rows)")
                    except Exception as e:
                        logger.warning(f"  Failed to load {metric} evaluations: {e}")
                        self.raw_evaluations[metric] = pd.DataFrame()
                else:
                    self.raw_evaluations[metric] = pd.DataFrame()

            if metrics_loaded:
                logger.info(f"  Loaded evaluations: {', '.join(metrics_loaded)}")

            # Load aggregated evaluations (cached for performance)
            agg_path = self.intermediates_dir / "aggregated_evaluations.json"
            if agg_path.exists():
                try:
                    with open(agg_path, "r") as f:
                        self.aggregated_evaluations = json.load(f)
                    logger.info("  Loaded aggregated evaluations cache")
                except Exception as e:
                    logger.warning(f"  Failed to load aggregated evaluations: {e}")
                    self.aggregated_evaluations = {}
            else:
                self.aggregated_evaluations = {}

            logger.info("  [OK] Intermediates loaded successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to load intermediates: {e}")
            import traceback

            traceback.print_exc()
            return False

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
        logger.info(f"Loading target data from {self.target_data_path}")
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

        # Handle 'as_of' column processing
        if "as_of" in df.columns:
            logger.info("Found 'as_of' column.")
            df["as_of"] = pd.to_datetime(df["as_of"])

            # Apply as_of date shift if configured
            shift_days = self.config.as_of_column_date_shift
            if shift_days != 0:
                logger.info(f"Applying as_of date shift of {shift_days} days.")
                df["as_of"] = df["as_of"] + pd.Timedelta(days=shift_days)

        return df

    def _extract_latest_ground_truth_and_history(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extracts the latest ground truth data from the target data and optionally processes historical snapshots.

        If an 'as_of' column is present:
        1. Identifies the latest 'as_of' date.
        2. Filters the DataFrame to only include rows from that latest date (Current Ground Truth).
        3. If historical data is NOT disabled, processes the full DataFrame into historical snapshots.

        If no 'as_of' column:
        Returns the original DataFrame as the ground truth.

        Args:
            df (pd.DataFrame): The standardized target data (potentially containing multiple as_of versions).

        Side Effect:
            Populate self.historical_target_data with historical target-data records

        Returns:
            pd.DataFrame: The current ground truth (latest as_of slice).
        """
        if "as_of" in df.columns:
            logger.info("Processing target data with 'as_of' versions...")

            # The actual most recent "as_of" date will be used as "Ground Truth",
            # Used for the default rendered target-data lines/values for all visualizations.
            # Other historical snapshots will be shown only when "Historical Target-Data Mode" toggle is on.
            latest_as_of = df["as_of"].max()
            logger.info(f"Latest 'as_of' date is {latest_as_of.date()}. Using this for current ground truth.")
            current_df = df[df["as_of"] == latest_as_of].copy()

            # Check if historical data feature is enabled
            if self.config.disable_historical_target_data:
                logger.info("Historical target data is DISABLED by config flag. Skipping historical snapshot processing.")
                # Leave self.historical_target_data as None (initialized in __init__)
            else:
                # Process historical snapshots: Map<as_of_date, Map<date, Map<location, data>>>
                logger.info("Processing historical target data snapshots from filtered data...")
                self.historical_target_data = process_historical_target_data(df, self.target_key_to_id_map, self.target_id_to_dvp_config)
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

        all_partitions = []

        # Find all subdirectories
        subdirs = [d for d in self.target_data_path.iterdir() if d.is_dir()]

        if not subdirs:
            raise FileNotFoundError(f"No subdirectories found in {self.target_data_path} for partitioned parquet mode")

        logger.info(f"  → Found {len(subdirs)} partition directories")

        for subdir in sorted(subdirs):
            # Extract as_of date from directory name
            dir_name = subdir.name

            as_of_date = None

            # Use Hive-style: as_of=YYYY-MM-DD
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

    def _get_models_to_load(self) -> list[str]:
        """
        Get list of model names to load, including baseline model if needed.

        This ensures baseline model is loaded for evaluation calculations even if
        it's not in available_models (which controls frontend display).

        Returns:
            list[str]: Model names to load from model-output directory
        """
        models_to_load = []

        # Add all available models (shown in frontend)
        if self.config.available_models:
            models_to_load = [m.model_name for m in self.config.available_models]

        # Add baseline model if not already included
        baseline = self.config.baseline_model_for_relative_WIS
        if baseline and baseline not in models_to_load:
            models_to_load.append(baseline)
            logger.info(f"  [+] Adding baseline model '{baseline}' for evaluation (not in available_models)")

        return models_to_load

    def _load_model_output_data(self) -> None:
        """
        Loads and prepares all model output data from the ``model-output`` directory.

        It iterates through each model subdirectory (including baseline if needed).
        See :attr:`~yaml_config_processor_pydantic.DashboardConfig.available_models`.

        The data is:
        1.  Loaded from CSVs/parquet files.
        2.  Renamed according to :attr:`~yaml_config_processor_pydantic.ModelOutputHeaderMapping`.
        3.  Calculated for ``horizon`` if missing.
        4.  Stored in unpivoted (long) format in self.model_output_unpivoted.

        Note: Pivoting happens later in run() after filtering.

        Returns:
            None (sets self.model_output_unpivoted as side effect)
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

        # Get models to load (includes baseline even if not in available_models)
        models_to_load = self._get_models_to_load()

        for model_name in models_to_load:
            # Checkout model's output in their designated folder
            model_dir = self.model_output_path / model_name
            if not model_dir.is_dir():
                logger.warning(f"  [!] Directory not found for model '{model_name}', skipping.")
                continue

            model_files = list(model_dir.glob("*.csv")) + list(model_dir.glob("*.parquet")) + list(model_dir.glob("*.pq"))
            if not model_files:
                logger.warning(f"  [!] No data files found for model '{model_name}', skipping.")
                continue

            logger.info(f"  [OK] Loading model '{model_name}': {len(model_files)} files")
            df_list = []
            for f in model_files:
                try:
                    if f.suffix == ".csv":
                        df_list.append(pd.read_csv(f, low_memory=False))
                    else:
                        df_list.append(pd.read_parquet(f))
                except Exception as e:
                    logger.error(f"Error loading file {f}: {e}")

            if not df_list:
                continue

            model_df = pd.concat(df_list, ignore_index=True)
            model_df["model"] = model_name
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

        # Ensure location is string type for consistency and parquet compatibility
        df = ensure_string_column(df, "location")

        # Normalize output_type_id to consistent string type
        # This prevents issues with mixed types (float 0.5 vs string "0.5") from different CSV files
        if "output_type_id" in df.columns:
            # Detect if there are mixed types
            original_types = df["output_type_id"].dropna().apply(type).unique()
            if len(original_types) > 1:
                logger.warning(f"  ⚠️  Mixed types detected in output_type_id: {[t.__name__ for t in original_types]}. Normalizing to string.")

            # Convert all output_type_id values to string for consistency
            # This ensures "0.5" (string) and 0.5 (float) are treated identically
            df["output_type_id"] = df["output_type_id"].astype(str)
            logger.info("  [OK] Normalized output_type_id to string type for consistency")

        # Enforce standard column order for consistency and easier debugging
        expected_cols = ["model", "reference_date", "target_end_date", "location", "target", "horizon", "output_type", "output_type_id", "value"]
        existing_cols = [c for c in expected_cols if c in df.columns]
        other_cols = [c for c in df.columns if c not in expected_cols]
        df = df[existing_cols + other_cols]
        logger.info("  [OK] Enforced standard column order")

        time_unit = self.config.time_unit
        if "horizon" not in df.columns:
            logger.info("Calculating 'horizon' column from date differences.")
            df["horizon"] = ((df["target_end_date"] - df["reference_date"]).dt.days / time_unit).astype(int)
        else:
            logger.info("'horizon' column already exists, using it.")

        # Validate schema before storing
        validate_model_output_schema(df)

        # Store unpivoted data for evaluations
        # Pivoting will happen later in the main run() procedure after filtering
        self.model_output_unpivoted = df
        logger.info("Stored unpivoted model output data (will be pivoted after filtering)")

    def _filter_data_by_config_specs(
        self,
        target_data_df: pd.DataFrame,
        model_output_df: pd.DataFrame,
        detected_locations: list,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Filters both target data and model output data by config-specified targets and detected locations.

        This ensures that downstream processing (including date range calculations) only operates
        on data that will actually be used by the dashboard, preventing wasted computation and
        incorrect date ranges from irrelevant data.

        Args:
            target_data_df (pd.DataFrame): Raw target data before filtering.
            model_output_df (pd.DataFrame): Raw model output data before filtering.
            detected_locations (list): List of location dictionaries with 'location' keys.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Filtered (target_data_df, model_output_df).
        """
        logger.info("Filtering data by config specifications...")

        # Extract valid location codes
        valid_location_codes = {loc["location"] for loc in detected_locations}
        logger.info(f"  → Valid locations: {len(valid_location_codes)} codes")

        # Extract valid target keys from config
        targets = self.config.targets or []
        valid_target_keys = {t.target_key_in_data for t in targets}
        logger.info(f"  → Valid targets: {valid_target_keys}")

        # Filter target data
        target_data_initial_rows = len(target_data_df)

        # Filter by location (if location column exists)
        if "location" in target_data_df.columns and valid_location_codes:
            # Normalize location codes in dataframe to string for comparison
            target_data_df = target_data_df[target_data_df["location"].astype(str).isin(valid_location_codes)].copy()
            logger.info(f"  → Target data after location filter: {len(target_data_df)} rows (removed {target_data_initial_rows - len(target_data_df)})")

        # Filter by target (if target column exists)
        if "target" in target_data_df.columns and valid_target_keys:
            target_data_df = target_data_df[target_data_df["target"].isin(valid_target_keys)].copy()
            logger.info(f"  → Target data after target filter: {len(target_data_df)} rows (removed {target_data_initial_rows - len(target_data_df)})")

        # Filter model output data
        model_output_initial_rows = len(model_output_df)

        # Filter by location
        if "location" in model_output_df.columns and valid_location_codes:
            model_output_df = model_output_df[model_output_df["location"].astype(str).isin(valid_location_codes)].copy()
            logger.info(f"  → Model output after location filter: {len(model_output_df)} rows (removed {model_output_initial_rows - len(model_output_df)})")

        # Filter by target
        if "target" in model_output_df.columns and valid_target_keys:
            model_output_df = model_output_df[model_output_df["target"].isin(valid_target_keys)].copy()
            logger.info(f"  → Model output after target filter: {len(model_output_df)} rows (removed {model_output_initial_rows - len(model_output_df)})")

        # Filter by output_type - only keep quantile predictions
        # This removes sample, pmf, and any other output types that are not supported
        if "output_type" in model_output_df.columns:
            pre_output_type_filter = len(model_output_df)
            model_output_df = model_output_df[model_output_df["output_type"] == "quantile"].copy()
            removed = pre_output_type_filter - len(model_output_df)
            logger.info(f"  → Model output after output_type filter (quantile only): {len(model_output_df)} rows (removed {removed})")

        # Ensure we still have data after filtering
        if target_data_df.empty:
            logger.warning("  [!] WARNING: Target data is empty after filtering!")
        if model_output_df.empty:
            logger.warning("  [!] WARNING: Model output data is empty after filtering!")

        logger.info("  [OK] Filtering complete")

        return target_data_df, model_output_df

    def _fix_missing_time_intervals(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills in missing time intervals in target data to ensure a complete time series.

        This prevents gaps in the chart visualizations. It generates a complete grid of
        dates (based on :attr:`~yaml_config_processor_pydantic.DashboardConfig.time_unit`)
        for all locations and targets, filling missing observations with -1.

        Now generates placeholders separately for each target, using target-specific date ranges.

        Args:
            target_data_df (pd.DataFrame): The pre-filtered target data.
            model_output_df (pd.DataFrame): The pre-filtered model output data (used to determine the full date range).

        Returns:
            pd.DataFrame: A DataFrame with continuous date ranges for every location/target.
        """
        logger.info("Fixing missing time intervals in target data (per-target date ranges)...")

        # Get all unique locations
        if "location" in target_data_df.columns:
            all_locations = target_data_df["location"].unique()
        else:
            all_locations = ["US"]  # Default single location

        # Handle targets for grid generation
        targets = self.config.targets or []
        target_keys = [t.target_key_in_data for t in targets] if targets else []

        time_unit_days = self.config.time_unit

        # Generate placeholders separately for each target using their specific date ranges
        all_fixed_dfs = []
        # Multi-target scenario: generate grids separately per target
        if "target" in target_data_df.columns and target_keys:
            for target_key in target_keys:
                target_id = self.target_key_to_id_map.get(target_key, target_key)

                # Get target-specific date range
                if target_id in self.date_ranges_per_target:
                    earliest_date = self.date_ranges_per_target[target_id]["earliestDate"]
                    latest_date = self.date_ranges_per_target[target_id]["latestDate"]
                else:
                    # Fallback: calculate from data
                    target_specific_df = target_data_df[target_data_df["target"] == target_key]
                    model_specific_df = model_output_df[model_output_df["target"] == target_key] if "target" in model_output_df.columns else model_output_df

                    if target_specific_df.empty:
                        logger.warning(f"No target data for target '{target_id}', skipping placeholder generation")
                        continue

                    earliest_date = target_specific_df["date"].min()
                    latest_date_target = target_specific_df["date"].max()
                    latest_date_model = model_specific_df["target_end_date"].max() if not model_specific_df.empty else latest_date_target
                    latest_date = max(latest_date_target, latest_date_model) if pd.notna(latest_date_model) else latest_date_target

                logger.info(f"  Date Range For Target '{target_id}': {earliest_date.date()} to {latest_date.date()}")

                # Generate date range for this specific target
                date_range = pd.date_range(start=earliest_date, end=latest_date, freq=f"{time_unit_days}D")

                # Create complete grid for this target: dates x locations
                complete_grid = pd.MultiIndex.from_product([date_range, all_locations, [target_key]], names=["date", "location", "target"])
                complete_df = pd.DataFrame(index=complete_grid).reset_index()

                # Get data for this specific target
                target_data_subset = target_data_df[target_data_df["target"] == target_key]

                # Merge with existing data
                fixed_target_df = pd.merge(complete_df, target_data_subset, on=["date", "location", "target"], how="left")

                # Fill missing observation values with -1
                fixed_target_df["observation"] = fixed_target_df["observation"].fillna(-1)

                all_fixed_dfs.append(fixed_target_df)

            # Combine all targets
            if all_fixed_dfs:
                fixed_df = pd.concat(all_fixed_dfs, ignore_index=True)
            else:
                logger.error("No valid target data to process")
                return target_data_df

        elif "location" in target_data_df.columns:
            # Single target or no target column: use overall date range
            earliest_date = target_data_df["date"].min()
            latest_date_target = target_data_df["date"].max()
            latest_date_model = model_output_df["target_end_date"].max()
            latest_date = max(latest_date_target, latest_date_model) if pd.notna(latest_date_model) else latest_date_target

            logger.info(f"Date range: {earliest_date.date()} to {latest_date.date()}")

            date_range = pd.date_range(start=earliest_date, end=latest_date, freq=f"{time_unit_days}D")
            complete_grid = pd.MultiIndex.from_product([date_range, all_locations], names=["date", "location"])
            complete_df = pd.DataFrame(index=complete_grid).reset_index()

            fixed_df = pd.merge(complete_df, target_data_df, on=["date", "location"], how="left")
            fixed_df["observation"] = fixed_df["observation"].fillna(-1)

        else:
            # Just date (single location, single target)
            earliest_date = target_data_df["date"].min()
            latest_date_target = target_data_df["date"].max()
            latest_date_model = model_output_df["target_end_date"].max()
            latest_date = max(latest_date_target, latest_date_model) if pd.notna(latest_date_model) else latest_date_target

            logger.info(f"Date range: {earliest_date.date()} to {latest_date.date()}")

            date_range = pd.date_range(start=earliest_date, end=latest_date, freq=f"{time_unit_days}D")
            complete_df = pd.DataFrame({"date": date_range})

            fixed_df = pd.merge(complete_df, target_data_df, on=["date"], how="left")
            fixed_df["observation"] = fixed_df["observation"].fillna(-1)

        logger.info("Missing time intervals fix complete.")

        return fixed_df

    def _detect_locations(self, target_data_df: pd.DataFrame) -> list:
        """
        Detects all unique locations from the provided data and configuration.

        This method implements a specific precedence logic to ensure the most accurate
        location names are displayed.

        The precedence order is:
        1.  **Custom Mapping File**: If ``custom_location_mapping_file_name`` is provided in config,
            ONLY these locations are used.
        2.  **Target Data**: Locations found in ``target_data_df``. If a ``location_name`` column exists,
            those names are used.
        3.  **Default Fallback**: Uses the built-in US FIPS code mapping (e.g., "01" -> "Alabama").

        Args:
            target_data_df (pd.DataFrame): The loaded ground truth data.

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
            logger.info(f"  Using locations from custom mapping file: {len(location_mapping)} locations")
            locations_list = [{"location": loc_code, "location_name": loc_name} for loc_code, loc_name in location_mapping.items()]
            # Sort by location code
            locations_list.sort(key=lambda x: x["location"])
            return locations_list

        # Priority 2: Detect from target-data
        logger.info("  No custom mapping file detected. Auto-detecting locations from target data...")

        detected_locations = {}  # Map of code -> name

        # Check target-data
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

        # Convert to list format
        locations_list = [{"location": loc_code, "location_name": loc_name} for loc_code, loc_name in detected_locations.items()]

        # Sort by location code
        locations_list.sort(key=lambda x: x["location"])

        logger.info(f"  [OK] Total detected locations: {len(locations_list)}")

        return locations_list

    def _calculate_overall_date_ranges_per_target(
        self,
        target_data_df: pd.DataFrame,
        model_output_df: pd.DataFrame,
    ) -> dict:
        """
        Calculate earliest and latest dates separately for each modeling target.

        This allows different targets to have different valid date ranges in the frontend,
        ensuring users can only select dates that are actually valid for the selected target.

        Args:
            target_data_df (pd.DataFrame): Pre-filtered target data
            model_output_df (pd.DataFrame): Pre-filtered model output data

        Returns:
            dict: {targetId: {'earliestDate': datetime, 'latestDate': datetime}}
        """
        logger.info("Calculating date ranges per target...")
        date_ranges = {}

        # Get unique targets from config
        targets = self.config.targets or []
        target_keys = [t.target_key_in_data for t in targets]

        if not target_keys:
            logger.warning("No targets defined in config, using 'default' target")
            target_keys = ["default"]

        for target_key in target_keys:
            # Get target ID from mapping
            target_id = self.target_key_to_id_map.get(target_key, target_key)

            # Filter target data for this specific target
            if "target" in target_data_df.columns:
                target_specific_df = target_data_df[target_data_df["target"] == target_key]
            else:
                target_specific_df = target_data_df

            # Filter model output for this specific target
            if "target" in model_output_df.columns:
                model_specific_df = model_output_df[model_output_df["target"] == target_key]
            else:
                model_specific_df = model_output_df

            if target_specific_df.empty and model_specific_df.empty:
                logger.warning(f"No data found for target: {target_id}")
                continue

            # Calculate date range for this target
            earliest_dates = []
            latest_dates = []

            if not target_specific_df.empty:
                earliest_dates.append(target_specific_df["date"].min())
                latest_dates.append(target_specific_df["date"].max())

            if not model_specific_df.empty:
                latest_dates.append(model_specific_df["target_end_date"].max())

            if earliest_dates and latest_dates:
                earliest_date = min(earliest_dates)
                latest_date = max(latest_dates)

                date_ranges[target_id] = {
                    "earliestDate": earliest_date,
                    "latestDate": latest_date,
                }

                logger.info(f"  Target '{target_id}': {earliest_date.date()} to {latest_date.date()}")

        return date_ranges

    def _track_model_availability_per_period(self, model_output_df: pd.DataFrame):
        """
        Track which models have data available for each forecast period.

        This enables the frontend to intelligently gray out and disable models
        that don't have data for the selected time range.

        Args:
            model_output_df (pd.DataFrame): The model output data with all models
        """
        logger.info("Tracking model availability per period...")

        if "model" not in model_output_df.columns:
            logger.warning("No 'model' column found, skipping model availability tracking")
            return

        # Get all unique models
        all_models = model_output_df["model"].unique().tolist()

        # Ensure target_end_date is datetime
        if not pd.api.types.is_datetime64_any_dtype(model_output_df["target_end_date"]):
            model_output_df = model_output_df.copy()
            model_output_df["target_end_date"] = pd.to_datetime(model_output_df["target_end_date"])

        # Track for both static and special periods
        special_periods = self.config.special_forecast_periods or []
        all_periods = list(self.config.forecast_periods) + list(special_periods)

        for period in all_periods:
            period_id = period.forecast_period_id if hasattr(period, "forecast_period_id") else period.special_period_id

            # Get date range for this period
            # Use non-filled target data to get correct anchor dates (excludes placeholders)
            target_for_anchor = self.target_data_non_filled if self.target_data_non_filled is not None else pd.DataFrame()
            date_range = self._get_period_date_range(period, target_for_anchor, model_output_df)
            if not date_range:
                logger.warning(f"Could not determine date range for period '{period_id}', skipping")
                continue

            start_date, end_date = date_range

            # Filter model output to this period
            period_df = model_output_df[(model_output_df["target_end_date"] >= start_date) & (model_output_df["target_end_date"] <= end_date)]

            # Get models that have data in this period
            models_with_data = period_df["model"].unique().tolist() if not period_df.empty else []

            # Store availability info
            self.model_availability_per_period[period_id] = {
                "availableModels": models_with_data,
                "unavailableModels": [m for m in all_models if m not in models_with_data],
                "startDate": to_utc_iso_string(start_date) if pd.notna(start_date) else None,
                "endDate": to_utc_iso_string(end_date) if pd.notna(end_date) else None,
            }

            logger.info(f"  Period '{period_id}': {len(models_with_data)}/{len(all_models)} models have data")

        logger.info("Model availability tracking complete.")

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
        logger.warning("  [!] No locations detected, defaulting to 'US'")
        return "US"

    def _get_evaluation_period_ids(self) -> list:
        """
        Get list of period IDs that will have evaluation data.

        This includes both static forecast periods and special/dynamic periods.
        Used to inform the frontend about available evaluation data folders.

        Returns:
            list: List of period ID strings
        """
        period_ids = []

        # Add static forecast periods
        for period in self.config.forecast_periods:
            period_ids.append(period.forecast_period_id)

        # Add special/dynamic periods
        special_periods = self.config.special_forecast_periods or []
        for period in special_periods:
            period_ids.append(period.special_period_id)

        return period_ids

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
        logger.info("")
        logger.info("=" * 60)
        logger.info("GENERATING METADATA")
        logger.info("=" * 60)

        # Get the date range
        all_dates = pd.concat([target_data_df["date"], model_output_df["target_end_date"]]).dropna()
        earliest_date_across_targets = all_dates.min()
        latest_date_across_targets = all_dates.max()

        # Get the latest reference date across all models, for default selection
        # Since all the models are toggled on by default, visualization guaranteed has prediction line thus
        latest_model_ref_date = model_output_df["reference_date"].max()

        # Build forecast period info with ongoing period detection
        logger.info("Building forecast period metadata...")
        forecast_periods_info = []
        ongoing_periods_metadata = {}  # Store for special period calculation

        for idx, period in enumerate(self.config.forecast_periods):
            # Compute ongoing period metadata
            ongoing_meta = compute_ongoing_period_metadata(period, target_data_df, model_output_df)

            period_info = {
                "forecastPeriodId": period.forecast_period_id,
                "displayString": period.display_string,
                "timeValue": f"{period.start_date.date()}/{period.end_date.date()}",
                "startDate": to_utc_iso_string(period.start_date),
                "endDate": to_utc_iso_string(period.end_date),
                "isDefaultSelected": period.is_default_selected,
            }

            # Add ongoing period specific fields
            if ongoing_meta["isOngoing"]:
                period_info["isOngoing"] = True
                period_info["actualEndDate"] = ongoing_meta["actualEndDate"]
                period_info["anchorDate"] = ongoing_meta["anchorDate"]
                period_info["configuredEndDate"] = ongoing_meta["configuredEndDate"]

                logger.info(f"  Ongoing period '{period.forecast_period_id}':")
                logger.info(f"    Configured end: {period.end_date.date()}")
                logger.info(f"    Actual end: {pd.to_datetime(ongoing_meta['actualEndDate']).date()}")
                logger.info(f"    Anchor: {pd.to_datetime(ongoing_meta['anchorDate']).date()}")

                # Store for special period calculation
                ongoing_periods_metadata[period.forecast_period_id] = ongoing_meta

            forecast_periods_info.append(period_info)

        # Process special/dynamic forecast periods
        special_periods = self.config.special_forecast_periods or []
        if special_periods:
            logger.info("Processing special forecast periods...")

        for period in special_periods:
            anchor_period_id = period.time_anchor.anchor_on

            # Get metadata for the period this special period anchors to
            if anchor_period_id not in ongoing_periods_metadata:
                logger.warning(f"Special period '{period.special_period_id}' anchors to '{anchor_period_id}' which is not an ongoing period. Skipping.")
                continue

            # Compute date range for special period
            special_meta = compute_special_period_date_range(period, ongoing_periods_metadata[anchor_period_id], self.config.time_unit)

            if not special_meta["startDate"] or not special_meta["endDate"]:
                logger.warning(f"Could not compute date range for special period '{period.special_period_id}'. Skipping.")
                continue

            period_meta = {
                "forecastPeriodId": period.special_period_id,
                "displayString": period.display_string,
                "timeValue": f"{pd.to_datetime(special_meta['startDate']).date()}/{pd.to_datetime(special_meta['endDate']).date()}",
                "startDate": special_meta["startDate"],
                "endDate": special_meta["endDate"],
                "isDefaultSelected": False,
                "isDynamic": True,
                "isSpecial": True,
                "anchorDate": special_meta["anchorDate"],
                "anchoredTo": special_meta["anchoredTo"],
            }
            forecast_periods_info.append(period_meta)

        # Build targets info with per-target date ranges
        targets_info = []
        default_target_id = None
        targets = self.config.targets or []
        for target in targets:
            target_info = {
                "targetId": target.target_id,
                "targetKeyInData": target.target_key_in_data,
                "displayString": target.task_display_string,
                "forecastPeriods": target.for_forecast_periods or [],
                "isDefaultSelected": target.is_default_selected,
                "dataValueProcessing": target.data_value_processing.model_dump() if target.data_value_processing else None,
            }

            # Add target-specific date range if available
            if target.target_id in self.date_ranges_per_target:
                date_range = self.date_ranges_per_target[target.target_id]
                target_info["earliestDate"] = to_utc_iso_string(date_range["earliestDate"]) if pd.notna(date_range["earliestDate"]) else None
                target_info["latestDate"] = to_utc_iso_string(date_range["latestDate"]) if pd.notna(date_range["latestDate"]) else None

            targets_info.append(target_info)

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
                # Historical data is enabled if: (1) not disabled by config AND (2) data was successfully processed
                "historicalTargetDataEnabled": not self.config.disable_historical_target_data and self.historical_target_data is not None,
                # Development mode flag - tells frontend which data path to use
                "developmentMode": self.dev_mode,
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
                # Global date range (for backward compatibility and overall bounds)
                "earliestDateAcrossTargets": to_utc_iso_string(earliest_date_across_targets) if pd.notna(earliest_date_across_targets) else None,
                "latestDateAcrossTargets": to_utc_iso_string(latest_date_across_targets) if pd.notna(latest_date_across_targets) else None,
                "defaultSelectedDate": to_utc_iso_string(latest_model_ref_date) if pd.notna(latest_model_ref_date) else None,
                # Note: Target-specific date ranges are available in targets[].earliestDate and targets[].latestDate
            },
            # === FORECAST PERIODS ===
            "forecastPeriods": forecast_periods_info,
            # === MODELS ===
            "models": {
                "list": model_configs,
                "baselineModel": self.config.baseline_model_for_relative_WIS if not self.skip_evaluations else None,
                # Model availability per period (for intelligent model selection UI)
                "availabilityPerPeriod": self.model_availability_per_period,
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
            # === EVALUATION SETTINGS ===
            "evaluations": {
                "coverageLevels": self.config.evaluation_coverage_levels if hasattr(self.config, "evaluation_coverage_levels") else [50, 95],
                "locationMapCoverageLevel": self.config.evaluation_coverage_level_for_location_map if hasattr(self.config, "evaluation_coverage_level_for_location_map") else 95,
                # List of period IDs that have evaluation data available (for lazy loading)
                "availablePeriodIds": self._get_evaluation_period_ids() if not self.skip_evaluations else [],
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
                "hasHistoricalData": not self.config.disable_historical_target_data and self.historical_target_data is not None,
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
                "evaluationsPage": {
                    "tabNames": {
                        "overviewTab": self.config.ui_customization.ui_evaluation_overview_tab_name,
                        "singleModelTab": self.config.ui_customization.ui_evaluation_single_model_tab_name,
                    },
                    "chartLogModeIndicatorText": self.config.ui_customization.ui_evaluation_chart_log_mode_indicator_text,
                    "overviewLocationMapTitle": self.config.ui_customization.ui_evaluation_overview_location_map_title,
                    "infoButtons": {
                        "overviewInfo": (
                            {
                                "title": self.config.ui_customization.ui_evaluation_overview_infobutton_content.title,
                                "content": self.config.ui_customization.ui_evaluation_overview_infobutton_content.content,
                            }
                            if self.config.ui_customization.ui_evaluation_overview_infobutton_content
                            else None
                        ),
                        "singleModelInfo": (
                            {
                                "title": self.config.ui_customization.ui_evaluation_single_model_infobutton_content.title,
                                "content": self.config.ui_customization.ui_evaluation_single_model_infobutton_content.content,
                            }
                            if self.config.ui_customization.ui_evaluation_single_model_infobutton_content
                            else None
                        ),
                        "overviewHorizonInfo": (
                            {
                                "title": self.config.ui_customization.ui_evaluation_overview_horizon_infobutton_content.title,
                                "content": self.config.ui_customization.ui_evaluation_overview_horizon_infobutton_content.content,
                            }
                            if self.config.ui_customization.ui_evaluation_overview_horizon_infobutton_content
                            else None
                        ),
                        "singleModelHorizonInfo": (
                            {
                                "title": self.config.ui_customization.ui_evaluation_single_model_horizon_infobutton_content.title,
                                "content": self.config.ui_customization.ui_evaluation_single_model_horizon_infobutton_content.content,
                            }
                            if self.config.ui_customization.ui_evaluation_single_model_horizon_infobutton_content
                            else None
                        ),
                    },
                    "locationMapColorScale": {
                        "colorTop": self.config.ui_customization.ui_evaluation_overview_location_map_color_scale.color_top,
                        "colorBase": self.config.ui_customization.ui_evaluation_overview_location_map_color_scale.color_base,
                        "colorBottom": self.config.ui_customization.ui_evaluation_overview_location_map_color_scale.color_bottom,
                        "colorNull": self.config.ui_customization.ui_evaluation_overview_location_map_color_scale.color_null,
                    },
                },
            },
            # === METADATA INFO ===
            "_meta": {
                "generatedAt": pd.Timestamp.now(tz="UTC").isoformat(),
            },
        }

        # Validate and extract default location
        default_location_code = self._extract_and_validate_default_location(locations_info, metadata["defaults"]["location"])
        metadata["defaults"]["location"] = default_location_code

        logger.info("Metadata generated.")
        return metadata

    def _generate_raw_evaluation_collection(self, target_data_df: pd.DataFrame, model_output_df: pd.DataFrame) -> dict:
        """
        Generate complete collection of raw evaluation scores.

        Calculates WIS, MAPE, Coverage for ALL data (no period filtering).
        Then calculates WIS Ratio against baseline model.

        Args:
            target_data_df: Complete target data with placeholder filtering applied
            model_output_df: Complete model output data (unpivoted)

        Returns:
            dict: Raw evaluation DataFrames {'wis', 'wis_ratio', 'mape', 'coverage'}
        """
        logger.info("=" * 60)
        logger.info("STEP 1: GENERATING RAW EVALUATION COLLECTION")
        logger.info("=" * 60)

        # Calculate base metrics using evaluation processor
        evaluation_results = self.evaluation_processor.evaluate_predictions(target_data_df=target_data_df, model_output_df=model_output_df)

        # Calculate WIS Ratio
        if "wis" in evaluation_results and not evaluation_results["wis"].empty:
            logger.info("Calculating WIS Ratios...")
            wis_ratio_df = self.evaluation_processor.calculate_wis_ratio(evaluation_results["wis"])
            if not wis_ratio_df.empty:
                evaluation_results["wis_ratio"] = wis_ratio_df

        total_evals = sum(len(df) for df in evaluation_results.values() if isinstance(df, pd.DataFrame))
        self.processing_stats["evaluations_calculated"] = total_evals

        logger.info("Raw evaluation collection complete.")
        return evaluation_results

    def _generate_aggregated_evaluation_collection(self, raw_evaluations: dict, affected_date_range: tuple = None) -> dict:
        """
        Generate aggregated evaluation statistics by forecast period.

        Takes the raw scores and groups them by configured forecast periods
        to produce frontend-ready aggregated statistics.

        Args:
            raw_evaluations: Dictionary of raw evaluation DataFrames
            affected_date_range: tuple (start_date, end_date) of changed data.
                               If None, re-aggregate ALL periods.
                               If provided, only re-aggregate periods overlapping with this range.

        Returns:
            dict: Aggregated evaluation data for AppDataEvaluationsPrecalculated
        """
        logger.info("=" * 60)
        logger.info("STEP 2: GENERATING AGGREGATED EVALUATION COLLECTION")
        logger.info("=" * 60)

        # Initialize structure with existing aggregates or empty
        precalculated = self.aggregated_evaluations.copy()
        if "iqr" not in precalculated:
            precalculated["iqr"] = {}
        if "locationMap_aggregates" not in precalculated:
            precalculated["locationMap_aggregates"] = {}
        if "detailedCoverage_aggregates" not in precalculated:
            precalculated["detailedCoverage_aggregates"] = {}

        # Get configuration values
        cov_levels = sorted([int(x) for x in (self.config.evaluation_coverage_levels or [50, 95])])
        location_map_cov_level = self.config.evaluation_coverage_level_for_location_map if hasattr(self.config, 'evaluation_coverage_level_for_location_map') else 95

        # Define all periods to aggregate over
        special_periods = self.config.special_forecast_periods or []
        all_periods = list[ForecastPeriodConfig](self.config.forecast_periods) + list[SpecialForecastPeriodConfig](special_periods)

        for period in all_periods:
            period_id = period.forecast_period_id if hasattr(period, "forecast_period_id") else period.special_period_id

            # Get date range for this period
            # Use non-filled target data to get correct anchor dates (excludes placeholders)
            target_for_anchor = self.target_data_non_filled if self.target_data_non_filled is not None else self.fixed_target_data
            date_range = self._get_period_date_range(period, target_for_anchor, self.model_output_unpivoted)
            if not date_range:
                logger.warning(f"Could not determine date range for period '{period_id}', skipping")
                continue
            start, end = date_range

            # Check if we need to re-aggregate this period
            if affected_date_range:
                aff_start, aff_end = affected_date_range
                # Check for overlap: start <= aff_end and end >= aff_start
                if not (start <= aff_end and end >= aff_start):
                    logger.info(f"Skipping static period '{period_id}' (No changes in {start.date()} - {end.date()})")
                    continue

            logger.info(f"Processing period: '{period_id}' ({start.date()} to {end.date()})")

            # Initialize/Clear period structure
            precalculated["iqr"][period_id] = {}
            precalculated["locationMap_aggregates"][period_id] = {}
            precalculated["detailedCoverage_aggregates"][period_id] = {}

            # Process location map aggregates FIRST (IQR depends on this)
            process_location_map_aggregates(raw_evaluations, period_id, start, end, precalculated, self.target_key_to_id_map, location_map_cov_level)

            # Process IQR statistics for boxplots (uses state_map_aggregates)
            process_iqr_stats(period_id, precalculated)

            # Process coverage aggregates
            process_coverage_aggregates(raw_evaluations, period_id, start, end, precalculated, cov_levels, self.target_key_to_id_map)

        logger.info("Aggregated evaluation collection complete.")
        return precalculated

    def _generate_raw_scores_by_period(self, raw_evaluations: dict) -> dict:
        """
        Organize ALL raw scores (not bounded by periods) for Single Model view.

        This allows the frontend to filter by any custom date range.
        Structure: target → metric → model → location → horizon → [all scores]

        Args:
            raw_evaluations: Dictionary of raw evaluation DataFrames

        Returns:
            dict: Raw scores organized by target for frontend consumption
        """
        logger.info("=" * 60)
        logger.info("STEP 3: ORGANIZING ALL RAW SCORES (NO PERIOD FILTERING)")
        logger.info("=" * 60)

        raw_scores_data = {}

        # Process WIS Ratio
        if "wis_ratio" in raw_evaluations and not raw_evaluations["wis_ratio"].empty:
            logger.info("Organizing WIS/Baseline raw scores...")
            organize_metric_all_data(raw_evaluations["wis_ratio"], "WIS/Baseline", "wis_ratio", raw_scores_data, self.target_key_to_id_map)

        # Process MAPE
        if "mape" in raw_evaluations and not raw_evaluations["mape"].empty:
            logger.info("Organizing MAPE raw scores...")
            organize_metric_all_data(raw_evaluations["mape"], "MAPE", "mape", raw_scores_data, self.target_key_to_id_map)

        logger.info("Raw scores organization complete.")
        return raw_scores_data

    def _write_output_files(
        self,
        target_data: dict,
        model_output_data: dict,
        metadata: dict,
        raw_scores_by_period: dict = None,
        aggregated_evaluations: dict = None,
    ):
        """
        Writes all processed data to JSON files in the public output directory.

        Directory Structure:
        - metadata.json (root)
        - forecast/targetData.json
        - forecast/modelOutputData.json
        - forecast/historical-target-data.json (optional)
        - evaluations/{period_id}/aggregates.json (per period)
        - evaluations/rawScores.json (single file with all raw scores)


        Args:
            target_data: Processed target data for forecast visualization
            model_output_data: Processed model output data for forecast visualization
            metadata: Metadata object
            raw_scores_by_period: Raw evaluation scores (all data, not filtered by period)
            aggregated_evaluations: Aggregated evaluation statistics (iqr, locationMap_aggregates, detailedCoverage_aggregates)
        """
        logger.info("=" * 60)
        logger.info("WRITING OUTPUT FILES")
        logger.info("=" * 60)
        logger.info(f"Output directory: {self.output_base_path}")

        self.output_base_path.mkdir(exist_ok=True, parents=True)

        # Write metadata (root level)
        metadata_file = self.output_base_path / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, separators=(",", ":"))
        self._track_file_written(metadata_file)

        # Create and populate forecast subdirectory
        forecast_dir = self.output_base_path / "forecast"
        forecast_dir.mkdir(exist_ok=True, parents=True)

        # Write forecast data
        gt_file = forecast_dir / "targetData.json"
        with open(gt_file, "w") as f:
            json.dump(target_data, f, cls=NpEncoder, separators=(",", ":"))
        self._track_file_written(gt_file)

        pred_file = forecast_dir / "modelOutputData.json"
        with open(pred_file, "w") as f:
            json.dump(model_output_data, f, cls=NpEncoder, separators=(",", ":"))
        self._track_file_written(pred_file)

        # Write historical data if available
        if self.historical_target_data:
            historical_file = forecast_dir / "historical-target-data.json"
            with open(historical_file, "w") as f:
                json.dump(self.historical_target_data, f, cls=NpEncoder, separators=(",", ":"))
            self._track_file_written(historical_file)

        # Create and populate evaluations subdirectory
        if aggregated_evaluations or raw_scores_by_period:
            eval_base_dir = self.output_base_path / "evaluations"
            eval_base_dir.mkdir(exist_ok=True, parents=True)

            # Write aggregated evaluations (organized by period)
            if aggregated_evaluations:
                # Get all period IDs from the aggregated data
                period_ids = set(aggregated_evaluations.get("iqr", {}).keys())

                logger.info(f"Writing aggregated evaluation data for {len(period_ids)} periods...")

                for period_id in period_ids:
                    # Create period-specific folder
                    period_dir = eval_base_dir / period_id
                    period_dir.mkdir(exist_ok=True, parents=True)

                    # Extract aggregated data for this period
                    period_aggregates = {
                        "iqr": aggregated_evaluations.get("iqr", {}).get(period_id, {}),
                        "locationMap_aggregates": aggregated_evaluations.get("locationMap_aggregates", {}).get(period_id, {}),
                        "detailedCoverage_aggregates": aggregated_evaluations.get("detailedCoverage_aggregates", {}).get(period_id, {}),
                    }

                    # Write aggregates for this period
                    aggregates_file = period_dir / "aggregates.json"
                    with open(aggregates_file, "w") as f:
                        json.dump(period_aggregates, f, cls=NpEncoder, separators=(",", ":"))
                    self._track_file_written(aggregates_file)

                    logger.info(f"  [OK] Written aggregated evaluation data for period: {period_id}")

            # Write raw scores (all data, not organized by period)
            if raw_scores_by_period:
                raw_scores_file = eval_base_dir / "rawScores.json"
                with open(raw_scores_file, "w") as f:
                    json.dump(raw_scores_by_period, f, cls=NpEncoder, separators=(",", ":"))
                self._track_file_written(raw_scores_file)
                logger.info("  [OK] Written all raw evaluation scores to evaluations/rawScores.json")

        logger.info("All output files written successfully!")

    def _track_file_written(self, file_path: Path):
        """Track files written for summary reporting."""
        self.processing_stats["files_written"] += 1
        self.processing_stats["output_files"].append(str(file_path.relative_to(self.project_root)))

    def _save_intermediates(self):
        """
        Save current state to intermediate files for future incremental updates.

        Saves:
        - Current target data (parquet)
        - Model output unpivoted data (parquet)
        - Raw evaluations per metric (separate parquet files)
        - Aggregated evaluations (JSON cache for performance)
        """
        try:
            logger.info("")
            logger.info("Saving intermediates for future incremental updates...")

            files_saved = []

            # Save target data
            if self.current_target_data is not None and not self.current_target_data.empty:
                target_path = self.intermediates_dir / "target_data.parquet"
                # Ensure location column is string type for parquet compatibility
                target_df_to_save = self.current_target_data.copy()
                target_df_to_save = ensure_string_column(target_df_to_save, "location")
                target_df_to_save.to_parquet(target_path)
                files_saved.append(f"target_data ({len(target_df_to_save)} rows)")

            # Save model output (unpivoted for evaluations)
            if self.model_output_unpivoted is not None and not self.model_output_unpivoted.empty:
                model_path = self.intermediates_dir / "model_output_unpivoted.parquet"
                # Ensure location column is string type for parquet compatibility
                model_df_to_save = self.model_output_unpivoted.copy()
                model_df_to_save = ensure_string_column(model_df_to_save, "location")
                model_df_to_save.to_parquet(model_path)
                files_saved.append(f"model_output ({len(model_df_to_save)} rows)")

            # Save raw evaluations (separate parquet per metric)
            if self.raw_evaluations:
                for metric, df in self.raw_evaluations.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        eval_path = self.intermediates_dir / f"raw_evaluations_{metric}.parquet"
                        df.to_parquet(eval_path)
                        files_saved.append(f"{metric}_evaluations ({len(df)} rows)")

            # Save aggregated evaluations cache (for faster incremental updates)
            if self.aggregated_evaluations:
                agg_path = self.intermediates_dir / "aggregated_evaluations.json"
                with open(agg_path, "w") as f:
                    json.dump(self.aggregated_evaluations, f, cls=NpEncoder)
                files_saved.append("aggregated_evaluations_cache")

            logger.info(f"  [OK] Saved: {', '.join(files_saved)}")

        except Exception as e:
            logger.error(f"Failed to save intermediates: {e}")
            import traceback

            traceback.print_exc()

    def _load_specific_model_files(self, file_rel_paths: list) -> pd.DataFrame:
        """
        Load specific model output files (for incremental updates).
        """
        all_dfs = []
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
        valid_rename_dict = {k: v for k, v in rename_dict.items() if k is not None}

        for rel_path in file_rel_paths:
            file_path = self.project_root / rel_path
            if not file_path.exists():
                continue

            try:
                df = pd.read_csv(file_path, low_memory=False)
                # Determine model name from path
                # Fallback: assume parent of file is model name
                model_name = file_path.parent.name

                df["model"] = model_name
                df.rename(columns=valid_rename_dict, inplace=True)

                # Ensure date columns
                for col in ["reference_date", "target_end_date"]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col])

                # Ensure location is string type
                df = ensure_string_column(df, "location")

                # Normalize output_type_id to consistent string type
                # This prevents issues with mixed types (float 0.5 vs string "0.5") from different CSV files
                if "output_type_id" in df.columns:
                    df["output_type_id"] = df["output_type_id"].astype(str)

                # Calculate horizon if missing
                time_unit = self.config.time_unit
                if "horizon" not in df.columns and "target_end_date" in df.columns and "reference_date" in df.columns:
                    df["horizon"] = ((df["target_end_date"] - df["reference_date"]).dt.days / time_unit).astype(int)

                all_dfs.append(df)

            except Exception as e:
                logger.error(f"Error loading file {rel_path}: {e}")

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            return combined_df
        return pd.DataFrame()

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

            # Always anchor on latest VALID target data date within the referenced period
            anchor_on_period_id = anchor_config.anchor_on
            range_calc = anchor_config.range_calculation
            time_unit = self.config.time_unit

            # Find the referenced period
            ref_period = next((p for p in self.config.forecast_periods if p.forecast_period_id == anchor_on_period_id), None)
            if not ref_period:
                logger.warning(f"Special period '{period_id}' references unknown period '{anchor_on_period_id}'. Skipping.")
                return None

            # Filter target data to the referenced period's static range
            if target_data_df.empty:
                logger.warning(f"No target data available for special period '{period_id}'. Skipping.")
                return None

            # Filter by date range of the referenced period
            # Note: ref_period.start_date/end_date are already datetime objects from Pydantic
            # Ensure target_data_df['date'] is datetime
            if not pd.api.types.is_datetime64_any_dtype(target_data_df["date"]):
                target_data_df = target_data_df.copy()
                target_data_df["date"] = pd.to_datetime(target_data_df["date"])

            # Filter to period range AND exclude placeholder observations (-1)
            # Placeholders represent dates with predictions but no ground truth yet
            if "observation" in target_data_df.columns:
                relevant_data = target_data_df[
                    (target_data_df["date"] >= ref_period.start_date)
                    & (target_data_df["date"] <= ref_period.end_date)
                    & (target_data_df["observation"] != -1)  # Exclude placeholders
                ]
            else:
                # Fallback if observation column missing
                relevant_data = target_data_df[(target_data_df["date"] >= ref_period.start_date) & (target_data_df["date"] <= ref_period.end_date)]

            if relevant_data.empty:
                logger.warning(f"No valid target data found within referenced period '{anchor_on_period_id}' for '{period_id}'. Skipping.")
                return None

            # Anchor date is the latest date with ACTUAL ground truth
            anchor_date = relevant_data["date"].max()

            if pd.isna(anchor_date):
                return None

            # Calculate start date: anchor_date + (range_calc * time_unit)
            # range_calc is negative (e.g., -1 for last 2 weeks)
            # 0 shift = just the anchor week.
            start_date = anchor_date + pd.Timedelta(days=range_calc * time_unit)
            end_date = anchor_date

            logger.info(f"  Dynamic Period '{period_id}': {start_date.date()} to {end_date.date()} (Anchored on {anchor_date.date()})")
            return start_date, end_date
        else:
            return period.start_date, period.end_date


def process_data(config: DashboardConfig, dev_mode: bool = False, skip_evaluations: bool = False, is_data_update_run: bool = False):
    """
    Main function to instantiate and run the data processor.
    This will be called by the main workflow orchestrator.

    Args:
        config (DashboardConfig): DashboardConfig object with all settings
        dev_mode (bool): If True, use development-mode-root/ directory
        skip_evaluations (bool): If True, skip evaluation metrics calculation
        is_data_update_run (bool): If True, halt if no previous run artifacts found
    """
    try:
        processor = DataProcessor(config, dev_mode=dev_mode, skip_evaluations=skip_evaluations)
        success = processor.run(is_data_update_run=is_data_update_run)
        if not success:
            raise RuntimeError("Data processing failed.")
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        raise
