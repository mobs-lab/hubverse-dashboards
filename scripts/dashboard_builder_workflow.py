# dashboard_builder_workflow.py

"""
Dashboard Builder Workflow
Main orchestrator for the Hubverse Dashboard data processing pipeline.
"""

import sys
import logging
import shutil
from pathlib import Path
from typing import Optional

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from yaml_config_processor_pydantic import load_and_validate_config, DashboardConfig
from data_processor import process_data
from data_fetcher import DataFetcher


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class DashboardBuilder:
    """
    Main orchestrator for the Hubverse Dashboard build pipeline.

    Coordinates the end-to-end workflow of loading/validating configuration,
    fetching remote data, obtaining user confirmation, and running the
    data-processing pipeline that produces frontend-ready JSON files.

    The typical procedure is:
        1. :meth:`run_config_validation` — load config, fetch data, prompt user
        2. :meth:`run_data_processing` — process target-data, model-output, and evaluations

    Attributes:
        config_path: Resolved path to the YAML configuration file.
        config: Validated :class:`DashboardConfig` instance (set after loading).
        project_root: Resolved project root directory.
        dev_mode: Whether development mode is active.
        skip_evaluations: Whether to skip evaluation metric calculation.
        is_data_update: Whether this is an incremental data-update run.
        data_fetcher: :class:`DataFetcher` instance for remote data retrieval.
    """

    def __init__(self, config_path: str = "config.yaml", dev_mode: bool = False, skip_evaluations: bool = False, is_data_update: bool = False):
        """
        Initialize the DashboardBuilder.

        Args:
            config_path: Path to the YAML configuration file.
                Defaults to ``"config.yaml"`` in the current working directory.
            dev_mode: If True, look for data under ``development-mode-root/``
                instead of the project root.
            skip_evaluations: If True, skip evaluation metrics calculation
                (WIS, Coverage, MAPE). The Evaluations page will be disabled.
            is_data_update: If True, perform an incremental data-update run
                that requires existing intermediates from a prior full build.
        """
        self.config_path = Path(config_path)
        self.config: Optional[DashboardConfig] = None
        self.project_root = self._get_project_root()
        self.dev_mode = dev_mode
        self.skip_evaluations = skip_evaluations
        self.is_data_update = is_data_update
        self.data_fetcher = DataFetcher(self.project_root, dev_mode=dev_mode)

    def _get_project_root(self) -> Path:
        """
        Determine the project root directory.

        Assumes this script lives in ``PROJECTROOT/scripts/``.

        Returns:
            Path: Absolute path to the project root directory.
        """
        # Assume script is in `PROJECTROOT/scripts`
        return Path(__file__).parent.parent

    def run_config_validation(self) -> bool:
        """
        Load configuration and validate before processing

        Returns:
            bool: True if user confirms to proceed, False otherwise
        """
        print("\n" + "=" * 80)
        print("HUBVERSE DASHBOARD BUILDER - Configuration Validation")
        print("=" * 80)

        # Step 1: Load and validate configuration
        if not self._load_configuration():
            return False

        # Step 2: Fetch remote data if configured
        if not self._fetch_remote_data():
            # If fetching failed but was attempted, we should stop
            if self.config.link_to_hubverse_compatible_data:
                return False

        self._prompt_to_continue()

        # Step 3: Ask user for confirmation
        return self._get_user_confirmation()

    def _fetch_remote_data(self) -> bool:
        """
        Fetch remote data from a Hubverse-compatible GitHub repository.

        Checks :attr:`config.link_to_hubverse_compatible_data` for a remote URL.
        If present, downloads data via :class:`DataFetcher` and syncs it to the
        appropriate local input directories, filtering to only configured models.

        Returns:
            bool: True if data was fetched successfully or no remote data was
                configured. False if the fetch was attempted but failed.
        """
        repo_url = self.config.link_to_hubverse_compatible_data

        if not repo_url:
            return True

        print("\n[Step 1.5] Fetching remote data...")
        print(f"Remote Repository: {repo_url}")

        # Let DataFetcher handle cache location based on dev_mode
        success, cache_path = self.data_fetcher.fetch_data(repo_url)

        if success:
            # Sync to input directories
            # Determine destination
            if self.dev_mode:
                dest = self.project_root / "development-mode-root"
            else:
                dest = self.project_root

            # Get configured model names to sync only those models
            configured_models = None
            if self.config and self.config.available_models:
                configured_models = [m.model_name for m in self.config.available_models]

                # Add baseline model if not already in list
                baseline = self.config.baseline_model_for_relative_WIS
                if baseline and baseline not in configured_models:
                    configured_models.append(baseline)
                    logger.info(f"Including baseline model '{baseline}' in sync")

            # Use the actual cache path returned
            self.data_fetcher.sync_to_destination(str(cache_path.relative_to(self.project_root)), dest, configured_models=configured_models)
            return True
        else:
            logger.error("Failed to fetch remote data.")
            return False

    def _load_configuration(self) -> bool:
        """
        Load and validate the YAML configuration file.

        Delegates to :func:`load_and_validate_config` from the Pydantic config
        processor. On success, stores the validated :class:`DashboardConfig`
        in :attr:`self.config`.

        Returns:
            bool: True if the configuration was loaded and validated
                successfully. False on any error (file not found, validation
                failure, or unexpected exception).
        """
        print("\n[Step 1/2] Loading configuration file...")
        print(f"Config path: {self.config_path}")
        if self.dev_mode:
            print("Dev mode: ON - will check for data in development-mode-root/")

        # Call the YAML config processor's load_config method
        try:
            self.config = load_and_validate_config(self.config_path, dev_mode=self.dev_mode)
            print("[OK] Configuration loaded and validated successfully\n")
            return True

        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            logger.error("Please create a config.yaml file in the project root.")
            logger.error("You can copy an example from config-file-examples/ and customize it:"
                         "\n  cp config-file-examples/config.yaml.example config.yaml")
            return False

        except ValueError as e:
            logger.error(f"Configuration validation failed: {e}")
            logger.error("\nPlease fix the errors in your config.yaml file and try again.")
            return False

        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            return False

    def _prompt_to_continue(self, message: str = "Press Enter to continue..."):
        """
        Pause execution and wait for the user to press Enter.

        Args:
            message: Prompt text displayed to the user.
                Defaults to ``"Press Enter to continue..."``.
        """
        input(f"\n{message}")

    def _get_user_confirmation(self) -> bool:
        """
        Prompt the user to confirm before proceeding to data processing.

        Displays a summary of completed steps and asks for a yes/no response.
        Re-prompts on invalid input until a valid answer is provided.

        Returns:
            bool: True if the user confirmed (``yes`` / ``y``), False if the
                user cancelled (``no`` / ``n``).
        """
        print("\n[Step 2/2] User Confirmation Required")
        print("=" * 80)
        print("\nConfiguration loaded successfully. Data fetching complete.")
        print("Ready to proceed with data processing.\n")

        while True:
            response = input("Do you want to proceed with data processing? (Yes/No): ").strip().lower()

            if response in ["yes", "y"]:
                print("\n[OK] User confirmed. Proceeding to data processing...\n")
                return True
            elif response in ["no", "n"]:
                print("\n[X] User cancelled. Please review your data and configuration.")
                print("  Update config.yaml if needed, then run this script again.\n")
                return False
            else:
                print("  Please enter 'yes' or 'no'")

    def _copy_auxiliary_files(self):
        """
        Copy auxiliary files from the data input directory to ``public/``.

        Looks for custom shapefiles specified in
        :attr:`config.spatial_config.custom_shape_file_name` and copies them
        so the frontend can access them at runtime. Falls back gracefully
        with a warning if the source directory or file is missing.

        Raises:
            Exception: Re-raised if the file copy operation itself fails.
        """
        logger.info("Checking for auxiliary files to copy...")

        # Determine source directory based on dev_mode
        if self.dev_mode:
            auxiliary_source = self.project_root / "development-mode-root" / "auxiliary-data"
        else:
            auxiliary_source = self.project_root / "auxiliary-data"

        # Target directory is always public
        public_dir = self.project_root / "public"
        public_dir.mkdir(exist_ok=True, parents=True)

        # Check if custom shapefile is specified in config
        custom_shape_file = self.config.spatial_config.custom_shape_file_name

        if custom_shape_file:
            logger.info(f"Custom shapefile specified: {custom_shape_file}")

            # Check if auxiliary directory exists
            if not auxiliary_source.exists():
                logger.warning(f"Auxiliary data directory not found: {auxiliary_source}")
                logger.warning("  Custom shapefile will not be available")
                return

            # Look for the shapefile in auxiliary directory
            shapefile_path = auxiliary_source / custom_shape_file

            if not shapefile_path.exists():
                logger.warning(f"Custom shapefile not found: {shapefile_path}")
                logger.warning("  The dashboard will fall back to default map")
                return

            # Copy to public directory
            target_path = public_dir / custom_shape_file
            try:
                shutil.copy2(shapefile_path, target_path)
                logger.info(f"  [OK] Copied custom shapefile to: {target_path.relative_to(self.project_root)}")
            except Exception as e:
                logger.error(f"Failed to copy shapefile: {e}")
                raise
        else:
            logger.info("  No custom shapefile specified, using default")

    def run_data_processing(self):
        """
        Data Processing

        This part of workflow handles:
        - Loading raw CSV files from `target-data/` and `model-output/`
        - Applying column mappings and standardizing data formats
        - Filtering and structuring data based on forecast periods and targets
        - Optionally calculating evaluations (if not skipped)
        - Exporting to frontend JSON format
        - Copying auxiliary files (custom shapefiles) to public directory
        """
        print("\n" + "=" * 80)
        print("Data Processing")
        print("=" * 80)

        if self.skip_evaluations:
            print("\n[!] Evaluations DISABLED - skipping evaluation calculation")
            print("  The dashboard will not include evaluation metrics (WIS, Coverage, MAPE)")

        try:
            # Copy auxiliary files before main processing
            self._copy_auxiliary_files()

            # Run main data processing
            process_data(self.config, dev_mode=self.dev_mode, skip_evaluations=self.skip_evaluations, is_data_update_run=self.is_data_update)
            print("\n[OK] Data processing core logic completed successfully.")
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            # Re-raise the exception to be caught by the main function
            raise


def main():
    """
    CLI entry point for the Hubverse Dashboard Builder.

    Parses command-line arguments and drives the two-phase build:

    1. **Configuration validation** — loads ``config.yaml``, fetches remote
       data if configured, and asks the user to confirm before proceeding.
    2. **Data processing** — runs the full pipeline (target-data, model-output,
       evaluations) via :meth:`DashboardBuilder.run_data_processing`.

    Supported CLI arguments:
        --config          Path to configuration file (default: ``config.yaml``).
        --dev             Run in local development mode using
                          ``development-mode-root/`` for data.
        --skip-evaluations  Skip evaluation metrics calculation.
        --update          Perform an incremental data-update run.

    Exits with code 0 on success, 1 on configuration validation failure.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Hubverse Dashboard Builder")
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Run in local development mode, using data from 'development-mode-root/' directory.",
    )
    parser.add_argument(
        "--skip-evaluations",
        action="store_true",
        help="Skip evaluation metrics calculation (WIS, Coverage, MAPE). Dashboard will disable the Evaluations page.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Data update run (requires existing intermediates from initial build).",
    )

    args = parser.parse_args()

    # Create builder instance
    builder = DashboardBuilder(config_path=args.config, dev_mode=args.dev, skip_evaluations=args.skip_evaluations, is_data_update=args.update)

    if not builder.run_config_validation():
        sys.exit(1)

    # Run Phase 2: Data Processing
    # (Currently just a placeholder)
    builder.run_data_processing()

    print("=" * 80)
    print("Dashboard builder completed successfully!")
    print("=" * 80)
    sys.exit(0)


if __name__ == "__main__":
    main()
