# dashboard_builder_workflow.py
# This script controls the main workflow of:
# 1. Read in `config.yaml`'s various settings
#   a. Check whether any required settings are missing, and return error for main Shell Script to return
#   b. Check for special processing mode (single-location or single-target)
#   c. Check for user-chosen standard (such as the model-output data naming standard)
# 2. Structure above settings
#   a. Reconstruct a sample target-data and model-output csv header + some row value, for user to confirm before proceeding
#   b. Exit and let user adjust settings if user wishes
# 3. Process target-data data
#   a. First check for initial processing vs. data-update
#   b. Use targets, forecast_periods, locations, target-data column naming and time_unit to process target-data
# 4. Process model-output data
#   a. First check for initial processing vs. data-update
#   b. use targets, forecast_periods, locations, model-output column naming, time_unit, PI intervals, horizons, etc. to process model-output data
# 5. Evaluations processing on target-data & model-output.
#   a. Check whether user is processing for the first time, or updating
#   b. Use target-data and model-output to process evaluations accordingly
# 6. Return and prompt user to check
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
from csv_shape_generator import generate_and_print_samples
from data_processor import process_data
from data_fetcher import DataFetcher


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class DashboardBuilder:
    """Main dashboard builder orchestrator"""

    def __init__(self, config_path: str = "config.yaml", dev_mode: bool = False, skip_evaluations: bool = False, is_data_update: bool = False):
        self.config_path = Path(config_path)
        self.config: Optional[DashboardConfig] = None
        self.project_root = self._get_project_root()
        self.dev_mode = dev_mode
        self.skip_evaluations = skip_evaluations
        self.is_data_update = is_data_update
        self.data_fetcher = DataFetcher(self.project_root, dev_mode=dev_mode)

    def _get_project_root(self) -> Path:
        """Get the project root directory"""
        # Assume script is in project_root/scripts/=]\65
        return Path(__file__).parent.parent

    def run_config_validation(self) -> bool:
        """
        Load configuration and show CSV samples for user validation

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

        # Step 3: Generate and display CSV samples
        self._display_csv_samples()

        # Step 4: Ask user for confirmation
        return self._get_user_confirmation()

    def _fetch_remote_data(self) -> bool:
        """
        Checks config for remote data URL and fetches it if present.
        Returns: 
            bool: True if successful or no remote data needed. False if fetch failed.
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
            
            # Use the actual cache path returned
            self.data_fetcher.sync_to_destination(str(cache_path.relative_to(self.project_root)), dest)
            return True
        else:
            logger.error("Failed to fetch remote data.")
            return False

    def _load_configuration(self) -> bool:
        """Load and validate the configuration file"""
        print("\n[Step 1/3] Loading configuration file...")
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
            logger.error("You can copy config.yaml.example and customize it for your data.")
            return False

        except ValueError as e:
            logger.error(f"Configuration validation failed: {e}")
            logger.error("\nPlease fix the errors in your config.yaml file and try again.")
            return False

        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            return False

    def _prompt_to_continue(self, message: str = "Press Enter to continue..."):
        """Pauses execution and waits for user to press Enter."""
        input(f"\n{message}")

    def _display_csv_samples(self):
        """Generate and display expected CSV structures"""
        print("[Step 2/3] Generating expected CSV structures...\n")

        try:
            generate_and_print_samples(self.config)
        except Exception as e:
            logger.error(f"Error generating CSV samples: {e}")
            raise

    def _get_user_confirmation(self) -> bool:
        """Ask user to confirm before proceeding"""
        print("\n[Step 3/3] User Confirmation Required")
        print("=" * 80)
        print("\nPlease review the expected CSV structures above and compare with")
        print("your actual data files to ensure they match.\n")
        print("Important checks:")
        print("  1. Column names match exactly (case-sensitive)")
        print("  2. Target identifiers match (e.g., 'wk inc flu hosp')")
        print("  3. Location codes are formatted correctly")
        print("  4. Horizons match your model outputs")
        print("  5. Features are present in your model outputs, such as Quantile levels\n")

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
        Copy auxiliary files (like custom shapefiles) from data input to public directory.
        This ensures custom map files are available for the frontend.
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
            process_data(
                self.config, 
                dev_mode=self.dev_mode, 
                skip_evaluations=self.skip_evaluations,
                is_data_update_run=self.is_data_update
            )
            print("\n[OK] Data processing core logic completed successfully.")
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            # Re-raise the exception to be caught by the main function
            raise


def main():
    """Main entry point"""
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
    builder = DashboardBuilder(
        config_path=args.config, 
        dev_mode=args.dev, 
        skip_evaluations=args.skip_evaluations,
        is_data_update=args.update
    )

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
