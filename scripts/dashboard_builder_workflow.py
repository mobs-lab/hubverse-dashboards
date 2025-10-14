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
from pathlib import Path
from typing import Optional

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from yaml_config_processor import load_config, DashboardConfig
from csv_shape_generator import generate_and_print_samples
from data_processor import process_data


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class DashboardBuilder:
    """Main dashboard builder orchestrator"""

    def __init__(self, config_path: str = "config.yaml", dev_mode: bool = False):
        self.config_path = Path(config_path)
        self.config: Optional[DashboardConfig] = None
        self.project_root = self._get_project_root()
        self.dev_mode = dev_mode

    def _get_project_root(self) -> Path:
        """Get the project root directory"""
        # Assume script is in project_root/scripts/
        return Path(__file__).parent.parent

    def run_config_validation(self) -> bool:
        """
        Phase 1: Load configuration and show CSV samples for user validation

        Returns:
            bool: True if user confirms to proceed, False otherwise
        """
        print("\n" + "=" * 80)
        print("HUBVERSE DASHBOARD BUILDER - Phase 1: Configuration Validation")
        print("=" * 80)

        # Step 1: Load and validate configuration
        if not self._load_configuration():
            return False

        self._prompt_to_continue()

        # Step 2: Check configuration completeness
        if not self._check_configuration_requirements():
            return False

        self._prompt_to_continue()

        # Step 3: Generate and display CSV samples
        self._display_csv_samples()

        # Step 4: Ask user for confirmation
        return self._get_user_confirmation()

    def _load_configuration(self) -> bool:
        """Load and validate the configuration file"""
        print("\n[Step 1/4] Loading configuration file...")
        print(f"Config path: {self.config_path}")

        try:
            self.config = load_config(self.config_path)
            print("✓ Configuration loaded and validated successfully\n")
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

    def _check_configuration_requirements(self) -> bool:
        """Check if configuration meets all requirements"""
        print("[Step 2/4] Checking configuration requirements...")

        warnings = []
        errors = []

        # Check data source links
        if not self.config.target_data_link and not self.config.model_output_link:
            warnings.append("No data source links specified. Assuming local data setup.")

        # Check forecast periods
        if not self.config.forecast_periods:
            warnings.append("No standard forecast periods defined. Only using special periods.")

        # Check if forecast periods are in chronological order
        if len(self.config.forecast_periods) > 1:
            for i in range(len(self.config.forecast_periods) - 1):
                if self.config.forecast_periods[i].start_date > self.config.forecast_periods[i + 1].start_date:
                    warnings.append("Forecast periods are not in chronological order.")
                    break

        # Check target configuration
        if not self.config.targets:
            errors.append("At least one target (modelling task) must be defined.")

        # Check model configuration
        if not self.config.models:
            errors.append("At least one model must be configured.")

        # Check that target forecast periods exist
        all_period_ids = self.config.get_all_period_ids()
        for target in self.config.targets:
            for period_id in target.forecast_periods:
                if period_id not in all_period_ids:
                    errors.append(f"Target '{target.target_column_in_target_data}' references undefined period: '{period_id}'")

        # Display warnings
        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"  ⚠ {warning}")

        # Display errors
        if errors:
            print("\nErrors:")
            for error in errors:
                print(f"  ✗ {error}")
            print("\n✗ Configuration check failed. Please fix the errors above.\n")
            return False

        print("✓ Configuration requirements check passed\n")
        return True

    def _display_csv_samples(self):
        """Generate and display expected CSV structures"""
        print("[Step 3/4] Generating expected CSV structures...\n")

        try:
            generate_and_print_samples(self.config)
        except Exception as e:
            logger.error(f"Error generating CSV samples: {e}")
            raise

    def _get_user_confirmation(self) -> bool:
        """Ask user to confirm before proceeding"""
        print("\n[Step 4/4] User Confirmation Required")
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
                print("\n✓ User confirmed. Proceeding to data processing...\n")
                return True
            elif response in ["no", "n"]:
                print("\n✗ User cancelled. Please review your data and configuration.")
                print("  Update config.yaml if needed, then run this script again.\n")
                return False
            else:
                print("  Please enter 'yes' or 'no'")

    def run_data_processing(self):
        """
        Phase 2: Data Processing

        This part of workflow handles:
        - Loading raw CSV files from `target-data/` and `model-output/`
        - Applying column mappings and standardizing data formats
        - Filtering and structuring data based on forecast periods and targets
        - (TODO) Calculating evaluations
        - (TODO) Exporting to frontend JSON format
        """
        print("\n" + "=" * 80)
        print("PHASE 2: Data Processing")
        print("=" * 80)

        try:
            process_data(self.config, dev_mode=self.dev_mode)
            print("\n✓ Data processing core logic completed successfully.")
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            # Re-raise the exception to be caught by the main function
            raise


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Hubverse Dashboard Builder - Configuration-driven data processing")
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--skip-confirmation",
        action="store_true",
        help="Skip user confirmation step (for automated workflows)",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Run in local development mode, using data from 'test-data-input/' directory.",
    )

    args = parser.parse_args()

    # Create builder instance
    builder = DashboardBuilder(config_path=args.config, dev_mode=args.dev)

    # Run Config Validation and Preview
    if args.skip_confirmation:
        # Load config without user confirmation
        try:
            builder.config = load_config(args.config)
            generate_and_print_samples(builder.config)
            print("\n✓ Configuration validated successfully (--skip-confirmation mode)")
            print("  Run without --skip-confirmation to proceed to data processing.\n")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            sys.exit(1)
    else:
        # Normal interactive mode
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
