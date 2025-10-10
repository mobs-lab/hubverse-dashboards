"""
CSV Shape Generator
Generates sample CSV structures based on configuration
to help users verify their data format matches expectations.
"""

from typing import Any, Dict, List
from datetime import datetime, timedelta
from yaml_config_processor import DashboardConfig

class CSVShapeGenerator:
    """Generates sample CSV structures for validation"""

    def __init__(self, config: DashboardConfig):
        self.config = config

    def _parse_date(self, date_input: Any) -> datetime:
        """Safely parse a date that may be a string or already a datetime object."""
        if isinstance(date_input, datetime):
            return date_input

        if isinstance(date_input, str):
            try:
                # Handle formats with 'Z' for UTC
                return datetime.fromisoformat(date_input.replace('Z', '+00:00'))
            except ValueError:
                # Fallback for other valid ISO formats
                return datetime.fromisoformat(date_input)

        # If the input is neither a string nor a datetime, raise an error
        raise TypeError(f"Expected a date string or datetime object, but received {type(date_input)}")

    def _format_date(self, dt: datetime) -> str:
        """Format datetime to YYYY-MM-DD string"""
        return dt.strftime("%Y-%m-%d")

    def generate_target_data_sample(self) -> Dict[str, Any]:
        """Generate expected target-data CSV structure"""
        required_cols = []
        optional_cols = []
        sample_rows = []

        # Required columns
        required_cols.append(
            {
                "user_name": self.config.column_mapping.date_col,
                "description": "Date of observation",
                "example": "2024-08-03",
            }
        )

        required_cols.append(
            {
                "user_name": self.config.column_mapping.observation_col,
                "description": "Observation value",
                "example": "125.5",
            }
        )

        # Location column (required unless single-location mode)
        if not self.config.is_single_location:
            if self.config.column_mapping.location_col:
                required_cols.append(
                    {
                        "user_name": self.config.column_mapping.location_col,
                        "description": "Location code",
                        "example": "01",
                    }
                )

        # Target column (required unless single-target mode)
        if not self.config.is_single_target:
            if self.config.column_mapping.target_col:
                required_cols.append(
                    {
                        "user_name": self.config.column_mapping.target_col,
                        "description": "Target identifier",
                        "example": self.config.targets[
                            0
                        ].corresponding_key_in_model_output
                        if self.config.targets
                        else "target_value",
                    }
                )

        # Optional columns
        if self.config.column_mapping.location_name_col:
            optional_cols.append(
                {
                    "user_name": self.config.column_mapping.location_name_col,
                    "description": "Location name (optional)",
                    "example": "Alabama",
                }
            )

        if self.config.column_mapping.as_of_col:
            optional_cols.append(
                {
                    "user_name": self.config.column_mapping.as_of_col,
                    "description": "As-of date for historical data (optional)",
                    "example": "2024-08-05",
                }
            )

        # Generate sample rows using actual dates from forecast_periods
        # Use the first forecast period that's available
        if self.config.forecast_periods:
            first_period = self.config.forecast_periods[0]
            start_date = self._parse_date(first_period.start_date)

            # Generate 3 sample dates using time_unit
            date1 = start_date
            date2 = start_date + timedelta(days=self.config.time_unit)
            date3 = start_date + timedelta(days=self.config.time_unit * 2)
        else:
            # Fallback to example dates if no forecast periods defined
            date1 = datetime(2024, 8, 3)
            date2 = datetime(2024, 8, 10)


        # Sample row 1
        sample_row_1 = {
            self.config.column_mapping.date_col: self._format_date(date1),
            self.config.column_mapping.observation_col: "125.5",
        }

        # Sample row 2
        sample_row_2 = {
            self.config.column_mapping.date_col: self._format_date(date1),
            self.config.column_mapping.observation_col: "43.2",
        }

        # Sample row 3
        sample_row_3 = {
            self.config.column_mapping.date_col: self._format_date(date2),
            self.config.column_mapping.observation_col: "132.1",
        }

        if (
            not self.config.is_single_location
            and self.config.column_mapping.location_col
        ):
            sample_row_1[self.config.column_mapping.location_col] = "01"
            sample_row_2[self.config.column_mapping.location_col] = "02"
            sample_row_3[self.config.column_mapping.location_col] = "01"

            if self.config.column_mapping.location_name_col:
                sample_row_1[self.config.column_mapping.location_name_col] = "Alabama"
                sample_row_2[self.config.column_mapping.location_name_col] = "Alaska"
                sample_row_3[self.config.column_mapping.location_name_col] = "Alabama"

        if not self.config.is_single_target and self.config.column_mapping.target_col:
            target_value = (
                self.config.targets[0].corresponding_key_in_model_output
                if self.config.targets
                else "target_value"
            )
            sample_row_1[self.config.column_mapping.target_col] = target_value
            sample_row_2[self.config.column_mapping.target_col] = target_value
            sample_row_3[self.config.column_mapping.target_col] = target_value

        sample_rows = [sample_row_1, sample_row_2, sample_row_3]

        return {
            "required_columns": required_cols,
            "optional_columns": optional_cols,
            "sample_rows": sample_rows,
        }

    def generate_model_output_sample(self) -> Dict[str, Any]:
        """Generate expected model-output CSV structure"""
        required_cols = []
        sample_rows = []

        # Required columns
        required_cols.append(
            {
                "user_name": self.config.column_mapping.reference_date_col,
                "description": "Reference date (forecast made on)",
                "example": "2024-08-03",
            }
        )

        required_cols.append(
            {
                "user_name": self.config.column_mapping.target_end_date_col,
                "description": "Target end date (forecast for)",
                "example": "2024-08-10",
            }
        )

        required_cols.append(
            {
                "user_name": self.config.column_mapping.model_target_col,
                "description": "Target identifier",
                "example": self.config.targets[0].corresponding_key_in_model_output
                if self.config.targets
                else "target_value",
            }
        )

        required_cols.append(
            {
                "user_name": self.config.column_mapping.horizon_col,
                "description": "Forecast horizon",
                "example": "0"
                if 0 in self.config.horizons
                else str(self.config.horizons[0]),
            }
        )

        if not self.config.is_single_location:
            required_cols.append(
                {
                    "user_name": self.config.column_mapping.location_col,
                    "description": "Location code",
                    "example": "01",
                }
            )

        required_cols.append(
            {
                "user_name": self.config.column_mapping.output_type_col,
                "description": 'Output type (should be "quantile")',
                "example": "quantile",
            }
        )

        required_cols.append(
            {
                "user_name": self.config.column_mapping.output_type_id_col,
                "description": "Quantile level",
                "example": "0.5",
            }
        )

        required_cols.append(
            {
                "user_name": self.config.column_mapping.value_col,
                "description": "Predicted value",
                "example": "125.0",
            }
        )

        # Get expected values
        expected_targets = [
            t.corresponding_key_in_model_output for t in self.config.targets
        ]
        expected_horizons = self.config.horizons
        expected_quantiles = self.config.get_all_quantiles()

        # Get reference date from first forecast period
        if self.config.forecast_periods:
            first_period = self.config.forecast_periods[0]
            reference_date = self._parse_date(first_period.start_date)
        else:
            reference_date = datetime(2024, 8, 3)

        # Select horizons to demonstrate (prefer 0 and positive values)
        available_horizons = self.config.horizons
        demo_horizons = []

        # Try to get horizon 0 first
        if 0 in available_horizons:
            demo_horizons.append(0)

        # Then get 1-2 other horizons (prefer positive, diverse values)
        other_horizons = [h for h in available_horizons if h != 0]
        if other_horizons:
            # Take up to 3 more horizons, spread out
            step = max(1, len(other_horizons) // 3)
            demo_horizons.extend(other_horizons[::step][:3])

        # Ensure we have at least one horizon
        if not demo_horizons:
            demo_horizons = [available_horizons[0]] if available_horizons else [0]

        # Limit to 4 horizons for demo purposes
        demo_horizons = demo_horizons[:4]

        # Select quantiles to demonstrate
        demo_quantiles = []
        if expected_quantiles:
            if "0.5" in expected_quantiles:
                demo_quantiles.append("0.5")  # Median is most important

            # Add lower and upper quantiles
            sorted_quantiles = sorted([float(q) for q in expected_quantiles])
            if len(sorted_quantiles) >= 3:
                demo_quantiles.insert(0, str(sorted_quantiles[0]))  # Lowest
                demo_quantiles.append(str(sorted_quantiles[-1]))     # Highest
            elif len(sorted_quantiles) >= 1 and "0.5" not in demo_quantiles:
                demo_quantiles.append(str(sorted_quantiles[0]))
        else:
            demo_quantiles = ["0.25", "0.5", "0.75"]

        # Limit to 2 quantiles to keep demo concise
        demo_quantiles = demo_quantiles[:2]

        # Location value for demos
        location_value = "01" if not self.config.is_single_location else None

        # Generate 4 rows for EACH target
        for target in self.config.targets:
            target_value = target.corresponding_key_in_model_output

            # Generate 4 diverse sample rows for this target
            # Strategy: Mix different horizons and quantiles
            rows_for_target = []

            # Row 1: First horizon, first quantile
            if len(demo_horizons) >= 1 and len(demo_quantiles) >= 1:
                horizon = demo_horizons[0]
                quantile = demo_quantiles[0]
                target_end_date = reference_date + timedelta(days=horizon * self.config.time_unit)

                row = {
                    self.config.column_mapping.reference_date_col: self._format_date(reference_date),
                    self.config.column_mapping.target_end_date_col: self._format_date(target_end_date),
                    self.config.column_mapping.model_target_col: target_value,
                    self.config.column_mapping.horizon_col: str(horizon),
                    self.config.column_mapping.output_type_col: "quantile",
                    self.config.column_mapping.output_type_id_col: quantile,
                    self.config.column_mapping.value_col: "120.5",
                }
                if not self.config.is_single_location:
                    row[self.config.column_mapping.location_col] = location_value
                rows_for_target.append(row)

            # Row 2: First horizon, second quantile (or same if only one)
            if len(demo_horizons) >= 1:
                horizon = demo_horizons[0]
                quantile = demo_quantiles[-1] if len(demo_quantiles) > 1 else demo_quantiles[0]
                target_end_date = reference_date + timedelta(days=horizon * self.config.time_unit)

                row = {
                    self.config.column_mapping.reference_date_col: self._format_date(reference_date),
                    self.config.column_mapping.target_end_date_col: self._format_date(target_end_date),
                    self.config.column_mapping.model_target_col: target_value,
                    self.config.column_mapping.horizon_col: str(horizon),
                    self.config.column_mapping.output_type_col: "quantile",
                    self.config.column_mapping.output_type_id_col: quantile,
                    self.config.column_mapping.value_col: "125.0",
                }
                if not self.config.is_single_location:
                    row[self.config.column_mapping.location_col] = location_value
                rows_for_target.append(row)

            # Row 3: Second horizon (if available), first quantile
            if len(demo_horizons) >= 2:
                horizon = demo_horizons[1]
                quantile = demo_quantiles[0]
                target_end_date = reference_date + timedelta(days=horizon * self.config.time_unit)

                row = {
                    self.config.column_mapping.reference_date_col: self._format_date(reference_date),
                    self.config.column_mapping.target_end_date_col: self._format_date(target_end_date),
                    self.config.column_mapping.model_target_col: target_value,
                    self.config.column_mapping.horizon_col: str(horizon),
                    self.config.column_mapping.output_type_col: "quantile",
                    self.config.column_mapping.output_type_id_col: quantile,
                    self.config.column_mapping.value_col: "130.2",
                }
                if not self.config.is_single_location:
                    row[self.config.column_mapping.location_col] = location_value
                rows_for_target.append(row)

            # Row 4: Third horizon (if available), second quantile
            if len(demo_horizons) >= 3:
                horizon = demo_horizons[2]
                quantile = demo_quantiles[-1] if len(demo_quantiles) > 1 else demo_quantiles[0]
                target_end_date = reference_date + timedelta(days=horizon * self.config.time_unit)

                row = {
                    self.config.column_mapping.reference_date_col: self._format_date(reference_date),
                    self.config.column_mapping.target_end_date_col: self._format_date(target_end_date),
                    self.config.column_mapping.model_target_col: target_value,
                    self.config.column_mapping.horizon_col: str(horizon),
                    self.config.column_mapping.output_type_col: "quantile",
                    self.config.column_mapping.output_type_id_col: quantile,
                    self.config.column_mapping.value_col: "132.0",
                }
                if not self.config.is_single_location:
                    row[self.config.column_mapping.location_col] = location_value
                rows_for_target.append(row)
            elif len(demo_horizons) >= 2:
                # If no third horizon, use second horizon with different quantile
                horizon = demo_horizons[1]
                quantile = demo_quantiles[-1] if len(demo_quantiles) > 1 else demo_quantiles[0]
                target_end_date = reference_date + timedelta(days=horizon * self.config.time_unit)

                row = {
                    self.config.column_mapping.reference_date_col: self._format_date(reference_date),
                    self.config.column_mapping.target_end_date_col: self._format_date(target_end_date),
                    self.config.column_mapping.model_target_col: target_value,
                    self.config.column_mapping.horizon_col: str(horizon),
                    self.config.column_mapping.output_type_col: "quantile",
                    self.config.column_mapping.output_type_id_col: quantile,
                    self.config.column_mapping.value_col: "128.5",
                }
                if not self.config.is_single_location:
                    row[self.config.column_mapping.location_col] = location_value
                rows_for_target.append(row)

            # Ensure we have exactly 4 rows by padding if necessary
            while len(rows_for_target) < 4:
                # Duplicate the last row with slight variation
                if rows_for_target:
                    last_row = rows_for_target[-1].copy()
                    # Vary the value slightly
                    current_val = float(last_row[self.config.column_mapping.value_col])
                    last_row[self.config.column_mapping.value_col] = str(current_val + 5.0)
                    rows_for_target.append(last_row)

            # Add these 4 rows to the main sample_rows
            sample_rows.extend(rows_for_target[:4])  # Ensure exactly 4 rows per target

        return {
            "required_columns": required_cols,
            "optional_columns": [],
            "sample_rows": sample_rows,
            "expected_values": {
                "targets": expected_targets,
                "horizons": expected_horizons,
                "output_type": "quantile",
                "output_type_ids": expected_quantiles,
            },
        }

    def print_target_data_sample(self):
        """Print formatted target-data sample structure"""
        sample = self.generate_target_data_sample()

        print("\n" + "=" * 80)
        print("TARGET-DATA Expected Structure")
        print("=" * 80)
        print(
            "\nBased on your configuration, your target-data CSV should have these columns:\n"
        )

        print("Required Columns:")
        for col in sample["required_columns"]:
            print(f"  • {col['user_name']:<25} ({col['description']})")

        if sample["optional_columns"]:
            print("\nOptional Columns:")
            for col in sample["optional_columns"]:
                print(f"  • {col['user_name']:<25} ({col['description']})")

        print("\nSample Rows (what your CSV should look like):")
        self._print_table(sample["sample_rows"])

    def print_model_output_sample(self):
        """Print formatted model-output sample structure"""
        sample = self.generate_model_output_sample()

        print("\n" + "=" * 80)
        print("MODEL-OUTPUT Expected Structure")
        print("=" * 80)
        print(
            "\nBased on your configuration, your model-output CSV should have these columns:\n"
        )

        print("Required Columns:")
        for col in sample["required_columns"]:
            print(f"  • {col['user_name']:<25} ({col['description']})")

        print("\nExpected Values:")
        exp = sample["expected_values"]
        print(f"  • target: {', '.join([f'"{t}"' for t in exp['targets']])}")
        print(f"  • horizons: {exp['horizons']}")
        print(f'  • output_type: "{exp["output_type"]}"')
        print(f"  • output_type_ids: {exp['output_type_ids']}")

        num_targets = len(self.config.targets)
        print(f"\nSample Rows (4 rows per target, {num_targets} target(s) = {len(sample['sample_rows'])} rows total):")
        self._print_table(sample["sample_rows"])

    def _print_table(self, rows: List[Dict[str, str]]):
        """Print rows as a formatted table"""
        if not rows:
            return

        # Get all column names
        all_cols = list(rows[0].keys())

        # Calculate column widths
        col_widths = {}
        for col in all_cols:
            max_width = len(col)
            for row in rows:
                if col in row:
                    max_width = max(max_width, len(str(row[col])))
            col_widths[col] = min(max_width + 2, 20)  # Cap at 20 chars for readability

        # Print header
        header_line = "┌"
        for i, col in enumerate(all_cols):
            header_line += "─" * col_widths[col]
            if i < len(all_cols) - 1:
                header_line += "┬"
        header_line += "┐"
        print(header_line)

        # Print column names
        col_line = "│"
        for col in all_cols:
            col_line += f" {col:<{col_widths[col] - 1}}│"
        print(col_line)

        # Print separator
        sep_line = "├"
        for i, col in enumerate(all_cols):
            sep_line += "─" * col_widths[col]
            if i < len(all_cols) - 1:
                sep_line += "┼"
        sep_line += "┤"
        print(sep_line)

        # Print rows
        for row in rows:
            row_line = "│"
            for col in all_cols:
                value = str(row.get(col, ""))
                if len(value) > col_widths[col] - 2:
                    value = value[: col_widths[col] - 5] + "..."
                row_line += f" {value:<{col_widths[col] - 1}}│"
            print(row_line)

        # Print bottom
        bottom_line = "└"
        for i, col in enumerate(all_cols):
            bottom_line += "─" * col_widths[col]
            if i < len(all_cols) - 1:
                bottom_line += "┴"
        bottom_line += "┘"
        print(bottom_line)

    def print_configuration_summary(self):
        """Print a summary of the configuration"""
        print("\n" + "=" * 80)
        print("Configuration Summary")
        print("=" * 80)

        print(f"\n✓ Time Unit: {self.config.time_unit} days")
        print(f"✓ Horizons: {self.config.horizons}")
        print(
            f"✓ Forecast Periods: {len(self.config.forecast_periods)} standard period(s)"
        )
        if self.config.dynamic_periods:
            print(
                f"✓ Special Periods: {len(self.config.dynamic_periods)} special period(s)"
            )
        print(f"✓ Targets: {len(self.config.targets)} modelling task(s)")
        for target in self.config.targets:
            print(
                f"    - {target.target_column_in_target_data} → {target.corresponding_key_in_model_output}"
            )
        print(f"✓ Models: {len(self.config.models)} model(s) configured")
        for model in self.config.models:
            print(f"    - {model.model_name}")
        print(
            f"✓ Prediction Intervals: {len(self.config.prediction_intervals)} level(s)"
        )
        for interval in self.config.prediction_intervals:
            print(f"    - {interval.level}% (quantiles: {interval.output_type_ids})")
        print(
            f"✓ Single Location Mode: {'Yes' if self.config.is_single_location else 'No'}"
        )
        if self.config.is_single_location:
            location_name = self.config.us_state_fips_mapping.get(
                self.config.single_location_mapping, "Unknown"
            )
            print(
                f"    Location: {self.config.single_location_mapping} ({location_name})"
            )
        else:
            print(
                f"    Locations will be auto-detected from your data files"
            )
        
        print(
            f"✓ Single Target Mode: {'Yes' if self.config.is_single_target else 'No'}"
        )

        print("\n" + "=" * 80)


def generate_and_print_samples(config: DashboardConfig):
    """Generate and print all sample structures"""
    generator = CSVShapeGenerator(config)

    # Print target-data sample
    generator.print_target_data_sample()

    # Print model-output sample
    generator.print_model_output_sample()

    # Print configuration summary
    generator.print_configuration_summary()
