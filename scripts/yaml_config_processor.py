"""
YAML Configuration Processor for Hubverse Dashboard
Parses, validates, and handles errors in config.yaml with comprehensive error checking.
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ValidationWarning:
    """Represents a validation warning"""

    message: str
    field: str
    severity: str = "WARNING"


@dataclass
class ValidationError:
    """Represents a validation error"""

    message: str
    field: str
    severity: str = "ERROR"


@dataclass
class ColumnMapping:
    """Maps user's column names to internal schema"""

    # Target data columns
    date_col: str
    observation_col: str
    location_col: Optional[str]
    location_name_col: Optional[str]
    target_col: Optional[str]
    as_of_col: Optional[str]

    # Model output columns
    reference_date_col: str
    target_end_date_col: str
    model_target_col: str
    horizon_col: str
    output_type_col: str
    output_type_id_col: str
    value_col: str


@dataclass
class ForecastPeriod:
    """Represents a forecast period (time range)"""

    period_id: str
    display_string: str
    start_date: datetime
    end_date: datetime
    is_special_period: bool = False
    is_default_selected: bool = False
    sub_display_value: Optional[str] = None
    time_anchor: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Ensure dates are datetime objects
        if isinstance(self.start_date, str):
            date_str = self.start_date.replace("T", " ").replace("Z", "")
            try:
                self.start_date = datetime.fromisoformat(date_str)
            except ValueError:
                self.start_date = datetime.strptime(date_str.split()[0], "%Y-%m-%d")

        if isinstance(self.end_date, str):
            date_str = self.end_date.replace("T", " ").replace("Z", "")
            try:
                self.end_date = datetime.fromisoformat(date_str)
            except ValueError:
                self.end_date = datetime.strptime(date_str.split()[0], "%Y-%m-%d")


@dataclass
class TargetConfig:
    """Configuration for a modelling task/target"""

    target_name: str  # The name/identifier from config (e.g., "COVID19 Admission Value")
    task_display_string: str  # Display name for frontend (e.g., "admission value")
    target_key_name_for_task: str  # Key to match in target-data and model-output
    forecast_periods: List[str]


@dataclass
class PredictionInterval:
    """Configuration for a prediction interval"""

    level: int
    output_type_ids: List[str]

    def __post_init__(self):
        self.output_type_ids = sorted([str(x) for x in self.output_type_ids], key=lambda x: float(x))


@dataclass
class ModelConfig:
    """Configuration for a model"""

    model_name: str
    color_hex: Optional[str] = None
    display_name: Optional[str] = None


@dataclass
class SpatialDataConfig:
    """Configuration for spatial data handling"""

    disable_map: bool
    custom_shape_file_name: Optional[str]
    custom_location_mapping_file_name: Optional[str]
    location_code_col_header: str
    location_name_col_header: str

    # Store implied configs from above options
    use_default_shape_file: bool = True
    use_default_location_mapping: bool = True

    shape_file_path: Optional[Path] = None
    location_mapping_path: Optional[Path] = None

    def __post_init__(self):
        """Compute derived properties"""
        self.use_default_shape_file = self.custom_shape_file_name is None
        self.use_default_location_mapping = self.custom_location_mapping_file_name is None


class DashboardConfig:
    """Main configuration class for the dashboard with comprehensive validation"""

    # Default color palette for models without specified colors (64 colors)
    DEFAULT_COLOR_PALETTE = [
        "#4CAF50",
        "#2196F3",
        "#FF9800",
        "#9C27B0",
        "#F44336",
        "#00BCD4",
        "#FFEB3B",
        "#795548",
        "#607D8B",
        "#E91E63",
        "#009688",
        "#FF5722",
        "#673AB7",
        "#3F51B5",
        "#03A9F4",
        "#00BCD4",
        "#4DB6AC",
        "#81C784",
        "#AED581",
        "#DCE775",
        "#FFF176",
        "#FFD54F",
        "#FFB74D",
        "#FF8A65",
        "#A1887F",
        "#90A4AE",
        "#CE93D8",
        "#BA68C8",
        "#AB47BC",
        "#8E24AA",
        "#7B1FA2",
        "#6A1B9A",
        "#EF5350",
        "#EC407A",
        "#AB47BC",
        "#7E57C2",
        "#5C6BC0",
        "#42A5F5",
        "#29B6F6",
        "#26C6DA",
        "#26A69A",
        "#66BB6A",
        "#9CCC65",
        "#D4E157",
        "#FFEE58",
        "#FFCA28",
        "#FFA726",
        "#FF7043",
        "#8D6E63",
        "#78909C",
        "#BDBDBD",
        "#9E9E9E",
        "#757575",
        "#616161",
        "#424242",
        "#212121",
        "#B39DDB",
        "#9FA8DA",
        "#90CAF9",
        "#81D4FA",
        "#80DEEA",
        "#80CBC4",
        "#A5D6A7",
        "#C5E1A5",
    ]

    def __init__(self, config_path: Union[str, Path]):
        self.config_path = Path(config_path)
        self.raw_config = self._load_yaml()
        self.validation_warnings: List[ValidationWarning] = []
        self.validation_errors: List[ValidationError] = []
        self._parse_and_validate()

    def _load_yaml(self) -> List[Dict]:
        """Load YAML configuration file"""
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
                if not config:
                    raise ValueError("Config file is empty")
                if not isinstance(config, list):
                    raise ValueError("Config file must have a list of dictionaries at root level")
                return config
        except FileNotFoundError:
            logger.error(f"Config file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML: {e}")
            raise

    def _add_error(self, field: str, message: str):
        """Add a validation error"""
        self.validation_errors.append(ValidationError(message=message, field=field))

    def _add_warning(self, field: str, message: str):
        """Add a validation warning"""
        self.validation_warnings.append(ValidationWarning(message=message, field=field))

    def _is_valid_hex_color(self, color: str) -> bool:
        """Validate hex color format (#RRGGBB or #RGB)"""
        if not color:
            return False
        pattern = r'^#(?:[0-9a-fA-F]{3}){1,2}$'
        return bool(re.match(pattern, color))

    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format"""
        if not url:
            return False
        # Basic URL validation
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, url))

    def _is_valid_quantile(self, value: str) -> bool:
        """Validate quantile value is between 0 and 1"""
        try:
            q = float(value)
            return 0 <= q <= 1
        except (ValueError, TypeError):
            return False

    def _parse_and_validate(self):
        """Parse and validate all configuration sections"""
        logger.info("Parsing configuration...")

        # Data source links - CHECK FOR CONFLICTS
        self.target_data_link = self._get_nested_value("links_to_hubverse_compatible_data", "target_data_link")
        self.model_output_link = self._get_nested_value("links_to_hubverse_compatible_data", "model_output_link")

        # Check if both local and online modes are enabled
        self._validate_data_source_links()

        # Forecast periods
        self.forecast_periods = self._parse_forecast_periods()
        self.dynamic_periods = self._parse_dynamic_periods()

        # Validate forecast periods
        self._validate_forecast_periods()

        # Location configuration
        self.is_single_location = self._get_value("is_single_location_forecast", False)
        self.single_location_mapping = self._parse_single_location_mapping()

        self.spatial_config = self._parse_spatial_data_config()
        self.location_mapping = self._load_location_mapping(self.spatial_config)

        # Validate single-location mode data
        self._validate_single_location_data()

        # TODO: Handle three different criteria and combinations:
        # - Disabling of Map or not
        # - Using custom shape file or not
        # - using custom spatial level—custom location mapping—or not

        # Target configuration
        self.is_single_target = self._get_value("is_single_forecast_target", False)
        self.targets = self._parse_targets()

        # Time unit (required) - VALIDATE RANGE
        self.time_unit = self._get_value("time_unit")
        if not self.time_unit:
            self._add_error("time_unit", "time_unit is required in config")
        else:
            self._validate_time_unit()

        # Horizons (required)
        self.horizons = self._get_value("horizons")
        if not self.horizons:
            self._add_error("horizons", "horizons list is required in config")
        else:
            self._validate_horizons()

        # Target data file format
        self.target_data_file_format = self._get_value("target_data_file_format")
        if not self.target_data_file_format:
            self.target_data_file_format = "csv"
            self._add_warning(
                "target_data_file_format",
                "target_data_file_format not specified, defaulting to 'csv'",
            )
        elif self.target_data_file_format not in ["csv", "parquet"]:
            self._add_error(
                "target_data_file_format",
                f"Invalid target_data_file_format: '{self.target_data_file_format}'. Must be 'csv' or 'parquet'",
            )

        # Single target data file name (for non-partitioned formats)
        self.single_target_data_file_name = self._get_value("single_target_data_file_name")

        # Check if partitioned parquet is being used
        self.is_partitioned_parquet = self._get_value("parquet_partitioned_by_as_of", False)

        # Validate that single_target_data_file_name is provided when needed
        if self.target_data_file_format in ["csv", "parquet"] and not self.is_partitioned_parquet:
            if not self.single_target_data_file_name:
                self._add_error(
                    "single_target_data_file_name",
                    "single_target_data_file_name is required when using csv or non-partitioned parquet format",
                )

        # Column mappings
        self.column_mapping = self._parse_column_mappings()

        # Target data observation format - DEFAULT IF MISSING
        self.target_data_observation_format = self._get_value("target_data_observation_format")
        if not self.target_data_observation_format:
            self.target_data_observation_format = "float"
            self._add_warning(
                "target_data_observation_format",
                "target_data_observation_format missing, defaulting to 'float'",
            )

        # Models - VALIDATE COLORS
        self.models = self._parse_models()
        self._validate_and_assign_model_colors()

        # Prediction intervals
        self.prediction_intervals = self._parse_prediction_intervals()
        self.evaluation_intervals = self._parse_evaluation_intervals()

        # Model output naming standard
        self.model_output_naming_standard = self._get_value("model_output_data_file_naming_standard", "ISODate")

        # Baseline model for evaluations - REQUIRED
        self.baseline_model_for_relative_wis = self._get_value("baseline_model_for_relative_WIS")
        if not self.baseline_model_for_relative_wis:
            self._add_error("baseline_model_for_relative_WIS", "baseline_model_for_relative_WIS is REQUIRED for model evaluation calculations")
        else:
            self._validate_baseline_model()

        # Final validation
        self._print_validation_results()

        if self.validation_errors:
            logger.error("✗ Configuration validation failed due to errors")
            raise ValueError("Configuration validation failed")

        logger.info("✓ Configuration parsed successfully")

    def _validate_data_source_links(self):
        """Check if both local and online data sources are configured"""
        has_online = bool(self.target_data_link or self.model_output_link)

        # Validate URL formats if online links provided
        if self.target_data_link and not self._is_valid_url(self.target_data_link):
            self._add_error(
                "target_data_link",
                f"Invalid URL format: '{self.target_data_link}'. Must be a valid HTTP/HTTPS URL.",
            )

        if self.model_output_link and not self._is_valid_url(self.model_output_link):
            self._add_error(
                "model_output_link",
                f"Invalid URL format: '{self.model_output_link}'. Must be a valid HTTP/HTTPS URL.",
            )

        # Check for local data directories
        project_root = self.config_path.parent
        has_local_target = (project_root / "target-data").exists()
        has_local_model = (project_root / "model-output").exists()
        has_local = has_local_target or has_local_model

        if has_online and has_local:
            self._add_error(
                "data_source",
                "Both local and online data sources are configured. "
                + "Please use either local directories (target-data/, model-output/) "
                + "OR online links, not both.",
            )
        elif not has_online and not has_local:
            self._add_error(
                "data_source",
                "No data source configured. Please either:\n"
                + "  1. Create local directories: target-data/ and model-output/, OR\n"
                + "  2. Specify online links in config: target_data_link and model_output_link",
            )

    def _validate_forecast_periods(self):
        """Validate forecast period configurations"""
        # Check for duplicate IDs
        seen_ids = set()
        seen_display_strings = set()

        for period in self.forecast_periods + self.dynamic_periods:
            # Check duplicate IDs
            if period.period_id in seen_ids:
                self._add_error(
                    "forecast_periods",
                    f"Duplicate forecast_period_id: '{period.period_id}'",
                )
            seen_ids.add(period.period_id)

            # Check duplicate display strings (WARNING only)
            if period.display_string in seen_display_strings:
                self._add_warning(
                    "forecast_periods",
                    f"Duplicate display_string: '{period.display_string}' " + f"(period: {period.period_id})",
                )
            seen_display_strings.add(period.display_string)

        # Check that static forecast periods are in chronological order
        if len(self.forecast_periods) > 1:
            for i in range(len(self.forecast_periods) - 1):
                current = self.forecast_periods[i]
                next_period = self.forecast_periods[i + 1]
                if current.start_date > next_period.start_date:
                    self._add_warning(
                        "forecast_periods",
                        f"Forecast periods are not in chronological order: "
                        f"'{current.period_id}' ({current.start_date.date()}) comes before "
                        f"'{next_period.period_id}' ({next_period.start_date.date()})",
                    )
                    break

        # Check each period for additional validations
        for period in self.forecast_periods + self.dynamic_periods:
            # Check start_date before end_date for non-special periods
            if not period.is_special_period and period.start_date > period.end_date:
                self._add_error(
                    "forecast_periods",
                    f"start_date is after end_date for period '{period.period_id}' "
                    + f"({period.start_date.strftime('%Y-%m-%d')} > {period.end_date.strftime('%Y-%m-%d')})",
                )

            # Check special period anchor/range calculation combo
            if period.is_special_period:
                if not period.time_anchor:
                    self._add_error(
                        "special_forecast_periods",
                        f"Special period '{period.period_id}' is missing 'time_anchor'.",
                    )
                    continue

                range_calc = period.time_anchor.get("range_calculation")
                anchor_on = period.time_anchor.get("anchor_on")
                anchor_mode = period.time_anchor.get("anchor_mode")

                if range_calc is None:
                    self._add_error(
                        "special_forecast_periods",
                        f"Special period '{period.period_id}' is missing 'range_calculation' inside 'time_anchor'.",
                    )
                elif not isinstance(range_calc, int):
                    self._add_error(
                        "special_forecast_periods",
                        f"Special period '{period.period_id}' 'range_calculation' must be an integer.",
                    )
                elif range_calc > 0:
                    self._add_error(
                        "special_forecast_periods",
                        f"Special period '{period.period_id}' must have a negative " + f"'range_calculation' to look backward in time (got {range_calc}).",
                    )

                if anchor_on is None:
                    self._add_error(
                        "special_forecast_periods",
                        f"Special period '{period.period_id}' is missing 'anchor_on' inside 'time_anchor'.",
                    )

                if anchor_mode not in ["target-data", "model-output"]:
                    self._add_error(
                        "special_forecast_periods",
                        f"Special period '{period.period_id}' has invalid 'anchor_mode'. Must be 'target-data' or 'model-output'.",
                    )

                # Check that anchor_on points to a valid dynamic forecast period
                if anchor_on:
                    all_static_periods = {p.period_id: p for p in self.forecast_periods}
                    if anchor_on not in all_static_periods:
                        self._add_error(
                            "special_forecast_periods",
                            f"Special period '{period.period_id}' anchors on an undefined forecast period '{anchor_on}'.",
                        )
                    else:
                        anchor_period = all_static_periods[anchor_on]
                        # Check if anchor period is dynamic (end date in future)
                        if anchor_period.end_date <= datetime.now():
                            self._add_warning(
                                "special_forecast_periods",
                                f"Special period '{period.period_id}' is anchored to a static (non-dynamic) "
                                + f"forecast period '{anchor_on}'. It will not update.",
                            )

        # Check that only one forecast_period has is_default_selected
        default_periods = [p.period_id for p in self.forecast_periods if p.is_default_selected]
        if len(default_periods) > 1:
            self._add_error(
                "forecast_periods",
                f"Only one forecast period can be set as default. Found: {', '.join(default_periods)}",
            )

    def _validate_single_location_data(self):
        """Validate location data configuration"""
        if self.is_single_location:
            # Require single_location_mapping when in single-location mode
            if not self.single_location_mapping:
                self._add_error(
                    "single_location_mapping",
                    "single_location_mapping is REQUIRED when is_single_location_forecast is True.\n"
                    "  Please specify a location code that exists in your location mapping.\n"
                    "  Examples:\n"
                    "    - For default US FIPS: '01' (Alabama), '06' (California), etc.\n"
                    "    - For custom mapping: use a code from your custom_location_mapping_file_name CSV",
                )
        else:
            # Multi-location mode: locations will be auto-detected from data
            logger.info("  ✓ Multi-location mode: will auto-detect locations from data files")

            # Log which mapping will be used for auto-detected locations
            if self.spatial_config.use_default_location_mapping:
                logger.info(f"  ✓ Using default US State FIPS mapping for location names ({len(self.location_mapping)} states)")
            else:
                logger.info(f"  ✓ Using custom location mapping for location names ({len(self.location_mapping)} locations)")

    def _parse_spatial_data_config(self) -> SpatialDataConfig:
        """Parse spatial data configuration with comprehensive validation"""

        # Get configuration values
        disable_map = self._get_value("disable_map_in_dashboard", False)
        custom_shape_file = self._get_value("custom_shape_file_name")
        custom_location_mapping = self._get_value("custom_location_mapping_file_name")
        location_code_col = self._get_value("location_code_col_header_name", "location")
        location_name_col = self._get_value("location_name_col_header_name", "location_name")

        # Create config object
        spatial_config = SpatialDataConfig(
            disable_map=disable_map,
            custom_shape_file_name=custom_shape_file,
            custom_location_mapping_file_name=custom_location_mapping,
            location_code_col_header=location_code_col,
            location_name_col_header=location_name_col,
        )

        # Get paths for validation
        project_root = self.config_path.parent
        auxiliary_data_dir = project_root / "auxiliary-data"

        # ===== VALIDATION LOGIC =====

        # Scenario: Map is DISABLED
        if spatial_config.disable_map:
            logger.info("  ℹ️  Map visualization is DISABLED")

            # Shape file is ignored when map is disabled
            if custom_shape_file:
                logger.info(f"  ℹ️  custom_shape_file_name '{custom_shape_file}' will be ignored (map is disabled)")

            # Validate custom location mapping if provided
            if custom_location_mapping:
                mapping_path = auxiliary_data_dir / custom_location_mapping
                if not mapping_path.exists():
                    self._add_error(
                        "custom_location_mapping_file_name",
                        f"Custom location mapping file not found: {mapping_path}\n  Please place the file in the auxiliary-data/ directory",
                    )
                else:
                    spatial_config.location_mapping_path = mapping_path
                    logger.info(f"  ✓ Custom location mapping: {custom_location_mapping}")
            else:
                logger.info("  ✓ Using default US State FIPS mapping")

        # Scenario: Map is ENABLED
        else:
            logger.info("  ✓ Map visualization is ENABLED")

            # Check shape file configuration
            if custom_shape_file:
                # Custom shape file provided
                shape_path = auxiliary_data_dir / custom_shape_file

                if not shape_path.exists():
                    self._add_error(
                        "custom_shape_file_name",
                        f"Custom shape file not found: {shape_path}\n"
                        f"  Please place the file in the auxiliary-data/ directory\n"
                        f"  Supported formats: .json (GeoJSON/TopoJSON), .geojson",
                    )
                else:
                    # Validate file extension
                    valid_extensions = [".json", ".geojson", ".topojson"]
                    if shape_path.suffix.lower() not in valid_extensions:
                        self._add_warning(
                            "custom_shape_file_name", f"Shape file has unexpected extension '{shape_path.suffix}'. Supported: {', '.join(valid_extensions)}"
                        )

                    spatial_config.shape_file_path = shape_path
                    spatial_config.use_default_shape_file = False
                    logger.info(f"  ✓ Custom shape file: {custom_shape_file}")

                    # Warn if custom shape file but no custom location mapping
                    if not custom_location_mapping:
                        self._add_error(
                            "custom_location_mapping_file_name",
                            "Using custom shape file without custom location mapping. "
                            "This may cause location code mismatches. "
                            "Consider providing a custom location mapping CSV that matches "
                            "your shape file's location codes.",
                        )
            else:
                # Using default US states shape file
                logger.info("  ✓ Using default US States shape file (states-10m.json)")
                spatial_config.use_default_shape_file = True

            # Check location mapping configuration
            if custom_location_mapping:
                mapping_path = auxiliary_data_dir / custom_location_mapping

                # Error if file not found
                if not mapping_path.exists():
                    self._add_error(
                        "custom_location_mapping_file_name",
                        f"Custom location mapping file not found: {mapping_path}\n  Please place the file in the auxiliary-data/ directory",
                    )
                else:
                    # Validate CSV format
                    if not mapping_path.suffix.lower() == ".csv":
                        self._add_error("custom_location_mapping_file_name", f"Location mapping file must be CSV format (got {mapping_path.suffix})")
                    else:
                        # Validate CSV structure
                        try:
                            import pandas as pd

                            mapping_df = pd.read_csv(mapping_path)

                            # Check required columns exist
                            if location_code_col not in mapping_df.columns:
                                self._add_error(
                                    "location_code_col_header_name",
                                    f"Column '{location_code_col}' not found in {custom_location_mapping}. Available columns: {', '.join(mapping_df.columns)}",
                                )

                            if location_name_col not in mapping_df.columns:
                                self._add_error(
                                    "location_name_col_header_name",
                                    f"Column '{location_name_col}' not found in {custom_location_mapping}. Available columns: {', '.join(mapping_df.columns)}",
                                )

                            # Check for duplicates
                            if location_code_col in mapping_df.columns:
                                duplicates = mapping_df[location_code_col].duplicated()
                                if duplicates.any():
                                    dup_codes = mapping_df[location_code_col][duplicates].tolist()
                                    self._add_warning("custom_location_mapping_file_name", f"Duplicate location codes found in mapping: {dup_codes[:5]}")

                            logger.info(f"  ✓ Custom location mapping validated: {len(mapping_df)} locations")

                        except Exception as e:
                            self._add_error("custom_location_mapping_file_name", f"Error validating location mapping CSV: {e}")

                    spatial_config.location_mapping_path = mapping_path
                    spatial_config.use_default_location_mapping = False
                    logger.info(f"  ✓ Custom location mapping: {custom_location_mapping}")
            else:
                # Using default FIPS mapping
                logger.info("  ✓ Using default US State FIPS mapping")
                spatial_config.use_default_location_mapping = True

                # Warn if using default mapping with custom shape file
                if custom_shape_file:
                    self._add_warning(
                        "spatial_data_config",
                        "Using default US FIPS mapping with custom shape file. "
                        "Location codes in your data must use US state FIPS codes "
                        "(01=Alabama, 02=Alaska, etc.) to match the default mapping.",
                    )

        return spatial_config

    def _load_location_mapping(self, spatial_config: SpatialDataConfig) -> Dict[str, str]:
        """Load location mapping from file or use default"""
        if not spatial_config.use_default_location_mapping and spatial_config.location_mapping_path:
            try:
                import pandas as pd

                # Read location code as string to avoid issues
                mapping_df = pd.read_csv(spatial_config.location_mapping_path, dtype={spatial_config.location_code_col_header: str})

                # Check for required columns one last time before loading
                if spatial_config.location_code_col_header not in mapping_df.columns:
                    self._add_error("custom_location_mapping_file_name", f"Location code column '{spatial_config.location_code_col_header}' not found.")
                    return self._load_us_state_fips_mapping()

                if spatial_config.location_name_col_header not in mapping_df.columns:
                    self._add_error("custom_location_mapping_file_name", f"Location name column '{spatial_config.location_name_col_header}' not found.")
                    return self._load_us_state_fips_mapping()

                mapping = dict(zip(mapping_df[spatial_config.location_code_col_header], mapping_df[spatial_config.location_name_col_header]))

                logger.info(f"  ✓ Loaded custom location mapping with {len(mapping)} entries")
                return mapping
            except Exception as e:
                self._add_error("custom_location_mapping_file_name", f"Failed to load location mapping CSV: {e}")
                return self._load_us_state_fips_mapping()  # Fallback
        else:
            return self._load_us_state_fips_mapping()

    def _validate_time_unit(self):
        """Validate time_unit value"""
        if self.time_unit < 1:
            self._add_error("time_unit", f"time_unit must be at least 1 day (got {self.time_unit})")

        if self.time_unit > 14:
            self._add_warning(
                "time_unit",
                f"time_unit is {self.time_unit} days, which is unusually large. " + "Most forecasting hubs use 7 days (weekly) or 1 day (daily).",
            )

    def _validate_horizons(self):
        """Validate horizons list"""
        if not isinstance(self.horizons, list):
            self._add_error("horizons", f"horizons must be a list (got {type(self.horizons).__name__})")
            return

        if len(self.horizons) == 0:
            self._add_error("horizons", "horizons list cannot be empty")
            return

        # Check that all horizons are integers
        invalid_horizons = []
        for h in self.horizons:
            if not isinstance(h, int):
                invalid_horizons.append(h)

        if invalid_horizons:
            self._add_error(
                "horizons",
                f"All horizons must be integers. Invalid values: {invalid_horizons}",
            )

        # Warn about negative horizons (nowcasting)
        negative_horizons = [h for h in self.horizons if isinstance(h, int) and h < 0]
        if negative_horizons:
            self._add_warning(
                "horizons",
                f"Negative horizons detected: {negative_horizons}. "
                + "These represent nowcasting (predictions for dates before reference_date).",
            )

    def _validate_and_assign_model_colors(self):
        """Validate model colors and assign defaults if missing"""
        models_without_colors = []
        models_with_invalid_colors = []

        for model in self.models:
            if not model.color_hex:
                models_without_colors.append(model.model_name)
            elif not self._is_valid_hex_color(model.color_hex):
                models_with_invalid_colors.append((model.model_name, model.color_hex))
                # Mark for reassignment
                model.color_hex = None
                models_without_colors.append(model.model_name)

        if models_with_invalid_colors:
            invalid_list = [f"{name} ({color})" for name, color in models_with_invalid_colors]
            self._add_error(
                "available_models",
                f"Invalid hex color format for model(s): {', '.join(invalid_list)}. "
                + "Colors must be in format #RRGGBB or #RGB (e.g., '#FF5733' or '#F53')",
            )

        if models_without_colors:
            self._add_warning(
                "available_models",
                f"{len(models_without_colors)} model(s) missing or have invalid color_hex, " + f"will use default color palette: {', '.join(models_without_colors)}",
            )

            # Assign colors from default palette
            color_idx = 0
            for model in self.models:
                if not model.color_hex:
                    model.color_hex = self.DEFAULT_COLOR_PALETTE[color_idx % len(self.DEFAULT_COLOR_PALETTE)]
                    color_idx += 1

    def _validate_baseline_model(self):
        """Validate baseline model for relative WIS"""
        # Check if baseline model is in available models
        model_names = [m.model_name for m in self.models]

        if self.baseline_model_for_relative_wis not in model_names:
            self._add_warning(
                "baseline_model_for_relative_WIS",
                f"Baseline model '{self.baseline_model_for_relative_wis}' "
                + "is not being used for visualization. "
                + f"Available models: {', '.join(model_names)}",
            )

    def _print_validation_results(self):
        """Print all validation warnings and errors"""
        if self.validation_warnings:
            print("\n" + "=" * 80)
            print("CONFIGURATION WARNINGS")
            print("=" * 80)
            for warning in self.validation_warnings:
                print(f"⚠ [{warning.field}] {warning.message}")

        if self.validation_errors:
            print("\n" + "=" * 80)
            print("CONFIGURATION ERRORS")
            print("=" * 80)
            for error in self.validation_errors:
                print(f"✗ [{error.field}] {error.message}")
            print("=" * 80)

    def _get_value(self, key: str, default=None) -> Any:
        """Get a value from flat config structure"""
        for item in self.raw_config:
            if isinstance(item, dict) and key in item:
                return item[key]
        return default

    def _get_nested_value(self, parent_key: str, child_key: str, default=None) -> Any:
        """Get a nested value from config"""
        for item in self.raw_config:
            if isinstance(item, dict) and parent_key in item:
                parent_data = item[parent_key]
                if isinstance(parent_data, list):
                    for sub_item in parent_data:
                        if isinstance(sub_item, dict) and child_key in sub_item:
                            return sub_item[child_key]
        return default

    def _parse_forecast_periods(self) -> List[ForecastPeriod]:
        """Parse static forecast periods"""
        periods = []

        for item in self.raw_config:
            if isinstance(item, dict) and "forecast_periods" in item:
                period_list = item["forecast_periods"]

                for period_item in period_list:
                    if not isinstance(period_item, dict):
                        continue

                    for period_id, period_data in period_item.items():
                        config_dict = {}
                        if isinstance(period_data, list):
                            for prop in period_data:
                                if isinstance(prop, dict):
                                    config_dict.update(prop)

                        try:
                            period = ForecastPeriod(
                                period_id=config_dict.get("forecast_period_id", period_id),
                                display_string=config_dict["display_string"],
                                start_date=config_dict["start_date"],
                                end_date=config_dict["end_date"],
                                is_special_period=False,
                                is_default_selected=config_dict.get("is_default_selected", False),
                            )
                            periods.append(period)
                            logger.info(f"  ✓ Parsed forecast period: {period.period_id}")
                        except KeyError as e:
                            self._add_error(
                                "forecast_periods",
                                f"Missing required field in forecast period {period_id}: {e}",
                            )

        return periods

    def _parse_dynamic_periods(self) -> List[ForecastPeriod]:
        """Parse dynamic/special forecast periods"""
        dynamic_periods = []

        for item in self.raw_config:
            if isinstance(item, dict) and "special_forecast_periods" in item:
                special_periods_list = item["special_forecast_periods"]

                # Handle case where no special_forecast_periods specified
                if not special_periods_list:
                    logger.info("  ✓ No special forecast periods defined")
                    continue

                for period_item in special_periods_list:
                    if not isinstance(period_item, dict):
                        continue

                    for period_id, period_data in period_item.items():
                        config_dict = {}
                        if isinstance(period_data, list):
                            for prop in period_data:
                                if isinstance(prop, dict):
                                    if "time_anchor" in prop:
                                        anchor_list = prop["time_anchor"]
                                        anchor_dict = {}
                                        for anchor_item in anchor_list:
                                            if isinstance(anchor_item, dict):
                                                anchor_dict.update(anchor_item)
                                        config_dict["time_anchor"] = anchor_dict
                                    else:
                                        config_dict.update(prop)

                        try:
                            period = ForecastPeriod(
                                period_id=config_dict["special_period_id"],
                                display_string=config_dict["display_string"],
                                start_date=datetime(2000, 1, 1),
                                end_date=datetime(2000, 1, 1),
                                is_special_period=True,
                                time_anchor=config_dict.get("time_anchor"),
                            )
                            dynamic_periods.append(period)
                            logger.info(f"  ✓ Parsed special period (runtime calc): {period.period_id}")
                        except KeyError as e:
                            self._add_error(
                                "special_forecast_periods",
                                f"Missing field in special period '{period_id}': {e}",
                            )

        return dynamic_periods

    def _parse_single_location_mapping(self) -> Optional[str]:
        """Parse single location mapping if applicable"""
        if not self.is_single_location:
            return None

        return self._get_value("single_location_mapping")

    def _load_us_state_fips_mapping(self) -> Dict[str, str]:
        """Load US state FIPS code to name mapping from reference file"""

        # Get path to reference file (relative to this script)
        reference_path = Path(__file__).parent / "us_state_fips_mapping.json"

        try:
            with open(reference_path, "r") as f:
                mapping = json.load(f)
                logger.info(f"  ✓ Loaded US state FIPS mapping ({len(mapping)} locations)")
                return mapping
        except FileNotFoundError:
            logger.warning(f"  ⚠ US state FIPS mapping file not found at {reference_path}")
            return {}
        except json.JSONDecodeError as e:
            logger.warning(f"  ⚠ Error parsing US state FIPS mapping: {e}")
            return {}

    def _parse_targets(self) -> List[TargetConfig]:
        """Parse target/modelling task configurations"""
        targets = []
        all_period_ids = None  # Will be computed if needed

        for item in self.raw_config:
            if isinstance(item, dict) and "targets" in item:
                target_list = item["targets"]

                for target_item in target_list:
                    if not isinstance(target_item, dict):
                        continue

                    for target_name, target_data in target_item.items():
                        config_dict = {}
                        for prop in target_data:
                            if isinstance(prop, dict):
                                config_dict.update(prop)

                        try:
                            # Handle missing forecast_periods - DEFAULT TO ALL SEASONS
                            forecast_periods = config_dict.get("for_forecast_periods")
                            if not forecast_periods:
                                # Lazy load all period IDs
                                if all_period_ids is None:
                                    all_period_ids = [p.period_id for p in self.forecast_periods]
                                    all_period_ids.extend([p.period_id for p in self.dynamic_periods])

                                forecast_periods = all_period_ids
                                self._add_warning(
                                    "targets",
                                    f"Target '{target_name}' missing 'for_forecast_periods', " + "defaulting to all available periods",
                                )

                            target = TargetConfig(
                                target_name=target_name,
                                task_display_string=config_dict["task_display_string"],
                                target_key_name_for_task=config_dict["target_key_name_for_task"],
                                forecast_periods=forecast_periods,
                            )
                            targets.append(target)
                            logger.info(f"  ✓ Parsed target: {target_name} → {target.target_key_name_for_task}")
                        except KeyError as e:
                            self._add_error(
                                "targets",
                                f"Missing required field in target {target_name}: {e}",
                            )

        return targets

    def _parse_column_mappings(self) -> ColumnMapping:
        """Parse column name mappings"""
        target_mapping = {}
        for item in self.raw_config:
            if isinstance(item, dict) and "target_data_header_mapping" in item:
                mapping_list = item["target_data_header_mapping"]
                for mapping in mapping_list:
                    if isinstance(mapping, dict):
                        target_mapping.update(mapping)

        model_output_mapping = {}
        for item in self.raw_config:
            if isinstance(item, dict) and "model_output_data_header_mapping" in item:
                mapping_list = item["model_output_data_header_mapping"]
                for mapping in mapping_list:
                    if isinstance(mapping, dict):
                        model_output_mapping.update(mapping)

        # Handle as_of_col based on target data file format and partitioning mode
        as_of_col_val = target_mapping.get("as_of_col_name")

        # Check if using partitioned parquet
        is_partitioned_parquet = self.target_data_file_format == "parquet" and self.is_partitioned_parquet

        if is_partitioned_parquet:
            # Partitioned parquet mode: use directory names for versioning
            logger.info("  ✓ Using partitioned parquet: historical target-data will be loaded from subdirectories")
            as_of_col_val = None  # Don't use as_of column, use directory-based versioning
        elif as_of_col_val:
            # Single file (CSV or parquet) with as_of column: use column-based aggregation
            logger.info(f"  ✓ Using '{as_of_col_val}' column for historical target-data versioning")
        else:
            # No historical data support
            logger.info("  ✓ No historical target-data versioning configured")

        return ColumnMapping(
            date_col=target_mapping.get("date_col_name", "date"),
            observation_col=target_mapping.get("observation_col_name", "value"),
            location_col=target_mapping.get("location_col_name"),
            location_name_col=target_mapping.get("location_name_col_name"),
            target_col=target_mapping.get("target_col_name"),
            as_of_col=as_of_col_val,
            reference_date_col=model_output_mapping.get("reference_date_col_name", "reference_date"),
            target_end_date_col=model_output_mapping.get("target_end_date_col_name", "target_end_date"),
            model_target_col=model_output_mapping.get("target_col_name", "target"),
            horizon_col=model_output_mapping.get("horizon_col_name", "horizon"),
            output_type_col=model_output_mapping.get("output_type_col_name", "output_type"),
            output_type_id_col=model_output_mapping.get("output_type_id_col_name", "output_type_id"),
            value_col=model_output_mapping.get("value_col_name", "value"),
        )

    def _parse_models(self) -> List[ModelConfig]:
        """Parse available models configuration"""
        models = []

        for item in self.raw_config:
            if isinstance(item, dict) and "available_models" in item:
                model_list = item["available_models"]

                for model_item in model_list:
                    if not isinstance(model_item, dict):
                        continue

                    for model_name, model_props in model_item.items():
                        props_dict = {}
                        if isinstance(model_props, list):
                            for prop in model_props:
                                if isinstance(prop, dict):
                                    props_dict.update(prop)

                        model = ModelConfig(
                            model_name=model_name,
                            color_hex=props_dict.get("color_hex"),
                            display_name=props_dict.get("display_name", model_name),
                        )
                        models.append(model)
                        logger.info(f"  ✓ Parsed model: {model_name}")

        return models

    def _parse_prediction_intervals(self) -> List[PredictionInterval]:
        """Parse prediction interval configurations"""
        intervals = []

        for item in self.raw_config:
            if isinstance(item, dict) and "prediction_intervals" in item:
                interval_list = item["prediction_intervals"]

                for interval_item in interval_list:
                    if not isinstance(interval_item, dict):
                        continue

                    for level, level_data in interval_item.items():
                        props_dict = {}
                        for prop in level_data:
                            if isinstance(prop, dict):
                                props_dict.update(prop)

                        try:
                            level_int = int(level)

                            # Validate level is between 0 and 100
                            if not 0 < level_int < 100:
                                self._add_error(
                                    "prediction_intervals",
                                    f"Prediction interval level must be between 1 and 99 (got {level_int})",
                                )
                                continue

                            output_type_ids = props_dict["uses_output_type_ids"]

                            # Validate quantile values
                            invalid_quantiles = [q for q in output_type_ids if not self._is_valid_quantile(q)]
                            if invalid_quantiles:
                                self._add_error(
                                    "prediction_intervals",
                                    f"Invalid quantile values for {level}% interval: {invalid_quantiles}. "
                                    + "Quantiles must be between 0 and 1.",
                                )
                                continue

                            # Validate that we have exactly 2 quantiles as boundary for lower and upper
                            if len(output_type_ids) != 2:
                                self._add_error(
                                    "prediction_intervals",
                                    f"Prediction interval {level}% must have exactly 2 quantiles (lower, upper). "
                                    + f"Got {len(output_type_ids)}: {output_type_ids}",
                                )
                                continue

                            interval = PredictionInterval(
                                level=level_int,
                                output_type_ids=output_type_ids,
                            )
                            intervals.append(interval)
                            logger.info(f"  ✓ Parsed prediction interval: {level}%")
                        except (KeyError, ValueError) as e:
                            self._add_error(
                                "prediction_intervals",
                                f"Invalid prediction interval '{level}': {e}",
                            )

        return intervals

    def _parse_evaluation_intervals(self) -> List[PredictionInterval]:
        """Parse evaluation prediction interval configurations"""
        intervals = []

        for item in self.raw_config:
            if isinstance(item, dict) and "evaluations_prediction_intervals" in item:
                interval_list = item["evaluations_prediction_intervals"]

                for interval_item in interval_list:
                    if not isinstance(interval_item, dict):
                        continue

                    for level, level_data in interval_item.items():
                        props_dict = {}
                        for prop in level_data:
                            if isinstance(prop, dict):
                                props_dict.update(prop)

                        try:
                            level_int = int(level)

                            # Validate level is between 0 and 100
                            if not 0 < level_int < 100:
                                self._add_warning(
                                    "evaluations_prediction_intervals",
                                    f"Evaluation interval level should be between 1 and 99 (got {level_int})",
                                )

                            output_type_ids = props_dict["uses_output_type_ids"]

                            # Validate quantile values
                            invalid_quantiles = [q for q in output_type_ids if not self._is_valid_quantile(q)]
                            if invalid_quantiles:
                                self._add_warning(
                                    "evaluations_prediction_intervals",
                                    f"Invalid quantile values for {level}% evaluation interval: {invalid_quantiles}. "
                                    + "Quantiles must be between 0 and 1.",
                                )
                                continue

                            # Validate that we have exactly 2 quantiles (lower and upper)
                            if len(output_type_ids) != 2:
                                self._add_warning(
                                    "evaluations_prediction_intervals",
                                    f"Evaluation interval {level}% should have exactly 2 quantiles (lower, upper). "
                                    + f"Got {len(output_type_ids)}: {output_type_ids}",
                                )

                            interval = PredictionInterval(
                                level=level_int,
                                output_type_ids=output_type_ids,
                            )
                            intervals.append(interval)
                        except (KeyError, ValueError) as e:
                            self._add_warning(
                                "evaluations_prediction_intervals",
                                f"Invalid evaluation interval '{level}': {e}",
                            )

        return intervals

    def get_all_quantiles(self) -> List[str]:
        """Get all unique quantile values needed"""
        quantiles = set()
        quantiles.add("0.5")  # Always include median

        for interval in self.prediction_intervals:
            quantiles.update(interval.output_type_ids)

        for interval in self.evaluation_intervals:
            quantiles.update(interval.output_type_ids)

        return sorted(list(quantiles), key=lambda x: float(x))

    def get_all_period_ids(self) -> List[str]:
        """Get all forecast period IDs (static + dynamic)"""
        period_ids = [p.period_id for p in self.forecast_periods]
        period_ids.extend([p.period_id for p in self.dynamic_periods])
        return period_ids

    def has_validation_errors(self) -> bool:
        """Check if there are any validation errors"""
        return len(self.validation_errors) > 0

    def has_validation_warnings(self) -> bool:
        """Check if there are any validation warnings"""
        return len(self.validation_warnings) > 0


def load_config(config_path: Union[str, Path] = "config.yaml") -> DashboardConfig:
    """
    Load and validate dashboard configuration

    Args:
        config_path: Path to config.yaml file

    Returns:
        DashboardConfig: Validated configuration object

    Raises:
        ValueError: If configuration validation fails
        FileNotFoundError: If config file doesn't exist
    """
    try:
        config = DashboardConfig(config_path)

        # Check for validation errors
        if config.has_validation_errors():
            error_count = len(config.validation_errors)
            warning_count = len(config.validation_warnings)

            logger.error(f"Configuration validation failed with {error_count} error(s) " + f"and {warning_count} warning(s)")
            raise ValueError("Invalid configuration - see errors above")

        # Log warnings if any
        if config.has_validation_warnings():
            warning_count = len(config.validation_warnings)
            logger.warning(f"Configuration loaded with {warning_count} warning(s) - see above")

        return config

    except Exception as e:
        if isinstance(e, ValueError) and "Invalid configuration" in str(e):
            raise  # Re-raise validation errors
        else:
            logger.error(f"Unexpected error loading configuration: {e}")
            raise


# Test function for development
def test_config_processor():
    """Test function to validate the config processor"""
    try:
        print("Testing YAML Config Processor...")
        config = load_config()

        print("✓ Configuration loaded successfully")
        print(f"✓ Found {len(config.forecast_periods)} forecast periods")
        print(f"✓ Found {len(config.dynamic_periods)} special periods")
        print(f"✓ Found {len(config.targets)} target(s)")
        print(f"✓ Found {len(config.models)} model(s)")
        print(f"✓ Time unit: {config.time_unit} days")
        print(f"✓ Horizons: {config.horizons}")
        print(f"✓ Quantiles needed: {config.get_all_quantiles()}")

        # Show location data info
        if config.is_single_location:
            location_name = config.location_mapping.get(config.single_location_mapping, "Unknown")
            print(f"✓ Single location mode: {config.single_location_mapping} ({location_name})")
        else:
            print("✓ Multi-location mode: will auto-detect from data files")
            print(f"✓ Location mapping reference loaded: {len(config.location_mapping)} locations")

        if config.has_validation_warnings():
            print(f"⚠ {len(config.validation_warnings)} warnings (see above)")

        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        return False


if __name__ == "__main__":
    # Run test when script is executed directly
    import sys

    success = test_config_processor()
    sys.exit(0 if success else 1)
