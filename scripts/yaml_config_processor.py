"""
YAML Configuration Processor for Hubverse Dashboard
Parses, validates, and handles errors in config.yaml with comprehensive error checking.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field

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
    is_dynamic: bool = False
    sub_display_value: Optional[str] = None
    time_anchor: Optional[Dict[str, Any]] = None
    range_calculation: Optional[int] = None

    def __post_init__(self):
        # Ensure dates are datetime objects
        if isinstance(self.start_date, str):
            date_str = self.start_date.replace("T", " ").replace("Z", "")
            try:
                self.start_date = datetime.fromisoformat(date_str)
            except:
                self.start_date = datetime.strptime(date_str.split()[0], "%Y-%m-%d")

        if isinstance(self.end_date, str):
            date_str = self.end_date.replace("T", " ").replace("Z", "")
            try:
                self.end_date = datetime.fromisoformat(date_str)
            except:
                self.end_date = datetime.strptime(date_str.split()[0], "%Y-%m-%d")


@dataclass
class TargetConfig:
    """Configuration for a modelling task/target"""

    target_column_in_target_data: str
    corresponding_key_in_model_output: str
    forecast_periods: List[str]
    display_name: Optional[str] = None


@dataclass
class PredictionInterval:
    """Configuration for a prediction interval"""

    level: int
    output_type_ids: List[str]

    def __post_init__(self):
        self.output_type_ids = sorted(
            [str(x) for x in self.output_type_ids], key=lambda x: float(x)
        )


@dataclass
class ModelConfig:
    """Configuration for a model"""

    model_name: str
    color_hex: Optional[str] = None
    display_name: Optional[str] = None


class DashboardConfig:
    """Main configuration class for the dashboard with comprehensive validation"""

    # Default color palette for models without specified colors
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
                    raise ValueError(
                        "Config file must have a list of dictionaries at root level"
                    )
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

    def _parse_and_validate(self):
        """Parse and validate all configuration sections"""
        logger.info("Parsing configuration...")

        # Data source links - CHECK FOR CONFLICTS
        self.target_data_link = self._get_nested_value(
            "links_to_hubverse_compatible_data", "target_data_link"
        )
        self.model_output_link = self._get_nested_value(
            "links_to_hubverse_compatible_data", "model_output_link"
        )

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
        self.location_data = self._parse_location_data()

        # Validate location data
        self._validate_location_data()

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

        # Column mappings
        self.column_mapping = self._parse_column_mappings()

        # Target data observation format - DEFAULT IF MISSING
        self.target_data_observation_format = self._get_value(
            "target_data_observation_format"
        )
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
        self.model_output_naming_standard = self._get_value(
            "model_output_data_file_naming_standard", "ISODate"
        )

        # Baseline model for evaluations - VALIDATE
        self.baseline_model_for_relative_wis = self._get_value(
            "baseline_model_for_relative_WIS"
        )
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
                    f"Duplicate display_string: '{period.display_string}' "
                    + f"(period: {period.period_id})",
                )
            seen_display_strings.add(period.display_string)

            # Check start_date before end_date
            if not period.is_dynamic and period.start_date > period.end_date:
                self._add_error(
                    "forecast_periods",
                    f"start_date is after end_date for period '{period.period_id}' "
                    + f"({period.start_date.strftime('%Y-%m-%d')} > {period.end_date.strftime('%Y-%m-%d')})",
                )

            # Check dynamic period anchor/range calculation combo
            if period.is_dynamic and period.time_anchor:
                anchor_on = period.time_anchor.get("anchor_on")
                range_calc = period.range_calculation

                if (
                    anchor_on == "earliest"
                    and range_calc is not None
                    and range_calc < 0
                ):
                    self._add_error(
                        "special_forecast_periods",
                        f"Dynamic period '{period.period_id}' with anchor_on='earliest' "
                        + f"cannot have negative range_calculation ({range_calc}). "
                        + f"For 'earliest', use positive numbers to go forward in time.",
                    )

                if anchor_on == "latest" and range_calc is not None and range_calc > 0:
                    self._add_error(
                        "special_forecast_periods",
                        f"Dynamic period '{period.period_id}' with anchor_on='latest' "
                        + f"should have negative range_calculation (got {range_calc}). "
                        + f"For 'latest', use negative numbers to go backward in time.",
                    )

    def _validate_location_data(self):
        """Validate location data configuration"""
        if self.is_single_location:
            return

        # Check if location_data has either csv_path or manual_mapping
        has_csv = bool(self.location_data.get("csv_path"))
        has_mapping = bool(self.location_data.get("manual_mapping"))

        if not has_csv and not has_mapping:
            self._add_error(
                "location_data",
                "Either location_data_csv_file_path or location_mapping must be provided "
                + "when not in single-location mode.",
            )
            return

        # Check for common US state code/name mismatches
        manual_mapping = self.location_data.get("manual_mapping", {})

        # Sample of correct mappings for validation
        correct_mappings = {
            "01": "Alabama",
            "02": "Alaska",
            "04": "Arizona",
            "06": "California",
            "36": "New York",
            "48": "Texas",
            "12": "Florida",
            "17": "Illinois",
        }

        for code, name in manual_mapping.items():
            if code in correct_mappings and name != correct_mappings[code]:
                self._add_warning(
                    "location_data",
                    f"Location code '{code}' mapped to '{name}', "
                    + f"expected '{correct_mappings[code]}'. "
                    + f"Assuming code is correct.",
                )

    def _validate_time_unit(self):
        """Validate time_unit value"""
        if self.time_unit < 1:
            self._add_error(
                "time_unit", f"time_unit must be at least 1 day (got {self.time_unit})"
            )

        if self.time_unit > 14:
            self._add_warning(
                "time_unit",
                f"time_unit is {self.time_unit} days, which is unusually large. "
                + f"Most forecasting hubs use 7 days (weekly) or 1 day (daily).",
            )

    def _validate_and_assign_model_colors(self):
        """Validate model colors and assign defaults if missing"""
        models_without_colors = []

        for model in self.models:
            if not model.color_hex:
                models_without_colors.append(model.model_name)

        if models_without_colors:
            self._add_warning(
                "available_models",
                f"{len(models_without_colors)} model(s) missing color_hex, "
                + f"will use default color palette: {', '.join(models_without_colors)}",
            )

            # Assign colors from default palette
            color_idx = 0
            for model in self.models:
                if not model.color_hex:
                    model.color_hex = self.DEFAULT_COLOR_PALETTE[
                        color_idx % len(self.DEFAULT_COLOR_PALETTE)
                    ]
                    color_idx += 1

    def _validate_baseline_model(self):
        """Validate baseline model for relative WIS"""
        if not self.baseline_model_for_relative_wis:
            self._add_warning(
                "baseline_model_for_relative_WIS",
                "No baseline model specified for relative WIS calculation. "
                + "Relative WIS evaluation will be disabled.",
            )
            return

        # Check if baseline model is in available models
        model_names = [m.model_name for m in self.models]

        if self.baseline_model_for_relative_wis not in model_names:
            self._add_warning(
                "baseline_model_for_relative_WIS",
                f"Baseline model '{self.baseline_model_for_relative_wis}' "
                + f"is not in available_models list. This may cause issues during evaluation.",
            )

        # Check if baseline model is the same as one of the models being evaluated
        # This is valid but should warn user
        if self.baseline_model_for_relative_wis in model_names:
            # This is actually OK - baseline can be one of the models
            pass

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
                                period_id=config_dict.get(
                                    "forecast_period_id", period_id
                                ),
                                display_string=config_dict["display_string"],
                                start_date=config_dict["start_date"],
                                end_date=config_dict["end_date"],
                                is_dynamic=False,
                            )
                            periods.append(period)
                            logger.info(
                                f"  ✓ Parsed forecast period: {period.period_id}"
                            )
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
                special_periods = item["special_forecast_periods"]

                for sp_item in special_periods:
                    # Parse dynamic periods (with time anchors)
                    if isinstance(sp_item, dict) and "dynamic_periods" in sp_item:
                        dyn_list = sp_item["dynamic_periods"]

                        for period_item in dyn_list:
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
                                        is_dynamic=True,
                                        time_anchor=config_dict.get("time_anchor"),
                                        range_calculation=config_dict.get(
                                            "range_calculation"
                                        ),
                                    )
                                    dynamic_periods.append(period)
                                    logger.info(
                                        f"  ✓ Parsed dynamic period (runtime calc): {period.period_id}"
                                    )
                                except KeyError as e:
                                    self._add_error(
                                        "special_forecast_periods",
                                        f"Missing field in dynamic period: {e}",
                                    )

        return dynamic_periods

    def _parse_single_location_mapping(self) -> Optional[Dict[str, str]]:
        """Parse single location mapping if applicable"""
        if not self.is_single_location:
            return None

        for item in self.raw_config:
            if isinstance(item, dict) and "single_location_mapping" in item:
                mapping_list = item["single_location_mapping"]
                if mapping_list:
                    mapping = {}
                    for entry in mapping_list:
                        if isinstance(entry, dict):
                            mapping.update(entry)
                    return mapping
        return None

    def _parse_location_data(self) -> Dict[str, Any]:
        """Parse location data configuration"""
        location_config = {}

        for item in self.raw_config:
            if isinstance(item, dict) and "location_data" in item:
                data_list = item["location_data"]

                for data_item in data_list:
                    if not isinstance(data_item, dict):
                        continue

                    if "location_data_csv_file_path" in data_item:
                        location_config["csv_path"] = data_item[
                            "location_data_csv_file_path"
                        ]

                    if "location_code_col_name" in data_item:
                        location_config["code_col"] = data_item[
                            "location_code_col_name"
                        ]

                    if "location_name_col_name" in data_item:
                        location_config["name_col"] = data_item[
                            "location_name_col_name"
                        ]

                    if "location_mapping" in data_item:
                        mapping_list = data_item["location_mapping"]
                        mapping = {}
                        for entry in mapping_list:
                            if isinstance(entry, dict):
                                mapping.update(entry)
                        location_config["manual_mapping"] = mapping

        return location_config

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

                    for target_col, target_data in target_item.items():
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
                                    all_period_ids = [
                                        p.period_id for p in self.forecast_periods
                                    ]
                                    all_period_ids.extend(
                                        [p.period_id for p in self.dynamic_periods]
                                    )

                                forecast_periods = all_period_ids
                                self._add_warning(
                                    "targets",
                                    f"Target '{target_col}' missing 'for_forecast_periods', "
                                    + f"defaulting to all available periods",
                                )

                            target = TargetConfig(
                                target_column_in_target_data=target_col,
                                corresponding_key_in_model_output=config_dict[
                                    "corresponding_key_in_model_output_target_column"
                                ],
                                forecast_periods=forecast_periods,
                                display_name=config_dict.get(
                                    "display_name", target_col
                                ),
                            )
                            targets.append(target)
                            logger.info(
                                f"  ✓ Parsed target: {target_col} → {target.corresponding_key_in_model_output}"
                            )
                        except KeyError as e:
                            self._add_error(
                                "targets",
                                f"Missing required field in target {target_col}: {e}",
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

        return ColumnMapping(
            date_col=target_mapping.get("date_col_name", "date"),
            observation_col=target_mapping.get("observation_col_name", "value"),
            location_col=target_mapping.get("location_col_name"),
            location_name_col=target_mapping.get("location_name_col_name"),
            target_col=target_mapping.get("target_col_name"),
            as_of_col=target_mapping.get("as_of_col_name"),
            reference_date_col=model_output_mapping.get(
                "reference_date_col_name", "reference_date"
            ),
            target_end_date_col=model_output_mapping.get(
                "target_end_date_col_name", "target_end_date"
            ),
            model_target_col=model_output_mapping.get("target_col_name", "target"),
            horizon_col=model_output_mapping.get("horizon_col_name", "horizon"),
            output_type_col=model_output_mapping.get(
                "output_type_col_name", "output_type"
            ),
            output_type_id_col=model_output_mapping.get(
                "output_type_id_col_name", "output_type_id"
            ),
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
                            interval = PredictionInterval(
                                level=int(level),
                                output_type_ids=props_dict["uses_output_type_ids"],
                            )
                            intervals.append(interval)
                            logger.info(f"  ✓ Parsed prediction interval: {level}%")
                        except (KeyError, ValueError) as e:
                            self._add_error(
                                "prediction_intervals",
                                f"Invalid prediction interval: {e}",
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
                            interval = PredictionInterval(
                                level=int(level),
                                output_type_ids=props_dict["uses_output_type_ids"],
                            )
                            intervals.append(interval)
                        except (KeyError, ValueError) as e:
                            self._add_warning(
                                "evaluations_prediction_intervals",
                                f"Invalid evaluation interval: {e}",
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

    def validate(self) -> Tuple[List[str], List[str]]:
        """
        Validate configuration and return tuple of (errors, warnings)

        Returns:
            Tuple[List[str], List[str]]: (error_messages, warning_messages)
        """
        # Basic required field validation (already done in parsing)
        error_messages = [error.message for error in self.validation_errors]
        warning_messages = [warning.message for warning in self.validation_warnings]

        # Additional cross-reference validations
        if not self.time_unit:
            error_messages.append("time_unit is required")

        if not self.horizons:
            error_messages.append("horizons list is required")

        if not self.forecast_periods and not self.dynamic_periods:
            error_messages.append("At least one forecast period must be defined")

        if not self.targets:
            error_messages.append("At least one target must be defined")

        if not self.models:
            error_messages.append("At least one model must be defined")

        if not self.prediction_intervals:
            error_messages.append("At least one prediction interval must be defined")

        # validation for location data
        if not self.is_single_location:
            has_csv = bool(self.location_data.get("csv_path"))
            has_mapping = bool(self.location_data.get("manual_mapping"))

            if not has_csv and not has_mapping:
                error_messages.append(
                    "location_data must provide either 'location_data_csv_file_path' "
                    + "or 'location_mapping' when not in single-location mode"
                )

        # Validate target forecast period references
        all_period_ids = self.get_all_period_ids()
        for target in self.targets:
            for period_id in target.forecast_periods:
                if period_id not in all_period_ids:
                    error_messages.append(
                        f"Target '{target.target_column_in_target_data}' "
                        + f"references undefined forecast period: '{period_id}'"
                    )

        return error_messages, warning_messages

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

            logger.error(
                f"Configuration validation failed with {error_count} error(s) "
                + f"and {warning_count} warning(s)"
            )
            raise ValueError("Invalid configuration - see errors above")

        # Log warnings if any
        if config.has_validation_warnings():
            warning_count = len(config.validation_warnings)
            logger.warning(
                f"Configuration loaded with {warning_count} warning(s) - see above"
            )

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

        print(f"✓ Configuration loaded successfully")
        print(f"✓ Found {len(config.forecast_periods)} forecast periods")
        print(f"✓ Found {len(config.dynamic_periods)} special periods")
        print(f"✓ Found {len(config.targets)} target(s)")
        print(f"✓ Found {len(config.models)} model(s)")
        print(f"✓ Time unit: {config.time_unit} days")
        print(f"✓ Horizons: {config.horizons}")
        print(f"✓ Quantiles needed: {config.get_all_quantiles()}")

        # Show location data info
        if config.is_single_location:
            print(f"✓ Single location mode: {config.single_location_mapping}")
        else:
            if config.location_data.get("manual_mapping"):
                num_locs = len(config.location_data["manual_mapping"])
                print(f"✓ Location data: {num_locs} locations from manual mapping")
            elif config.location_data.get("csv_path"):
                print(
                    f"✓ Location data: from CSV file {config.location_data['csv_path']}"
                )

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
