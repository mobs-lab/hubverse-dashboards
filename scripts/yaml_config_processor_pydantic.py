"""
Pydantic-based YAML Configuration Processor for Hubverse Dashboard
Provides type-safe validation, better error messages, and automatic schema generation.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ConfigDict,
    HttpUrl,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# Base Configuration Models
# =============================================================================


class ForecastPeriodConfig(BaseModel):
    """Configuration for a forecast period (time range)"""

    model_config = ConfigDict(str_strip_whitespace=True)

    forecast_period_id: str = Field(
        ...,
        description="Unique identifier for this forecast period",
        pattern=r"^[a-zA-Z0-9_-]+$",
        examples=["season-2024-2025", "round-1"],
    )
    start_date: datetime = Field(..., description="Start date of forecast period (ISO format)")
    end_date: datetime = Field(..., description="End date of forecast period (ISO format)")
    display_string: str = Field(..., description="Human-readable name shown in dashboard", min_length=1, max_length=100)
    is_default_selected: bool = Field(default=False, description="Whether this period is selected by default (only one allowed)")

    @field_validator("end_date")
    @classmethod
    def end_after_start(cls, v: datetime, info) -> datetime:
        """Validate end_date is after start_date"""
        if "start_date" in info.data and v <= info.data["start_date"]:
            raise ValueError("end_date must be after start_date")
        return v

    @property
    def is_dynamic(self) -> bool:
        """Check if this is a dynamic (ongoing) forecast period"""
        return self.end_date > datetime.now()


class TimeAnchorConfig(BaseModel):
    """Time anchor configuration for special forecast periods"""

    anchor_on: str = Field(..., description="ID of forecast period to anchor to")
    anchor_mode: Literal["target-data", "model-output"] = Field(..., description="What data to use for calculating current date")
    range_calculation: int = Field(..., description="Number of time units backwards (must be negative)", lt=0)


class SpecialForecastPeriodConfig(BaseModel):
    """Configuration for special/dynamic forecast periods"""

    special_period_id: str = Field(..., pattern=r"^[a-zA-Z0-9_-]+$")
    display_string: str = Field(..., min_length=1)
    time_anchor: TimeAnchorConfig


class TargetConfig(BaseModel):
    """Configuration for a forecasting target/task"""

    target_id: str = Field(..., description="Unique identifier for this target", pattern=r"^[a-zA-Z0-9_-]+$")
    task_display_string: str = Field(..., description="Display name in dashboard", min_length=1)
    target_key_in_data: str = Field(..., description="Key to match in target/output_type column")
    for_forecast_periods: Optional[List[str]] = Field(default=None, description="Forecast period IDs to use (null = all periods)")
    is_default_selected: bool = Field(default=False, description="Whether this target is selected by default (only one allowed)")


class PredictionIntervalConfig(BaseModel):
    """Configuration for a prediction interval"""

    level: int = Field(..., ge=1, le=99, description="Prediction interval level (1-99)")
    uses_output_type_ids: List[str] = Field(
        ...,
        description="Quantile values for lower and upper bounds",
        min_length=2,
        max_length=2,
    )

    @field_validator("uses_output_type_ids")
    @classmethod
    def validate_quantiles(cls, v: List[str]) -> List[str]:
        """Ensure quantiles are valid and in ascending order"""
        try:
            quantiles = [float(q) for q in v]
        except ValueError:
            raise ValueError("Quantiles must be numeric strings")

        if not all(0 <= q <= 1 for q in quantiles):
            raise ValueError("Quantiles must be between 0 and 1")

        if quantiles[0] >= quantiles[1]:
            raise ValueError("Quantiles must be in ascending order")

        return v


class ModelConfig(BaseModel):
    """Configuration for a forecast model"""

    model_name: str = Field(..., min_length=1)
    color_hex: Optional[str] = Field(
        default=None,
        pattern=r"^#[0-9a-fA-F]{6}$",
        description="Hex color code for visualization",
    )
    display_name: Optional[str] = Field(default=None, description="Display name (defaults to model_name)")

    @model_validator(mode="after")
    def set_display_name(self):
        """Set display_name to model_name if not provided"""
        if not self.display_name:
            self.display_name = self.model_name
        return self


class SpatialDataConfig(BaseModel):
    """Spatial/location data configuration"""

    disable_map_in_dashboard: bool = Field(default=False)
    custom_shape_file_name: Optional[str] = None
    custom_location_mapping_file_name: Optional[str] = None
    location_code_col_header_name: str = Field(default="location")
    location_name_col_header_name: str = Field(default="location_name")


class TargetDataHeaderMapping(BaseModel):
    """Column name mappings for target data"""

    date_col_name: str = Field(default="date")
    observation_col_name: str = Field(default="observation")
    location_col_name: str = Field(default="location")
    location_name_col_name: Optional[str] = Field(default="location_name")
    target_col_name: Optional[str] = Field(default="target")
    as_of_col_name: Optional[str] = Field(default=None)


class ModelOutputHeaderMapping(BaseModel):
    """Column name mappings for model output data"""

    reference_date_col_name: str = Field(default="reference_date")
    target_end_date_col_name: str = Field(default="target_end_date")
    target_col_name: str = Field(default="target")
    horizon_col_name: str = Field(default="horizon")
    location_col_name: str = Field(default="location")
    output_type_col_name: str = Field(default="output_type")
    output_type_id_col_name: str = Field(default="output_type_id")
    value_col_name: str = Field(default="value")


# =============================================================================
# Main Dashboard Configuration
# =============================================================================


class DashboardConfig(BaseModel):
    """
    Main configuration for Hubverse Dashboard

    This model validates all dashboard configuration options and provides
    helpful error messages for common mistakes.
    """

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    # Data Source
    link_to_hubverse_compatible_data: Optional[str] = Field(default=None, description="Set to null for local data")
    target_data_link: Optional[HttpUrl] = None
    model_output_link: Optional[HttpUrl] = None

    # Forecast Periods
    forecast_periods: List[ForecastPeriodConfig] = Field(..., min_length=1, description="At least one forecast period required")
    special_forecast_periods: Optional[List[SpecialForecastPeriodConfig]] = Field(default=None)

    # Location Configuration
    is_single_location_forecast: bool = Field(default=False)
    single_location_mapping: Optional[str] = None
    spatial_config: SpatialDataConfig = Field(default_factory=SpatialDataConfig)

    # Target Configuration
    is_single_forecast_target: bool = Field(default=False)
    targets: Optional[List[TargetConfig]] = None
    time_unit: int = Field(..., ge=1, le=365, description="Time unit in days (typically 1 or 7)")

    # Target Data Configuration
    target_data_file_format: Literal["csv", "parquet"] = Field(default="csv")
    parquet_partitioned_by_as_of: bool = Field(default=False)
    single_target_data_file_name: Optional[str] = None
    target_data_header_mapping: TargetDataHeaderMapping = Field(default_factory=TargetDataHeaderMapping)
    target_data_observation_format: Literal["integer", "float"] = Field(default="float")

    # Model Output Configuration
    available_models: List[ModelConfig] = Field(..., min_length=1)
    model_output_data_file_naming_standard: Literal["ISODate", "Alphanumeric"] = Field(default="ISODate")
    model_output_data_header_mapping: ModelOutputHeaderMapping = Field(default_factory=ModelOutputHeaderMapping)

    # Prediction Configuration
    prediction_intervals: List[PredictionIntervalConfig]
    horizons: List[int] = Field(..., min_length=1)

    # Evaluation Configuration
    evaluations_prediction_intervals: Optional[List[PredictionIntervalConfig]] = None
    baseline_model_for_relative_WIS: str

    # Default Selections
    default_selected_location: Optional[Union[str, Dict[str, str]]] = Field(default=None, description="Default location code or dict mapping")
    default_selected_prediction_intervals: Optional[List[str]] = Field(default=None, description="Default prediction interval levels")
    default_selected_horizon: Optional[int] = Field(default=None, description="Default horizon value")
    default_selected_prediction_intervals_for_evaluations: Optional[List[str]] = Field(
        default=None, description="Default evaluation prediction interval levels"
    )

    # ==========================================================================
    # Cross-Field Validators
    # ==========================================================================

    @model_validator(mode="after")
    def validate_single_location_requires_mapping(self):
        """Ensure single_location_mapping provided when in single-location mode"""
        if self.is_single_location_forecast and not self.single_location_mapping:
            raise ValueError("single_location_mapping is REQUIRED when is_single_location_forecast is True")
        return self

    @model_validator(mode="after")
    def validate_multi_target_requires_targets(self):
        """Ensure targets defined when not in single-target mode"""
        if not self.is_single_forecast_target:
            if not self.targets or len(self.targets) == 0:
                raise ValueError("targets list is REQUIRED when is_single_forecast_target is False")
        return self

    @model_validator(mode="after")
    def validate_single_file_name_required(self):
        """Ensure single_target_data_file_name provided when needed"""
        if self.target_data_file_format in ["csv", "parquet"]:
            if not self.parquet_partitioned_by_as_of:
                if not self.single_target_data_file_name:
                    raise ValueError("single_target_data_file_name is REQUIRED when using CSV or non-partitioned parquet format")
        return self

    @model_validator(mode="after")
    def validate_only_one_default_period(self):
        """Ensure only one forecast period is default"""
        default_count = sum(1 for period in self.forecast_periods if period.is_default_selected)
        if default_count > 1:
            raise ValueError(f"Only one forecast period can be default, found {default_count}")
        return self

    @model_validator(mode="after")
    def validate_only_one_default_target(self):
        """Ensure only one target is default"""
        if self.targets:
            default_targets = [t.target_id for t in self.targets if t.is_default_selected]
            if len(default_targets) > 1:
                raise ValueError(f"Only one target can be default. Found: {', '.join(default_targets)}")
        return self

    @model_validator(mode="after")
    def validate_baseline_in_models(self):
        """Warn if baseline model not in available_models"""
        model_names = [m.model_name for m in self.available_models]
        if self.baseline_model_for_relative_WIS not in model_names:
            logger.warning(f"Baseline model '{self.baseline_model_for_relative_WIS}' not in available_models: {model_names}")
        return self

    @model_validator(mode="after")
    def validate_special_periods_anchor(self):
        """Ensure special period anchors reference valid periods"""
        if not self.special_forecast_periods:
            return self

        period_ids = {p.forecast_period_id for p in self.forecast_periods}

        for special in self.special_forecast_periods:
            if special.time_anchor.anchor_on not in period_ids:
                raise ValueError(f"Special period '{special.special_period_id}' anchors on undefined forecast period '{special.time_anchor.anchor_on}'")
        return self

    @model_validator(mode="after")
    def set_evaluation_intervals_default(self):
        """Set evaluation intervals to prediction intervals if not provided"""
        if not self.evaluations_prediction_intervals:
            self.evaluations_prediction_intervals = self.prediction_intervals
        return self

    @model_validator(mode="after")
    def validate_default_selections(self):
        """Validate default selection values"""
        # Validate default prediction intervals
        if self.default_selected_prediction_intervals:
            available_levels = {str(pi.level) for pi in self.prediction_intervals}
            invalid = [pi for pi in self.default_selected_prediction_intervals if str(pi) not in available_levels]
            if invalid:
                logger.warning(f"Default prediction intervals {invalid} not in configured intervals. Available: {list(available_levels)}")

        # Validate default horizon
        if self.default_selected_horizon is not None:
            if self.default_selected_horizon not in self.horizons:
                logger.warning(f"Default horizon {self.default_selected_horizon} not in configured horizons: {self.horizons}")

        return self

    # ==========================================================================
    # Helper Methods
    # ==========================================================================

    def get_all_quantiles(self) -> List[str]:
        """Get all unique quantile values needed"""
        quantiles = {"0.5"}  # Always include median

        for interval in self.prediction_intervals:
            quantiles.update(interval.uses_output_type_ids)

        if self.evaluations_prediction_intervals:
            for interval in self.evaluations_prediction_intervals:
                quantiles.update(interval.uses_output_type_ids)

        return sorted(list(quantiles), key=lambda x: float(x))

    def get_all_period_ids(self) -> List[str]:
        """Get all forecast period IDs"""
        period_ids = [p.forecast_period_id for p in self.forecast_periods]
        if self.special_forecast_periods:
            period_ids.extend([p.special_period_id for p in self.special_forecast_periods])
        return period_ids

    def get_default_location(self) -> Optional[str]:
        """Extract default location code from config"""
        if not self.default_selected_location:
            return None
        if isinstance(self.default_selected_location, dict):
            return list(self.default_selected_location.keys())[0]
        return self.default_selected_location


# =============================================================================
# Config Loading Function
# =============================================================================


def flatten_yaml_list_structure(raw_config: List[Dict]) -> Dict[str, Any]:
    """
    Flatten the YAML list-of-dicts structure to single dict.
    Handles nested structures for forecast_periods, targets, etc.
    """
    flat_config = {}

    for item in raw_config:
        if not isinstance(item, dict):
            continue

        for key, value in item.items():
            # Handle nested list structures (forecast_periods, targets, etc.)
            if key in [
                "forecast_periods",
                "special_forecast_periods",
                "targets",
                "available_models",
                "prediction_intervals",
                "evaluations_prediction_intervals",
            ]:
                if isinstance(value, list):
                    processed_list = []
                    for list_item in value:
                        if isinstance(list_item, dict):
                            # Extract the nested structure
                            for sub_key, sub_value in list_item.items():
                                if isinstance(sub_value, list):
                                    # Flatten nested list of dicts
                                    flattened = {}
                                    for prop in sub_value:
                                        if isinstance(prop, dict):
                                            # Handle time_anchor specially
                                            if "time_anchor" in prop:
                                                anchor_list = prop["time_anchor"]
                                                anchor_dict = {}
                                                for anchor_item in anchor_list:
                                                    if isinstance(anchor_item, dict):
                                                        anchor_dict.update(anchor_item)
                                                flattened["time_anchor"] = anchor_dict
                                            else:
                                                flattened.update(prop)

                                    # For models and intervals, preserve the name as key
                                    if key in ["available_models", "prediction_intervals", "evaluations_prediction_intervals"]:
                                        if key == "available_models":
                                            flattened["model_name"] = sub_key
                                        elif key in ["prediction_intervals", "evaluations_prediction_intervals"]:
                                            flattened["level"] = int(sub_key)

                                    processed_list.append(flattened)
                    flat_config[key] = processed_list
            elif key == "spatial_config":
                # Flatten spatial config if it exists
                continue  # Will be handled by extracting individual fields
            elif key == "target_data_header_mapping":
                # Flatten target data header mapping
                if isinstance(value, list):
                    mapping_dict = {}
                    for mapping in value:
                        if isinstance(mapping, dict):
                            mapping_dict.update(mapping)
                    flat_config["target_data_header_mapping"] = mapping_dict
            elif key == "model_output_data_header_mapping":
                # Flatten model output header mapping
                if isinstance(value, list):
                    mapping_dict = {}
                    for mapping in value:
                        if isinstance(mapping, dict):
                            mapping_dict.update(mapping)
                    flat_config["model_output_data_header_mapping"] = mapping_dict
            else:
                flat_config[key] = value

    # Extract spatial config fields
    spatial_fields = [
        "disable_map_in_dashboard",
        "custom_shape_file_name",
        "custom_location_mapping_file_name",
        "location_code_col_header_name",
        "location_name_col_header_name",
    ]
    spatial_config = {}
    for field in spatial_fields:
        if field in flat_config:
            spatial_config[field] = flat_config.pop(field)
    if spatial_config:
        flat_config["spatial_config"] = spatial_config

    return flat_config


def load_config_pydantic(config_path: Union[str, Path] = "config.yaml", dev_mode: bool = False) -> DashboardConfig:
    """
    Load and validate dashboard configuration using Pydantic

    Args:
        config_path: Path to config.yaml file
        dev_mode: If True, look for data in test-data-input/ instead of project root

    Returns:
        DashboardConfig: Validated configuration object

    Raises:
        ValidationError: If configuration is invalid
    """
    from pydantic import ValidationError

    logger.info("=" * 80)
    logger.info("PYDANTIC-BASED CONFIG VALIDATION")
    logger.info("=" * 80)

    config_path = Path(config_path)

    try:
        # Load YAML file
        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)
            if not raw_config or not isinstance(raw_config, list):
                raise ValueError("Config file must have a list of dictionaries at root level")

        # Flatten structure
        flat_config = flatten_yaml_list_structure(raw_config)

        # Validate with Pydantic
        config = DashboardConfig(**flat_config)

        logger.info("✓ Configuration validated successfully with Pydantic!")
        logger.info(f"✓ Found {len(config.forecast_periods)} forecast periods")
        logger.info(f"✓ Found {len(config.targets or [])} target(s)")
        logger.info(f"✓ Found {len(config.available_models)} model(s)")
        logger.info(f"✓ Time unit: {config.time_unit} days")
        logger.info(f"✓ Horizons: {config.horizons}")

        # Display default selections
        logger.info("\nDefault Selections:")
        logger.info(f"  • Location: {config.get_default_location()}")
        logger.info(f"  • Horizon: {config.default_selected_horizon}")
        logger.info(f"  • Prediction Intervals: {config.default_selected_prediction_intervals}")
        default_target = next((t.target_id for t in (config.targets or []) if t.is_default_selected), None)
        logger.info(f"  • Target: {default_target}")

        return config

    except ValidationError as e:
        print("\n" + "=" * 80)
        print("CONFIGURATION VALIDATION ERRORS")
        print("=" * 80)
        for error in e.errors():
            field = " -> ".join(str(loc) for loc in error["loc"])
            print(f"✗ [{field}] {error['msg']}")
            if "ctx" in error:
                print(f"   Context: {error['ctx']}")
        print("=" * 80 + "\n")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def export_json_schema(output_path: str = "config_schema.json"):
    """Export configuration schema as JSON Schema for documentation and tooling"""
    schema = DashboardConfig.model_json_schema()

    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2)

    logger.info(f"✓ JSON Schema exported to: {output_path}")
    logger.info(f"  Schema contains {len(schema.get('properties', {}))} top-level properties")


# =============================================================================
# Test Function
# =============================================================================


def test_pydantic_config():
    """Test function to validate the Pydantic config processor"""
    try:
        print("\n" + "=" * 80)
        print("Testing Pydantic YAML Config Processor...")
        print("=" * 80 + "\n")

        config = load_config_pydantic()

        print("\n" + "=" * 80)
        print("CONFIGURATION SUMMARY")
        print("=" * 80)
        print(f"Forecast Periods: {len(config.forecast_periods)}")
        print(f"Targets: {len(config.targets or [])}")
        print(f"Models: {len(config.available_models)}")
        print(f"Quantiles needed: {config.get_all_quantiles()}")
        print("=" * 80 + "\n")

        # Export JSON schema
        export_json_schema()

        return True

    except Exception as e:
        print(f"\n✗ Error: {e}\n")
        return False


if __name__ == "__main__":
    import sys

    success = test_pydantic_config()
    sys.exit(0 if success else 1)
