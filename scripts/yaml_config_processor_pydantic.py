"""
Pydantic-based YAML Configuration Processor for Hubverse Dashboard
Provides type-safe validation, better error messages, and automatic schema generation.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

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
    start_date: datetime = Field(..., description="The inclusive start date of the forecast period (ISO format).")
    end_date: datetime = Field(..., description="The inclusive end date of the forecast period (ISO format).")
    display_string: str = Field(..., description="Human-readable name shown in the dashboard's period selector.", min_length=1, max_length=100)
    is_default_selected: bool = Field(
        default=False, description="If true, this period will be selected by default when the dashboard loads. Only one period can be the default."
    )

    @field_validator("end_date")
    @classmethod
    def end_after_start(cls, v: datetime, info) -> datetime:
        """Validates that the period end date occurs after the start date."""
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
    range_calculation: int = Field(..., description="Number of time units backwards (must be negative)", lt=0)


class SpecialForecastPeriodConfig(BaseModel):
    """Configuration for special/dynamic forecast periods"""

    special_period_id: str = Field(..., pattern=r"^[a-zA-Z0-9_-]+$")
    display_string: str = Field(..., min_length=1)
    time_anchor: TimeAnchorConfig


class ScalingFactorConfig(BaseModel):
    """Configuration for scaling target and model output data values."""

    target_data: float = Field(default=1.0)
    model_output: float = Field(default=1.0)


class RoundingDecimalsConfig(BaseModel):
    """Configuration for rounding data values"""

    # Can only be non-negative integer, representing number of decimals places
    target_data: float = Field(default=2, ge=0)
    model_output: float = Field(default=2, ge=0)


class DataValueProcessingConfig(BaseModel):
    """Configuration for processing data values (scaling and rounding)."""

    scaling_factor: Optional[ScalingFactorConfig] = Field(default_factory=ScalingFactorConfig)
    rounding_decimals: Optional[RoundingDecimalsConfig] = Field(default_factory=RoundingDecimalsConfig)


class TargetConfig(BaseModel):
    """
    Configuration for a forecasting target.
    Each target represents a distinct outcome you are predicting, such as 'COVID-19 Admissions' or 'Flu-related ED Visits'.
    """

    target_id: str = Field(..., description="Unique identifier for this target", pattern=r"^[a-zA-Z0-9_-]+$")
    task_display_string: str = Field(..., description="Display name for this target in the dashboard UI.", min_length=1)
    target_key_in_data: str = Field(
        ...,
        description="The exact string identifier for this target as it appears in the 'target' column of your data files.",
        examples=["wk inc covid hosp", "wk flu hosp"],
    )
    for_forecast_periods: Optional[List[str]] = Field(
        default=None,
        description="If specified, this target will only be available for the listed forecast period IDs. If null, it is available for all periods.",
    )
    is_default_selected: bool = Field(default=False, description="If true, this target will be selected by default. Only one target can be the default.")
    data_value_processing: Optional[DataValueProcessingConfig] = Field(default_factory=DataValueProcessingConfig)


class PredictionIntervalConfig(BaseModel):
    """
    Configuration for a single prediction interval.
    This defines the shaded confidence interval regions around the median prediction line in the forecast chart.
    """

    level: int = Field(..., ge=1, le=99, description="The prediction interval level (e.g., 50, 90, 95). This is the value shown to the user.")
    uses_output_type_ids: List[str] = Field(
        ...,
        description="A pair of quantile values [lower, upper] that define the bounds of the interval.",
        min_length=2,
        max_length=2,
        examples=[["0.05", "0.95"], ["0.25", "0.75"]],
    )

    @field_validator("uses_output_type_ids", mode="after")
    @classmethod
    def validate_quantiles(cls, v: List[str]) -> List[str]:
        """Ensure quantiles are valid and in ascending order"""
        try:
            quantiles = [float(q) for q in v]
        except ValueError:
            raise ValueError("Quantiles must be numeric strings")

        if not all(0 <= float(q) <= 1 for q in quantiles):
            raise ValueError("Quantiles must be between 0 and 1")

        if float(quantiles[0]) >= float(quantiles[1]):
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


class SpatialDataConfig(BaseModel):
    """Spatial/location data configuration"""

    disable_map_in_dashboard: bool = Field(default=False)
    custom_shape_file_name: Optional[str] = None
    custom_location_mapping_file_name: Optional[str] = None
    location_code_col_header_name: str = Field(default="location")
    location_name_col_header_name: str = Field(default="location_name")


class TargetDataHeaderMapping(BaseModel):
    """Column name mappings for your ground truth (target) data file."""

    date_col_name: str = Field(default="date", description="Column containing the date of the observation.")
    observation_col_name: str = Field(
        default="observation", description="Column containing the observed numerical value. Missing values will be filled with -1."
    )
    location_col_name: str = Field(
        default="location", description="Column containing the location code (e.g., 'US', '01'). Required for multi-location dashboards."
    )
    location_name_col_name: Optional[str] = Field(
        default="location_name", description="Column containing the human-readable location name (e.g., 'Massachusetts', 'Washington')."
    )
    target_col_name: Optional[str] = Field(
        default="target", description="Column containing the target identifier (e.g., 'wk inc covid hosp'). Required for multi-target dashboards."
    )
    as_of_col_name: Optional[str] = Field(
        default=None, description="If provided, enables historical data versioning. This column should contain the date the data was reported."
    )


class ModelOutputHeaderMapping(BaseModel):
    """Column name mappings for your model output files."""

    reference_date_col_name: str = Field(default="reference_date", description="Column containing the date when the forecast was made.")
    target_end_date_col_name: str = Field(default="target_end_date", description="Column containing the date that is being forecasted.")
    target_col_name: str = Field(default="target", description="Column containing the target identifier being predicted.")
    horizon_col_name: str = Field(default="horizon", description="Column containing the forecast horizon value (in units of `time_unit`).")
    location_col_name: str = Field(default="location", description="Column containing the location code.")
    output_type_col_name: str = Field(default="output_type", description="Column identifying the prediction type. Must contain 'quantile'.")
    output_type_id_col_name: str = Field(
        default="output_type_id", description="Column containing the quantile level for 'quantile' rows (e.g., 0.05, 0.5, 0.95)."
    )
    value_col_name: str = Field(default="value", description="Column containing the predicted numerical value.")


class InfoButtonContent(BaseModel):
    """Configuration for InfoButton popup content"""

    title: str = Field(..., min_length=1, max_length=100, description="InfoButton dialog title")
    content: str = Field(..., min_length=1, description="InfoButton dialog content (supports markdown/HTML)")


class NavButtonConfig(BaseModel):
    """Configuration for header navigation buttons"""

    button_text: str = Field(..., min_length=1, max_length=50)
    nav_to_page: Optional[Literal["Forecast", "Evaluation"]] = None
    nav_to_external: bool = Field(default=False)
    nav_to_link: Optional[str] = None

    @model_validator(mode="after")
    def validate_navigation(self):
        """Ensure either page or external link is provided"""
        if self.nav_to_external and not self.nav_to_link:
            raise ValueError("nav_to_link is required when nav_to_external is True")
        if not self.nav_to_external and not self.nav_to_page:
            raise ValueError("Either nav_to_page or nav_to_external must be provided")
        return self


class MapColorScaleConfig(BaseModel):
    """Configuration for location map color gradient scale"""

    color_top: str = Field(default="#00495F", pattern=r"^#[0-9A-Fa-f]{6}$", description="Hex color for top of gradient (worse performance)")
    color_base: str = Field(default="#E9E9E9", pattern=r"^#[0-9A-Fa-f]{6}$", description="Hex color for baseline/middle (neutral)")
    color_bottom: str = Field(default="#6A9629", pattern=r"^#[0-9A-Fa-f]{6}$", description="Hex color for bottom of gradient (better performance)")
    color_null: str = Field(default="#363b43", pattern=r"^#[0-9A-Fa-f]{6}$", description="Hex color for locations with no data")


class UICustomizationConfig(BaseModel):
    """
    All UI customization options for the dashboard
    """

    # Header customization
    ui_header_title_name: str = Field(default="FluForecast", min_length=1, max_length=100, description="Dashboard header title")
    ui_header_nav_btn: Optional[List[NavButtonConfig]] = Field(default=None, description="Navigation buttons configuration")

    # Forecast page customizations
    ui_forecast_header_chart_name: Optional[str] = Field(default="Weekly Hospital Admissions Forecast", max_length=200)
    ui_forecast_header_hist_td_toggle_text: Optional[str] = Field(default="Show Admissions at Time of Forecast", max_length=200)
    disable_location_info_display: bool = Field(default=False)

    # Forecast Page InfoButton content customizations
    ui_forecast_header_infobutton_content: Optional[InfoButtonContent] = None
    ui_forecast_settings_horizon_infobutton_content: Optional[InfoButtonContent] = None

    # Evaluation Page Tab Bar Customization
    ui_evaluation_overview_tab_name: Optional[str] = Field(default="Season Overview", max_length=50)
    ui_evaluation_single_model_tab_name: Optional[str] = Field(default="Single-Model", max_length=50)

    # Evaluation Page Overview Tab UI Customization
    ui_evaluation_chart_log_mode_indicator_text: Optional[str] = Field(default="Use Log Scale", max_length=100)
    ui_evaluation_overview_location_map_title: Optional[str] = Field(default="Location-Specific", max_length=100)

    # Evaluation Page InfoButton content customizations
    ui_evaluation_overview_infobutton_content: Optional[InfoButtonContent] = None
    ui_evaluation_single_model_infobutton_content: Optional[InfoButtonContent] = None
    ui_evaluation_overview_horizon_infobutton_content: Optional[InfoButtonContent] = None
    ui_evaluation_single_model_horizon_infobutton_content: Optional[InfoButtonContent] = None

    # Evaluation Page Overview Tab Location-Specific Map Color Gradient Scale color customizations
    ui_evaluation_overview_location_map_color_scale: Optional[MapColorScaleConfig] = Field(default_factory=MapColorScaleConfig)

    @model_validator(mode="after")
    def validate_nav_buttons_no_duplicates(self):
        """Ensure no duplicate internal page links in navigation"""
        if self.ui_header_nav_btn:
            internal_pages = [btn.nav_to_page for btn in self.ui_header_nav_btn if btn.nav_to_page]
            if len(internal_pages) != len(set(internal_pages)):
                duplicates = [page for page in internal_pages if internal_pages.count(page) > 1]
                raise ValueError(f"Duplicate internal navigation pages found: {set(duplicates)}. Each page ('Forecast', 'Evaluation') can only appear once. ")
        return self


# =============================================================================
# Main Dashboard Configuration
# =============================================================================


class DashboardConfig(BaseModel):
    """
    Main configuration for Hubverse Dashboard

    This model validates all dashboard configuration options and provides
    helpful error messages for common mistakes.
    """

    # Strip whitespace, re-evaluate when data change, and ignore unwarranted configurations
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    # Data Source
    link_to_hubverse_compatible_data: Optional[str] = Field(
        default=None, description="URL to a remote, Hubverse-compatible data repository on GitHub. Set to null for local data."
    )
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
    targets: List[TargetConfig]
    time_unit: int = Field(
        ..., ge=1, le=365, description="The time unit in days between forecast points. This is crucial for calculating forecast horizons correctly."
    )

    # Target Data Configuration
    target_data_file_format: Literal["csv", "parquet"] = Field(default="csv")
    parquet_partitioned_by_as_of: bool = Field(default=False)
    single_target_data_file_name: Optional[str] = None
    disable_historical_target_data: bool = Field(
        default=False,
        description="Special flag for turning off the historical target data functionalities. "
        "Default is False. If True, 'as_of' column will be ignored (except for extracting latest ground truth), "
        "and the dashboard will not process historical snapshots. Frontend visualization related to historical "
        "target data will be disabled (historical target data toggle and the orange line).",
    )
    as_of_column_date_shift: int = Field(
        default=0, description="Shift 'as_of' dates by this many days. Useful if as_of dates are on a different grid than target/reference dates."
    )
    target_data_header_mapping: TargetDataHeaderMapping = Field(default_factory=TargetDataHeaderMapping)
    target_data_observation_format: Literal["integer", "float"] = Field(default="float")

    @model_validator(mode="after")
    def validate_as_of_shift(self):
        """Validate that as_of_column_date_shift is within reasonable bounds (less than time_unit)"""
        if abs(self.as_of_column_date_shift) > self.time_unit:
            raise ValueError(
                f"as_of_column_date_shift ({self.as_of_column_date_shift}) cannot be larger than time_unit ({self.time_unit}). "
                "This would cause misalignment of forecast cycles."
            )

        # Warn if historical data is disabled but as_of shift is set
        if self.disable_historical_target_data and self.as_of_column_date_shift != 0:
            logger.warning(
                f"as_of_column_date_shift is set to {self.as_of_column_date_shift} but disable_historical_target_data is True. "
                "The shift will still be applied to extract the latest ground truth data."
            )

        return self

    # Model Output Configuration
    available_models: List[ModelConfig] = Field(..., min_length=1)
    model_output_data_file_naming_standard: Literal["ISODate", "Alphanumeric"] = Field(default="ISODate")
    model_output_data_header_mapping: ModelOutputHeaderMapping = Field(default_factory=ModelOutputHeaderMapping)

    # Prediction Configuration
    prediction_intervals: List[PredictionIntervalConfig]
    horizons: List[int] = Field(..., min_length=1)

    # Evaluation Configuration
    evaluations_prediction_intervals: Optional[List[PredictionIntervalConfig]] = None
    baseline_model_for_relative_WIS: str = Field(
        ..., description="The model ID to use as a baseline for calculating Relative WIS. This model acts as a benchmark for performance comparison."
    )
    evaluation_coverage_levels: List[int] = Field(default=[50, 95], description="List of integer percentages (0-100) for evaluation coverage calculation.")
    evaluation_coverage_level_for_location_map: int = Field(
        default=95,
        ge=1,
        le=99,
        description="Single coverage level percentage (1-99) used for location map aggregates. "
        "This can overlap with evaluation_coverage_levels or be separate. Defaults to 95."
    )

    # Default Selections
    default_selected_location: Optional[Union[str, Dict[str, str]]] = Field(
        default=None, description="Default location to display when the dashboard loads. Can be a location code string or a code-name dictionary."
    )
    default_selected_prediction_intervals: Optional[List[str]] = Field(default=None, description="Default prediction interval levels")
    default_selected_horizon: Optional[int] = Field(default=None, description="Default horizon value")
    default_selected_prediction_intervals_for_evaluations: Optional[List[str]] = Field(
        default=None, description="Default evaluation prediction interval levels"
    )

    # UI Customizations
    ui_customization: UICustomizationConfig = Field(default_factory=UICustomizationConfig)

    @field_validator("evaluation_coverage_levels")
    @classmethod
    def validate_coverage_levels(cls, v: List[int]) -> List[str]:
        """Validate coverage levels and convert to list of strings"""
        if not v:
            # Return empty list or defaults depending on logic downstream,
            # but defaults are set in Field().
            # However, if user provides empty list [], we might want defaults.
            # But Field default only applies if key is missing.
            return []

        for level in v:
            if not isinstance(level, int):
                raise ValueError(f"Coverage level must be an integer, got {type(level)}")
            if level <= 0 or level >= 100:
                raise ValueError(f"Coverage level must be between 0 and 100 (exclusive), got {level}")

        # Sort and convert to strings as requested
        return sorted([str(level) for level in set(v)], key=lambda x: int(x))

    # ==========================================================================
    # Cross-Field Validators
    # ==========================================================================

    @model_validator(mode="after")
    def validate_single_location_requires_mapping(self):
        """Ensures `single_location_mapping` is provided when in single-location mode."""
        if self.is_single_location_forecast and not self.single_location_mapping:
            raise ValueError("single_location_mapping is REQUIRED when is_single_location_forecast is True")
        return self

    @model_validator(mode="after")
    def validate_single_file_name_required(self):
        """Ensures `single_target_data_file_name` is provided for CSV or non-partitioned Parquet data."""
        if self.target_data_file_format in ["csv", "parquet"]:
            if not self.parquet_partitioned_by_as_of:
                if not self.single_target_data_file_name:
                    raise ValueError("single_target_data_file_name is REQUIRED when using CSV or non-partitioned parquet format")
        return self

    @model_validator(mode="after")
    def validate_only_one_default_period(self):
        """Ensures that only one forecast period has `is_default_selected` set to true."""
        default_count = sum(1 for period in self.forecast_periods if period.is_default_selected)
        if default_count > 1:
            raise ValueError(f"Only one forecast period can be default, found {default_count}")
        return self

    @model_validator(mode="after")
    def validate_only_one_default_target(self):
        """Ensures that only one target has `is_default_selected` set to true."""
        default_targets = [t.target_id for t in self.targets if t.is_default_selected]
        if len(default_targets) > 1:
            raise ValueError(f"Only one target can be default. Found: {', '.join(default_targets)}")
        return self

    @model_validator(mode="after")
    def assign_model_colors(self):
        """Assigns unique colors from a default palette to any models that do not have a `color_hex` specified."""
        default_palette = [
            "#9ceb94",
            "#3fc49e",
            "#45cded",
            "#0292d1",
            "#7bb1ff",
            "#5f5fd6",
            "#d36f54",
            "#e89c31",
            "#a855f7",
            "#ec4899",
            "#22c55e",
            "#f59e0b",
            "#ef4444",
            "#8b5cf6",
            "#06b6d4",
        ]

        assigned_colors = {m.color_hex.lower() for m in self.available_models if m.color_hex}
        palette_idx = 0

        for model in self.available_models:
            if not model.color_hex:
                # Find next available color
                while palette_idx < len(default_palette):
                    color = default_palette[palette_idx]
                    palette_idx += 1
                    if color.lower() not in assigned_colors:
                        object.__setattr__(model, "color_hex", color)
                        assigned_colors.add(color.lower())
                        break

        return self

    @model_validator(mode="after")
    def validate_baseline_in_models(self):
        """Warns the user if the specified baseline model is not in the list of available models."""
        model_names = [m.model_name for m in self.available_models]
        if self.baseline_model_for_relative_WIS not in model_names:
            logger.warning(f"Baseline model '{self.baseline_model_for_relative_WIS}' not in available_models: {model_names}")
        return self

    @model_validator(mode="after")
    def validate_special_periods_anchor(self):
        """Ensures that special period anchors reference valid, defined forecast periods."""
        if not self.special_forecast_periods:
            return self

        period_ids = {p.forecast_period_id for p in self.forecast_periods}

        for special in self.special_forecast_periods:
            if special.time_anchor.anchor_on not in period_ids:
                raise ValueError(f"Special period '{special.special_period_id}' anchors on undefined forecast period '{special.time_anchor.anchor_on}'")
        return self

    @model_validator(mode="after")
    def set_evaluation_intervals_default(self):
        """Sets `evaluations_prediction_intervals` to match `prediction_intervals` if it's not explicitly provided."""
        if not self.evaluations_prediction_intervals:
            self.evaluations_prediction_intervals = self.prediction_intervals
        return self

    @model_validator(mode="after")
    def validate_default_selections(self):
        """Validates that the default selections for PIs and horizons exist in the available options."""
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

        # Add quantiles needed for configured coverage levels
        if self.evaluation_coverage_levels:
            for level_str in self.evaluation_coverage_levels:
                # level_str is now a string like "50", "95"
                level = int(level_str)
                alpha = 1.0 - (level / 100.0)
                lower = alpha / 2.0
                upper = 1.0 - (alpha / 2.0)

                # Round to reasonable precision to match data (usually 3 or 4 decimals)
                # Hubverse data typically uses 0.025, 0.975, etc.
                quantiles.add(f"{lower:.3g}")
                quantiles.add(f"{upper:.3g}")

        # Add quantiles needed for location map coverage level
        # This ensures the location map level is available even if not in the main list
        if hasattr(self, 'evaluation_coverage_level_for_location_map') and self.evaluation_coverage_level_for_location_map:
            level = self.evaluation_coverage_level_for_location_map
            alpha = 1.0 - (level / 100.0)
            lower = alpha / 2.0
            upper = 1.0 - (alpha / 2.0)
            quantiles.add(f"{lower:.3g}")
            quantiles.add(f"{upper:.3g}")

        return sorted(list(quantiles), key=lambda x: float(x))

    def get_all_period_ids(self) -> List[str]:
        """Get all forecast period IDs"""
        period_ids = [p.forecast_period_id for p in self.forecast_periods]
        if self.special_forecast_periods:
            period_ids.extend([p.special_period_id for p in self.special_forecast_periods])
        return period_ids

    def get_default_location(self) -> Optional[str]:
        """
        Extract default location CODE from config.
        Handles both dict format {"US": "US"} and string format "US"
        Returns location code as string or None if not configured
        """
        if not self.default_selected_location:
            return None
        if isinstance(self.default_selected_location, dict):
            # Extract first key from dict
            return list(self.default_selected_location.keys())[0] if self.default_selected_location else None
        return str(self.default_selected_location)

    def get_location_mapping(self) -> Dict[str, str]:
        """Get location mapping (loaded at runtime)"""
        return getattr(self, "_location_mapping", {})


# =============================================================================
# Config Loading Function
# =============================================================================


def load_and_validate_config(config_path: Union[str, Path] = "config.yaml", dev_mode: bool = False) -> DashboardConfig:
    """
    Load and validate dashboard configuration using Pydantic from a structured YAML file.

    Args:
        config_path: Path to config.yaml file.
        dev_mode: If True, look for data in development-mode-root/ instead of project root.

    Returns:
        DashboardConfig: Validated configuration object.
    """
    from pydantic import ValidationError

    logger.info("=" * 80)
    logger.info("PYDANTIC-BASED CONFIG VALIDATION")
    logger.info("=" * 80)

    config_path = Path(config_path)

    try:
        # 1. Load YAML file
        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)
            if not raw_config or not isinstance(raw_config, dict):
                raise ValueError("Config file must be a dictionary (mapping) at the root level.")

        # 2. Pre-process dictionary-based structures into lists for Pydantic
        if "available_models" in raw_config and isinstance(raw_config.get("available_models"), dict):
            raw_config["available_models"] = [{"model_name": name, **props} for name, props in raw_config["available_models"].items()]

        for key in ["prediction_intervals", "evaluations_prediction_intervals"]:
            if key in raw_config and isinstance(raw_config.get(key), dict):
                raw_config[key] = [{"level": int(level), **props} for level, props in raw_config[key].items()]

        # 3. Validate with Pydantic
        config = DashboardConfig(**raw_config)

        # Load location mapping
        location_mapping = _load_location_mapping(config, config_path, dev_mode)
        object.__setattr__(config, "_location_mapping", location_mapping)

        # Display default selections
        logger.info("\nDefault Selections:")
        logger.info(f"  - Location: {config.get_default_location()}")
        logger.info(f"  - Horizon: {config.default_selected_horizon}")
        logger.info(f"  - Prediction Intervals: {config.default_selected_prediction_intervals}")
        default_target = next((t.target_id for t in (config.targets or []) if t.is_default_selected), None)
        logger.info(f"  - Target: {default_target}")

        return config

    except ValidationError as e:
        print("\n" + "=" * 80)
        print("CONFIGURATION VALIDATION ERRORS")
        print("=" * 80)
        for error in e.errors():
            field = " -> ".join(str(loc) for loc in error["loc"])
            print(f"[X] [{field}] {error['msg']}")
            if "ctx" in error:
                print(f"    Context: {error['ctx']}")
        print("=" * 80 + "\n")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def _load_us_state_fips_mapping() -> Dict[str, str]:
    """Load US state FIPS code to name mapping from reference file"""
    reference_path = Path(__file__).parent / "us_state_fips_mapping.json"

    try:
        with open(reference_path, "r") as f:
            mapping = json.load(f)
            logger.info(f"  [OK] Loaded US state FIPS mapping ({len(mapping)} locations)")
            return mapping
    except FileNotFoundError:
        logger.warning(f"  [!] US state FIPS mapping file not found at {reference_path}")
        return {}
    except json.JSONDecodeError as e:
        logger.warning(f"  [!] Error parsing US state FIPS mapping: {e}")
        return {}


def _load_location_mapping(config: DashboardConfig, config_path: Path, dev_mode: bool) -> Dict[str, str]:
    """
    Load location mapping with the following order:
    1. Custom location mapping file (highest priority)
    2. Default US FIPS mapping (fallback)
    """
    # Determine base path
    project_root = config_path.parent
    data_base_path = project_root / "development-mode-root" if dev_mode else project_root
    auxiliary_data_dir = data_base_path / "auxiliary-data"

    # Check if custom location mapping is specified
    custom_location_file = config.spatial_config.custom_location_mapping_file_name

    if custom_location_file:
        mapping_path = auxiliary_data_dir / custom_location_file
        if mapping_path.exists():
            try:
                import pandas as pd

                # Read location code as string to preserve leading zeros (e.g., "01" for Alabama)
                mapping_df = pd.read_csv(mapping_path, dtype={config.spatial_config.location_code_col_header_name: str})

                # Validate required columns exist
                code_col = config.spatial_config.location_code_col_header_name
                name_col = config.spatial_config.location_name_col_header_name

                if code_col not in mapping_df.columns:
                    logger.error(f"  [X] Column '{code_col}' not found in {custom_location_file}")
                    logger.warning("  [!] Falling back to default US FIPS mapping")
                    return _load_us_state_fips_mapping()

                if name_col not in mapping_df.columns:
                    logger.error(f"  [X] Column '{name_col}' not found in {custom_location_file}")
                    logger.warning("  [!] Falling back to default US FIPS mapping")
                    return _load_us_state_fips_mapping()

                location_mapping = dict(zip(mapping_df[code_col].astype(str), mapping_df[name_col].astype(str)))

                logger.info(f" [OK] Loaded custom location mapping with {len(location_mapping)} entries from {custom_location_file}")
                return location_mapping
            except Exception as e:
                logger.warning(f"  [!] Failed to load custom location mapping: {e}")
                logger.warning("  [!] Falling back to default US FIPS mapping")

    # Use default US FIPS mapping as fallback
    return _load_us_state_fips_mapping()


def export_json_schema(output_path: str = "config_schema.json"):
    """Export configuration schema as JSON Schema for documentation and tooling"""
    schema = DashboardConfig.model_json_schema()

    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2)

    logger.info(f"[OK] JSON Schema exported to: {output_path}")
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

        config = load_and_validate_config()

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
        print(f"\n[X] Error: {e}\n")
        return False


if __name__ == "__main__":
    import sys

    success = test_pydantic_config()
    sys.exit(0 if success else 1)
