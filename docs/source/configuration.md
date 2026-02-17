# Configuration Reference

This page documents the full configuration schema for `config.yaml`. All configuration is validated at build time using [Pydantic v2](https://docs.pydantic.dev/) models defined in `scripts/yaml_config_processor_pydantic.py`.

To get started, copy the example configuration from `config-file-examples/`:

```bash
cp config-file-examples/config.yaml.example config.yaml
```

```{tip}
The `config-file-examples/` directory also contains pre-made configurations for specific data hubs (COVID-19, FluSight, RSV) that you can use as starting points.
```

---

## Main Configuration

The root-level configuration object. All sections below are nested within this.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.DashboardConfig
   :members:
   :exclude-members: model_config, validate_as_of_shift, validate_single_location_requires_mapping, validate_single_file_name_required, validate_only_one_default_period, validate_only_one_default_target, assign_model_colors, validate_baseline_in_models, validate_special_periods_anchor, set_evaluation_intervals_default, validate_default_selections, validate_coverage_levels
```

---

## Sub-Components

### Forecast Periods

Define time ranges for your forecasting data. Each period appears as a selectable option in the dashboard's period selector.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.ForecastPeriodConfig
   :members:
   :exclude-members: model_config
```

### Special Forecast Periods

Dynamic time windows (e.g., "Last 2 Weeks") calculated relative to the latest available data. These require an ongoing forecast period (one with `end_date` in the future) to anchor to.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.SpecialForecastPeriodConfig
   :members:
   :exclude-members: model_config
```

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.TimeAnchorConfig
   :members:
   :exclude-members: model_config
```

### Targets & Tasks

Define the outcomes you are forecasting. Each target represents a distinct quantity (e.g., "COVID-19 Admissions", "Flu ED Visits").

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.TargetConfig
   :members:
   :exclude-members: model_config
```

### Data Value Processing

Optional scaling and rounding applied to data values per target.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.DataValueProcessingConfig
   :members:
   :exclude-members: model_config
```

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.ScalingFactorConfig
   :members:
   :exclude-members: model_config
```

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.RoundingDecimalsConfig
   :members:
   :exclude-members: model_config
```

### Models

Configure which forecast models to display in the dashboard.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.ModelConfig
   :members:
   :exclude-members: model_config
```

### Prediction Intervals

Define the confidence interval bands displayed around the median forecast line.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.PredictionIntervalConfig
   :members:
   :exclude-members: model_config
```

### Spatial Configuration

Configure geographic location handling and map visualization.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.SpatialDataConfig
   :members:
   :exclude-members: model_config
```

### Column Mappings

Map your data file column names to the dashboard's expected standard names.

#### Target Data Columns

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.TargetDataHeaderMapping
   :members:
   :exclude-members: model_config
```

#### Model Output Columns

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.ModelOutputHeaderMapping
   :members:
   :exclude-members: model_config
```

### UI Customization

Customize the dashboard's visual appearance without modifying React code.

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.UICustomizationConfig
   :members:
   :exclude-members: model_config
```

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.NavButtonConfig
   :members:
   :exclude-members: model_config
```

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.InfoButtonContent
   :members:
   :exclude-members: model_config
```

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.MapColorScaleConfig
   :members:
   :exclude-members: model_config
```

---

## JSON Schema

A JSON Schema representation of the configuration is available at `config_schema.json` in the project root. This can be used with IDE extensions for autocompletion and validation while editing `config.yaml`.

To regenerate the schema from the Pydantic models:

```bash
cd scripts
python yaml_config_processor_pydantic.py
```

See the [Configuration JSON Schema](config_schema.md) page for the rendered schema.

---

## Validation Rules

The Pydantic configuration processor enforces these cross-field rules:

1. **Single default period**: Only one `forecast_period` can have `is_default_selected: true`.
2. **Single default target**: Only one `target` can have `is_default_selected: true`.
3. **Single location requires mapping**: `single_location_mapping` is required when `is_single_location_forecast: true`.
4. **File name required for non-partitioned data**: `single_target_data_file_name` is required for CSV and non-partitioned Parquet formats.
5. **Special period anchors must exist**: `time_anchor.anchor_on` must reference a defined `forecast_period_id`.
6. **as_of shift bounds**: `as_of_column_date_shift` absolute value cannot exceed `time_unit`.
7. **Automatic color assignment**: Models without a `color_hex` are automatically assigned colors from a default palette.
