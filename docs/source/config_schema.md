# Configuration JSON Schema

The `config.yaml` configuration is validated by Pydantic models and can also be represented as a [JSON Schema](https://json-schema.org/). The schema file `config_schema.json` is located in the project root.

## Using the Schema

### IDE Autocompletion

Many editors support JSON Schema validation for YAML files. For example, to enable autocompletion and inline validation in VS Code, add a schema reference comment at the top of your `config.yaml`:

```yaml
# yaml-language-server: $schema=./config_schema.json
---
# Your configuration here...
```

### Regenerating the Schema

The JSON Schema is auto-generated from the Pydantic models. To regenerate after modifying `yaml_config_processor_pydantic.py`:

```bash
cd scripts
python yaml_config_processor_pydantic.py
```

This runs the test function which calls `export_json_schema()`.

```{note}
The committed `config_schema.json` may be out of date if Pydantic models have been modified since the last regeneration. Always regenerate before relying on the schema file for external tooling.
```

## Schema Reference

The schema is generated from the {class}`~yaml_config_processor_pydantic.DashboardConfig` Pydantic model and its sub-models. Refer to the [Configuration Reference](configuration.md) for detailed documentation of each field.

### Required Top-Level Properties

| Property | Type | Description |
|----------|------|-------------|
| `forecast_periods` | array | At least one forecast period definition |
| `time_unit` | integer (1--365) | Time unit in days between forecast points |
| `available_models` | array | At least one model configuration |
| `prediction_intervals` | array | Prediction interval definitions |
| `horizons` | array of integers | Available forecast horizons |
| `baseline_model_for_relative_WIS` | string | Baseline model name for WIS ratio calculations |

### Optional Properties with Defaults

| Property | Default | Description |
|----------|---------|-------------|
| `link_to_hubverse_compatible_data` | `null` | URL to remote Hubverse-compatible repo |
| `is_single_location_forecast` | `false` | Single-location mode flag |
| `target_data_file_format` | `"csv"` | Target data format (`csv` or `parquet`) |
| `parquet_partitioned_by_as_of` | `false` | Whether parquet is partitioned by `as_of` |
| `disable_historical_target_data` | `false` | Skip historical data processing |
| `as_of_column_date_shift` | `0` | Days to shift `as_of` dates |
| `evaluation_coverage_levels` | `[50, 95]` | Coverage levels for evaluation |
| `evaluation_coverage_level_for_location_map` | `95` | Coverage level for map visualization |

### Defined Sub-Schemas

The following sub-schemas are defined in the `$defs` section:

- `ForecastPeriodConfig` -- Forecast period time range
- `SpecialForecastPeriodConfig` -- Dynamic/relative time windows
- `TimeAnchorConfig` -- Anchor configuration for special periods
- `TargetConfig` -- Forecasting target definition
- `ModelConfig` -- Model configuration
- `PredictionIntervalConfig` -- Prediction interval bounds
- `SpatialDataConfig` -- Location/map settings
- `TargetDataHeaderMapping` -- Target data column name mapping
- `ModelOutputHeaderMapping` -- Model output column name mapping
