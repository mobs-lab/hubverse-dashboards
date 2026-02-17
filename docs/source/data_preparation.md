# Data Preparation

The Hubverse Dashboard ingests data from a standardized directory structure adhering to the [Hubverse Data Format](https://docs.hubverse.io/en/latest/user-guide/hub-structure.html). This page details the required formats, directory layout, and configuration options.

## Directory Structure

Your project root (or `development-mode-root/` in dev mode) should contain:

```text
hubverse-dashboard/
├── config.yaml                          # Dashboard configuration
├── target-data/                         # Ground truth / observed data
│   ├── time-series.csv                  # Single CSV file (or .parquet)
│   └── (or) as_of=2024-01-01/          # Partitioned parquet directories
│       └── data.parquet
├── model-output/                        # Forecast submissions
│   ├── MOBS-GLEAM_COVID/               # One directory per model
│   │   ├── 2024-01-01-MOBS-GLEAM_COVID.csv
│   │   └── 2024-01-08-MOBS-GLEAM_COVID.csv
│   ├── CovidHub-baseline/
│   │   └── ...
│   └── ...
└── auxiliary-data/                      # Supporting files
    ├── locations.csv                    # Location code-to-name mapping
    └── custom-map.json                  # Custom GeoJSON/TopoJSON (optional)
```

---

## Target Data (Ground Truth)

The target data file contains the observed values you are forecasting (e.g., actual hospital admissions).

### Supported Formats

| Format | Config Setting | Notes |
|--------|---------------|-------|
| **CSV** | `target_data_file_format: "csv"` | Single file, specified by `single_target_data_file_name` |
| **Parquet** (single file) | `target_data_file_format: "parquet"`, `parquet_partitioned_by_as_of: false` | Single file, specified by `single_target_data_file_name` |
| **Parquet** (partitioned) | `target_data_file_format: "parquet"`, `parquet_partitioned_by_as_of: true` | Directory partitioned by `as_of` date |

### Required Columns

The column names are configurable via `target_data_header_mapping` in `config.yaml`:

| Standard Name | Config Key | Required | Description |
|--------------|------------|----------|-------------|
| `date` | `date_col_name` | Yes | Observation date (ISO format: `YYYY-MM-DD`) |
| `observation` | `observation_col_name` | Yes | Numerical observed value. Missing data is filled with `-1`. |
| `location` | `location_col_name` | For multi-location | Location code (e.g., `"US"`, `"01"`) |
| `target` | `target_col_name` | For multi-target | Target identifier (e.g., `"wk inc covid hosp"`) |
| `location_name` | `location_name_col_name` | No | Human-readable location name |
| `as_of` | `as_of_col_name` | Conditional | Report date for historical data versioning. Required if historical target data is enabled and not using partitioned parquet. |

```{note}
Column names in your data files can differ from the standard names above. Use `target_data_header_mapping` to map your column names to the expected names.
```

### Historical Target Data

If your target data includes an `as_of` column (or uses partitioned parquet by `as_of`), the dashboard can show "what we knew at the time of forecast." This enables the historical data toggle in the frontend.

- Set `disable_historical_target_data: true` to skip historical processing (faster builds).
- Use `as_of_column_date_shift` if your `as_of` dates are on a different day-of-week grid than your reference dates.

### Data Value Processing

Per-target scaling and rounding can be configured under `data_value_processing`:

```yaml
targets:
  - target_id: "flu-proportion"
    data_value_processing:
      scaling_factor:
        target_data: 100       # Convert 0-1 proportion to 0-100 percentage
        model_output: 100
      rounding_decimals:
        target_data: 2
        model_output: 2
```

---

## Model Output (Forecasts)

Each forecasting model gets its own subdirectory inside `model-output/`.

### File Naming

Controlled by `model_output_data_file_naming_standard`:

| Standard | Example Filename | Description |
|----------|-----------------|-------------|
| `ISODate` (default) | `2024-01-15-ModelName.csv` | ISO date prefix |
| `Alphanumeric` | `round-1-ModelName.csv` | Alphanumeric prefix |

### Required Columns

Configurable via `model_output_data_header_mapping`:

| Standard Name | Config Key | Description |
|--------------|------------|-------------|
| `reference_date` | `reference_date_col_name` | Date the forecast was made |
| `target_end_date` | `target_end_date_col_name` | Date being predicted |
| `target` | `target_col_name` | Target identifier |
| `horizon` | `horizon_col_name` | Forecast horizon (in units of `time_unit`) |
| `location` | `location_col_name` | Location code |
| `output_type` | `output_type_col_name` | Must include `"quantile"` |
| `output_type_id` | `output_type_id_col_name` | Quantile level (e.g., `0.05`, `0.5`, `0.95`) |
| `value` | `value_col_name` | Predicted value |

### Horizon Calculation

If the `horizon` column is missing, it is automatically calculated:

```
horizon = (target_end_date - reference_date) / time_unit
```

where `time_unit` is specified in `config.yaml` (typically `7` for weekly forecasts).

---

## Auxiliary Data

Optional supporting files placed in `auxiliary-data/`:

### Location Mapping

A CSV file mapping location codes to human-readable names. Configured via `spatial_config`:

```yaml
spatial_config:
  custom_location_mapping_file_name: "locations.csv"
  location_code_col_header_name: "location"
  location_name_col_header_name: "location_name"
```

If not provided, the system falls back to a built-in US FIPS code mapping (`scripts/us_state_fips_mapping.json`).

### Custom Map Shapefile

A GeoJSON or TopoJSON file for the geographic map visualization:

```yaml
spatial_config:
  custom_shape_file_name: "custom-map.json"
```

If not provided, the default US states map (`public/states-10m.json`) is used. Custom shapefiles are automatically copied to `public/` during the build.

---

## Prediction Intervals

Configure which prediction intervals to display in charts:

```yaml
prediction_intervals:
  "50":
    uses_output_type_ids: ["0.25", "0.75"]
  "90":
    uses_output_type_ids: ["0.05", "0.95"]
```

Each interval requires exactly two quantile values (lower and upper bounds) that must exist in your model output data.

---

## Evaluation Configuration

For model performance metrics, configure:

- `baseline_model_for_relative_WIS` -- The model used as denominator in WIS Ratio calculations. Must exist in `model-output/`.
- `evaluation_coverage_levels` -- List of coverage percentages to calculate (e.g., `[50, 95]`).
- `evaluation_coverage_level_for_location_map` -- Single coverage level for the geographic map visualization.

```{note}
Evaluation processing can be skipped entirely by using Options 2 or 4 in the build menu (or the `--skip-evaluations` CLI flag). This disables the Evaluations page in the dashboard.
```
