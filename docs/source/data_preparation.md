# Data Preparation

The Hubverse Dashboard relies on a strict directory structure for data ingestion. This design adheres to the [Hubverse Data Format](https://hubverse.io/en/latest/user-guide/model-output.html).

## Directory Structure

Your project root should look like this:

```text
hubverse-dashboard/
├── config.yaml
├── target-data/
│   ├── covid-hospital-admissions.csv
│   └── (or) time-series.parquet
└── model-output/
    ├── MOBS-GLEAM_COVID/
    │   ├── 2024-01-01-MOBS-GLEAM_COVID.csv
    │   └── 2024-01-08-MOBS-GLEAM_COVID.csv
    ├── Model-NAME-SECOND/
    │   └── ...
    └── ...
```

## Target Data (Ground Truth)

This file contains the observed data you are forecasting (e.g., actual hospital admissions).

*   **Format**: CSV or Parquet
*   **Required Columns**:
    *   `date`: ISO format date (YYYY-MM-DD)
    *   `observation`: The numerical value
    *   `location`: Location code (e.g., "US", "01")
    *   `target`: Target identifier (if multi-target)

```{note}
You can customize the column names in `config.yaml` under `target_data_header_mapping`.
```

## Model Output (Forecasts)

Each model gets its own subdirectory inside `model-output/`.

*   **Format**: CSV files following Hubverse standard.
*   **Naming**: `YYYY-MM-DD-ModelName.csv`
*   **Required Columns**:
    *   `reference_date`: Date the forecast was made
    *   `target`: The specific target ID
    *   `horizon`: Number of time steps ahead
    *   `target_end_date`: Date being predicted
    *   `location`: Location code
    *   `output_type`: Must be "quantile"
    *   `output_type_id`: The quantile level (0.01 - 0.99)
    *   `value`: The predicted value
