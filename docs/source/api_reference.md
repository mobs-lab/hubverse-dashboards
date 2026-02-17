# API Reference

This section documents the Python modules that power the Hubverse Dashboard's data processing pipeline. All modules are located in the `scripts/` directory.

---

## Configuration Validation

### yaml_config_processor_pydantic

Pydantic-based configuration validation with type-safe models and automatic JSON Schema generation.

```{eval-rst}
.. automodule:: yaml_config_processor_pydantic
   :members: load_and_validate_config, export_json_schema, DashboardConfig
   :show-inheritance:
```

For the complete Pydantic model documentation, see the [Configuration Reference](configuration.md).

---

## Pipeline Orchestration

### dashboard_builder_workflow

Top-level orchestrator that coordinates configuration validation, data fetching, and data processing.

```{eval-rst}
.. automodule:: dashboard_builder_workflow
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Data Fetching

### data_fetcher

Handles cloning and updating remote Hubverse-compatible GitHub repositories.

```{eval-rst}
.. automodule:: data_fetcher
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Core Data Processing

### data_processor

The central data processing engine. Handles ingestion, transformation, evaluation, and output generation.

```{eval-rst}
.. automodule:: data_processor
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Evaluation

### evaluation_processor

Calculates evaluation metrics: WIS (Weighted Interval Score), MAPE (Mean Absolute Percentage Error), Coverage, and WIS Ratio.

```{eval-rst}
.. automodule:: evaluation_processor
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Change Detection

### manifest_manager

File-level change detection using MD5 checksums for incremental data-update runs.

```{eval-rst}
.. automodule:: manifest_manager
   :members:
   :undoc-members:
   :show-inheritance:
```

### utils_change_detection

Processed-data-level change identification for target data revisions and new predictions.

```{eval-rst}
.. automodule:: utils_change_detection
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Data Structuring Utilities

### utils_data_structuring

Transforms pandas DataFrames into nested dictionary structures optimized for the React frontend's JSON consumption.

```{eval-rst}
.. automodule:: utils_data_structuring
   :members:
   :undoc-members:
   :show-inheritance:
```

### utils_data

Low-level utility functions for date conversion, JSON encoding (handling NumPy/Pandas types), and DataFrame type management.

```{eval-rst}
.. automodule:: utils_data
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Evaluation Aggregation

### utils_evaluation_aggregation

Aggregation functions for evaluation metrics, producing IQR statistics, location map aggregates, and coverage aggregates organized by forecast period.

```{eval-rst}
.. automodule:: utils_evaluation_aggregation
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Forecast Period Utilities

### utils_forecast_period

Helper functions for computing ongoing and special/dynamic forecast period date ranges.

```{eval-rst}
.. automodule:: utils_forecast_period
   :members:
   :undoc-members:
   :show-inheritance:
```

---

## Model Output Validation

### utils_model_output_validation

Schema validation and quantile pivoting for model output data.

```{eval-rst}
.. automodule:: utils_model_output_validation
   :members:
   :undoc-members:
   :show-inheritance:
```
