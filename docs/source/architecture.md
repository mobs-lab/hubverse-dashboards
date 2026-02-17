# Architecture

This document provides a comprehensive overview of the Hubverse Dashboard architecture, covering the full lifecycle from raw data to interactive visualization.

## System Overview

The dashboard operates in two distinct phases:

1. **Build Phase (Python)** -- Ingests, validates, transforms, and evaluates raw forecast data into optimized JSON files.
2. **Runtime Phase (React/Next.js)** -- Loads the static JSON files to render the interactive dashboard in the browser.

```{mermaid}
graph TD
    A["Raw Data<br/>(target-data, model-output, auxiliary-data)"] -->|ingest| B["Python Data Pipeline"]
    C["config.yaml"] -->|validate via Pydantic| B
    B -->|transform & evaluate| D["Optimized JSON Files<br/>(public/data/)"]
    D -->|fetch at load time| E["React/Next.js Frontend"]
    E -->|render| F["Interactive Dashboard UI"]
```

---

## Build Phase: Python Data Pipeline

The build phase is orchestrated by `build_dashboard.sh`, which presents an interactive menu and delegates to the Python pipeline. The pipeline supports two processing modes:

- **From-Scratch Mode**: Processes all data from source files. Used for initial builds or complete rebuilds.
- **Data-Update Mode**: Detects changes via file manifests and processes only modified data. Significantly faster for routine updates.

### Pipeline Stages

The diagram below shows the full processing pipeline with all major stages:

```{mermaid}
graph TD
    subgraph "Stage 1: Configuration"
        S1A["Load config.yaml"] --> S1B["Pydantic Validation<br/>(yaml_config_processor_pydantic.py)"]
        S1B --> S1C["Validated DashboardConfig Object"]
    end

    subgraph "Stage 2: Data Fetching"
        S2A["Check for Remote Data URL"] --> S2B{"Remote URL<br/>configured?"}
        S2B -->|Yes| S2C["Clone/Update Git Repo<br/>(data_fetcher.py)"]
        S2C --> S2D["Sync to Local Directories"]
        S2B -->|No| S2E["Use Local Data Directories"]
    end

    subgraph "Stage 3: Change Detection (Data-Update Only)"
        S3A["Load Previous Manifest<br/>(manifest_manager.py)"] --> S3B["Scan Current File Checksums"]
        S3B --> S3C["Compare Against Manifest"]
        S3C --> S3D["Identify Changed Files"]
    end

    subgraph "Stage 4: Data Processing"
        S4A["Load Target Data<br/>(CSV or Parquet)"] --> S4B["Column Renaming &<br/>Standardization"]
        S4B --> S4C["Extract Latest Ground Truth<br/>& Historical Snapshots"]
        S4D["Load Model Output<br/>(per-model directories)"] --> S4E["Schema Validation &<br/>Quantile Pivoting"]
        S4E --> S4F["Filter by Config Specs<br/>(targets, locations, horizons)"]
        S4C --> S4G["Fill Missing Time Intervals"]
    end

    subgraph "Stage 5: Evaluation"
        S5A["Calculate WIS<br/>(Weighted Interval Score)"] --> S5D["Aggregate by<br/>Forecast Period"]
        S5B["Calculate MAPE"] --> S5D
        S5C["Calculate Coverage"] --> S5D
        S5A2["Calculate WIS Ratio<br/>(vs. Baseline)"] --> S5D
        S5D --> S5E["Location Map Aggregates<br/>IQR Statistics<br/>Coverage Aggregates"]
    end

    subgraph "Stage 6: Output"
        S6A["Generate Metadata JSON"] --> S6D["Write to public/data/"]
        S6B["Structure Frontend JSON<br/>(targetData, modelOutput,<br/>historical-target-data)"] --> S6D
        S6C["Evaluation JSON<br/>(aggregates, rawScores)"] --> S6D
        S6D --> S6E["Save Intermediates &<br/>Update Manifest"]
    end

    S1C --> S2A
    S2D --> S3A
    S2E --> S3A
    S3D --> S4A
    S3D --> S4D
    S4F --> S5A
    S4F --> S5B
    S4F --> S5C
    S4F --> S5A2
    S4G --> S6B
    S5E --> S6C
```

### Stage 1: Configuration Validation

- **Script**: `scripts/yaml_config_processor_pydantic.py`
- **Key class**: {class}`~yaml_config_processor_pydantic.DashboardConfig`
- The user's `config.yaml` is loaded and validated using **Pydantic v2** models.
- Validation includes: type checking, value range constraints, cross-field validation (e.g., only one default period/target), and automatic model color assignment.
- A JSON Schema (`config_schema.json`) can be exported for external tooling support.
- See the [Configuration Reference](configuration.md) for the full schema documentation.

### Stage 2: Data Fetching

- **Script**: `scripts/data_fetcher.py`
- **Key class**: {class}`~data_fetcher.DataFetcher`
- If `link_to_hubverse_compatible_data` is configured, the system clones (or updates) the remote GitHub repository into a local cache (`.data_cache`).
- Only configured model directories are synced to reduce bandwidth and disk usage.
- In development mode, the cache is stored under `development-mode-root/.data_cache`.

### Stage 3: Change Detection

- **Script**: `scripts/manifest_manager.py`
- **Key class**: {class}`~manifest_manager.ManifestManager`
- Only active during **data-update runs** (Options 5/6 in the build menu).
- Maintains a JSON manifest (`intermediates/manifest.json`) with MD5 checksums for all source files.
- On each update run, scans current files and compares against the manifest to identify:
  - New files (added since last run)
  - Modified files (checksum changed)
  - Deleted files (no longer present)
- Changes are organized by domain: `target_data`, `model_output` (per-model), and `auxiliary_data`.

### Stage 4: Data Processing

- **Script**: `scripts/data_processor.py`
- **Key class**: {class}`~data_processor.DataProcessor`
- **Target data** is loaded from CSV or Parquet (including partitioned-by-`as_of` Parquet). Columns are renamed to standard names based on `target_data_header_mapping`. If historical data is enabled, multiple `as_of` snapshots are extracted.
- **Model output** is loaded per-model from subdirectories. Data is validated ({func}`~utils_model_output_validation.validate_model_output_schema`), then quantile rows are pivoted from long to wide format ({func}`~utils_model_output_validation.pivot_quantiles`).
- Data is filtered by configured targets, locations, and horizons.
- Missing time intervals are filled with placeholder observations (`-1`) so the frontend chart renders continuous date axes.

#### Key Concepts

- **Two model output representations**: The processor maintains both unpivoted (long-format, for evaluations) and pivoted (wide-format, for frontend JSON) versions of model output.
- **Intermediates**: Processed data is saved as Parquet files in `intermediates/` for reuse during data-update runs. This avoids reprocessing unchanged data.
- **Data Value Processing**: Per-target scaling factors and rounding can be applied to both target data and model output values.

### Stage 5: Evaluation Processing

- **Script**: `scripts/evaluation_processor.py`
- **Key class**: {class}`~evaluation_processor.EvaluationProcessor`
- Calculates three core evaluation metrics:

| Metric | Formula | Description |
|--------|---------|-------------|
| **WIS** | `1/(K+0.5) * (0.5*\|y - median\| + sum_k[alpha_k/2 * IS_k])` | Weighted Interval Score -- measures the accuracy and calibration of quantile forecasts |
| **MAPE** | `\|truth - median\| / \|truth\| * 100` | Mean Absolute Percentage Error -- measures relative prediction accuracy |
| **Coverage** | `1 if truth in [q_lower, q_upper], else 0` | Whether the truth falls within the prediction interval |

- **WIS Ratio** (`WIS / Baseline WIS`) measures each model's performance relative to a baseline model.
- Coverage is calculated for multiple configurable levels (e.g., 50%, 95%) and converted to percentages (0--100).

#### Aggregation Pipeline

After raw scores are computed, they are aggregated per forecast period by utility functions in `scripts/utils_evaluation_aggregation.py`:

1. **Location Map Aggregates** ({func}`~utils_evaluation_aggregation.process_location_map_aggregates`): Sum/count per location per horizon -- powers the geographic map visualization.
2. **IQR Statistics** ({func}`~utils_evaluation_aggregation.process_iqr_stats`): Percentile statistics (q05, q25, median, q75, q95) across location averages -- powers boxplot charts.
3. **Coverage Aggregates** ({func}`~utils_evaluation_aggregation.process_coverage_aggregates`): Coverage sum/count per model per horizon per level -- powers the coverage chart.

#### Incremental Evaluation Updates

During data-update runs, evaluations are recalculated only for affected prediction rows:

- **Trigger A**: New or modified model prediction files produce new prediction keys.
- **Trigger B**: Revised target data (new or changed observations) triggers re-evaluation of all existing predictions at those dates/locations.
- Rows from both triggers are deduplicated and evaluated together.
- Only forecast periods overlapping with the affected date range are re-aggregated.

### Stage 6: Output Generation

- **Script**: `scripts/data_processor.py` (methods `_write_output_files`, `_generate_metadata`)
- Output files are written to `public/data/` (production) or `public/test-data-output/` (dev mode).

#### Output File Structure

```text
public/data/
├── metadata.json                    # Dashboard configuration, locations, date ranges, model info
├── targetData.json                  # Ground truth data (location → date → target → observation)
├── modelOutputData.json             # Forecast data (model → location → ref_date → predictions)
├── historical-target-data.json      # Historical snapshots (as_of → date → location → data)
└── evaluations/
    ├── rawScores.json               # All raw scores (target → metric → model → loc → horizon → scores)
    ├── <period-id>/
    │   └── aggregates.json          # Period-specific aggregated stats (IQR, location map, coverage)
    └── <another-period-id>/
        └── aggregates.json
```

- **`metadata.json`** is loaded first by the frontend to initialize the Redux store.
- Heavy data files are loaded on-demand as the user navigates the dashboard.
- Intermediates (Parquet caches, manifest) are saved for future incremental runs.

---

## Runtime Phase: React/Next.js Frontend

The frontend is a **Next.js** (React) application using:

- **Redux Toolkit** for global state management
- **D3.js** for data visualization (line charts, maps, boxplots)
- **Tailwind CSS** for styling

### Data Loading Strategy

1. On initial page load, the `DataProvider` fetches `metadata.json`.
2. The Redux store is initialized with configuration options (locations, models, periods, targets).
3. Heavy data files (`targetData.json`, `modelOutputData.json`) are fetched as needed.
4. Evaluation data is loaded lazily when the user navigates to the Evaluations page.

### Key Frontend Pages

| Page | Description |
|------|-------------|
| **Forecast** | Interactive line chart with forecast predictions, ground truth, historical data toggle, and location selector |
| **Evaluation -- Overview** | Season overview with IQR boxplots, location-specific map visualization, and coverage chart |
| **Evaluation -- Single Model** | Detailed per-model evaluation with spatial and temporal filtering |

---

## Development Mode

Development mode provides an isolated environment for testing:

| Aspect | Production Mode | Development Mode |
|--------|-----------------|------------------|
| Data input | `target-data/`, `model-output/` | `development-mode-root/target-data/`, etc. |
| Data output | `public/data/` | `public/test-data-output/` |
| Intermediates | `intermediates/` | `development-mode-root/intermediates/` |
| Remote cache | `.data_cache/` | `development-mode-root/.data_cache/` |

---

## Script Reference

| Script | Purpose |
|--------|---------|
| `build_dashboard.sh` | Interactive entry point with menu-driven build options |
| `scripts/yaml_config_processor_pydantic.py` | Pydantic-based configuration validation |
| `scripts/dashboard_builder_workflow.py` | Top-level orchestrator connecting all pipeline stages |
| `scripts/data_fetcher.py` | Remote data cloning and syncing |
| `scripts/data_processor.py` | Core data processing, evaluation, and output generation |
| `scripts/evaluation_processor.py` | WIS, MAPE, Coverage metric calculations |
| `scripts/manifest_manager.py` | File change detection via checksums |
| `scripts/utils_change_detection.py` | Processed-data-level change identification |
| `scripts/utils_data_structuring.py` | DataFrame-to-nested-dict transformation for frontend JSON |
| `scripts/utils_data.py` | Date conversion, JSON encoding, type utilities |
| `scripts/utils_evaluation_aggregation.py` | IQR, location map, and coverage aggregation |
| `scripts/utils_forecast_period.py` | Dynamic/special forecast period date calculation |
| `scripts/utils_model_output_validation.py` | Model output schema validation and quantile pivoting |
| `dev-tools/generate_test_target_data.py` | Synthetic test data generator |
| `dev-tools/data_inspector.py` | CSV/Parquet file inspector for debugging |
