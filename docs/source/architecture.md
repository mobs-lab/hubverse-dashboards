# Architecture

This document explains the data flow from raw CSVs to the React frontend.

## High-Level Overview

The dashboard operates in two distinct phases:

1.  **Build Phase (Python)**: Ingests, validates, and transforms raw data into optimized JSON.
2.  **Runtime Phase (React)**: Loads the static JSON files to render the interactive dashboard.

```{mermaid}
graph TD
    A[Raw Data] -->|ingest| B(Python Data Processor)
    C[config.yaml] -->|validate| B
    B -->|transform| D[Optimized JSON]
    D -->|load| E[React Frontend]
    E -->|render| F[User Interface]
```

## Data Flow Details

### 1. Configuration Validation
*   **Script**: `scripts/yaml_config_processor_pydantic.py`
*   Uses **Pydantic** to strictly validate the user's `config.yaml`.
*   Ensures dates are valid, horizons are consistent, and files exist.

### 2. Data Processing
*   **Script**: `scripts/data_processor.py`
*   Reads `target-data` and `model-output`.
*   Standardizes column names based on mapping.
*   Calculates metadata (detected locations, available dates).
*   **Output**: Generates `metadata.json`, `targetData.json`, and `modelOutputData.json` in the `public/data/` folder.

### 3. Frontend Hydration
*   **Component**: `src/app/providers/DataProvider.tsx`
*   On load, fetches `metadata.json` first.
*   Initializes the Redux store with configuration options.
*   Fetches the heavy data files (`targetData.json`) only when needed.

