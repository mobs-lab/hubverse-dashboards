# Hubverse Dashboard Documentation

Welcome to the **Hubverse Dashboard** documentation.

This project is a template for quickly spinning up a [Hubverse](https://hubverse.io)-compatible dashboard that visualizes forecast predictions and evaluation metrics. By customizing a single YAML configuration file, you can set up the dashboard to work with your data without modifying the frontend code.

## Key Features

- **Configurable via YAML** -- All data source, display, and evaluation settings are controlled through `config.yaml`.
- **Pydantic Validation** -- Configuration is validated at build time with detailed, actionable error messages.
- **Incremental Updates** -- After an initial build, only changed data is reprocessed via file-level change detection.
- **Evaluation Metrics** -- Automatic calculation of WIS, WIS Ratio, MAPE, and Coverage with geographic and temporal aggregations.
- **Customizable UI** -- Dashboard branding, navigation, chart titles, and info buttons are all configurable.

## Table of Contents

```{toctree}
:maxdepth: 2
:caption: User Guide

getting_started
configuration
config_schema
data_preparation
ui_customization
```

```{toctree}
:maxdepth: 2
:caption: Developer Reference

architecture
api_reference
```

## Quick Links

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
