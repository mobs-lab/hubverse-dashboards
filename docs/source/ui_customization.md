# UI Customization

The dashboard UI is designed to be customizable through `config.yaml` without modifying the React frontend code. All UI options are nested under the `ui_customization` key.

---

## Header & Navigation

### Dashboard Title

```yaml
ui_customization:
  ui_header_title_name: "COVID-19Forecast"
```

The title appears in the dashboard header. If your title follows the convention `AbcdeForecast`, the dashboard will selectively bold "Abcde" and render "Forecast" in normal weight. Any other naming pattern will be rendered fully bold.

### Navigation Buttons

```yaml
ui_customization:
  ui_header_nav_btn:
    - button_text: "About"
      nav_to_external: true
      nav_to_link: "https://example.org/about"
    - button_text: "Forecast"
      nav_to_page: "Forecast"
    - button_text: "Evaluation"
      nav_to_page: "Evaluation"
```

Each button can either navigate to an internal dashboard page (`Forecast` or `Evaluation`) or to an external URL. The button order in the YAML is preserved in the rendered header.

```{note}
The Evaluation button is automatically hidden if the dashboard is built without evaluations (Options 2/4 or `--skip-evaluations`).
```

---

## Forecast Page Visual Reference

The annotated diagram below illustrates which parts of the Forecast page interface are controlled by `config.yaml` options.

```{image} _static/images/forecast_ui_config.png
:alt: Forecast page UI customization reference diagram
:class: bg-primary mb-1
:width: 800px
:align: center
```

### Chart Header

```yaml
ui_customization:
  ui_forecast_header_chart_name: "Weekly Hospital COVID-19 Admissions Forecast"
```

This text appears as the main chart title above the forecast visualization.

### Historical Data Toggle

```yaml
ui_customization:
  ui_forecast_header_hist_td_toggle_text: "Show COVID-19 Admissions at Time of Forecast"
```

Customizes the label text for the historical target data toggle button. This toggle is hidden when `disable_historical_target_data: true`.

### Location Info Display

```yaml
ui_customization:
  disable_location_info_display: false
```

When `true`, hides the location name display above the chart header.

### Info Buttons

Info buttons provide contextual help popups. Each accepts a `title` and `content` (supports HTML/Markdown):

```yaml
ui_customization:
  ui_forecast_header_infobutton_content:
    title: "About This Dashboard"
    content: "This dashboard visualizes hospital admission forecasts..."

  ui_forecast_settings_horizon_infobutton_content:
    title: "What Are Forecast Horizons?"
    content: "A horizon of 1 means predicting 1 time unit ahead..."
```

---

## Evaluation Page Visual Reference

The annotated diagram below illustrates which parts of the Evaluation Overview page interface are controlled by `config.yaml` options.

```{image} _static/images/evaluation_ui_config.png
:alt: Evaluation page UI customization reference diagram
:class: bg-primary mb-1
:width: 800px
:align: center
```

### Tab Names

The Evaluation page has two tabs that can be renamed:

```yaml
ui_customization:
  ui_evaluation_overview_tab_name: "Season Overview"          # Default
  ui_evaluation_single_model_tab_name: "Single-Model"         # Default
```

### Overview Tab

```yaml
ui_customization:
  ui_evaluation_chart_log_mode_indicator_text: "Use Log Scale"
  ui_evaluation_overview_location_map_title: "Location-Specific"
```

- **Log Scale Toggle Text**: Changes the label on the log-scale toggle for the IQR boxplot charts.
- **Location Map Title**: This string is prepended to the selected metric name in the map visualization header (e.g., "Location-Specific WIS/Baseline").

### Info Buttons (Evaluation)

```yaml
ui_customization:
  ui_evaluation_overview_infobutton_content:
    title: "Season Overview"
    content: "This page shows aggregated evaluation metrics..."

  ui_evaluation_overview_horizon_infobutton_content:
    title: "Forecast Horizons"
    content: "Select one or more horizons to filter evaluations..."

  ui_evaluation_single_model_infobutton_content:
    title: "Single Model Evaluations"
    content: "View detailed metrics for one model at a time..."

  ui_evaluation_single_model_horizon_infobutton_content:
    title: "Forecast Horizons"
    content: "Different from the Overview page's horizon selector..."
```

### Location Map Color Scale

Customize the gradient colors used for the geographic evaluation map:

```yaml
ui_customization:
  ui_evaluation_overview_location_map_color_scale:
    color_top: "#00495F"      # Worse performance (navy blue)
    color_base: "#E9E9E9"     # Baseline/neutral (light grey)
    color_bottom: "#6A9629"   # Better performance (green)
    color_null: "#363b43"     # No data (dark grey)
```

The color interpretation depends on the selected metric:
- **WIS/Baseline**: Lower values (closer to `color_bottom`) indicate better performance.
- **MAPE**: Lower values indicate better accuracy.
- **Coverage**: Values closer to the nominal level indicate better calibration.

---

## Full Pydantic Schema

For the complete, auto-generated schema of all UI customization options, see:

```{eval-rst}
.. autopydantic_model:: yaml_config_processor_pydantic.UICustomizationConfig
   :members:
   :exclude-members: model_config
```
