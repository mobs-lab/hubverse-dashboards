# UI Customization

The dashboard UI is also designed to be customized, meaning you can customize the branding, titles, and navigation to fit your needs without touching the React frontend code.

## Visual Overview

The diagram below illustrates which parts of the interface are controlled by `config.yaml`.

```{image} _static/images/forecast_ui_config.png
:alt: UI Customization Diagram
:class: bg-primary mb-1
:width: 800px
:align: center
```

## Customizable Elements

### 1. Header & Navigation

Located in `ui_customization.header`:

- **Title**: Sets the main dashboard title (e.g., "COVID-19 Forecast").
- **Navigation Buttons**: You can add links to external sites (like your "About" page) or change the text used to indicate link to Forecast/Evaluation dashboard pages.

### 2. Chart Titles

Located in `ui_customization.forecastPage`:

- **Main Chart Title**: customize the text above the primary forecast visualization.
- **Toggles**: Rename the text for the "Show Historical Data" toggle to match your specific context.

### 3. Chart Elements

Located in `ui_customization.forecastPage`:


### 3. Info Buttons

You can provide context to your users by customizing the "Info" (i) buttons. These support Markdown text, allowing you to include links or formatted explanations about your methodology.
