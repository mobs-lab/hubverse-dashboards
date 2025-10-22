# Overall dashboard-building workflow flowchart

```mermaid.js
---
config:
  layout: dagre
---
flowchart TD
    Start(["User Runs build_dashboard.sh"]) --> LoadConfig["Load config.yaml"]
    LoadConfig --> ParseConfig{"Parse &amp; Validate<br>Configuration"}
    ParseConfig -- Invalid --> Error1["Display Errors"]
    Error1 --> End1(["Exit: Fix Config"])
    ParseConfig -- Valid --> GenSample["Generate Sample<br>CSV Structures"]
    GenSample --> ShowTarget["Show Target-Data<br>Sample Shape"]
    ShowTarget --> ShowModel["Show Model-Output<br>Sample Shape"]
    ShowModel --> ShowSummary["Show Configuration<br>Summary"]
    ShowSummary --> Confirm{"User Confirms<br>Proceed?"}
    Confirm -- No --> End2(["Exit: Review Data"])
    Confirm -- Yes --> LoadData["Load Raw CSV Files"]
    LoadData --> MapColumns["Map User Columns<br>to Internal Schema"]
    MapColumns --> ValidateData{"Data Matches<br>Config?"}
    ValidateData -- No --> Error2["Display Data Errors"]
    Error2 --> End3(["Exit: Fix Data"])
    ValidateData -- Yes --> ProcessForecastPeriods["Partition by<br>Forecast Periods"]
    ProcessForecastPeriods --> ProcessTargets["Process Each<br>Modelling Target"]
    ProcessTargets --> ProcessModels@{ label: "Process Each<br>Model's Predictions" }
    ProcessModels --> CalcEvals["Calculate<br>Evaluations"]
    CalcEvals --> Export["Export to<br>Frontend JSON"]
    Export --> End4(["Success: Dashboard Ready"])
    ProcessModels@{ shape: rect}
    style Start fill:#4CAF50,color:#fff
    style End1 fill:#f44336,color:#fff
    style Confirm fill:#FFC107
    style End2 fill:#FF9800,color:#fff
    style End3 fill:#f44336,color:#fff
    style End4 fill:#4CAF50,color:#fff
```

# Spatial Data Multi-Scenario Decision Matrix

| Scenario | disable_map | custom_shape_file example | custom_location_mapping example | Validation        | Result                                   | Use Case                                                          |
| -------- | ----------- | ----------------- | ----------------------- | ----------------- | ---------------------------------------- | ----------------------------------------------------------------- |
| **A**    | `False`     | `null`            | `null`                  | ✅ PASS           | Default US States map + FIPS codes       | Standard US state-level forecasting                               |
| **B**    | `False`     | `null`            | `"custom.csv"`       | ⚠️ PASS + WARNING | Default US shape + Custom location names | Custom display names for US states (must use FIPS codes in data)  |
| **C**    | `False`     | `"counties.json"` | `null`                  | ❌ ERROR          | Configuration error                      | Invalid: Custom shapes require custom mapping                     |
| **D**    | `False`     | `"counties.json"` | `"county_mapping.csv"`  | ✅ PASS           | Custom shape + Custom mapping            | County/district/custom spatial level forecasting                  |
| **E**    | `True`      | `null`            | `null`                  | ✅ PASS           | No map, default FIPS dropdown            | Dropdown-only interface with US states                            |
| **F**    | `True`      | `null`            | `"custom.csv"`          | ✅ PASS           | No map, custom locations dropdown        | Dropdown-only with custom locations (e.g., hospitals, facilities) |
| **G**    | `True`      | `"any.json"`      | `null`                  | ℹ️ INFO           | No map, default FIPS dropdown            | Shape file ignored (map disabled)                                 |
| **H**    | `True`      | `"any.json"`      | `"custom.csv"`          | ℹ️ INFO           | No map, custom locations dropdown        | Shape file ignored (map disabled)                                 |
