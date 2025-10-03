# dashboard_builder_workflow.py
# This script controls the main workflow of:
# 1. Read in `config.yaml`'s various settings
#   a. Check whether any required settings are missing, and return error for main Shell Script to return
#   b. Check for special processing mode (single-location or single-target)
#   c. Check for user-chosen standard (such as the model-output data naming standard)
# 2. Structure above settings
#   a. Reconstruct a sample target-data and model-output csv header + some row value, for user to confirm before proceeding
#   b. Exit and let user adjust settings if user wishes
# 3. Process target-data data
#   a. First check for initial processing vs. data-update
#   b. Use targets, forecast_periods, locations, target-data column naming and time_unit to process target-data
# 4. Process model-output data
#   a. First check for initial processing vs. data-update
#   b. use targets, forecast_periods, locations, model-output column naming, time_unit, PI intervals, horizons, etc. to process model-output data
# 5. Evaluations processing on target-data & model-output.
#   a. Check whether user is processing for the first time, or updating
#   b. Use target-data and model-output to process evaluations accordingly
# 6. Return and prompt user to check
