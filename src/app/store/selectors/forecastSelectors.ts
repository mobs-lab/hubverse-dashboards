// src/store/selector/forecastSelectors.ts
// Selectors specific to the Forecast page

import { createSelector } from '@reduxjs/toolkit';
import { RootState } from '../index';
import { 
  selectLocationMapping, 
  selectTargets, 
  selectDateConstraintsForTarget,
  selectModelNames,
} from './sharedSelectors';
// ============================================
// Forecast-Specific Data Selectors
// ============================================

export const selectTargetData = (state: RootState) => state.coreDataStore.targetData;

export const selectModelOutput = (state: RootState) => state.coreDataStore.modelOutput;

export const selectHistoricalTargetData = (state: RootState) =>
  state.historicalTargetDataStore.data;

export const selectForecastSettings = (state: RootState) => state.forecastSettings;

// ============================================
// Forecast-Specific Computed Selectors
// ============================================

/**
 * Get the currently selected target from forecast settings
 */
export const selectCurrentTarget = createSelector(
  [selectTargets, (state: RootState) => state.forecastSettings.selectedTargetId],
  (targets, selectedTargetId) => {
    return targets.find((t) => t.targetId == selectedTargetId);
  }
);

/**
 * Get data value processing config for current target
 */
export const selectCurrentTargetDataProcessing = createSelector([selectCurrentTarget], (target) => {
  return target?.dataValueProcessing ?? null;
});

/**
 * Get date constraints for the currently selected target in forecast page
 */
export const selectForecastDateConstraints = (state: RootState) => {
  const selectedTargetId = state.forecastSettings.selectedTargetId;
  return selectDateConstraintsForTarget(selectedTargetId)(state);
};

// ============================================
// Target Data Selectors
// ============================================

/**
 * Get target data for a specific location, period, and date range
 */
export const selectTargetDataFiltered = createSelector(
  [
    selectTargetData,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.selectedTargetId,
    (state: RootState) => state.forecastSettings.timeFilterRangeStart,
    (state: RootState) => state.forecastSettings.timeFilterRangeEnd,
  ],
  (targetData, locationCode, selectedTargetId, startDate, endDate) => {
    if (!targetData) {
      console.warn('[selectTargetDataFiltered] No target data available. Returning [].');
      return [];
    }

    // Logic remains valid because targetData is now Map<Location, ...> directly
    const locationData = targetData[locationCode];

    if (!locationData) {
      console.warn(
        `[selectTargetDataFiltered] No data found for locationCode: ${locationCode}. Returning [].`
      );
      return [];
    }
    // Filter by date range and targets
    const filtered: Array<{
      date: Date;
      targetId: string;
      observation: number | null;
    }> = [];
    Object.entries(locationData).forEach(([dateStr, dateData]) => {
      const safeDate = new Date(dateStr + 'T00:00:00Z'); // Treat date string as UTC
      const isDateInRange = safeDate >= startDate && safeDate <= endDate;
      if (isDateInRange) {
        Object.entries(dateData).forEach(([targetId, targetValue]) => {
          const isTargetIncluded = targetId === selectedTargetId;

          if (isTargetIncluded) {
            filtered.push({
              date: safeDate,
              targetId,
              observation: targetValue.observation,
            });
          }
        });
      }
    });
    return filtered.sort((a, b) => a.date.getTime() - b.date.getTime());
  }
);

// ============================================
// Model Output Selectors
// ============================================

/**
 * Get model output for selected settings
 */
export const selectModelOutputFiltered = createSelector(
  [
    selectModelOutput,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.selectedModels,
    (state: RootState) => state.forecastSettings.selectedTargetId,
    (state: RootState) => state.forecastSettings.selectedHorizon,
    (state: RootState) => state.forecastSettings.userSelectedDate,
  ],
  (
    modelOutput,
    locationCode,
    selectedModels,
    selectedTargetId,
    selectedHorizon,
    userSelectedDate
  ) => {
    if (!modelOutput) {
      console.warn('[selectModelOutputFiltered] No model output data available. Returning {}.');
      return {};
    }

    const filteredOutput: any = {};
    const referenceDateStr = userSelectedDate.toISOString().split('T')[0];

    selectedModels.forEach((modelName) => {
      const modelData = modelOutput[modelName]?.[locationCode]?.[referenceDateStr];
      if (modelData?.predictions) {
        const predictionsForModel: any = {};
        Object.entries(modelData.predictions).forEach(([targetDate, prediction]) => {
          // Filter by horizon
          const isHorizonIncluded =
            prediction.horizon !== null && prediction.horizon <= selectedHorizon;
          // Filter by target
          const isTargetIncluded = prediction.targetId === selectedTargetId;

          if (isHorizonIncluded && isTargetIncluded) {
            predictionsForModel[targetDate] = prediction;
          }
        });

        if (Object.keys(predictionsForModel).length > 0) {
          filteredOutput[modelName] = predictionsForModel;
        }
      }
    });

    return filteredOutput;
  }
);

// ============================================
// Historical Target Data Selectors
// ============================================

/**
 * Get available as_of dates for historical data
 */
export const selectHistoricalAsOfDates = createSelector(
  [selectHistoricalTargetData],
  (historicalData): string[] => {
    if (!historicalData) return [];
    return Object.keys(historicalData).sort((a, b) => b.localeCompare(a));
  }
);

/**
 * Get entire historical time series for the currently selected context
 * Returns: Array<{ date: Date; observation: number | null }>
 */
export const selectHistoricalTimeSeries = createSelector(
  [
    selectHistoricalTargetData,
    (state: RootState) => state.forecastSettings.userSelectedDate,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.selectedTargetId,
    (state: RootState) => state.forecastSettings.timeFilterRangeStart,
    (state: RootState) => state.forecastSettings.timeFilterRangeEnd,
  ],
  (historicalData, asOfDate, locationCode, targetId, startDate, endDate) => {
    if (!historicalData || !asOfDate) return [];

    const asOfDateISO = asOfDate.toISOString().split('T')[0];
    const asOfData = historicalData[asOfDateISO];

    if (!asOfData) return [];

    const series: Array<{ date: Date; observation: number | null }> = [];

    // Iterate through all dates in the snapshot
    Object.entries(asOfData).forEach(([dateStr, dateData]) => {
      const date = new Date(dateStr + 'T00:00:00Z'); // UTC

      // Filter by time range
      if (date >= startDate && date <= endDate) {
        const locationData = dateData[locationCode];

        if (locationData && locationData[targetId]) {
          series.push({
            date,
            observation: locationData[targetId].observation ?? null,
          });
        }
      }
    });
    const result = series.sort((a, b) => a.date.getTime() - b.date.getTime());
    console.debug('selectHistoricalTimeSeries', result);
    return result;
  }
);

/**
 * Get selected location name from selected location code
 */
export const selectSelectedLocationName = createSelector(
  [selectForecastSettings, selectLocationMapping],
  (forecastSettings, locationMapping): string => {
    let locationCode = forecastSettings.selectedLocationCode;

    // Safety: If locationCode is somehow an object (like {"US": "US"}), extract the key
    if (typeof locationCode === 'object' && locationCode !== null) {
      console.warn(
        '[selectSelectedLocationName] Location code is an object, extracting key:',
        locationCode
      );
      locationCode = Object.keys(locationCode)[0];
    }

    // Ensure it's a string
    locationCode = String(locationCode || 'US');

    return locationMapping[locationCode]?.locationName || locationCode;
  }
);

// ============================================
// Model Availability for Forecast Page
// ============================================

/**
 * Get model availability info for the current forecast date range
 * Returns models sorted with available first, unavailable last
 * 
 * Logic: Data-driven approach - checks which models actually have data points 
 * within the selected date range by examining the actual model output data
 */
export const selectForecastModelAvailability = createSelector(
  [
    selectModelNames,
    selectModelOutput,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.timeFilterRangeStart,
    (state: RootState) => state.forecastSettings.timeFilterRangeEnd,
  ],
  (allModels, modelOutput, locationCode, startDate, endDate) => {
    console.debug('[selectForecastModelAvailability] Checking availability (data-driven) for date range:', {
      start: startDate?.toISOString(),
      end: endDate?.toISOString(),
      location: locationCode,
      totalModels: allModels.length,
    });

    // Safety check
    if (!allModels || allModels.length === 0) {
      console.warn('[selectForecastModelAvailability] No models in config');
      return {
        sortedModels: [],
        availableModels: new Set<string>(),
        unavailableModels: new Set<string>(),
      };
    }

    if (!modelOutput) {
      console.warn('[selectForecastModelAvailability] No model output data loaded - assuming all models available');
      return {
        sortedModels: allModels,
        availableModels: new Set(allModels),
        unavailableModels: new Set<string>(),
      };
    }

    // Check each model to see if it has ANY data in the selected date range
    const modelsWithData = new Set<string>();

    for (const modelName of allModels) {
      const modelData = modelOutput[modelName]?.[locationCode];
      
      if (!modelData) {
        continue; // No data for this model at this location
      }

      // Check if any reference dates in the model data fall within our range OR
      // if any target dates (predictions) fall within our range
      let hasDataInRange = false;
      
      for (const [refDateStr, refData] of Object.entries(modelData)) {
        const refDate = new Date(refDateStr);
        
        // Check if reference date is in range
        if (refDate >= startDate && refDate <= endDate) {
          hasDataInRange = true;
          break;
        }
        
        // Check if any predictions have target dates in range
        if (refData.predictions) {
          for (const targetDateStr of Object.keys(refData.predictions)) {
            const targetDate = new Date(targetDateStr);
            if (targetDate >= startDate && targetDate <= endDate) {
              hasDataInRange = true;
              break;
            }
          }
        }
        
        if (hasDataInRange) break;
      }
      
      if (hasDataInRange) {
        modelsWithData.add(modelName);
      }
    }

    const availableModelsList = Array.from(modelsWithData);
    const unavailableModelsList = allModels.filter((m) => !modelsWithData.has(m));

    // Sort models: available first (maintaining original order), then unavailable
    const sortedModels = [
      ...allModels.filter((m) => modelsWithData.has(m)),
      ...allModels.filter((m) => !modelsWithData.has(m)),
    ];

    console.debug('[selectForecastModelAvailability] Result (data-driven):', {
      available: availableModelsList.length,
      unavailable: unavailableModelsList.length,
      availableModels: availableModelsList,
      unavailableModels: unavailableModelsList,
    });

    return {
      sortedModels,
      availableModels: new Set(availableModelsList),
      unavailableModels: new Set(unavailableModelsList),
    };
  }
);
