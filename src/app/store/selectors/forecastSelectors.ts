// src/store/selector/forecastSelectors.ts

import { createSelector } from '@reduxjs/toolkit';
import { RootState } from '../index';
import { TargetData } from '@/types/domains/forecasting';

// ============================================
// Basic Selectors
// ============================================

export const selectConfig = (state: RootState) => state.configStore.config;

export const selectLocationMapping = (state: RootState) =>
  state.configStore.config?.locationMapping ?? {};

export const selectForecastPeriodOptions = (state: RootState) =>
  state.auxiliaryDataStore.forecastPeriodOptions;

// [UPDATED] Selects the root monolithic target data
export const selectTargetData = (state: RootState) => state.coreDataStore.targetData;

// [UPDATED] Selects the root monolithic model output
export const selectModelOutput = (state: RootState) => state.coreDataStore.modelOutput;

export const selectHistoricalTargetData = (state: RootState) =>
  state.historicalTargetDataStore.data;

export const selectForecastSettings = (state: RootState) => state.forecastSettings;

// ============================================
// Config-Derived Selectors
// ============================================

export const selectEvaluationsEnabled = (state: RootState) =>
  state.configStore.config?.evaluationsEnabled ?? false;

export const selectModelNames = (state: RootState) =>
  Object.keys(state.configStore.config?.modelColorMap ?? {});

export const selectModelColorMap = (state: RootState) =>
  state.configStore.config?.modelColorMap ?? {};

export const selectHorizons = (state: RootState) => state.configStore.config?.horizons ?? [];

export const selectPredictionIntervalOptions = (state: RootState) =>
  state.configStore.config?.predictionIntervals ?? [];

export const selectTargets = (state: RootState) => state.configStore.config?.targets ?? [];

export const selectCurrentTarget = createSelector(
  [selectTargets, (state: RootState) => state.forecastSettings.selectedTargetId],
  (targets, selectedTargetId) => {
    return targets.find((t) => t.targetId == selectedTargetId);
  }
);

export const selectCurrentTargetDataProcessing = createSelector([selectCurrentTarget], (target) => {
  return target?.dataValueProcessing ?? null;
});

export const selectTimeUnit = (state: RootState) => state.configStore.config?.timeUnit ?? 7;

// ============================================
// Location Selectors
// ============================================

// Get list of location objects for dropdowns/maps
export const selectLocationList = createSelector(
  [selectLocationMapping],
  (locationMapping): Array<{ code: string; name: string; nameAlt?: string }> => {
    return Object.entries(locationMapping).map(([code, data]) => ({
      code,
      name: data.locationName,
      nameAlt: data.locationNameAlt,
    }));
  }
);

// Get location name from code
export const selectLocationName = (locationCode: string) =>
  createSelector([selectLocationMapping], (locationMapping): string => {
    return locationMapping[locationCode]?.locationName || locationCode;
  });

// ============================================
// Target Data Selectors
// ============================================

/**
 * [UPDATED] Smart Lookup: Get target data filtered by a specific forecast period's time range.
 * This maintains backward compatibility with components that expect data "for a period"
 * while using the new monolithic data structure.
 */
export const selectTargetDataForPeriod = (forecastPeriodId: string) =>
  createSelector(
    [selectTargetData, selectForecastPeriodOptions],
    (targetData, periodOptions): TargetData | undefined => {
      const period = periodOptions[forecastPeriodId];
      if (!period || !targetData) return undefined;

      // Create a subset of targetData that only falls within period.startDate and period.endDate
      const subset: TargetData = {};

      Object.entries(targetData).forEach(([location, dateMap]) => {
        const filteredDateMap: any = {};
        let hasData = false;

        Object.entries(dateMap).forEach(([dateStr, val]) => {
          // Simple string comparison for ISO dates often works, but Date object is safer
          const d = new Date(dateStr);
          if (d >= period.startDate && d <= period.endDate) {
            filteredDateMap[dateStr] = val;
            hasData = true;
          }
        });

        if (hasData) {
          subset[location] = filteredDateMap;
        }
      });

      return subset;
    }
  );

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

// ============================================
// Date Constraint Selectors
// ============================================

/**
 * Get the earliest and latest dates available in the data
 */
export const selectDateConstraints = createSelector([selectConfig], (config) => {
  return {
    earliestDate: config?.earliestDate ? new Date(config.earliestDate) : new Date(),
    latestDate: config?.latestDate ? new Date(config.latestDate) : new Date(),
  };
});

// ============================================
// Map Data Selectors
// ============================================

/**
 * Get map shape data (TopoJSON/GeoJSON)
 */
export const selectMapData = (state: RootState) => state.auxiliaryDataStore.mapData;

// ============================================
// Legacy Compatibility Selectors
// ============================================

/**
 * Legacy selector for location data array (for compatibility with old components)
 * Returns location list derived from locationMapping
 */
export const selectLocationData = createSelector([selectLocationList], (locationList) => {
  if (!locationList || locationList.length === 0) {
    console.warn('Warning: selectLocationData: No location data available');
    return [];
  }
  return locationList;
});

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
