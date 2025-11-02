// src/store/selector/forecastSelectors.ts

import { createSelector } from '@reduxjs/toolkit';
import { RootState } from '../index';
import { TargetData } from '@/types/domains/forecasting';

// ============================================
// Basic Selectors
// ============================================

export const selectConfig = (state: RootState) => state.configStore.config;

export const selectLocationMapping = (state: RootState) => state.auxiliaryDataStore.locationMapping;

export const selectForecastPeriodOptions = (state: RootState) =>
  state.auxiliaryDataStore.forecastPeriodOptions;

export const selectTargetData = (state: RootState) => state.coreDataStore.targetDataCollection;

export const selectModelOutput = (state: RootState) => state.coreDataStore.modelOutputCollection;

export const selectHistoricalTargetData = (state: RootState) => state.historicalTargetDataStore.data;

export const selectForecastSettings = (state: RootState) => state.forecastSettings;

// ============================================
// Config-Derived Selectors
// ============================================

export const selectEvaluationsEnabled = (state: RootState) =>
  state.configStore.config?.evaluationsEnabled ?? false;

export const selectModelNames = (state: RootState) =>
  state.configStore.config?.models.map((m) => m.modelName) ?? [];

export const selectModelColorMap = (state: RootState) => state.configStore.config?.modelColorMap ?? {};

export const selectHorizons = (state: RootState) => state.configStore.config?.horizons ?? [];

export const selectPredictionIntervalOptions = (state: RootState) =>
  state.configStore.config?.predictionIntervals ?? [];

export const selectTargets = (state: RootState) => state.configStore.config?.targets ?? [];

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
 * Get target data for a specific forecast period
 */
export const selectTargetDataForPeriod = (forecastPeriodId: string) =>
  createSelector([selectTargetData], (targetData): TargetData | undefined => {
    return targetData[forecastPeriodId];
  });

/**
 * Get target data for a specific location, period, and date range
 */
export const selectTargetDataFiltered = createSelector(
  [
    selectTargetData,
    (state: RootState) => state.forecastSettings.selectedForecastPeriod,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.selectedTargetIds,
    (state: RootState) => state.forecastSettings.timeFilterRangeStart,
    (state: RootState) => state.forecastSettings.timeFilterRangeEnd,
  ],
  (targetData, forecastPeriod, locationCode, targetIds, startDate, endDate) => {
    if (!forecastPeriod) return [];

    const periodData = targetData[forecastPeriod.forecastPeriodId];
    if (!periodData || !periodData[locationCode]) return [];

    const locationData = periodData[locationCode];

    // Filter by date range and targets
    const filtered: Array<{
      date: string;
      targetId: string;
      observation: number;
    }> = [];

    Object.entries(locationData).forEach(([dateStr, dateData]) => {
      const date = new Date(dateStr);
      if (date >= startDate && date <= endDate) {
        Object.entries(dateData).forEach(([targetId, targetData]) => {
          if (targetIds.includes(targetId)) {
            filtered.push({
              date: dateStr,
              targetId,
              observation: targetData.observation,
            });
          }
        });
      }
    });

    return filtered.sort((a, b) => a.date.localeCompare(b.date));
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
    (state: RootState) => state.forecastSettings.selectedForecastPeriod,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.selectedModels,
    (state: RootState) => state.forecastSettings.selectedTargetIds,
    (state: RootState) => state.forecastSettings.selectedHorizons,
    (state: RootState) => state.forecastSettings.timeFilterRangeStart,
    (state: RootState) => state.forecastSettings.timeFilterRangeEnd,
  ],
  (
    modelOutput,
    forecastPeriod,
    locationCode,
    selectedModels,
    selectedTargets,
    selectedHorizons,
    startDate,
    endDate
  ) => {
    if (!forecastPeriod) return {};

    const periodData = modelOutput[forecastPeriod.forecastPeriodId];
    if (!periodData) return {};

    const filtered: any = {};

    selectedModels.forEach((modelName) => {
      if (!periodData[modelName]) return;

      const modelData = periodData[modelName];
      if (!modelData[locationCode]) return;

      const locationData = modelData[locationCode];

      filtered[modelName] = {};

      Object.entries(locationData).forEach(([refDate, refDateData]) => {
        const date = new Date(refDate);
        if (date >= startDate && date <= endDate) {
          filtered[modelName][refDate] = {};

          selectedHorizons.forEach((horizon) => {
            if (refDateData[horizon]) {
              filtered[modelName][refDate][horizon] = {};

              selectedTargets.forEach((targetId) => {
                if (refDateData[horizon][targetId]) {
                  filtered[modelName][refDate][horizon][targetId] = refDateData[horizon][targetId];
                }
              });
            }
          });
        }
      });
    });

    return filtered;
  }
);

/**
 * Get predictions for a specific reference date (for chart interaction)
 */
export const selectPredictionsForReferenceDate = (referenceDate: Date) =>
  createSelector(
    [
      selectModelOutput,
      (state: RootState) => state.forecastSettings.selectedForecastPeriod,
      (state: RootState) => state.forecastSettings.selectedLocationCode,
      (state: RootState) => state.forecastSettings.selectedModels,
      (state: RootState) => state.forecastSettings.selectedTargetIds,
      selectHorizons,
      selectTimeUnit,
    ],
    (modelOutput, forecastPeriod, locationCode, models, targets, allHorizons, timeUnit) => {
      if (!forecastPeriod) return {};

      const periodData = modelOutput[forecastPeriod.forecastPeriodId];
      if (!periodData) return {};

      const refDateStr = referenceDate.toISOString().split('T')[0];
      const result: any = {};

      models.forEach((modelName) => {
        const modelData = periodData[modelName]?.[locationCode]?.[refDateStr];
        if (!modelData) return;

        result[modelName] = {};

        allHorizons.forEach((horizon) => {
          if (modelData[horizon]) {
            result[modelName][horizon] = {};

            targets.forEach((targetId) => {
              if (modelData[horizon][targetId]) {
                // Calculate target date from reference date + horizon
                const targetDate = new Date(referenceDate);
                targetDate.setDate(targetDate.getDate() + horizon * timeUnit);

                result[modelName][horizon][targetId] = {
                  ...modelData[horizon][targetId],
                  targetDate: targetDate.toISOString().split('T')[0],
                };
              }
            });
          }
        });
      });

      return result;
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
    return Object.keys(historicalData).sort();
  }
);

/**
 * Get historical target data for a specific as_of date
 */
export const selectHistoricalDataForAsOfDate = (asOfDate: string) =>
  createSelector(
    [
      selectHistoricalTargetData,
      (state: RootState) => state.forecastSettings.selectedLocationCode,
      (state: RootState) => state.forecastSettings.selectedTargetIds,
    ],
    (historicalData, locationCode, targetIds) => {
      const asOfData = historicalData[asOfDate];
      if (!asOfData || !asOfData[locationCode]) return [];

      const locationData = asOfData[locationCode];
      const result: Array<{
        date: string;
        targetId: string;
        observation: number;
      }> = [];

      Object.entries(locationData).forEach(([dateStr, dateData]) => {
        Object.entries(dateData).forEach(([targetId, targetData]) => {
          if (targetIds.includes(targetId)) {
            result.push({
              date: dateStr,
              targetId,
              observation: targetData.observation,
            });
          }
        });
      });

      return result.sort((a, b) => a.date.localeCompare(b.date));
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
    const locationCode = forecastSettings.selectedLocationCode;
    return locationMapping[locationCode]?.locationName || locationCode;
  }
);
