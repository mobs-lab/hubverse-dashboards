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

export const selectTargetData = (state: RootState) => state.coreDataStore.targetDataCollection;

export const selectModelOutput = (state: RootState) => state.coreDataStore.modelOutputCollection;

export const selectHistoricalTargetData = (state: RootState) =>
  state.historicalTargetDataStore.data;

export const selectForecastSettings = (state: RootState) => state.forecastSettings;

// ============================================
// Config-Derived Selectors
// ============================================

export const selectEvaluationsEnabled = (state: RootState) =>
  state.configStore.config?.evaluationsEnabled ?? false;

export const selectModelNames = (state: RootState) =>
  state.configStore.config?.models.map((m) => m.modelName) ?? [];

export const selectModelColorMap = (state: RootState) =>
  state.configStore.config?.modelColorMap ?? {};

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
    (state: RootState) => state.forecastSettings.selectedTargetId,
    (state: RootState) => state.forecastSettings.timeFilterRangeStart,
    (state: RootState) => state.forecastSettings.timeFilterRangeEnd,
  ],
  (targetData, forecastPeriod, locationCode, selectedTargetId, startDate, endDate) => {
    if (!forecastPeriod) {
      console.warn('[selectTargetDataFiltered] No forecastPeriod selected. Returning [].');
      return [];
    }

    const periodData = targetData[forecastPeriod.forecastPeriodId];
    if (!periodData) {
      console.warn(
        `[selectTargetDataFiltered] No data found for forecast period: ${forecastPeriod.forecastPeriodId}. Returning [].`
      );
      return [];
    }
    const periodDataByLocation = periodData[locationCode];

    if (!periodDataByLocation) {
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
    Object.entries(periodDataByLocation).forEach(([dateStr, dateData]) => {
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
    (state: RootState) => state.forecastSettings.selectedForecastPeriod,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.selectedModels,
    (state: RootState) => state.forecastSettings.selectedTargetId,
    (state: RootState) => state.forecastSettings.selectedHorizon,
    (state: RootState) => state.forecastSettings.userSelectedDate,
  ],
  (
    modelOutput,
    forecastPeriod,
    locationCode,
    selectedModels,
    selectedTargetId,
    selectedHorizon,
    userSelectedDate
  ) => {
    if (!forecastPeriod) {
      console.warn('[selectModelOutputFiltered] No forecastPeriod selected. Returning {}.');
      return {};
    }

    const periodData = modelOutput[forecastPeriod.forecastPeriodId];
    if (!periodData) {
      console.warn(
        `[selectModelOutputFiltered] No data for forecast period: ${forecastPeriod.forecastPeriodId}. Returning {}.`
      );
      return {};
    }

    const filteredOutput: any = {};
    const referenceDateStr = userSelectedDate.toISOString().split('T')[0];

    selectedModels.forEach((modelName) => {
      const modelData = periodData[modelName]?.[locationCode]?.[referenceDateStr];
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
 * Get historical target data for a specific as_of date, location, and targets
 * Backend structure: as_of -> date -> location -> {observation, target}
 */
export const selectHistoricalDataPoint = createSelector(
  [
    selectHistoricalTargetData,
    (state: RootState) => state.forecastSettings.selectedHistoricalAsOfDate,
    (state: RootState) => state.forecastSettings.selectedLocationCode,
    (state: RootState) => state.forecastSettings.selectedTargetId,
    (state: RootState, date: Date) => date, // Pass date as an argument to the selector
  ],
  (historicalData, asOfDate, locationCode, targetId, date) => {
    console.debug('[selectHistoricalDataPoint] Called with:', {
      hasHistoricalData: !!historicalData,
      asOfDate,
      locationCode,
      targetId,
      date: date?.toISOString(),
      historicalDataKeys: historicalData ? Object.keys(historicalData) : [],
    });

    if (!historicalData || !asOfDate) {
      console.debug('[selectHistoricalDataPoint] Missing data or asOfDate:', {
        hasHistoricalData: !!historicalData,
        asOfDate,
      });
      return null;
    }

    const dateStr = date.toISOString().split('T')[0];
    const asOfData = historicalData[asOfDate];
    
    console.debug('[selectHistoricalDataPoint] Looking for date:', {
      dateStr,
      hasAsOfData: !!asOfData,
      asOfDataKeys: asOfData ? Object.keys(asOfData).slice(0, 5) : [],
    });

    const dateData = asOfData?.[dateStr];
    
    console.debug('[selectHistoricalDataPoint] Date data:', {
      hasDateData: !!dateData,
      dateDataKeys: dateData ? Object.keys(dateData) : [],
    });

    const locationData = dateData?.[locationCode];

    console.debug('[selectHistoricalDataPoint] Location data:', {
      hasLocationData: !!locationData,
      locationData,
    });

    if (!locationData) {
      console.debug('[selectHistoricalDataPoint] No location data found');
      return null;
    }

    // IMPORTANT: Target field is required in historical data
    // Check if the target matches the currently selected target
    if (!locationData.target) {
      console.warn('[selectHistoricalDataPoint] Historical data missing target field!', {
        asOfDate,
        dateStr,
        locationCode,
        locationData,
      });
      return null;
    }

    if (locationData.target !== targetId) {
      console.debug('[selectHistoricalDataPoint] Target mismatch (filtering out):', {
        expectedTarget: targetId,
        actualTarget: locationData.target,
      });
      return null;
    }

    const observation = locationData.observation ?? null;
    console.debug('[selectHistoricalDataPoint] Returning observation:', observation);
    return observation;
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
