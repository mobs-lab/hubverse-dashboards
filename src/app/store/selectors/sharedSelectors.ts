// src/store/selectors/sharedSelectors.ts
// Shared selectors used by both Forecast and Evaluations pages

import { createSelector } from '@reduxjs/toolkit';
import { RootState } from '../index';

// ============================================
// Config Selectors (Shared)
// ============================================

export const selectConfig = (state: RootState) => state.configStore.config;

export const selectConfigLoaded = (state: RootState) => state.configStore.isLoaded;

// ============================================
// Model Selectors (Shared)
// ============================================

/**
 * Get list of model names from config
 */
export const selectModelNames = (state: RootState) =>
  Object.keys(state.configStore.config?.modelColorMap ?? {});

/**
 * Get model color mapping
 */
export const selectModelColorMap = (state: RootState) =>
  state.configStore.config?.modelColorMap ?? {};

// ============================================
// Location Selectors (Shared)
// ============================================

/**
 * Get location mapping from config
 */
export const selectLocationMapping = (state: RootState) =>
  state.configStore.config?.locationMapping ?? {};

/**
 * Get list of location objects for dropdowns/maps
 */
export const selectLocationList = createSelector(
  [selectLocationMapping],
  (locationMapping): Array<{ 
    locationCode: string; 
    locationName: string; 
    locationNameAlt?: string 
  }> => {
    return Object.entries(locationMapping).map(([code, data]) => ({
      locationCode: code,
      locationName: data.locationName,
      locationNameAlt: data.locationNameAlt,
    }));
  }
);

/**
 * Legacy selector for compatibility with old components
 * Returns location list in the format expected by older components
 */
export const selectLocationData = createSelector([selectLocationList], (locationList) => {
  if (!locationList || locationList.length === 0) {
    console.warn('[selectLocationData] No location data available');
    return [];
  }
  return locationList;
});

/**
 * Get location name from code
 */
export const selectLocationName = (locationCode: string) =>
  createSelector([selectLocationMapping], (locationMapping): string => {
    return locationMapping[locationCode]?.locationName || locationCode;
  });

// ============================================
// Target Selectors (Shared)
// ============================================

/**
 * Get all targets from config
 */
export const selectTargets = (state: RootState) => 
  state.configStore.config?.targets ?? [];

/**
 * Get default target ID
 */
export const selectDefaultTargetId = (state: RootState) =>
  state.configStore.config?.defaultTargetId || '';

// ============================================
// Temporal Selectors (Shared)
// ============================================

/**
 * Get time unit (in days) from config
 */
export const selectTimeUnit = (state: RootState) => 
  state.configStore.config?.timeUnit ?? 7;

/**
 * Get available horizons from config
 */
export const selectHorizons = (state: RootState) => 
  state.configStore.config?.horizons ?? [];

/**
 * Get date constraints from config
 */
export const selectDateConstraints = createSelector([selectConfig], (config) => {
  return {
    earliestDate: config?.earliestDate ? new Date(config.earliestDate) : new Date(),
    latestDate: config?.latestDate ? new Date(config.latestDate) : new Date(),
    defaultSelectedDate: config?.defaultSelectedDate ? new Date(config.defaultSelectedDate) : new Date(),
  };
});

// ============================================
// Prediction Interval Selectors (Shared)
// ============================================

/**
 * Get available prediction intervals from config
 */
export const selectPredictionIntervalOptions = (state: RootState) =>
  state.configStore.config?.predictionIntervals ?? [];

/**
 * Get default prediction intervals
 */
export const selectDefaultPredictionIntervals = (state: RootState) =>
  state.configStore.config?.defaultPredictionIntervals ?? [];

// ============================================
// Feature Flag Selectors (Shared)
// ============================================

/**
 * Check if evaluations are enabled
 */
export const selectEvaluationsEnabled = (state: RootState) =>
  state.configStore.config?.evaluationsEnabled ?? false;

/**
 * Check if historical target data is enabled
 */
export const selectHistoricalTargetDataEnabled = (state: RootState) =>
  state.configStore.config?.historicalTargetDataEnabled ?? false;

/**
 * Check if map is disabled
 */
export const selectIsMapDisabled = (state: RootState) =>
  state.configStore.config?.disableMapInDashboard ?? false;

/**
 * Check if single location mode
 */
export const selectIsSingleLocation = (state: RootState) =>
  state.configStore.config?.isSingleLocation ?? false;

// ============================================
// Map Data Selectors (Shared)
// ============================================

/**
 * Get map shape data (TopoJSON/GeoJSON)
 */
export const selectMapData = (state: RootState) => 
  state.auxiliaryDataStore.mapData;

// ============================================
// Forecast Period Selectors (Shared)
// ============================================

/**
 * Get forecast period options
 */
export const selectForecastPeriodOptions = (state: RootState) =>
  state.auxiliaryDataStore.forecastPeriodOptions;

