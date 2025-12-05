// src/store/selectors/evaluationSelectors.ts
// Selectors specific to the Evaluations page

import { createSelector } from "@reduxjs/toolkit";
import { RootState } from "../index";
import { BoxplotStats } from "@/types/domains/evaluations";

// Import shared selectors that are also used by evaluations
import { selectTargets } from "./sharedSelectors";

// ============================================
// Data Loading Status Selectors
// ============================================

/**
 * Check if any evaluation aggregates data has been loaded
 */
export const selectIsJsonDataLoaded = (state: RootState) => {
  return state.evaluationDataStore.loadedPeriods.length > 0;
};

/**
 * Check if data for a specific period has been loaded
 */
export const selectIsPeriodLoaded = (periodId: string) => (state: RootState) => {
  return state.evaluationDataStore.loadedPeriods.includes(periodId);
};

// ============================================
// Target Selection
// ============================================

/**
 * Get currently selected target for Season Overview evaluations
 * Falls back to config default, then first target, then 'default'
 */
export const selectEvaluationSelectedTargetId = createSelector(
  [
    (state: RootState) => state.evaluationsSeasonOverviewSettings.selectedTargetId,
    (state: RootState) => state.configStore.config?.defaultTargetId,
    selectTargets,
  ],
  (evalTargetId, configDefaultTargetId, targets) => {
    // Priority: evaluation settings > config default > first target > 'default'
    return evalTargetId || configDefaultTargetId || targets[0]?.targetId || 'default';
  }
);

/**
 * Get currently selected target for Single Model evaluations
 * Falls back to config default, then first target, then 'default'
 */
export const selectSingleModelSelectedTargetId = createSelector(
  [
    (state: RootState) => state.evaluationsSingleModelSettings.selectedTargetId,
    (state: RootState) => state.configStore.config?.defaultTargetId,
    selectTargets,
  ],
  (singleModelTargetId, configDefaultTargetId, targets) => {
    // Priority: single model settings > config default > first target > 'default'
    return singleModelTargetId || configDefaultTargetId || targets[0]?.targetId || 'default';
  }
);

// ============================================
// Boxplot IQR Calculation Helpers
// ============================================

/**
 * Calculate boxplot statistics from a list of values (location averages)
 */
function calculateBoxplotStats(values: number[]): BoxplotStats | null {
  if (!values || values.length === 0) return null;
  
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  
  const percentile = (p: number): number => {
    const index = (p / 100) * (n - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    if (lower === upper) return sorted[lower];
    return sorted[lower] * (upper - index) + sorted[upper] * (index - lower);
  };
  
  const sum = values.reduce((a, b) => a + b, 0);
  
  return {
    q05: percentile(5),
    q25: percentile(25),
    median: percentile(50),
    q75: percentile(75),
    q95: percentile(95),
    min: sorted[0],
    max: sorted[n - 1],
    mean: sum / n,
    count: n,
  };
}

/**
 * Calculate IQR for a given metric, model, and set of horizons from location map aggregates
 * This is used for multi-horizon selections where we need to compute on-the-fly
 */
function calculateMultiHorizonIQR(
  locationMapData: any,
  metric: string,
  model: string,
  horizons: number[]
): BoxplotStats | null {
  const metricData = locationMapData?.[metric]?.[model];
  if (!metricData) return null;
  
  // Get all locations
  const allLocations = Object.keys(metricData);
  
  // Calculate average for each location across selected horizons
  const locationAverages: number[] = [];
  
  for (const location of allLocations) {
    let totalSum = 0;
    let totalCount = 0;
    
    for (const horizon of horizons) {
      const horizonData = metricData[location]?.[String(horizon)];
      if (horizonData && horizonData.count > 0) {
        totalSum += horizonData.sum;
        totalCount += horizonData.count;
      }
    }
    
    if (totalCount > 0) {
      locationAverages.push(totalSum / totalCount);
    }
  }
  
  return calculateBoxplotStats(locationAverages);
}

// ============================================
// Season Overview Data Selectors
// ============================================

/**
 * Combined selector for Season Overview components
 * Provides all necessary data for Season Overview visualizations
 */
export const selectSeasonOverviewData = createSelector(
  [
    selectIsJsonDataLoaded,
    (state: RootState) => state.evaluationsSeasonOverviewSettings.selectedDynamicTimePeriod,
    (state: RootState) => state.evaluationsSeasonOverviewSettings.evalSOTimeRangeOptions,
    (state: RootState) => state.evaluationsSeasonOverviewSettings.evaluationSeasonOverviewHorizon,
    (state: RootState) => state.evaluationsSeasonOverviewSettings.evaluationSeasonOverviewSelectedModels,
    (state: RootState) => state.evaluationDataStore.precalculated,
    selectEvaluationSelectedTargetId,
  ],
  (isJsonLoaded, selectedPeriodName, evalSOTimeRangeOptions, horizons, selectedModels, precalculatedData, targetId) => {
    // Always return a valid structure, even if data is not loaded
    const defaultReturn = {
      seasonId: selectedPeriodName || "",
      selectedPeriod: null,
      horizons: horizons || [],
      selectedModels: selectedModels || [],
      targetId,
      iqrData: {},
      locationMapData: {},
      coverageData: {},
    };

    if (!isJsonLoaded) {
      return defaultReturn;
    }

    const selectedPeriod = evalSOTimeRangeOptions.find((p) => p.name === selectedPeriodName);
    if (!selectedPeriod) {
      console.debug("Selected period not found:", selectedPeriodName);
      return defaultReturn;
    }

    const seasonId = selectedPeriodName;

    return {
      seasonId,
      selectedPeriod,
      horizons,
      selectedModels,
      targetId,
      iqrData: precalculatedData?.iqr?.[seasonId]?.[targetId] || {},
      locationMapData: precalculatedData?.locationMap_aggregates?.[seasonId]?.[targetId] || {},
      coverageData: precalculatedData?.detailedCoverage_aggregates?.[seasonId]?.[targetId] || {},
    };
  }
);

/**
 * Selector for IQR data that handles both single-horizon (pre-calculated) 
 * and multi-horizon (calculated on-the-fly) cases
 */
export const selectIQRDataForBoxplot = createSelector(
  [
    selectSeasonOverviewData,
  ],
  (seasonOverviewData) => {
    const { iqrData, locationMapData, horizons, selectedModels } = seasonOverviewData;
    
    // If single horizon is selected, use pre-calculated data directly
    if (horizons.length === 1) {
      const horizonKey = String(horizons[0]);
      return {
        type: 'precalculated' as const,
        data: {
          "WIS/Baseline": Object.fromEntries(
            selectedModels.map(model => [model, iqrData?.["WIS/Baseline"]?.[model]?.[horizonKey] || null])
          ),
          "MAPE": Object.fromEntries(
            selectedModels.map(model => [model, iqrData?.["MAPE"]?.[model]?.[horizonKey] || null])
          ),
        }
      };
    }
    
    // Multi-horizon: calculate IQR on-the-fly from location map aggregates
    const calculatedIQR: Record<string, Record<string, BoxplotStats | null>> = {
      "WIS/Baseline": {},
      "MAPE": {},
    };
    
    for (const model of selectedModels) {
      calculatedIQR["WIS/Baseline"][model as string] = calculateMultiHorizonIQR(
        locationMapData, 
        "WIS/Baseline", 
        model as string, 
        horizons
      );
      calculatedIQR["MAPE"][model as string] = calculateMultiHorizonIQR(
        locationMapData, 
        "MAPE", 
        model as string, 
        horizons
      );
    }
    
    return {
      type: 'calculated' as const,
      data: calculatedIQR,
    };
  }
);

// ============================================
// Helper Selectors
// ============================================

/**
 * Helper selector for checking if we should use JSON or fall back to CSV
 */
export const selectShouldUseJsonData = createSelector([selectIsJsonDataLoaded], (isLoaded) => {
  return isLoaded;
});

/**
 * Selector for checking if season overview has valid data structure
 */
export const selectHasSeasonOverviewData = createSelector([selectSeasonOverviewData], (seasonOverviewData) => {
  return seasonOverviewData && 
    Object.keys(seasonOverviewData.iqrData).length > 0 && 
    seasonOverviewData.selectedModels.length > 0;
});

/**
 * Get available horizons from config
 */
export const selectAvailableHorizons = (state: RootState) => {
  return state.configStore.config?.horizons || [];
};

/**
 * Get available targets for evaluation UI
 */
export const selectAvailableTargets = createSelector(
  [selectTargets],
  (targets) => targets.map(t => ({
    targetId: t.targetId,
    displayString: t.displayString,
  }))
);

