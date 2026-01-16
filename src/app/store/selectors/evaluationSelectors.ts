// src/store/selectors/evaluationSelectors.ts
// Selectors specific to the Evaluations page

import { createSelector } from "@reduxjs/toolkit";
import { RootState } from "../index";
import { BoxplotStats } from "@/types/domains/evaluations";

// Import shared selectors that are also used by evaluations
import { 
  selectTargets, 
  selectDateConstraintsForTarget,
  selectModelAvailabilityPerPeriod,
  selectModelNames,
} from "./sharedSelectors";

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
  // Access path: locationMapData[metric][model][location][horizon]
  const metricData = locationMapData?.[metric]?.[model];
  if (!metricData) {
    console.debug(`calculateMultiHorizonIQR: No data found for ${metric}/${model}`);
    return null;
  }
  
  // Get all locations
  const allLocations = Object.keys(metricData);
  if (allLocations.length === 0) {
    console.debug(`calculateMultiHorizonIQR: No locations found for ${metric}/${model}`);
    return null;
  }
  
  // Calculate average for each location across selected horizons
  const locationAverages: number[] = [];
  
  for (const location of allLocations) {
    let totalSum = 0;
    let totalCount = 0;
    
    for (const horizon of horizons) {
      const horizonKey = String(horizon);
      const horizonData = metricData[location]?.[horizonKey];
      if (horizonData && horizonData.count > 0) {
        totalSum += horizonData.sum;
        totalCount += horizonData.count;
      }
    }
    
    if (totalCount > 0) {
      locationAverages.push(totalSum / totalCount);
    }
  }
  
  if (locationAverages.length === 0) {
    console.debug(`calculateMultiHorizonIQR: No valid location averages for ${metric}/${model}, horizons: ${horizons.join(',')}`);
    return null;
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
    (state: RootState) => state.evaluationsSeasonOverviewSettings.selectedEvalOverviewTimePeriod,
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
    
    // Normalize selectedModels to ensure we have primitive strings
    const normalizedModels = selectedModels.map(m => String(m));
    
    // If single horizon is selected, use pre-calculated data directly
    if (horizons.length === 1) {
      const horizonKey = String(horizons[0]);
      // Cast iqrData to any to access dynamic metric keys
      const iqrDataAny = iqrData as Record<string, Record<string, Record<string, BoxplotStats>>>;
      
      return {
        type: 'precalculated' as const,
        data: {
          "WIS/Baseline": Object.fromEntries(
            normalizedModels.map(model => [model, iqrDataAny?.["WIS/Baseline"]?.[model]?.[horizonKey] || null])
          ),
          "MAPE": Object.fromEntries(
            normalizedModels.map(model => [model, iqrDataAny?.["MAPE"]?.[model]?.[horizonKey] || null])
          ),
        }
      };
    }
    
    // Multi-horizon: calculate IQR on-the-fly from location map aggregates
    const calculatedIQR: Record<string, Record<string, BoxplotStats | null>> = {
      "WIS/Baseline": {},
      "MAPE": {},
    };
    
    // Check if location map data is available
    const hasLocationMapData = Object.keys(locationMapData).length > 0;
    if (!hasLocationMapData) {
      console.debug("selectIQRDataForBoxplot: No location map data available for multi-horizon calculation");
    }
    
    for (const model of normalizedModels) {
      calculatedIQR["WIS/Baseline"][model] = calculateMultiHorizonIQR(
        locationMapData, 
        "WIS/Baseline", 
        model, 
        horizons
      );
      calculatedIQR["MAPE"][model] = calculateMultiHorizonIQR(
        locationMapData, 
        "MAPE", 
        model, 
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

/**
 * Get date constraints for the currently selected target in Season Overview
 */
export const selectSeasonOverviewDateConstraints = (state: RootState) => {
  const selectedTargetId = selectEvaluationSelectedTargetId(state);
  return selectDateConstraintsForTarget(selectedTargetId)(state);
};

/**
 * Get date constraints for the currently selected target in Single Model view
 */
export const selectSingleModelDateConstraints = (state: RootState) => {
  const selectedTargetId = selectSingleModelSelectedTargetId(state);
  return selectDateConstraintsForTarget(selectedTargetId)(state);
};

// ============================================
// Model Availability for Evaluation Pages
// ============================================

/**
 * Get model availability for Season Overview (uses selected period)
 * Uses pre-calculated availability lists from metadata for the specific period
 */
export const selectSeasonOverviewModelAvailability = createSelector(
  [
    selectModelNames,
    selectModelAvailabilityPerPeriod,
    (state: RootState) => state.evaluationsSeasonOverviewSettings.selectedEvalOverviewTimePeriod,
  ],
  (allModels, availabilityData, selectedPeriodId) => {
    /* console.debug('[selectSeasonOverviewModelAvailability] Checking for period:', {
      periodId: selectedPeriodId,
      totalModels: allModels.length,
    }); */

    // Safety check
    if (!allModels || allModels.length === 0) {
      console.warn('[selectSeasonOverviewModelAvailability] No models in config');
      return {
        sortedModels: [],
        availableModels: new Set<string>(),
        unavailableModels: new Set<string>(),
      };
    }

    // Get availability data for the specific period
    const periodData = availabilityData[selectedPeriodId];

    if (!periodData) {
      console.warn(`[selectSeasonOverviewModelAvailability] No availability data for period ${selectedPeriodId} - assuming all available`);
      return {
        sortedModels: allModels,
        availableModels: new Set(allModels),
        unavailableModels: new Set<string>(),
      };
    }

    const availableModelsList = periodData.availableModels || [];
    const unavailableModelsFromPeriod = periodData.unavailableModels || [];
    
    // Create sets for quick lookup
    const availableSet = new Set(availableModelsList);
    const unavailableSet = new Set(unavailableModelsFromPeriod);
    
    // Any model not in either list should be treated as unavailable
    const allUnavailable = allModels.filter(m => !availableSet.has(m));

    // Sort models: available first, then unavailable (maintaining original order)
    const sortedModels = [
      ...allModels.filter((m) => availableSet.has(m)),
      ...allModels.filter((m) => !availableSet.has(m)),
    ];

    /* console.debug('[selectSeasonOverviewModelAvailability] Result:', {
      availableModels: availableModelsList,
      unavailableModels: allUnavailable,
    }); */

    return {
      sortedModels,
      availableModels: availableSet,
      unavailableModels: new Set(allUnavailable),
    };
  }
);

/**
 * Get model availability for Single Model view (uses custom date range)
 * Data-driven approach - checks which models actually have data in the selected range
 * 
 * Logic: Examines actual model output data to determine availability, not period metadata
 */
export const selectSingleModelAvailability = createSelector(
  [
    selectModelNames,
    (state: RootState) => state.coreDataStore.modelOutput,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewSelectedStateCode,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewDateStart,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationSingleModelViewDateEnd,
  ],
  (allModels, modelOutput, locationCode, startDate, endDate) => {
    /* console.debug('[selectSingleModelAvailability] Checking availability (data-driven) for date range:', {
      start: startDate?.toISOString(),
      end: endDate?.toISOString(),
      location: locationCode,
      totalModels: allModels.length,
    }); */

    // Safety check
    if (!allModels || allModels.length === 0) {
      console.warn('[selectSingleModelAvailability] No models in config');
      return {
        sortedModels: [],
        availableModels: new Set<string>(),
        unavailableModels: new Set<string>(),
      };
    }

    if (!modelOutput) {
      console.warn('[selectSingleModelAvailability] No model output data loaded - assuming all models available');
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

    /* console.debug('[selectSingleModelAvailability] Result (data-driven):', {
      available: availableModelsList.length,
      unavailable: unavailableModelsList.length,
      availableModels: availableModelsList,
      unavailableModels: unavailableModelsList,
    }); */

    return {
      sortedModels,
      availableModels: new Set(availableModelsList),
      unavailableModels: new Set(unavailableModelsList),
    };
  }
);
