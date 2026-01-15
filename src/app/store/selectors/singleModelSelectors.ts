// src/store/selectors/singleModelSelectors.ts
// Selectors specific to Single Model evaluation view

import { createSelector } from "@reduxjs/toolkit";
import { RootState } from "../index";
import { selectSingleModelSelectedTargetId } from "./evaluationSelectors";

// ============================================
// Core Data Status
// ============================================

/** Check if core data (target + model output) is loaded */
export const selectIsCoreDataLoaded = (state: RootState) => state.coreDataStore.isLoaded;



// ============================================
// Single Model Time Series Selectors
// ============================================

/**
 * Main selector for Single Model time series visualization data
 * Combines predictions and ground truth for a specific model/location/season/horizon
 */
export const selectSingleModelTimeSeriesData = createSelector(
  [
    (state: RootState) => state.coreDataStore.isLoaded,
    (state: RootState) => state.coreDataStore.modelOutput,
    (state: RootState) => state.coreDataStore.targetData,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewModel,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewSelectedStateCode,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationSingleModelViewHorizon,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewDateStart,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationSingleModelViewDateEnd,
    (state: RootState) => state.evaluationsSingleModelSettings.selectedTargetId,
  ],
  (isLoaded, modelOutput, targetData, modelName, locationCode, horizon, dateStart, dateEnd, targetId) => {
    if (!isLoaded || !modelOutput || !targetData || !modelName) {
      return {
        data: [],
        metadata: {
          displayStartDate: new Date(),
          displayEndDate: new Date(),
        },
      };
    }

    // Get model data for the selected location
    const modelData = modelOutput[modelName]?.[locationCode];
    if (!modelData) {
      console.warn("No model data for:", modelName, locationCode);
      return {
        data: [],
        metadata: {
          displayStartDate: dateStart,
          displayEndDate: dateEnd,
        },
      };
    }

    // Get target data for the selected location
    const locationTargetData = targetData[locationCode];
    if (!locationTargetData) {
      console.warn("No target data for location:", locationCode);
      return {
        data: [],
        metadata: {
          displayStartDate: dateStart,
          displayEndDate: dateEnd,
        },
      };
    }

    // Combine predictions and ground truth
    const combinedData: any[] = [];
    const predictionsByTargetDate = new Map<string, any>();

    // Collect all predictions for the selected horizon within date range
    Object.entries(modelData).forEach(([refDateStr, refData]: [string, any]) => {
      const refDate = new Date(refDateStr);
      // Removed refDate filter here, as we filter by targetDate now

      if (refData.predictions) {
        Object.entries(refData.predictions).forEach(([targetDateStr, pred]: [string, any]) => {
          const targetDate = new Date(targetDateStr);
          // Filter by target date being in the selected range
          if (targetDate >= dateStart && targetDate <= dateEnd) {
             if (pred.horizon === horizon && pred.targetId === targetId) {
                predictionsByTargetDate.set(targetDateStr, {
                  referenceDate: refDate,
                  targetDate: targetDate,
                  prediction: pred,
                });
             }
          }
        });
      }
    });

    // Add ground truth data within date range
    Object.entries(locationTargetData).forEach(([dateStr, dateData]: [string, any]) => {
      const date = new Date(dateStr);
      if (date < dateStart || date > dateEnd){
        // console.debug("No target data for date:", date);
        return;
      }

      const targetInfo = dateData[targetId];
      if (targetInfo && targetInfo.observation !== null && targetInfo.observation >= 0) {
        combinedData.push({
          referenceDate: date, // Keep referenceDate for backward compatibility if needed, but for GT it's target date
          targetDate: date,    // Explicit target date property
          groundTruth: {
            admissions: targetInfo.observation,
          },
          prediction: null,
        });
      }
    });

    // Add predictions
    predictionsByTargetDate.forEach((predInfo) => {
      combinedData.push({
        referenceDate: predInfo.referenceDate,
        targetDate: predInfo.targetDate, // Explicit target date property
        groundTruth: null,
        prediction: {
          targetDate: predInfo.targetDate,
          horizon: predInfo.prediction.horizon,
          median: predInfo.prediction.value_median,
          q05: predInfo.prediction.prediction_intervals?.['90']?.pi_value_low,
          q25: predInfo.prediction.prediction_intervals?.['50']?.pi_value_low,
          q75: predInfo.prediction.prediction_intervals?.['50']?.pi_value_high,
          q95: predInfo.prediction.prediction_intervals?.['90']?.pi_value_high,
        },
      });
    });

    // Sort by target date
    combinedData.sort((a, b) => a.targetDate.getTime() - b.targetDate.getTime());

    return {
      data: combinedData,
      metadata: {
        displayStartDate: dateStart,
        displayEndDate: dateEnd,
      },
    };
  }
);

// ============================================
// Single Model Score Selectors
// ============================================

/**
 * Selector for evaluation scores from JSON that syncs with time series data
 * Provides raw score data points for the Single Model score line chart
 */
export const selectSingleModelScoreDataFromJSON = createSelector(
  [
    (state: RootState) => state.evaluationDataStore.rawScores,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewModel,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewSelectedStateCode,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationSingleModelViewHorizon,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationSingleModelViewScoresOption,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewDateStart,
    (state: RootState) => state.evaluationsSingleModelSettings.evaluationSingleModelViewDateEnd,
    selectSingleModelSelectedTargetId,
  ],
  (rawScores, modelName, stateCode, horizon, scoreOption, dateStart, dateEnd, targetId) => {
    if (!rawScores || !modelName) {
      console.debug("Missing required data for score selector");
      return [];
    }

    // Raw scores are now stored with 'all' as the key (single file with all data)
    const allScoresData = rawScores['all'];
    if (!allScoresData) {
      console.debug("Raw scores not loaded yet");
      return [];
    }
    
    // Navigate with target layer: rawScores['all'][target][metric][model][location][horizon]
    const targetData = allScoresData[targetId];
    if (!targetData) {
      console.debug("No score data for target:", targetId);
      return [];
    }

    const metric = scoreOption === "WIS/Baseline" ? "WIS/Baseline" : "MAPE";
    const metricData = targetData[metric];
    if (!metricData) {
      console.debug("No score data for metric:", metric);
      return [];
    }

    // Navigate the nested structure
    const scoreData = metricData?.[modelName]?.[stateCode]?.[horizon];

    if (!scoreData || !Array.isArray(scoreData)) {
      console.debug("No score data found for:", { metric, modelName, stateCode, horizon });
      return [];
    }

    // Filter scores to match the time range
    const filteredScores = scoreData.filter((entry) => {
      const targetDate = new Date(entry.targetEndDate);
      return targetDate >= dateStart && targetDate <= dateEnd;
    });

    console.debug(`[selectSingleModelScoreDataFromJSON] Found ${filteredScores.length} score entries for ${metric}`, {
      modelName,
      stateCode,
      horizon,
      targetId,
      metric,
      dateRange: { start: dateStart.toISOString(), end: dateEnd.toISOString() }
    });

    // Convert ISO strings back to Date objects for the component and sort by target date
    const processedScores = filteredScores
      .map((entry) => ({
        referenceDate: new Date(entry.referenceDate),
        targetEndDate: new Date(entry.targetEndDate),
        score: entry.score,
      }))
      .sort((a, b) => a.targetEndDate.getTime() - b.targetEndDate.getTime());



    return processedScores;
  }
);
