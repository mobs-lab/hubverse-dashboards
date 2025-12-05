// Custom hook for lazy loading evaluations data per period
import { getDataPath } from '@/config/devMode';
import { useDataContext } from '@/providers/DataProvider';
import {
  addPrecalculatedData,
  addRawScores,
} from '@/store/data-slices/domains/evaluationDataSlice';
import { useAppDispatch, useAppSelector } from '@/store/hooks';
import { useCallback, useState } from 'react';

interface UseEvaluationsDataReturn {
  isLoading: boolean;
  areAggregatesLoaded: boolean;
  areRawScoresLoaded: boolean;
  error: string | null;
  loadAggregatesForPeriod: (periodId: string) => Promise<void>;
  loadRawScoresForPeriod: (periodId: string) => Promise<void>;
  loadDefaultPeriodAggregates: () => Promise<void>;
  loadDefaultPeriodRawScores: () => Promise<void>;
  // Legacy methods for backward compatibility
  loadAggregates: () => Promise<void>;
  loadRawScores: () => Promise<void>;
}

export const useEvaluationsData = (): UseEvaluationsDataReturn => {
  const dispatch = useAppDispatch();
  const { updateLoadingState } = useDataContext();

  // Get config and state
  const config = useAppSelector((state) => state.configStore.config);
  const { loadedPeriods, loadedRawScoreSeasons } = useAppSelector(
    (state) => state.evaluationDataStore
  );

  // Get available period IDs and default period
  const availablePeriodIds = config?.evaluationAvailablePeriodIds || [];
  const defaultPeriodId = config?.defaultForecastPeriodId || '';

  // Get currently selected period from Season Overview settings
  const selectedSeasonOverviewPeriod = useAppSelector(
    (state) => state.evaluationsSeasonOverviewSettings.selectedDynamicTimePeriod
  );
  
  // Get currently selected period from Single Model settings
  const selectedSingleModelPeriod = useAppSelector(
    (state) => state.evaluationsSingleModelSettings.evaluationsSingleModelViewSeasonId
  );

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  /**
   * Load aggregates (IQR, locationMap_aggregates, detailedCoverage_aggregates) for a specific period
   */
  const loadAggregatesForPeriod = useCallback(
    async (periodId: string) => {
      // Check if already loaded
      if (loadedPeriods.includes(periodId)) {
        console.log(`Aggregates for period ${periodId} already loaded, skipping`);
        return;
      }

      // Check if period is available
      if (availablePeriodIds.length > 0 && !availablePeriodIds.includes(periodId)) {
        console.warn(`Period ${periodId} not in available period IDs:`, availablePeriodIds);
        return;
      }

      setIsLoading(true);
      setError(null);
      updateLoadingState('evaluationScores', true);
      updateLoadingState('evaluationDetailedCoverage', true);

      try {
        console.log(`Loading aggregates for period: ${periodId}`);
        const dataPath = getDataPath();

        const response = await fetch(`${dataPath}/evaluations/${periodId}/aggregates.json`);
        if (!response.ok) {
          throw new Error(`Failed to load aggregates for ${periodId}: ${response.statusText}`);
        }

        const aggregatesData = await response.json();

        // Dispatch to store - the data from per-period file is flat (not nested by periodId)
        dispatch(
          addPrecalculatedData({
            periodId,
            data: aggregatesData,
          })
        );

        console.log(`Aggregates for period ${periodId} loaded successfully`);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error';
        console.warn(`Failed to load aggregates for period ${periodId}:`, errorMessage);
        setError(errorMessage);
      } finally {
        setIsLoading(false);
        updateLoadingState('evaluationScores', false);
        updateLoadingState('evaluationDetailedCoverage', false);
      }
    },
    [dispatch, loadedPeriods, availablePeriodIds, updateLoadingState]
  );

  /**
   * Load raw scores for a specific period
   */
  const loadRawScoresForPeriod = useCallback(
    async (periodId: string) => {
      // Check if already loaded
      if (loadedRawScoreSeasons.includes(periodId)) {
        console.log(`Raw scores for period ${periodId} already loaded, skipping`);
        return;
      }

      // Check if period is available
      if (availablePeriodIds.length > 0 && !availablePeriodIds.includes(periodId)) {
        console.warn(`Period ${periodId} not in available period IDs:`, availablePeriodIds);
        return;
      }

      setIsLoading(true);

      try {
        console.log(`Loading raw scores for period: ${periodId}`);
        const dataPath = getDataPath();

        const response = await fetch(`${dataPath}/evaluations/${periodId}/rawScores.json`);
        if (!response.ok) {
          throw new Error(`Failed to load raw scores for ${periodId}: ${response.statusText}`);
        }

        const rawScoresData = await response.json();

        // Dispatch to store - the data from per-period file is flat (not nested by seasonId)
        dispatch(
          addRawScores({
            seasonId: periodId,
            data: rawScoresData,
          })
        );

        console.log(`Raw scores for period ${periodId} loaded successfully`);
      } catch (err) {
        console.error(`Failed to load raw scores for period ${periodId}:`, err);
        // Don't clear all data, just log the error
      } finally {
        setIsLoading(false);
      }
    },
    [dispatch, loadedRawScoreSeasons, availablePeriodIds]
  );

  /**
   * Load aggregates for the default/selected period (convenience method)
   */
  const loadDefaultPeriodAggregates = useCallback(async () => {
    // Priority: selected period > default period > first available
    const periodToLoad =
      selectedSeasonOverviewPeriod || 
      defaultPeriodId || 
      (availablePeriodIds.length > 0 ? availablePeriodIds[0] : null);

    if (!periodToLoad) {
      console.warn('No period available to load aggregates');
      return;
    }

    await loadAggregatesForPeriod(periodToLoad);
  }, [selectedSeasonOverviewPeriod, defaultPeriodId, availablePeriodIds, loadAggregatesForPeriod]);

  /**
   * Load raw scores for the default/selected period (convenience method)
   */
  const loadDefaultPeriodRawScores = useCallback(async () => {
    // Priority: single model selected period > selected period > default period > first available
    const periodToLoad =
      selectedSingleModelPeriod ||
      selectedSeasonOverviewPeriod || 
      defaultPeriodId || 
      (availablePeriodIds.length > 0 ? availablePeriodIds[0] : null);

    if (!periodToLoad) {
      console.warn('No period available to load raw scores');
      return;
    }

    await loadRawScoresForPeriod(periodToLoad);
  }, [selectedSingleModelPeriod, selectedSeasonOverviewPeriod, defaultPeriodId, availablePeriodIds, loadRawScoresForPeriod]);

  // Legacy method aliases for backward compatibility with existing components
  const loadAggregates = loadDefaultPeriodAggregates;
  const loadRawScores = loadDefaultPeriodRawScores;

  return {
    isLoading,
    areAggregatesLoaded: loadedPeriods.length > 0,
    areRawScoresLoaded: loadedRawScoreSeasons.length > 0,
    error,
    loadAggregatesForPeriod,
    loadRawScoresForPeriod,
    loadDefaultPeriodAggregates,
    loadDefaultPeriodRawScores,
    // Legacy aliases
    loadAggregates,
    loadRawScores,
  };
};
