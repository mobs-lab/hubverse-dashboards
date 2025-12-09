// Custom hook for lazy loading evaluations data per period
import { getDataPath } from '@/config/devMode';
import { useDataContext } from '@/providers/DataProvider';
import {
  addPrecalculatedData,
  addRawScores,
} from '@/store/data-slices/domains/evaluationDataSlice';
import { useAppDispatch, useAppSelector } from '@/store/hooks';
import { useCallback, useRef, useState } from 'react';

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

  // Track failed periods to prevent repetitive fetch attempts
  // Using refs to persist across renders without causing re-renders
  const failedAggregatePeriodsRef = useRef<Set<string>>(new Set());
  const failedRawScorePeriodsRef = useRef<Set<string>>(new Set());

  // Track in-flight requests to prevent duplicate concurrent fetches
  const aggregatesLoadingRef = useRef<Set<string>>(new Set());
  const rawScoresLoadingRef = useRef<Set<string>>(new Set());

  /**
   * Load aggregates (IQR, locationMap_aggregates, detailedCoverage_aggregates) for a specific period
   */
  const loadAggregatesForPeriod = useCallback(
    async (periodId: string) => {
      // Guard: No period ID provided
      if (!periodId) {
        console.warn('No period ID provided for loading aggregates');
        return;
      }

      // Guard: Already loaded successfully
      if (loadedPeriods.includes(periodId)) {
        console.debug(`Aggregates for period ${periodId} already loaded, skipping`);
        return;
      }

      // Guard: Previously failed - don't retry automatically
      if (failedAggregatePeriodsRef.current.has(periodId)) {
        console.debug(`Aggregates for period ${periodId} previously failed, skipping`);
        return;
      }

      // Guard: Currently loading - prevent duplicate fetches
      if (aggregatesLoadingRef.current.has(periodId)) {
        console.debug(`Aggregates for period ${periodId} already loading, skipping`);
        return;
      }

      // Guard: Period not in available list (if list is provided)
      if (availablePeriodIds.length > 0 && !availablePeriodIds.includes(periodId)) {
        console.warn(`Period ${periodId} not in available period IDs:`, availablePeriodIds);
        failedAggregatePeriodsRef.current.add(periodId);
        return;
      }

      // Mark as loading
      aggregatesLoadingRef.current.add(periodId);
      setIsLoading(true);
      setError(null);
      updateLoadingState('evaluationScores', true);
      updateLoadingState('evaluationDetailedCoverage', true);

      try {
        console.log(`Loading aggregates for period: ${periodId}`);
        const dataPath = getDataPath();

        const response = await fetch(`${dataPath}/evaluations/${periodId}/aggregates.json`);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        // Parse JSON with explicit error handling for invalid JSON (NaN, Infinity, etc.)
        let aggregatesData;
        try {
          const responseText = await response.text();
          aggregatesData = JSON.parse(responseText);
        } catch (parseError) {
          throw new Error(`JSON parse error: ${parseError instanceof Error ? parseError.message : 'Invalid JSON in response'}`);
        }

        // Validate data structure
        if (!aggregatesData || typeof aggregatesData !== 'object') {
          throw new Error('Invalid aggregates data format');
        }

        // Dispatch to store
        dispatch(
          addPrecalculatedData({
            periodId,
            data: aggregatesData,
          })
        );

        console.log(`Aggregates for period ${periodId} loaded successfully`);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error';
        console.error(`Failed to load aggregates for period ${periodId}:`, errorMessage);

        // Mark as failed to prevent repetitive fetches
        failedAggregatePeriodsRef.current.add(periodId);
        setError(`Failed to load evaluation data for ${periodId}: ${errorMessage}`);

        // Don't break the app - just continue with empty data
      } finally {
        // Clean up loading state
        aggregatesLoadingRef.current.delete(periodId);
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
      // Guard: No period ID provided
      if (!periodId) {
        console.warn('No period ID provided for loading raw scores');
        return;
      }

      // Guard: Already loaded successfully
      if (loadedRawScoreSeasons.includes(periodId)) {
        console.debug(`Raw scores for period ${periodId} already loaded, skipping`);
        return;
      }

      // Guard: Previously failed - don't retry automatically
      if (failedRawScorePeriodsRef.current.has(periodId)) {
        console.debug(`Raw scores for period ${periodId} previously failed, skipping`);
        return;
      }

      // Guard: Currently loading - prevent duplicate fetches
      if (rawScoresLoadingRef.current.has(periodId)) {
        console.debug(`Raw scores for period ${periodId} already loading, skipping`);
        return;
      }

      // Guard: Period not in available list (if list is provided)
      if (availablePeriodIds.length > 0 && !availablePeriodIds.includes(periodId)) {
        console.warn(`Period ${periodId} not in available period IDs:`, availablePeriodIds);
        failedRawScorePeriodsRef.current.add(periodId);
        return;
      }

      // Mark as loading
      rawScoresLoadingRef.current.add(periodId);
      setIsLoading(true);

      try {
        console.log(`Loading raw scores for period: ${periodId}`);
        const dataPath = getDataPath();

        const response = await fetch(`${dataPath}/evaluations/${periodId}/rawScores.json`);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        // Parse JSON with explicit error handling for invalid JSON (NaN, Infinity, etc.)
        let rawScoresData;
        try {
          const responseText = await response.text();
          rawScoresData = JSON.parse(responseText);
        } catch (parseError) {
          throw new Error(`JSON parse error: ${parseError instanceof Error ? parseError.message : 'Invalid JSON in response'}`);
        }

        // Validate data structure
        if (!rawScoresData || typeof rawScoresData !== 'object') {
          throw new Error('Invalid raw scores data format');
        }

        // Dispatch to store
        dispatch(
          addRawScores({
            seasonId: periodId,
            data: rawScoresData,
          })
        );

        console.log(`Raw scores for period ${periodId} loaded successfully`);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error';
        console.error(`Failed to load raw scores for period ${periodId}:`, errorMessage);

        // Mark as failed to prevent repetitive fetches
        failedRawScorePeriodsRef.current.add(periodId);
        setError(`Failed to load raw scores for ${periodId}: ${errorMessage}`);

        // Don't break the app - just continue with empty data
      } finally {
        // Clean up loading state
        rawScoresLoadingRef.current.delete(periodId);
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
      console.warn('No period available to load aggregates - evaluations may not be configured');
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
      console.warn('No period available to load raw scores - evaluations may not be configured');
      return;
    }

    await loadRawScoresForPeriod(periodToLoad);
  }, [
    selectedSingleModelPeriod,
    selectedSeasonOverviewPeriod,
    defaultPeriodId,
    availablePeriodIds,
    loadRawScoresForPeriod,
  ]);

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
