import { useEffect, useRef } from 'react';
import { useAppDispatch, useAppSelector } from '@/store/hooks';
import { updateSelectedHistoricalAsOfDate } from '@/store/data-slices/settings/SettingsSliceForecastPage';
import { selectHistoricalAsOfDates } from '@/store/selectors';
import { useDataContext } from '@/providers/DataProvider';

/**
 * Custom hook to manage historical target data logic
 *
 * Responsibilities:
 * 1. Lazy load historical data when mode is first enabled
 * 2. Sync selectedHistoricalAsOfDate with userSelectedDate (the date clicked on chart)
 * 3. Ensure as_of date is valid and within available dates
 */
export const useHistoricalTargetData = () => {
  const dispatch = useAppDispatch();
  const { loadHistoricalDataIfNeeded, loadingStates } = useDataContext();
  
  // Ref to track if we've attempted to load to prevent infinite retries on failure
  const hasAttemptedLoadRef = useRef(false);

  // Get relevant state
  const historicalDataMode = useAppSelector(
    (state) => state.forecastSettings.historicalTargetDataMode
  );
  const isHistoricalDataLoaded = useAppSelector(
    (state) => state.historicalTargetDataStore.isLoaded
  );
  const selectedHistoricalAsOfDate = useAppSelector(
    (state) => state.forecastSettings.selectedHistoricalAsOfDate
  );
  const userSelectedDate = useAppSelector((state) => state.forecastSettings.userSelectedDate);
  const availableAsOfDates = useAppSelector(selectHistoricalAsOfDates);

  // Reset attempt ref if mode is toggled off
  useEffect(() => {
    if (!historicalDataMode) {
      hasAttemptedLoadRef.current = false;
    }
  }, [historicalDataMode]);

  // useEffect 1: Lazy load historical data when mode is enabled
  useEffect(() => {
    // Only try to load if:
    // 1. Mode is enabled
    // 2. Data is NOT loaded yet
    // 3. We haven't already attempted to load (prevents infinite loop on 404/failure)
    // 4. It is not currently loading (checked via loadingStates)
    if (
      historicalDataMode && 
      !isHistoricalDataLoaded && 
      !hasAttemptedLoadRef.current && 
      !loadingStates.historicalTargetData
    ) {
      console.debug('[useHistoricalTargetData] Loading historical data...');
      hasAttemptedLoadRef.current = true;
      loadHistoricalDataIfNeeded();
    }
  }, [
    historicalDataMode, 
    isHistoricalDataLoaded, 
    loadHistoricalDataIfNeeded, 
    loadingStates.historicalTargetData
  ]);

  // useEffect 2: Sync selectedHistoricalAsOfDate with userSelectedDate
  // This ensures historical data shows what was known AS OF the selected reference date
  useEffect(() => {
    if (isHistoricalDataLoaded && historicalDataMode && availableAsOfDates.length > 0) {
      const userSelectedDateISO = userSelectedDate.toISOString().split('T')[0];

      // Find the matching or most recent as_of date <= userSelectedDate
      // availableAsOfDates is sorted descending (most recent first)
      // Should always be the same date, but for safety still search
      const matchingAsOfDate = availableAsOfDates.find(
        (asOfDate) => asOfDate <= userSelectedDateISO
      );

      if (matchingAsOfDate && matchingAsOfDate !== selectedHistoricalAsOfDate) {
        dispatch(updateSelectedHistoricalAsOfDate(matchingAsOfDate));
      }
    }
  }, [
    isHistoricalDataLoaded,
    historicalDataMode,
    userSelectedDate,
    selectedHistoricalAsOfDate,
    availableAsOfDates,
    dispatch,
  ]);

  return {
    historicalDataMode,
    isHistoricalDataLoaded,
    selectedHistoricalAsOfDate,
    availableAsOfDates,
  };
};
