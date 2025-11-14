import { useEffect } from 'react';
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
  const { loadHistoricalDataIfNeeded } = useDataContext();

  // Get relevant state
  const historicalDataMode = useAppSelector((state) => state.forecastSettings.historicalTargetDataMode);
  const isHistoricalDataLoaded = useAppSelector((state) => state.historicalTargetDataStore.isLoaded);
  const selectedHistoricalAsOfDate = useAppSelector((state) => state.forecastSettings.selectedHistoricalAsOfDate);
  const userSelectedDate = useAppSelector((state) => state.forecastSettings.userSelectedDate);
  const availableAsOfDates = useAppSelector(selectHistoricalAsOfDates);

  // useEffect 1: Lazy load historical data when mode is enabled
  useEffect(() => {
    if (historicalDataMode && !isHistoricalDataLoaded) {
      console.debug('[useHistoricalTargetData] Loading historical data...');
      loadHistoricalDataIfNeeded();
    }
  }, [historicalDataMode, isHistoricalDataLoaded, loadHistoricalDataIfNeeded]);

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

