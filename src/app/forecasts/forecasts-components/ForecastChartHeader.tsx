import { updateHistoricalDataMode, updateSelectedHistoricalAsOfDate } from "@/store/data-slices/settings/SettingsSliceForecastPage";
import { useAppDispatch, useAppSelector } from "@/store/hooks";
import { Switch } from "@/styles/material-tailwind-wrapper";
import React, { useEffect } from "react";
import { weeklyHospitalAdmissionsInfo } from "types/infobutton-content";
import InfoButton from "../../components/InfoButton";
import { useDataContext } from "@/providers/DataProvider";
import { selectHistoricalAsOfDates } from "@/store/selectors";

const ForecastChartHeader: React.FC = () => {
  const dispatch = useAppDispatch();
  const { loadHistoricalDataIfNeeded } = useDataContext();
  const historicalDataMode = useAppSelector((state) => state.forecastSettings.historicalTargetDataMode);
  const historicalDataEnabled = useAppSelector(
    (state) => state.configStore.config?.historicalTargetDataEnabled
  );
  const isHistoricalDataLoaded = useAppSelector((state) => state.historicalTargetDataStore.isLoaded);
  const selectedHistoricalAsOfDate = useAppSelector((state) => state.forecastSettings.selectedHistoricalAsOfDate);
  const availableAsOfDates = useAppSelector(selectHistoricalAsOfDates);

  // Auto-select the most recent as_of date when data is loaded and no date is selected
  useEffect(() => {
    if (isHistoricalDataLoaded && !selectedHistoricalAsOfDate && availableAsOfDates.length > 0) {
      // availableAsOfDates is sorted descending, so [0] is the most recent
      dispatch(updateSelectedHistoricalAsOfDate(availableAsOfDates[0]));
    }
  }, [isHistoricalDataLoaded, selectedHistoricalAsOfDate, availableAsOfDates, dispatch]);

  const handleHistoricalDataModeToggle = async () => {
    const newMode = !historicalDataMode;
    dispatch(updateHistoricalDataMode(newMode));
    
    // Lazy load historical data if enabling and not yet loaded
    if (newMode && !isHistoricalDataLoaded) {
      await loadHistoricalDataIfNeeded();
    }
  };

  return (
    <div className='flex flex-row justify-between align-middle items-center px-4 overflow-ellipsis whitespace-nowrap'>
      <div className='flex flex-shrink justify-start items-center'>
        <h2 className='util-responsive-text util-text-limit mr-2'> Weekly Hospital Admissions Forecast</h2>
        <InfoButton title='Weekly Hospital Admissions' content={weeklyHospitalAdmissionsInfo} />
      </div>
      {historicalDataEnabled && (
        <div className='flex flex-shrink justify-end items-center'>
          <p className='mr-3 md:text-sm sm:text-xs'>Show Data Available at Time of Forecast</p>
          <Switch
            checked={historicalDataMode}
            onChange={handleHistoricalDataModeToggle}
            color='blue'
            label={historicalDataMode ? "On" : "Off"}
            crossOrigin={undefined}
          />
        </div>
      )}
    </div>
  );
};
export default ForecastChartHeader;
