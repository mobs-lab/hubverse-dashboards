// components/SettingsPanel.tsx
'use client';

import InfoButton from '@/shared-components/InfoButton';
import SettingsStateMap from '@/shared-components/SettingsStateMap';
import {
  updateSelectedForecastPeriod,
  updateSelectedHorizons,
  updateSelectedLocation,
  updateSelectedModels,
  updateSelectedPredictionIntervals,
  updateSelectedTargets,
  updateTimeFilterEnd,
  updateTimeFilterStart,
  updateYScale,
} from '@/store/data-slices/settings/SettingsSliceForecastPage';
import { useAppDispatch, useAppSelector } from '@/store/hooks';
import {
  selectConfig,
  selectDateConstraints,
  selectHorizons,
  selectLocationList,
  selectModelColorMap,
  selectModelNames,
  selectPredictionIntervalOptions,
  selectTargets,
} from '@/store/selectors/forecastSelectors';
import { Radio, Typography } from '@/styles/material-tailwind-wrapper';
import React, { useMemo, useState } from 'react';
import { horizonSelectorsInfo } from 'types/infobutton-content';
import SettingsStyledDatePicker from './SettingsStyledDatePicker';

const SettingsPanel: React.FC = () => {
  /* Redux-Managed State Variables */
  const dispatch = useAppDispatch();

  // Get config-driven data from selectors
  const locationList = useAppSelector(selectLocationList);
  const modelNames = useAppSelector(selectModelNames);
  const modelColorMap = useAppSelector(selectModelColorMap);
  const horizons = useAppSelector(selectHorizons);
  const predictionIntervalOptions = useAppSelector(selectPredictionIntervalOptions);
  const targets = useAppSelector(selectTargets);
  const config = useAppSelector(selectConfig);
  const { earliestDate, latestDate } = useAppSelector(selectDateConstraints);

  // Get current settings from Redux
  const {
    selectedLocationCode,
    selectedModels,
    selectedHorizons: selectedHorizonsList,
    selectedTargetIds,
    timeFilterRangeStart: dateStart,
    timeFilterRangeEnd: dateEnd,
    selectedPredictionIntervals,
    selectedForecastPeriod,
    yAxisScale,
  } = useAppSelector((state) => state.forecastSettings);

  // Check if we have multiple targets to display
  const hasMultipleTargets = targets.length > 1;

  // Local state for UI interactions
  const [isModelListExpanded, setIsModelListExpanded] = useState(false);
  const [locationSearchText, setLocationSearchText] = useState('');

  // Get forecast period options as array
  const forecastPeriodOptions = useMemo(() => {
    if (!config?.forecastPeriodOptions) return [];
    return Object.entries(config.forecastPeriodOptions).map(([id, period]) => ({
      ...period,
      forecastPeriodId: id,
    }));
  }, [config]);

  // Fuzzy search for locations
  const filteredLocations = useMemo(() => {
    if (!locationSearchText) return locationList;
    const searchLower = locationSearchText.toLowerCase();
    return locationList.filter(
      (loc: { code: string; name: string; nameAlt?: string }) =>
        loc.name.toLowerCase().includes(searchLower) ||
        loc.code.toLowerCase().includes(searchLower) ||
        (loc.nameAlt && loc.nameAlt.toLowerCase().includes(searchLower))
    );
  }, [locationList, locationSearchText]);

  // Model list preview/expansion
  const displayedModels = isModelListExpanded ? modelNames : modelNames.slice(0, 5);
  const hasMoreModels = modelNames.length > 5;

  // Event Handlers
  const onLocationChange = (locationCode: string) => {
    dispatch(updateSelectedLocation(locationCode));
  };

  const onModelSelectionChange = (modelName: string, checked: boolean) => {
    if (checked) {
      dispatch(updateSelectedModels([...selectedModels, modelName]));
    } else {
      dispatch(updateSelectedModels(selectedModels.filter((model) => model !== modelName)));
    }
  };

  const onHorizonChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const horizon = Number(event.target.value);
    dispatch(updateSelectedHorizons([horizon]));
  };

  const onDateStartSelectionChange = (date: Date | null) => {
    if (date && date >= earliestDate && date <= dateEnd) {
      dispatch(updateTimeFilterStart(date));
    } else {
      console.error('SettingsPanel.tsx: Invalid dateStart selection');
    }
  };

  const onDateEndSelectionChange = (date: Date | null) => {
    if (date && date >= dateStart && date <= latestDate) {
      dispatch(updateTimeFilterEnd(date));
    } else {
      console.error('SettingsPanel.tsx: Invalid dateEnd selection');
    }
  };

  const onForecastPeriodChange = (timeValue: string) => {
    const selectedOption = forecastPeriodOptions.find((option) => option.timeValue === timeValue);
    if (selectedOption) {
      dispatch(updateSelectedForecastPeriod(selectedOption));
      dispatch(updateTimeFilterStart(selectedOption.startDate));
      dispatch(updateTimeFilterEnd(selectedOption.endDate));
    }
  };

  const handleShowAllDates = () => {
    dispatch(updateTimeFilterStart(earliestDate));
    dispatch(updateTimeFilterEnd(latestDate));
  };

  const onYAxisScaleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    dispatch(updateYScale(event.target.value as 'linear' | 'log'));
  };

  const onPredictionIntervalChange = (interval: string, checked: boolean) => {
    if (checked) {
      dispatch(updateSelectedPredictionIntervals([...selectedPredictionIntervals, interval]));
    } else {
      dispatch(
        updateSelectedPredictionIntervals(
          selectedPredictionIntervals.filter((pi) => pi !== interval)
        )
      );
    }
  };

  const onTargetSelectionChange = (targetId: string, checked: boolean) => {
    if (checked) {
      dispatch(updateSelectedTargets([...selectedTargetIds, targetId]));
    } else {
      dispatch(updateSelectedTargets(selectedTargetIds.filter((t) => t !== targetId)));
    }
  };

  const handleShowAllModels = () => {
    dispatch(updateSelectedModels(modelNames));
  };

  return (
    <div className="bg-mobs-lab-color-filterspane text-white fill-white flex-col h-full w-full rounded-md overflow-scroll util-responsive-text-settings">
      {/* <div className="flex-grow nowrap overflow-y-auto p-4 util-no-sb-length"> */}
      {/* Location Selector */}
      <div className="mb-6 w-full justify-stretch items-stretch">
        <Typography variant="h6" className="text-white" placeholder="">
          Select Location
        </Typography>

        <div className="w-full">
          <SettingsStateMap pageSelected="forecast" />
        </div>

        {/* Location search input */}
        <input
          type="text"
          placeholder="Search locations..."
          value={locationSearchText}
          onChange={(e) => setLocationSearchText(e.target.value)}
          className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 mb-2"
        />

        {/* Location dropdown */}
        <select
          value={selectedLocationCode}
          onChange={(e) => onLocationChange(e.target.value)}
          className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-4 px-2 overflow-ellipsis"
        >
          {filteredLocations.map((location: { code: string; name: string; nameAlt?: string }) => (
            <option key={location.code} value={location.code}>
              {location.name}
            </option>
          ))}
        </select>
      </div>

      {/* Models Selector with Collapsible List */}
      <div className="mb-2 w-full overflow-ellipsis">
        <Typography variant="h6" className="text-white mb-1" placeholder="">
          Models
        </Typography>
        <div className="space-y-2 h-full overflow-y-auto pr-1">
          {displayedModels.map((model) => (
            <label
              key={model}
              className="inline-flex items-center text-white hover:bg-gray-700 rounded cursor-pointer w-full"
            >
              <span
                className="w-[1em] h-[1em] border-2 rounded-sm mr-2"
                style={{
                  backgroundColor: selectedModels.includes(model)
                    ? modelColorMap[model]
                    : 'transparent',
                  borderColor: modelColorMap[model],
                }}
              />
              <input
                type="checkbox"
                className="sr-only"
                checked={selectedModels.includes(model)}
                onChange={(e) => onModelSelectionChange(model, e.target.checked)}
              />
              <span className="ml-2 xs:text-sm">{model}</span>
            </label>
          ))}
        </div>

        {/* Expand/Collapse area */}
        {hasMoreModels && (
          <button
            className="w-full mt-2 bg-[#5d636a]/50 hover:bg-[#5d636a]/70 text-white py-1 px-2 rounded text-sm transition-colors"
            onClick={() => setIsModelListExpanded(!isModelListExpanded)}
          >
            {isModelListExpanded
              ? `Show Less (${modelNames.length} total)`
              : `Show More (${modelNames.length - 5} more)`}
          </button>
        )}

        {/* Show All Models button */}
        <button
          className="w-full mt-2 bg-[#5d636a] hover:bg-blue-600 text-white py-1 px-2 rounded text-sm"
          onClick={handleShowAllModels}
        >
          Show All Models
        </button>
      </div>

      {/* Forecast Period Selector */}
      <div className="mb-4 w-full justify-stretch items-stretch py-4">
        <div className="mb-2 w-full">
          <Typography variant="h6" className="text-white" placeholder="">
            Forecast Period
          </Typography>
          <select
            id="settings-panel-period-select"
            value={selectedForecastPeriod?.timeValue || ''}
            onChange={(e) => onForecastPeriodChange(e.target.value)}
            className="text-white border-[#5d636a] border-2 flex-wrap bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 overflow-ellipsis"
          >
            {forecastPeriodOptions.map((option: any) => (
              <option key={option.forecastPeriodId} value={option.timeValue}>
                {option.displayString}
              </option>
            ))}
          </select>
        </div>

        <div className="mb-2 w-full">
          <Typography variant="h6" className="text-white" placeholder="">
            Start Date
          </Typography>
          <SettingsStyledDatePicker
            value={dateStart}
            onChange={onDateStartSelectionChange}
            minDate={earliestDate}
            maxDate={dateEnd}
            className="w-full border-[#5d636a] border-2 rounded-md"
          />
        </div>

        <div className="mb-2 w-full">
          <Typography variant="h6" className="text-white" placeholder="">
            End Date
          </Typography>
          <SettingsStyledDatePicker
            value={dateEnd}
            onChange={onDateEndSelectionChange}
            minDate={dateStart}
            maxDate={latestDate}
            className="w-full border-[#5d636a] border-2 rounded-md"
          />
        </div>
        <button
          className="bg-[#5d636a] text-white rounded text-sm w-full"
          onClick={handleShowAllDates}
        >
          Show All
        </button>
      </div>

      {/* Target Selector - Only show if multiple targets exist */}
      {hasMultipleTargets && (
        <div className="mb-4 w-full">
          <Typography variant="h6" className="text-white mb-2" placeholder="">
            Targets
          </Typography>
          <div className="space-y-2">
            {targets.map((target) => (
              <label
                key={target.targetId}
                className="flex items-center text-white hover:bg-gray-700 rounded cursor-pointer px-2 py-1"
              >
                <input
                  type="checkbox"
                  className="form-checkbox text-blue-600 mr-2"
                  checked={selectedTargetIds.includes(target.targetId)}
                  onChange={(e) => onTargetSelectionChange(target.targetId, e.target.checked)}
                />
                <span className="xs:text-sm">{target.displayString}</span>
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Horizon Selector - Dropdown */}
      <div className="mb-4 flex-col">
        <div className="flex flex-row flex-nowrap justify-start items-center gap-1">
          <Typography variant="h6" className="text-white flex-shrink" placeholder="">
            Horizon
          </Typography>
          <InfoButton content={horizonSelectorsInfo} title={'Forecast Horizons'}></InfoButton>
        </div>

        <select
          value={selectedHorizonsList[0] || horizons[0]}
          onChange={onHorizonChange}
          className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 mt-2"
        >
          {horizons.map((horizon) => (
            <option key={horizon} value={horizon}>
              {horizon} {config?.timeUnit === 7 ? 'weeks' : 'days'} ahead
            </option>
          ))}
        </select>
      </div>

      {/* Y-Axis Scale */}
      <div className="mb-4 w-full">
        <Typography variant="h6" className="text-white" placeholder="">
          Y-Axis Scale
        </Typography>
        {['linear', 'log'].map((value) => (
          <Radio
            key={value}
            name="yAxisRadioBtn"
            value={value}
            label={value === 'linear' ? 'Linear' : 'Logarithmic'}
            onChange={(e) => onYAxisScaleChange(e)}
            className="text-white"
            labelProps={{ className: 'text-white' }}
            defaultChecked={value === 'linear'}
            crossOrigin=""
          />
        ))}
      </div>

      {/* Prediction Interval Selector - Dropdown/Checkbox */}
      <div className="mb-2 flex-col justify-stretch items-stretch flex-wrap w-full">
        <Typography variant="h6" className="text-white" placeholder="">
          Prediction Interval
        </Typography>
        <div className="flex flex-col gap-2 mt-2">
          {predictionIntervalOptions.map((interval) => (
            <label key={interval.level} className="flex items-center text-white">
              <input
                type="checkbox"
                className="form-checkbox text-blue-600 mr-2"
                checked={selectedPredictionIntervals.includes(interval.level)}
                onChange={(e) => onPredictionIntervalChange(interval.level, e.target.checked)}
              />
              <span>{interval.level}% PI</span>
            </label>
          ))}
          <button
            className={`flex flex-wrap rounded p-1 ${
              selectedPredictionIntervals.length === 0
                ? 'bg-blue-600 text-white'
                : 'bg-[#5d636a] text-white'
            }`}
            onClick={() => dispatch(updateSelectedPredictionIntervals([]))}
          >
            None
          </button>
        </div>
      </div>
      {/* </div> */}

      {/* <div className="mt-auto p-2 border-t border-gray-700">
        <Image src="/epistorm-logo.png" width={300} height={120} alt="Epistorm Logo" priority />
      </div> */}
    </div>
  );
};

export default SettingsPanel;
