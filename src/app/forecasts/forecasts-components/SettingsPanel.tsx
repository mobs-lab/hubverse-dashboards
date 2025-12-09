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
  updateSelectedTarget,
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
} from '@/store/selectors';
import { Radio, Typography } from '@/styles/material-tailwind-wrapper';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';
import React, { useMemo, useRef, useState, useEffect } from 'react';
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
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);

  // Get current settings from Redux
  const {
    selectedLocationCode,
    selectedModels,
    selectedHorizon,
    selectedTargetId,
    timeFilterRangeStart: dateStart,
    timeFilterRangeEnd: dateEnd,
    selectedPredictionIntervals,
    selectedForecastPeriod,
    yAxisScale,
  } = useAppSelector((state) => state.forecastSettings);

  // Check if we have multiple targets to display
  const hasMultipleTargets = targets.length > 1;
  const isSingleLocation = config?.isSingleLocation ?? false;

  // Local state for UI interactions
  const [isModelListExpanded, setIsModelListExpanded] = useState(false);
  const [locationSearchText, setLocationSearchText] = useState('');
  const [isLocationDropdownOpen, setIsLocationDropdownOpen] = useState(false);
  const [isPredictionIntervalDropdownOpen, setIsPredictionIntervalDropdownOpen] = useState(false);
  const locationDropdownRef = useRef<HTMLDivElement>(null);
  const predictionIntervalDropdownRef = useRef<HTMLDivElement>(null);

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
      (loc: { locationCode: string; locationName: string; locationNameAlt?: string }) =>
        loc.locationName.toLowerCase().includes(searchLower) ||
        loc.locationCode.toLowerCase().includes(searchLower) ||
        (loc.locationNameAlt && loc.locationNameAlt.toLowerCase().includes(searchLower))
    );
  }, [locationList, locationSearchText]);

  // Model list preview/expansion
  const displayedModels = isModelListExpanded ? modelNames : modelNames.slice(0, 4);
  const hasMoreModels = modelNames.length > 4;

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
    dispatch(updateSelectedHorizons(horizon));
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
    let updatedIntervals;
    if (checked) {
      updatedIntervals = [...selectedPredictionIntervals, interval];
    } else {
      updatedIntervals = selectedPredictionIntervals.filter((pi) => pi !== interval);
    }
    updatedIntervals.sort((a, b) => parseInt(a) - parseInt(b));
    dispatch(updateSelectedPredictionIntervals(updatedIntervals));
  };

  const onTargetSelectionChange = (targetId: string) => {
    dispatch(updateSelectedTarget(targetId));
  };

  const handleShowAllModels = () => {
    dispatch(updateSelectedModels(modelNames));
  };

  // Handle clicking outside dropdowns to close them
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        locationDropdownRef.current &&
        !locationDropdownRef.current.contains(event.target as Node)
      ) {
        setIsLocationDropdownOpen(false);
      }
      if (
        predictionIntervalDropdownRef.current &&
        !predictionIntervalDropdownRef.current.contains(event.target as Node)
      ) {
        setIsPredictionIntervalDropdownOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Get selected location name for display
  const selectedLocationName = useMemo(() => {
    const location = locationList.find((loc) => loc.locationCode === selectedLocationCode);
    return location ? location.locationName : '';
  }, [locationList, selectedLocationCode]);

  return (
    <div className="bg-mobs-lab-color-filterspane text-white fill-white flex-col h-full w-full rounded-md overflow-scroll p-4 pb-20 util-responsive-text-settings">
      {/* <div className="flex-grow nowrap overflow-y-auto p-4 util-no-sb-length"> */}
      {/* Location Selector */}
      {!isSingleLocation && (
        <div className="mb-6 w-full justify-stretch items-stretch">
          <Typography variant="h6" className="text-white" placeholder="">
            Select Location
          </Typography>

          <div className="w-full">
            <SettingsStateMap pageSelected="forecast" />
          </div>

          {/* Combined Location Search and Dropdown Combobox */}
          <div ref={locationDropdownRef} className="relative w-full">
            <div className="relative">
              <input
                type="text"
                placeholder="Search or select location..."
                value={locationSearchText || selectedLocationName}
                onChange={(e) => {
                  setLocationSearchText(e.target.value);
                  setIsLocationDropdownOpen(true);
                }}
                onFocus={() => setIsLocationDropdownOpen(true)}
                className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 pr-10"
              />
              <button
                type="button"
                onClick={() => setIsLocationDropdownOpen(!isLocationDropdownOpen)}
                className="absolute right-2 top-1/2 transform -translate-y-1/2 text-white"
              >
                {isLocationDropdownOpen ? (
                  <ChevronUpIcon className="h-5 w-5" />
                ) : (
                  <ChevronDownIcon className="h-5 w-5" />
                )}
              </button>
            </div>

            {/* Dropdown list */}
            {isLocationDropdownOpen && (
              <div className="absolute z-50 w-full mt-1 bg-mobs-lab-color-filterspane border-2 border-[#5d636a] rounded-md max-h-60 overflow-y-auto shadow-lg">
                {filteredLocations.length > 0 ? (
                  filteredLocations.map(
                    (location: { locationCode: string; locationName: string; locationNameAlt?: string }) => (
                      <div
                        key={location.locationCode}
                        onClick={() => {
                          onLocationChange(location.locationCode);
                          setLocationSearchText('');
                          setIsLocationDropdownOpen(false);
                        }}
                        className={`px-3 py-2 cursor-pointer hover:bg-gray-700 ${
                          location.locationCode === selectedLocationCode ? 'bg-gray-700' : ''
                        }`}
                      >
                        {location.locationName}
                      </div>
                    )
                  )
                ) : (
                  <div className="px-3 py-2 text-gray-400">No locations found</div>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Models Selector with Collapsible List */}
      <div className="mb-2 w-full overflow-ellipsis">
        <Typography variant="h6" className="text-white mb-1" placeholder="">
          Models
        </Typography>
        <div className="relative">
          <div
            className={`space-y-2 overflow-y-auto pr-1 transition-all duration-300 ${
              isModelListExpanded ? 'max-h-96' : 'max-h-40'
            }`}
          >
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

          {/* Expand/Collapse button - anchored to bottom with gradient overlay */}
          {hasMoreModels && (
            <div className="relative">
              {!isModelListExpanded && (
                <div className="absolute bottom-0 left-0 right-0 h-12 bg-gradient-to-t from-mobs-lab-color-filterspane to-transparent pointer-events-none" />
              )}
              <button
                className="w-full mt-2 bg-[#5d636a]/60 hover:bg-[#5d636a]/90 text-white py-2 px-2 rounded text-sm transition-all duration-200 flex items-center justify-center gap-2 shadow-md hover:shadow-lg"
                onClick={() => setIsModelListExpanded(!isModelListExpanded)}
              >
                {isModelListExpanded ? (
                  <>
                    <ChevronUpIcon className="h-4 w-4" />
                    <span>Show Less ({modelNames.length} total)</span>
                  </>
                ) : (
                  <>
                    <ChevronDownIcon className="h-4 w-4" />
                    <span>Show More ({modelNames.length - 4} more)</span>
                  </>
                )}
              </button>
            </div>
          )}
        </div>

        {/* Select All Models button */}
        <button
          className="w-full mt-2 bg-[#5d636a] hover:bg-blue-600 text-white py-1 px-2 rounded text-sm transition-colors"
          onClick={handleShowAllModels}
        >
          Select All Models
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

      {/* Target Selector */}
      <div className="mb-4 w-full">
        <Typography variant="h6" className="text-white mb-2" placeholder="">
          Target
        </Typography>
        {/* If only one target available, this will just become a static display */}
        <select
          value={selectedTargetId}
          onChange={(e) => onTargetSelectionChange(e.target.value)}
          className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2"
          disabled={!hasMultipleTargets}
        >
          {targets.map(
            (target: { targetId: string; displayString: string; targetKeyInData: string }) => (
              <option key={target.targetId} value={target.targetId}>
                {target.displayString}
              </option>
            )
          )}
        </select>
      </div>

      {/* Horizon Selector - Dropdown */}
      <div className="mb-4 flex-col">
        <div className="flex flex-row flex-nowrap justify-start items-center gap-1">
          <Typography variant="h6" className="text-white flex-shrink" placeholder="">
            Horizon
          </Typography>
          <InfoButton
            content={
              uiConfig?.forecastPage.infoButtons.horizonInfo?.content ? (
                <div
                  dangerouslySetInnerHTML={{
                    __html: uiConfig.forecastPage.infoButtons.horizonInfo.content,
                  }}
                />
              ) : (
                horizonSelectorsInfo
              )
            }
            title={uiConfig?.forecastPage.infoButtons.horizonInfo?.title || 'Forecast Horizons'}
          />
        </div>

        <select
          value={selectedHorizon ?? horizons[0]}
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
        <div className="flex">
          {['linear', 'log'].map((value) => (
            <Radio
              key={value}
              name="yAxisRadioBtn"
              value={value}
              label={value === 'linear' ? 'Linear' : 'Logarithmic'}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => onYAxisScaleChange(e)}
              className="text-white"
              labelProps={{ className: 'text-white' }}
              checked={yAxisScale === value}
              crossOrigin=""
            />
          ))}
        </div>
      </div>

      {/* Prediction Interval Selector - Multi-select Dropdown */}
      <div className="mb-2 w-full">
        <Typography variant="h6" className="text-white mb-2" placeholder="">
          Prediction Interval
        </Typography>
        <div className="flex gap-2 items-stretch">
          {/* Dropdown */}
          <div ref={predictionIntervalDropdownRef} className="relative flex-1">
            <button
              type="button"
              onClick={() => setIsPredictionIntervalDropdownOpen(!isPredictionIntervalDropdownOpen)}
              className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 flex items-center justify-between"
            >
              <span>
                {selectedPredictionIntervals.length === 0
                  ? 'No intervals selected'
                  : `${selectedPredictionIntervals.length} interval${selectedPredictionIntervals.length !== 1 ? 's' : ''} selected`}
              </span>
              {isPredictionIntervalDropdownOpen ? (
                <ChevronUpIcon className="h-5 w-5" />
              ) : (
                <ChevronDownIcon className="h-5 w-5" />
              )}
            </button>

            {/* Dropdown menu */}
            {isPredictionIntervalDropdownOpen && (
              <div className="absolute z-50 w-full mt-1 bg-mobs-lab-color-filterspane border-2 border-[#5d636a] rounded-md max-h-60 overflow-y-auto shadow-lg">
                {predictionIntervalOptions.map((interval) => (
                  <label
                    key={interval.level}
                    className="flex items-center px-3 py-2 hover:bg-gray-700 cursor-pointer"
                  >
                    <input
                      type="checkbox"
                      className="form-checkbox text-blue-600 mr-2 h-4 w-4"
                      checked={selectedPredictionIntervals.includes(interval.level)}
                      onChange={(e) => onPredictionIntervalChange(interval.level, e.target.checked)}
                    />
                    <span>{interval.level}% PI</span>
                  </label>
                ))}
              </div>
            )}
          </div>

          {/* None button */}
          <button
            className={`px-4 py-2 rounded ${
              selectedPredictionIntervals.length === 0
                ? 'bg-blue-600 text-white'
                : 'bg-[#5d636a] text-white hover:bg-blue-600'
            }`}
            onClick={() => {
              dispatch(updateSelectedPredictionIntervals([]));
              setIsPredictionIntervalDropdownOpen(false);
            }}
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
