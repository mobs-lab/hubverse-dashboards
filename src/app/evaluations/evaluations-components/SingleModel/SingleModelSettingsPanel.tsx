'use client';

import React, { useEffect, useMemo, useRef, useState } from 'react';

import {
  selectHorizons,
  selectLocationData,
  selectModelColorMap,
} from '@/store/selectors';
import { 
  selectSingleModelDateConstraints,
  selectSingleModelAvailability,
} from '@/store/selectors/evaluationSelectors';
import { ForecastPeriodOption } from '@/types/domains/forecasting';

import SettingsStateMap from '@/shared-components/SettingsStateMap';

import { useAppDispatch, useAppSelector } from '@/store/hooks';

import {
  setSingleModelSelectedTargetId,
  updateEvaluationScores,
  updateEvaluationSingleModelViewDateEnd,
  updateEvaluationSingleModelViewDateStart,
  updateEvaluationSingleModelViewHorizon,
  updateEvaluationSingleModelViewSelectedState,
  updateEvaluationsSingleModelViewModel,
  updateEvaluationsSingleModelViewSeasonId,
} from '@/store/data-slices/settings/SettingsSliceEvaluationSingleModel';

import { Typography } from '@/styles/material-tailwind-wrapper';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';

import SettingsStyledDatePicker from '@/forecasts/forecasts-components/SettingsStyledDatePicker';
import InfoButton from '@/shared-components/InfoButton';
import Image from 'next/image';
import { horizonSelectorsInfo } from 'types/infobutton-content';

const SingleModelSettingsPanel: React.FC = () => {
  /* Redux-Managed State Variables */
  const dispatch = useAppDispatch();
  const modelColorMap = useAppSelector(selectModelColorMap);
  const locationData = useAppSelector(selectLocationData);
  const availableHorizons = useAppSelector(selectHorizons);
  // Use target-specific date constraints based on currently selected target
  const { earliestDate, latestDate } = useAppSelector(selectSingleModelDateConstraints);
  
  // Get model availability info (sorted with available first, unavailable last)
  const { sortedModels: modelNames, availableModels, unavailableModels } = useAppSelector(selectSingleModelAvailability);

  // Get UI customization from config
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);
  const horizonInfoConfig = uiConfig?.evaluationsPage?.infoButtons?.singleModelHorizonInfo;

  const [scoreOptions] = useState(['WIS/Baseline', 'MAPE']);

  // Local state for location dropdown
  const [locationSearchText, setLocationSearchText] = useState('');
  const [isLocationDropdownOpen, setIsLocationDropdownOpen] = useState(false);
  const locationDropdownRef = useRef<HTMLDivElement>(null);
  
  // Local state for model list expansion
  const [isModelListExpanded, setIsModelListExpanded] = useState(false);

  // Evaluation-specific state
  const {
    evaluationsSingleModelViewSelectedStateCode,
    evaluationsSingleModelViewModel,
    evaluationSingleModelViewHorizon,
    evaluationSingleModelViewScoresOption,
    evaluationsSingleModelViewDateStart,
    evaluationSingleModelViewDateEnd,
    evaluationsSingleModelViewSeasonId,
    evaluationSingleModelViewSeasonOptions,
    selectedTargetId,
    availableTargets,
  } = useAppSelector((state) => state.evaluationsSingleModelSettings);

  // State selection handlers (reused from forecast)
  const onStateSelectionChange = (stateNum: string) => {
    const selectedState = locationData.find((state) => state.locationCode === stateNum);
    if (selectedState) {
      dispatch(
        updateEvaluationSingleModelViewSelectedState({
          stateName: selectedState.locationName,
          stateNum: selectedState.locationCode,
        })
      );
    }
  };

  // Model selection handler (single model only)
  const onModelSelectionChange = (modelName: string) => {
    // Don't allow selection of unavailable models
    if (unavailableModels.has(modelName)) {
      return;
    }
    dispatch(updateEvaluationsSingleModelViewModel(modelName));
  };

  // Horizon handler
  const onHorizonChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    dispatch(updateEvaluationSingleModelViewHorizon(Number(event.target.value)));
  };

  // Effect: Auto-switch to first available model if current selection becomes unavailable
  useEffect(() => {
    if (unavailableModels.has(evaluationsSingleModelViewModel)) {
      // Find first available model
      const firstAvailableModel = modelNames.find(m => availableModels.has(m));
      if (firstAvailableModel && firstAvailableModel !== evaluationsSingleModelViewModel) {
        dispatch(updateEvaluationsSingleModelViewModel(firstAvailableModel));
      }
    }
  }, [
    evaluationsSingleModelViewDateStart,
    evaluationSingleModelViewDateEnd,
    unavailableModels,
    availableModels,
    modelNames,
    evaluationsSingleModelViewModel,
    dispatch,
  ]);

  // Season selection handler (shared with forecast)
  const onSeasonSelectionChange = (seasonIdentifier: string) => {
    const selectedOption = evaluationSingleModelViewSeasonOptions.find(
      (option) =>
        option.forecastPeriodID === seasonIdentifier
    );

    if (selectedOption) {
      dispatch(updateEvaluationsSingleModelViewSeasonId(selectedOption.forecastPeriodID));
      dispatch(updateEvaluationSingleModelViewDateStart(selectedOption.startDate));
      dispatch(updateEvaluationSingleModelViewDateEnd(selectedOption.endDate));
    }
  };

  // Date selection handlers
  const onDateStartSelectionChange = (date: Date | null) => {
    if (date && date >= earliestDate && date <= evaluationSingleModelViewDateEnd) {
      dispatch(updateEvaluationSingleModelViewDateStart(date));
    }
  };

  const onDateEndSelectionChange = (date: Date | null) => {
    if (date && date >= evaluationsSingleModelViewDateStart && date <= latestDate) {
      dispatch(updateEvaluationSingleModelViewDateEnd(date));
    }
  };

  const handleShowAllDates = () => {
    dispatch(updateEvaluationSingleModelViewDateStart(earliestDate));
    dispatch(updateEvaluationSingleModelViewDateEnd(latestDate));
  };

  // Add handler
  const onScoreSelectionChange = (value: string) => {
    dispatch(updateEvaluationScores(value));
  };

  // Target selection handler
  const onTargetSelectionChange = (targetId: string) => {
    dispatch(setSingleModelSelectedTargetId(targetId));
  };

  // Fuzzy search for locations
  const filteredLocations = useMemo(() => {
    if (!locationSearchText) return locationData;
    const searchLower = locationSearchText.toLowerCase();
    return locationData.filter(
      (loc) =>
        loc.locationName.toLowerCase().includes(searchLower) ||
        loc.locationCode.toLowerCase().includes(searchLower) ||
        (loc.locationNameAlt && loc.locationNameAlt.toLowerCase().includes(searchLower))
    );
  }, [locationData, locationSearchText]);

  // Get selected location name for display
  const selectedLocationName = useMemo(() => {
    const location = locationData.find(
      (loc) => loc.locationCode === evaluationsSingleModelViewSelectedStateCode
    );
    return location ? location.locationName : '';
  }, [locationData, evaluationsSingleModelViewSelectedStateCode]);
  
  // Model list preview/expansion
  const displayedModels = isModelListExpanded ? modelNames : modelNames.slice(0, 4);
  const hasMoreModels = modelNames.length > 4;

  // Handle clicking outside location dropdown to close it
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        locationDropdownRef.current &&
        !locationDropdownRef.current.contains(event.target as Node)
      ) {
        setIsLocationDropdownOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div className="bg-mobs-lab-color-filterspane text-white fill-white flex flex-col h-full rounded-md overflow-hidden util-responsive-text-settings">
      <div className="flex-grow nowrap overflow-y-auto p-4 util-no-sb-length">
        <div className="mb-4 w-full overflow-ellipsis">
          <Typography variant="h6" className="text-white" placeholder="">
            Select Location
          </Typography>
          <div className="w-full">
            <SettingsStateMap pageSelected="evaluations" />
          </div>

          {/* Combined Location Search and Dropdown Combobox */}
          <div ref={locationDropdownRef} className="relative w-full">
            <div className="relative">
              <input
                type="text"
                placeholder="Search or select location..."
                value={
                  isLocationDropdownOpen || locationSearchText
                    ? locationSearchText
                    : selectedLocationName
                }
                onChange={(e) => {
                  setLocationSearchText(e.target.value);
                  if (!isLocationDropdownOpen) {
                    setIsLocationDropdownOpen(true);
                  }
                }}
                onFocus={() => {
                  setLocationSearchText('');
                  setIsLocationDropdownOpen(true);
                }}
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
                  filteredLocations.map((location) => (
                    <div
                      key={location.locationCode}
                      onClick={() => {
                        onStateSelectionChange(location.locationCode);
                        setLocationSearchText('');
                        setIsLocationDropdownOpen(false);
                      }}
                      className={`px-3 py-2 cursor-pointer hover:bg-gray-700 ${
                        location.locationCode === evaluationsSingleModelViewSelectedStateCode
                          ? 'bg-gray-700'
                          : ''
                      }`}
                    >
                      {location.locationName}
                    </div>
                  ))
                ) : (
                  <div className="px-3 py-2 text-gray-400">No locations found</div>
                )}
              </div>
            )}
          </div>
        </div>

        <div className="mb-2 w-full overflow-ellipsis">
          <Typography variant="h6" className="text-white mb-2" placeholder="">
            Models
          </Typography>
          <div className="relative">
            <div
              className={`space-y-2 overflow-y-auto pr-1 transition-all duration-300 ${
                isModelListExpanded ? 'max-h-96' : 'max-h-40'
              }`}
            >
              {displayedModels.map((model) => {
                const isUnavailable = unavailableModels.has(model);
                return (
                  <label
                    key={model}
                    className={`inline-flex items-center rounded w-full ${
                      isUnavailable
                        ? 'text-gray-500 cursor-not-allowed opacity-50'
                        : 'text-white hover:bg-gray-700 cursor-pointer'
                    }`}
                    title={isUnavailable ? 'No data available for selected date range' : ''}
                  >
                    <span
                      className="w-[1em] h-[1em] border-2 rounded-sm mr-2"
                      style={{
                        backgroundColor:
                          evaluationsSingleModelViewModel === model && !isUnavailable
                            ? modelColorMap[model]
                            : 'transparent',
                        borderColor: modelColorMap[model],
                        opacity: isUnavailable ? 0.4 : 1,
                      }}
                    />
                    <input
                      type="radio"
                      className="sr-only"
                      checked={evaluationsSingleModelViewModel === model}
                      disabled={isUnavailable}
                      onChange={() => onModelSelectionChange(model)}
                    />
                    <span className="ml-2 xs:text-sm">{model}</span>
                  </label>
                );
              })}
            </div>

            {/* Expand/Collapse button */}
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
        </div>

        <div className="mb-2 w-full">
          <div className="flex flex-row flex-nowrap justify-start items-center gap-1">
            <Typography variant="h6" className="text-white flex-shrink" placeholder="">
              Horizon
            </Typography>
            <InfoButton
              content={horizonInfoConfig?.content || horizonSelectorsInfo}
              title={horizonInfoConfig?.title || 'Forecast Horizons'}
            ></InfoButton>
          </div>
          <select
            value={evaluationSingleModelViewHorizon}
            onChange={onHorizonChange}
            className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 mt-2"
          >
            {availableHorizons.map((horizon) => (
              <option key={horizon} value={horizon}>
                {horizon}
              </option>
            ))}
          </select>
        </div>

        {/* Target Selection - only show if multiple targets available */}
        {availableTargets.length > 1 && (
          <div className="w-full mb-2">
            <Typography variant="h6" className="text-white" placeholder="">
              Target
            </Typography>
            <select
              value={selectedTargetId}
              onChange={(e) => onTargetSelectionChange(e.target.value)}
              className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2"
            >
              {availableTargets.map((target) => (
                <option key={target.targetId} value={target.targetId}>
                  {target.displayString}
                </option>
              ))}
            </select>
          </div>
        )}

        <div className="w-full mb-4">
          <Typography variant="h6" className="text-white mb-1" placeholder="">
            Forecast Period
          </Typography>
          <select
            id={'settings-panel-season-select'}
            value={evaluationsSingleModelViewSeasonId}
            onChange={(e) => onSeasonSelectionChange(e.target.value)}
            className={
              'text-white border-[#5d636a] border-2 flex-wrap bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 overflow-ellipsis'
            }
          >
            {evaluationSingleModelViewSeasonOptions.map((option: ForecastPeriodOption) => (
              <option key={option.index} value={option.forecastPeriodID}>
                {option.displayString}
              </option>
            ))}
          </select>

          {/* Custom Date Range Pickers */}
          <div className="mt-2">
            <Typography variant="h6" className="text-white mb-1" placeholder="">
              Start Date
            </Typography>
            <SettingsStyledDatePicker
              value={evaluationsSingleModelViewDateStart}
              onChange={onDateStartSelectionChange}
              minDate={earliestDate}
              maxDate={evaluationSingleModelViewDateEnd}
              className="w-full border-[#5d636a] border-2 rounded-md"
            />
          </div>

          <div className="mt-2">
            <Typography variant="h6" className="text-white mb-1" placeholder="">
              End Date
            </Typography>
            <SettingsStyledDatePicker
              value={evaluationSingleModelViewDateEnd}
              onChange={onDateEndSelectionChange}
              minDate={evaluationsSingleModelViewDateStart}
              maxDate={latestDate}
              className="w-full border-[#5d636a] border-2 rounded-md"
            />
          </div>

          <button
            className="bg-[#5d636a] text-white rounded text-sm w-full mt-2 py-1"
            onClick={handleShowAllDates}
          >
            Show All
          </button>
        </div>
        <div className="w-full justify-stretch items-stretch mb-2">
          <Typography variant="h6" className="text-white" placeholder="">
            Score
          </Typography>
          <select
            value={evaluationSingleModelViewScoresOption}
            onChange={(e) => onScoreSelectionChange(e.target.value)}
            className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full p-2"
          >
            {scoreOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="mt-auto p-2 border-t border-gray-700">
        <Image
          src="/epistorm-logo.png"
          width={300}
          height={120}
          alt="Epistorm Logo"
          className="mx-auto"
          priority
        />
      </div>
    </div>
  );
};

export default SingleModelSettingsPanel;
