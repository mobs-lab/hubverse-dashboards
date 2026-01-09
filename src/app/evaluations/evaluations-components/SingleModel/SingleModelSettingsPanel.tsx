"use client";

import React, { useState, useRef, useEffect, useMemo } from "react";

import { selectModelColorMap, selectModelNames, selectLocationData, selectHorizons, selectDateConstraints } from "@/store/selectors";
import { ForecastPeriodOption } from "@/types/domains/forecasting";

import SettingsStateMap from "@/shared-components/SettingsStateMap";

import { useAppDispatch, useAppSelector } from "@/store/hooks";

import {
  updateEvaluationScores,
  updateEvaluationSingleModelViewDateEnd,
  updateEvaluationSingleModelViewDateStart,
  updateEvaluationSingleModelViewHorizon,
  updateEvaluationSingleModelViewSelectedState,
  updateEvaluationsSingleModelViewModel,
  updateEvaluationsSingleModelViewSeasonId,
  setSingleModelSelectedTargetId,
} from "@/store/data-slices/settings/SettingsSliceEvaluationSingleModel";

import { Radio, Typography } from "@/styles/material-tailwind-wrapper";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/outline";

import InfoButton from "@/shared-components/InfoButton";
import Image from "next/image";
import { horizonSelectorsInfo } from "types/infobutton-content";
import SettingsStyledDatePicker from "@/forecasts/forecasts-components/SettingsStyledDatePicker";

const SingleModelSettingsPanel: React.FC = () => {
  /* Redux-Managed State Variables */
  const dispatch = useAppDispatch();
  const modelColorMap = useAppSelector(selectModelColorMap);
  const modelNames = useAppSelector(selectModelNames);
  const locationData = useAppSelector(selectLocationData);
  const availableHorizons = useAppSelector(selectHorizons);
  const { earliestDate, latestDate } = useAppSelector(selectDateConstraints);

  // Get UI customization from config
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);
  const horizonInfoConfig = uiConfig?.evaluationsPage?.infoButtons?.singleModelHorizonInfo;

  const [scoreOptions] = useState(["WIS/Baseline", "MAPE"]);
  
  // Local state for location dropdown
  const [locationSearchText, setLocationSearchText] = useState('');
  const [isLocationDropdownOpen, setIsLocationDropdownOpen] = useState(false);
  const locationDropdownRef = useRef<HTMLDivElement>(null);

  // Evaluation-specific state
  const {
    evaluationsSingleModelViewSelectedStateCode,
    evaluationsSingleModelViewModel,
    evaluationSingleModelViewHorizon,
    evaluationSingleModelViewScoresOption,
    evaluationsSingleModelViewDateStart,
    evaluationSingleModelViewDateEnd,
    evaluationsSingleModelViewSeasonId, // <-- Use seasonId from state
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
    dispatch(updateEvaluationsSingleModelViewModel(modelName));
  };

  // Horizon handler
  const onHorizonChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    dispatch(updateEvaluationSingleModelViewHorizon(Number(event.target.value)));
  };

  // Season selection handler (shared with forecast)
  const onSeasonSelectionChange = (seasonIdentifier: string) => {
    // The identifier could be a seasonId (for full range) or a label (for dynamic)
    const selectedOption = evaluationSingleModelViewSeasonOptions.find(
      (option) => option.forecastPeriodID === seasonIdentifier || option.timeValue === seasonIdentifier
    );

    if (selectedOption) {
      dispatch(updateEvaluationsSingleModelViewSeasonId(selectedOption.forecastPeriodID)); // <-- Dispatch seasonId
      dispatch(updateEvaluationSingleModelViewDateStart(selectedOption.startDate));
      dispatch(updateEvaluationSingleModelViewDateEnd(selectedOption.endDate));
    }
  };

  // Date selection handlers
  const onDateStartSelectionChange = (date: Date | null) => {
    if (date && date >= earliestDate && date <= evaluationSingleModelViewDateEnd) {
      dispatch(updateEvaluationSingleModelViewDateStart(date));
    } else {
      console.error('SingleModelSettingsPanel: Invalid dateStart selection');
    }
  };

  const onDateEndSelectionChange = (date: Date | null) => {
    if (date && date >= evaluationsSingleModelViewDateStart && date <= latestDate) {
      dispatch(updateEvaluationSingleModelViewDateEnd(date));
    } else {
      console.error('SingleModelSettingsPanel: Invalid dateEnd selection');
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
    const location = locationData.find((loc) => loc.locationCode === evaluationsSingleModelViewSelectedStateCode);
    return location ? location.locationName : '';
  }, [locationData, evaluationsSingleModelViewSelectedStateCode]);

  // Handle clicking outside location dropdown to close it
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (locationDropdownRef.current && !locationDropdownRef.current.contains(event.target as Node)) {
        setIsLocationDropdownOpen(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return (
    <div className='bg-mobs-lab-color-filterspane text-white fill-white flex flex-col h-full rounded-md overflow-hidden util-responsive-text-settings'>
      <div className='flex-grow nowrap overflow-y-auto p-4 util-no-sb-length'>
        <div className='mb-4 w-full overflow-ellipsis'>
          <Typography variant='h6' className='text-white' placeholder=''>
            Select Location
          </Typography>
          <div className='w-full'>
            <SettingsStateMap pageSelected='evaluations' />
          </div>

          {/* Combined Location Search and Dropdown Combobox */}
          <div ref={locationDropdownRef} className="relative w-full">
            <div className="relative">
              <input
                type="text"
                placeholder="Search or select location..."
                value={isLocationDropdownOpen || locationSearchText ? locationSearchText : selectedLocationName}
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
                        location.locationCode === evaluationsSingleModelViewSelectedStateCode ? 'bg-gray-700' : ''
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

        <div className='mb-2 w-full overflow-ellipsis'>
          <Typography variant='h6' className='text-white mb-2'>
            Models
          </Typography>
          <div className='space-y-2 h-full overflow-y-auto pr-1'>
            {modelNames.map((model) => (
              <label key={model} className='inline-flex items-center text-white hover:bg-gray-700 rounded cursor-pointer w-full'>
                <span
                  className='w-[1em] h-[1em] border-2 rounded-sm mr-2'
                  style={{
                    backgroundColor: evaluationsSingleModelViewModel === model ? modelColorMap[model] : "transparent",
                    borderColor: modelColorMap[model],
                  }}
                />
                <input
                  type='radio'
                  className='sr-only'
                  checked={evaluationsSingleModelViewModel === model}
                  onChange={() => onModelSelectionChange(model)}
                />
                <span className='ml-2 xs:text-sm'>{model}</span>
              </label>
            ))}
          </div>
        </div>

        <div className='mb-2 w-full'>
          <div className='flex flex-row flex-nowrap justify-start items-center gap-1'>
            <Typography variant='h6' className='text-white flex-shrink'>
              Horizon
            </Typography>
            <InfoButton 
              content={horizonInfoConfig?.content || horizonSelectorsInfo} 
              title={horizonInfoConfig?.title || "Forecast Horizons"}
            ></InfoButton>
          </div>
          <select
            value={evaluationSingleModelViewHorizon}
            onChange={onHorizonChange}
            className='text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 mt-2'>
            {availableHorizons.map((horizon) => (
              <option key={horizon} value={horizon}>
                {horizon}
              </option>
            ))}
          </select>
        </div>

        {/* Target Selection - only show if multiple targets available */}
        {availableTargets.length > 1 && (
          <div className='w-full mb-2'>
            <Typography variant='h6' className='text-white'>
              Target
            </Typography>
            <select
              value={selectedTargetId}
              onChange={(e) => onTargetSelectionChange(e.target.value)}
              className='text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2'>
              {availableTargets.map((target) => (
                <option key={target.targetId} value={target.targetId}>
                  {target.displayString}
                </option>
              ))}
            </select>
          </div>
        )}

        <div className='w-full mb-4'>
          <Typography variant='h6' className='text-white mb-1' placeholder=''>
            Season
          </Typography>
          <select
            id={"settings-panel-season-select"}
            value={evaluationsSingleModelViewSeasonId} // <-- Bind value to seasonId
            onChange={(e) => onSeasonSelectionChange(e.target.value)}
            className={
              "text-white border-[#5d636a] border-2 flex-wrap bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 overflow-ellipsis"
            }>
            {evaluationSingleModelViewSeasonOptions.map((option: ForecastPeriodOption) => (
              <option key={option.index} value={option.forecastPeriodID}>
                {option.displayString}
              </option>
            ))}
          </select>

          {/* Custom Date Range Pickers */}
          <div className='mt-2'>
            <Typography variant='h6' className='text-white mb-1' placeholder=''>
              Start Date
            </Typography>
            <SettingsStyledDatePicker
              value={evaluationsSingleModelViewDateStart}
              onChange={onDateStartSelectionChange}
              minDate={earliestDate}
              maxDate={evaluationSingleModelViewDateEnd}
              className='w-full border-[#5d636a] border-2 rounded-md'
            />
          </div>

          <div className='mt-2'>
            <Typography variant='h6' className='text-white mb-1' placeholder=''>
              End Date
            </Typography>
            <SettingsStyledDatePicker
              value={evaluationSingleModelViewDateEnd}
              onChange={onDateEndSelectionChange}
              minDate={evaluationsSingleModelViewDateStart}
              maxDate={latestDate}
              className='w-full border-[#5d636a] border-2 rounded-md'
            />
          </div>

          <button
            className='bg-[#5d636a] text-white rounded text-sm w-full mt-2 py-1'
            onClick={handleShowAllDates}>
            Show All
          </button>
        </div>
        <div className='w-full justify-stretch items-stretch mb-2'>
          <Typography variant='h6' className='text-white'>
            Score
          </Typography>
          <select
            value={evaluationSingleModelViewScoresOption}
            onChange={(e) => onScoreSelectionChange(e.target.value)}
            className='text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full p-2'>
            {scoreOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className='mt-auto p-2 border-t border-gray-700'>
        <Image src='/epistorm-logo.png' width={300} height={120} alt='Epistorm Logo' className='mx-auto' priority/>
      </div>
    </div>
  );
};

export default SingleModelSettingsPanel;
