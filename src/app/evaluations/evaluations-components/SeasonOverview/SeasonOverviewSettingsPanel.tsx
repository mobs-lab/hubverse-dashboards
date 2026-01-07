"use client";

import React, { useState, useRef, useEffect } from "react";

import { selectModelColorMap, selectModelNames, selectHorizons } from "@/store/selectors";

import { useAppDispatch, useAppSelector } from "@/store/hooks";
import {
  setEvaluationSeasonOverviewHorizon,
  updateSelectedDynamicTimePeriod,
  toggleModelSelection,
  selectAllModels,
  setSelectedTargetId,
} from "@/store/data-slices/settings/SettingsSliceEvaluationSeasonOverview";

import { Radio, Typography, List, ListItem, ListItemPrefix } from "@/styles/material-tailwind-wrapper";
import Image from "next/image";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/outline";

import { horizonSelectorsInfo } from "types/infobutton-content";
import InfoButton from "@/shared-components/InfoButton";

// Season Overview Settings Panel
export const SeasonOverviewSettings = () => {
  const dispatch = useAppDispatch();
  const { 
    evaluationSeasonOverviewHorizon, 
    selectedDynamicTimePeriod, 
    evalSOTimeRangeOptions, 
    evaluationSeasonOverviewSelectedModels,
    selectedTargetId,
    availableTargets,
  } = useAppSelector((state) => state.evaluationsSeasonOverviewSettings);
  const modelColorMap = useAppSelector(selectModelColorMap);
  const modelNames = useAppSelector(selectModelNames);
  const availableHorizons = useAppSelector(selectHorizons);

  // Get UI customization from config
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);
  const horizonInfoConfig = uiConfig?.evaluationsPage?.infoButtons?.overviewHorizonInfo;

  // Local state for horizon dropdown
  const [isHorizonDropdownOpen, setIsHorizonDropdownOpen] = useState(false);
  const horizonDropdownRef = useRef<HTMLDivElement>(null);

  // Handle clicking outside horizon dropdown to close it
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (horizonDropdownRef.current && !horizonDropdownRef.current.contains(event.target as Node)) {
        setIsHorizonDropdownOpen(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // Check if "Last 2 Weeks" is selected
  const isLastTwoWeeksSelected = selectedDynamicTimePeriod === "last-2-weeks";

  // Check if horizons 2 or 3 are selected
  const hasIncompatibleHorizonsSelected = evaluationSeasonOverviewHorizon.some((h) => h >= 2);

  const handleModelToggle = (modelName: string) => {
    dispatch(toggleModelSelection(modelName));
  };

  const handleSelectAllModels = () => {
    dispatch(selectAllModels());
  };

  // Horizon handler
  const onHorizonChange = (selected: number, checked: boolean) => {
    let newHorizons: number[] = [];
    if (checked) {
      // Adding a horizon
      if (selected >= 2 && isLastTwoWeeksSelected) {
        // If trying to select horizon 2 or 3 while "Last 2 Weeks" is selected,
        // show a warning or notification to user (or handle silently)
        console.debug("Cannot select horizon 2 or 3 with Last 2 Weeks period");
        return; // Prevent selection
      }
      // Add and sort the horizons
      newHorizons = [...evaluationSeasonOverviewHorizon, selected].sort((a, b) => a - b);
    } else {
      // Removing a horizon
      newHorizons = evaluationSeasonOverviewHorizon.filter((h) => h !== selected);
    }
    dispatch(setEvaluationSeasonOverviewHorizon(newHorizons));
  };

  // Aggregation period change handler
  const onDynamicTimePeriodChange = (tpName: string) => {
    // If selecting "Last 2 Weeks" but incompatible horizons are selected
    if (tpName === "last-2-weeks" && hasIncompatibleHorizonsSelected) {
      // Either show a warning to the user or automatically remove the incompatible horizons
      const compatibleHorizons = evaluationSeasonOverviewHorizon.filter((h) => h < 2);
      dispatch(setEvaluationSeasonOverviewHorizon(compatibleHorizons));
    }

    dispatch(updateSelectedDynamicTimePeriod(tpName));
  };

  // Determine if a time period should be disabled
  const isTimePeriodDisabled = (periodId: string) => {
    return periodId === "last-2-weeks" && hasIncompatibleHorizonsSelected;
  };

  // Determine if a horizon should be disabled
  const isHorizonDisabled = (horizon: number) => {
    return horizon >= 2 && isLastTwoWeeksSelected;
  };

  const handleShowAllHorizons = () => {
    if (isLastTwoWeeksSelected) {
      // Only show compatible horizons for Last 2 Weeks period
      const compatibleHorizons = availableHorizons.filter((h) => h < 2);
      dispatch(setEvaluationSeasonOverviewHorizon(compatibleHorizons));
    } else {
      // Show all available horizons from config
      dispatch(setEvaluationSeasonOverviewHorizon([...availableHorizons]));
    }
  };

  const handleDeselectAllHorizons = () => {
    dispatch(setEvaluationSeasonOverviewHorizon([]));
  };

  // Target selection handler
  const onTargetSelectionChange = (targetId: string) => {
    dispatch(setSelectedTargetId(targetId));
  };

  return (
    <div className='bg-mobs-lab-color-filterspane text-white fill-white flex flex-col h-full rounded-md overflow-hidden util-responsive-text-settings'>
      <div className='flex-grow nowrap overflow-y-auto p-4 util-no-sb-length'>
        <div className='mb-4 w-full overflow-ellipsis'>
          <Typography variant='h6' className='text-white mb-2' placeholder=''>
            Models
          </Typography>
          <div className='space-y-2 h-full overflow-y-auto pr-1'>
            {modelNames.map((model) => (
              <label key={model} className='inline-flex items-center text-white hover:bg-gray-700 rounded cursor-pointer w-full'>
                <span
                  className='w-[1em] h-[1em] border-2 rounded-sm mr-2'
                  style={{
                    backgroundColor: evaluationSeasonOverviewSelectedModels.includes(model) ? modelColorMap[model] : "transparent",
                    borderColor: modelColorMap[model],
                  }}
                />
                <input
                  type='checkbox'
                  className='sr-only'
                  checked={evaluationSeasonOverviewSelectedModels.includes(model)}
                  onChange={() => handleModelToggle(model)}
                />
                <span className='ml-2 xs:text-sm'>{model}</span>
              </label>
            ))}
          </div>
          <button
            onClick={handleSelectAllModels}
            className='w-full mt-2 bg-[#5d636a] hover:bg-blue-600 text-white py-1 px-2 rounded text-sm'>
            Select All
          </button>
        </div>

        <div className='mb-4 w-full'>
          <div className='flex flex-row flex-nowrap justify-start items-center gap-1 mb-2'>
            <Typography variant='h6' className='text-white flex-shrink' placeholder=''>
              Horizon
            </Typography>
            <InfoButton 
              content={horizonInfoConfig?.content || horizonSelectorsInfo} 
              title={horizonInfoConfig?.title || "Forecast Horizons"}
            ></InfoButton>
          </div>

          {/* Multi-select Dropdown for Horizons */}
          <div className="flex gap-2 items-stretch">
            <div ref={horizonDropdownRef} className="relative flex-1">
              <button
                type="button"
                onClick={() => setIsHorizonDropdownOpen(!isHorizonDropdownOpen)}
                className="text-white border-[#5d636a] border-2 bg-mobs-lab-color-filterspane rounded-md w-full py-2 px-2 flex items-center justify-between"
              >
                <span>
                  {evaluationSeasonOverviewHorizon.length === 0
                    ? "No horizons selected"
                    : evaluationSeasonOverviewHorizon.length === availableHorizons.length
                    ? "All horizons selected"
                    : `${evaluationSeasonOverviewHorizon.length} horizon${evaluationSeasonOverviewHorizon.length !== 1 ? "s" : ""} selected`}
                </span>
                {isHorizonDropdownOpen ? <ChevronUpIcon className="h-5 w-5" /> : <ChevronDownIcon className="h-5 w-5" />}
              </button>

              {/* Dropdown menu */}
              {isHorizonDropdownOpen && (
                <div className="absolute z-50 w-full mt-1 bg-mobs-lab-color-filterspane border-2 border-[#5d636a] rounded-md max-h-60 overflow-y-auto shadow-lg">
                  {availableHorizons.map((horizon) => {
                    const disabled = isHorizonDisabled(horizon);
                    return (
                      <label
                        key={horizon}
                        className={`flex items-center px-3 py-2 hover:bg-gray-700 cursor-pointer ${
                          disabled ? "opacity-50 cursor-not-allowed" : ""
                        }`}
                      >
                        <input
                          type="checkbox"
                          className="form-checkbox text-blue-600 mr-2 h-4 w-4"
                          checked={evaluationSeasonOverviewHorizon.includes(horizon)}
                          onChange={(e) => onHorizonChange(horizon, e.target.checked)}
                          disabled={disabled}
                        />
                        <span>{horizon}</span>
                      </label>
                    );
                  })}
                </div>
              )}
            </div>

            {/* Select All / None buttons */}
            <button
              className="px-4 py-2 rounded bg-[#5d636a] text-white hover:bg-blue-600"
              onClick={handleShowAllHorizons}
            >
              All
            </button>
            <button
              className="px-4 py-2 rounded bg-[#5d636a] text-white hover:bg-blue-600"
              onClick={handleDeselectAllHorizons}
            >
              None
            </button>
          </div>
        </div>

        {/* Target Selection - only show if multiple targets available */}
        {availableTargets.length > 1 && (
          <div className='mb-4 w-full'>
            <Typography variant='h6' className='text-white mb-1' placeholder=''>
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

        <div className='mb-2'>
          <Typography variant='h6' className='text-white mb-1' placeholder=''>
            Time Period
          </Typography>
          <List placeholder=''>
            {evalSOTimeRangeOptions.map((period) => (
              <ListItem
                key={period.name}
                className={`p-0 mb-1 ${isTimePeriodDisabled(period.name) ? "opacity-50" : ""}`}
                disabled={isTimePeriodDisabled(period.name)}
                placeholder=''>
                <label htmlFor={`period-${period.name}`} className='flex w-full cursor-pointer items-center py-1 px-0'>
                  <ListItemPrefix className='mr-2' placeholder=''>
                    <Radio
                      name='seasonAggregationRadioBtn'
                      id={`period-${period.name}`}
                      value={period.displayString}
                      onChange={() => onDynamicTimePeriodChange(period.name)}
                      checked={selectedDynamicTimePeriod === period.name}
                      disabled={isTimePeriodDisabled(period.name)}
                      className='hover:before:opacity-0 border-white'
                      color='blue-gray'
                      ripple={false}
                      crossOrigin=''
                      containerProps={{
                        className: "p-0",
                      }}
                    />
                  </ListItemPrefix>
                  <Typography className='font-medium text-white' placeholder=''>
                    {period.displayString}
                    {period.isDynamic && period.name === selectedDynamicTimePeriod && period.subDisplayValue && (
                      <span className='text-sm ml-1 opacity-80'>{period.subDisplayValue}</span>
                    )}
                  </Typography>
                </label>
              </ListItem>
            ))}
          </List>
        </div>
      </div>

      <div className='mt-auto p-2 border-t border-gray-700'>
        <Image src='/epistorm-logo.png' width={300} height={120} alt='Epistorm Logo' className='mx-auto' priority />
      </div>
    </div>
  );
};
