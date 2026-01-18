'use client';

import React, { useState, useRef, useEffect } from 'react';

import { selectModelColorMap, selectHorizons } from '@/store/selectors';
import { selectSeasonOverviewModelAvailability } from '@/store/selectors/evaluationSelectors';

import { useAppDispatch, useAppSelector } from '@/store/hooks';
import {
  setEvaluationSeasonOverviewHorizon,
  updateSelectedEvalOverviewTimePeriod,
  toggleModelSelection,
  selectAllModels,
  setSelectedTargetId,
} from '@/store/data-slices/settings/SettingsSliceEvaluationSeasonOverview';

import {
  Radio,
  Typography,
  List,
  ListItem,
  ListItemPrefix,
} from '@/styles/material-tailwind-wrapper';
import Image from 'next/image';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';

import { horizonSelectorsInfo } from 'types/infobutton-content';
import InfoButton from '@/shared-components/InfoButton';

// Season Overview Settings Panel
export const SeasonOverviewSettings = () => {
  const dispatch = useAppDispatch();
  const {
    evaluationSeasonOverviewHorizon,
    selectedEvalOverviewTimePeriod,
    evalSOTimeRangeOptions,
    evaluationSeasonOverviewSelectedModels,
    selectedTargetId,
    availableTargets,
  } = useAppSelector((state) => state.evaluationsSeasonOverviewSettings);
  const modelColorMap = useAppSelector(selectModelColorMap);
  const availableHorizons = useAppSelector(selectHorizons);

  // Get model availability info (sorted with available first, unavailable last)
  const {
    sortedModels: modelNames,
    availableModels,
    unavailableModels,
  } = useAppSelector(selectSeasonOverviewModelAvailability);

  // Get UI customization from config
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);
  const horizonInfoConfig = uiConfig?.evaluationsPage?.infoButtons?.overviewHorizonInfo;

  // Local state for horizon dropdown
  const [isHorizonDropdownOpen, setIsHorizonDropdownOpen] = useState(false);
  const horizonDropdownRef = useRef<HTMLDivElement>(null);
  
  // Local state for model list expansion
  const [isModelListExpanded, setIsModelListExpanded] = useState(false);

  // Handle clicking outside horizon dropdown to close it
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        horizonDropdownRef.current &&
        !horizonDropdownRef.current.contains(event.target as Node)
      ) {
        setIsHorizonDropdownOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Effect: Auto-deselect unavailable models when period changes
  useEffect(() => {
    const selectedUnavailableModels = evaluationSeasonOverviewSelectedModels.filter((m) =>
      unavailableModels.has(m)
    );
    if (selectedUnavailableModels.length > 0) {
      // Remove unavailable models from selection
      const newSelectedModels = evaluationSeasonOverviewSelectedModels.filter((m) =>
        availableModels.has(m)
      );
      // Update selection to only include available models
      newSelectedModels.forEach((model) => {
        if (!evaluationSeasonOverviewSelectedModels.includes(model)) {
          dispatch(toggleModelSelection(model));
        }
      });
      // Deselect unavailable models
      selectedUnavailableModels.forEach((model) => {
        dispatch(toggleModelSelection(model));
      });
    }
  }, [selectedEvalOverviewTimePeriod]);

  const handleModelToggle = (modelName: string) => {
    // Don't allow toggling unavailable models
    if (unavailableModels.has(modelName)) {
      return;
    }
    dispatch(toggleModelSelection(modelName));
  };

  const handleToggleAllModels = () => {
    // Only work with models that have data available
    const availableModelsList = modelNames.filter((m) => availableModels.has(m));
    
    // Check if all available models are currently selected
    const allAvailableSelected = availableModelsList.every((m) =>
      evaluationSeasonOverviewSelectedModels.includes(m)
    );
    
    if (allAvailableSelected) {
      // Deselect all
      dispatch(selectAllModels([]));
    } else {
      // Select all available
      dispatch(selectAllModels(availableModelsList));
    }
  };
  
  // Determine toggle button text
  const availableModelsList = modelNames.filter((m) => availableModels.has(m));
  const allAvailableSelected =
    availableModelsList.every((m) => evaluationSeasonOverviewSelectedModels.includes(m)) &&
    availableModelsList.length > 0;
  const toggleButtonText = allAvailableSelected ? 'Toggle All Models Off' : 'Toggle All Models On';
  
  // Model list preview/expansion
  const displayedModels = isModelListExpanded ? modelNames : modelNames.slice(0, 4);
  const hasMoreModels = modelNames.length > 4;

  // Horizon handler
  const onHorizonChange = (selected: number, checked: boolean) => {
    let newHorizons: number[] = [];
    if (checked) {
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
    dispatch(updateSelectedEvalOverviewTimePeriod(tpName));
  };

  const handleShowAllHorizons = () => {
    // Show all available horizons from config
    dispatch(setEvaluationSeasonOverviewHorizon([...availableHorizons]));
  };

  const handleDeselectAllHorizons = () => {
    dispatch(setEvaluationSeasonOverviewHorizon([]));
  };

  // Target selection handler
  const onTargetSelectionChange = (targetId: string) => {
    dispatch(setSelectedTargetId(targetId));
  };

  return (
    <div className="bg-mobs-lab-color-filterspane text-white fill-white flex flex-col h-full rounded-md overflow-hidden util-responsive-text-settings">
      <div className="flex-grow nowrap overflow-y-auto p-4 util-no-sb-length">
        <div className="mb-4 w-full overflow-ellipsis">
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
                    title={isUnavailable ? 'No data available for selected period' : ''}
                  >
                    <span
                      className="w-[1em] h-[1em] border-2 rounded-sm mr-2"
                      style={{
                        backgroundColor:
                          evaluationSeasonOverviewSelectedModels.includes(model) && !isUnavailable
                            ? modelColorMap[model]
                            : 'transparent',
                        borderColor: modelColorMap[model],
                        opacity: isUnavailable ? 0.4 : 1,
                      }}
                    />
                    <input
                      type="checkbox"
                      className="sr-only"
                      checked={evaluationSeasonOverviewSelectedModels.includes(model)}
                      disabled={isUnavailable}
                      onChange={() => handleModelToggle(model)}
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
          
          {/* Toggle All Models button */}
          <button
            onClick={handleToggleAllModels}
            className="w-full mt-2 bg-[#5d636a] hover:bg-blue-600 text-white py-1 px-2 rounded text-sm transition-colors"
          >
            {toggleButtonText}
          </button>
        </div>

        <div className="mb-4 w-full">
          <div className="flex flex-row flex-nowrap justify-start items-center gap-1 mb-2">
            <Typography variant="h6" className="text-white flex-shrink" placeholder="">
              Horizon
            </Typography>
            <InfoButton
              content={horizonInfoConfig?.content || horizonSelectorsInfo}
              title={horizonInfoConfig?.title || 'Forecast Horizons'}
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
                    ? 'No horizons selected'
                    : evaluationSeasonOverviewHorizon.length === availableHorizons.length
                      ? 'All horizons selected'
                      : `${evaluationSeasonOverviewHorizon.length} horizon${evaluationSeasonOverviewHorizon.length !== 1 ? 's' : ''} selected`}
                </span>
                {isHorizonDropdownOpen ? (
                  <ChevronUpIcon className="h-5 w-5" />
                ) : (
                  <ChevronDownIcon className="h-5 w-5" />
                )}
              </button>

              {/* Dropdown menu */}
              {isHorizonDropdownOpen && (
                <div className="absolute z-50 w-full mt-1 bg-mobs-lab-color-filterspane border-2 border-[#5d636a] rounded-md max-h-60 overflow-y-auto shadow-lg">
                  {availableHorizons.map((horizon) => {
                    return (
                      <label
                        key={horizon}
                        className={`flex items-center px-3 py-2 hover:bg-gray-700 cursor-pointer`}
                      >
                        <input
                          type="checkbox"
                          className="form-checkbox text-blue-600 mr-2 h-4 w-4"
                          checked={evaluationSeasonOverviewHorizon.includes(horizon)}
                          onChange={(e) => onHorizonChange(horizon, e.target.checked)}
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
          <div className="mb-4 w-full">
            <Typography variant="h6" className="text-white mb-1" placeholder="">
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

        <div className="mb-2">
          <Typography variant="h6" className="text-white mb-1" placeholder="">
            Time Period
          </Typography>
          <List placeholder="">
            {evalSOTimeRangeOptions.map((period) => (
              <ListItem key={period.name} className={`p-0 mb-1`} placeholder="">
                <label
                  htmlFor={`period-${period.name}`}
                  className="flex w-full cursor-pointer items-center py-1 px-0"
                >
                  <ListItemPrefix className="mr-2" placeholder="">
                    <Radio
                      name="seasonAggregationRadioBtn"
                      id={`period-${period.name}`}
                      value={period.displayString}
                      onChange={() => onDynamicTimePeriodChange(period.name)}
                      checked={selectedEvalOverviewTimePeriod === period.name}
                      className="hover:before:opacity-0 border-white"
                      color="blue-gray"
                      ripple={false}
                      crossOrigin=""
                      containerProps={{
                        className: 'p-0',
                      }}
                    />
                  </ListItemPrefix>
                  <Typography className="font-medium text-white" placeholder="">
                    {period.displayString}
                    {period.isDynamic &&
                      period.name === selectedEvalOverviewTimePeriod &&
                      period.subDisplayValue && (
                        <span className="text-sm ml-1 opacity-80">{period.subDisplayValue}</span>
                      )}
                  </Typography>
                </label>
              </ListItem>
            ))}
          </List>
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
