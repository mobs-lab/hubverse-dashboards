import React, { useEffect, useState } from 'react';
import { useAppDispatch, useAppSelector } from '@/store/hooks';
import { selectSeasonOverviewModelAvailability } from '@/store/selectors/evaluationSelectors';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';

import {
  setMapSelectedModel,
  setMapSelectedScoringOption,
  setUseLogColorScale,
} from '@/store/data-slices/settings/SettingsSliceEvaluationSeasonOverview';

interface MapSelectorPanelProps {
  className?: string;
}

const MapSelectorPanel: React.FC<MapSelectorPanelProps> = ({ className }) => {
  const dispatch = useAppDispatch();
  const { mapSelectedModel, mapSelectedScoringOption, useLogColorScale } = useAppSelector(
    (state) => state.evaluationsSeasonOverviewSettings
  );
  // Get model availability info (sorted with available first, unavailable last)
  const { sortedModels: modelNames, availableModels, unavailableModels } = useAppSelector(selectSeasonOverviewModelAvailability);

  // Local state for model list expansion
  const [isModelListExpanded, setIsModelListExpanded] = useState(false);

  const scoringOptions = [
    { id: 'WIS/Baseline', label: 'WIS/Baseline' },
    { id: 'MAPE', label: 'MAPE' },
    { id: 'Coverage', label: 'Coverage' },
  ];
  
  // Model list preview/expansion
  const displayedModels = isModelListExpanded ? modelNames : modelNames.slice(0, 4);
  const hasMoreModels = modelNames.length > 4;

  const handleModelChange = (modelName: string) => {
    // Don't allow selection of unavailable models
    if (unavailableModels.has(modelName)) {
      return;
    }
    dispatch(setMapSelectedModel(modelName));
  };

  const handleScoringOptionChange = (option: 'WIS/Baseline' | 'MAPE' | 'Coverage') => {
    dispatch(setMapSelectedScoringOption(option));
  };

  const handleLogScaleToggle = () => {
    dispatch(setUseLogColorScale(!useLogColorScale));
  };

  // Effect: Auto-switch to first available model if current selection becomes unavailable
  useEffect(() => {
    if (unavailableModels.has(mapSelectedModel)) {
      const firstAvailableModel = modelNames.find(m => availableModels.has(m));
      if (firstAvailableModel) {
        dispatch(setMapSelectedModel(firstAvailableModel));
      }
    }
  }, [modelNames, availableModels, unavailableModels, mapSelectedModel, dispatch]);

  return (
    <div className={`bg-gray-800 bg-opacity-80 text-white p-3 rounded-t-md ${className}`}>
      <div className="mb-4">
        <h3 className="text-sm font-semibold mb-2">Scoring Metric</h3>
        <div className="space-y-1">
          {scoringOptions.map((option) => (
            <div key={option.id} className="flex items-center">
              <input
                type="radio"
                id={`scoring-${option.id}`}
                name="scoringOption"
                value={option.id}
                checked={mapSelectedScoringOption === option.id}
                onChange={() =>
                  handleScoringOptionChange(option.id as 'WIS/Baseline' | 'MAPE' | 'Coverage')
                }
                className="ml-1 mr-2"
              />
              <label htmlFor={`scoring-${option.id}`} className="text-xs cursor-pointer">
                {option.label}
              </label>
            </div>
          ))}
        </div>
      </div>

      <div className="mb-4">
        <div className="flex items-center">
          <input
            type="checkbox"
            id="log-scale-toggle"
            checked={useLogColorScale}
            onChange={handleLogScaleToggle}
            className="mr-2 ml-1"
          />
          <label htmlFor="log-scale-toggle" className="text-xs cursor-pointer">
            Log Color Scale
          </label>
        </div>
      </div>

      <div>
        <h3 className="text-sm font-semibold mb-2">Model</h3>
        <div className="relative">
          <div
            className={`space-y-1 overflow-y-auto pr-1 transition-all duration-300 ${
              isModelListExpanded ? 'max-h-64' : 'max-h-40'
            }`}
          >
            {displayedModels.map((model) => {
              const isUnavailable = unavailableModels.has(model);
              return (
                <div
                  key={model}
                  className={`flex items-center p-1 rounded ${
                    isUnavailable
                      ? 'opacity-50 cursor-not-allowed'
                      : 'hover:bg-gray-700 cursor-pointer'
                  }`}
                  onClick={() => handleModelChange(model)}
                  title={isUnavailable ? 'No data available for selected period' : ''}
                >
                  <div
                    className="w-4 h-4 rounded-sm mr-2 flex-shrink-0 border border-solid"
                    style={{
                      backgroundColor: mapSelectedModel === model && !isUnavailable ? 'silver' : 'transparent',
                      borderColor: 'silver',
                      opacity: isUnavailable ? 0.4 : 1,
                    }}
                  />
                  <span className={`text-xs truncate ${isUnavailable ? 'text-gray-500' : 'cursor-pointer'}`}>{model}</span>
                </div>
              );
            })}
          </div>

          {/* Expand/Collapse button */}
          {hasMoreModels && (
            <div className="relative mt-2">
              {!isModelListExpanded && (
                <div className="absolute -top-10 left-0 right-0 h-10 bg-gradient-to-t from-gray-800 to-transparent pointer-events-none" />
              )}
              <button
                className="w-full bg-gray-700 hover:bg-gray-600 text-white py-1 px-2 rounded text-xs transition-all duration-200 flex items-center justify-center gap-1"
                onClick={() => setIsModelListExpanded(!isModelListExpanded)}
              >
                {isModelListExpanded ? (
                  <>
                    <ChevronUpIcon className="h-3 w-3" />
                    <span>Less ({modelNames.length} total)</span>
                  </>
                ) : (
                  <>
                    <ChevronDownIcon className="h-3 w-3" />
                    <span>More ({modelNames.length - 4})</span>
                  </>
                )}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default MapSelectorPanel;
