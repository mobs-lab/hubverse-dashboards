// src/app/store/forecastSettingsSlice.ts
import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { EvaluationSeasonOverviewTimeRangeOption } from '@/types/domains/evaluations';

interface EvaluationsSeasonOverviewSettingsState {
  /* Target Selection */
  selectedTargetId: string;
  availableTargets: { targetId: string; displayString: string }[];

  /* Model Related*/
  evaluationSeasonOverviewHorizon: number[]; //how many weeks ahead from reference date (matching surveillance week's number) should we look for as target_end_date in predictions to draw the intervals
  evaluationSeasonOverviewSelectedModels: string[];

  /* Time Range Related */
  evalSOTimeRangeOptions: EvaluationSeasonOverviewTimeRangeOption[];
  selectedDynamicTimePeriod: string;

  /* Map selection panel related */
  mapSelectedModel: string;
  mapSelectedScoringOption: 'WIS/Baseline' | 'MAPE' | 'Coverage';
  useLogColorScale: boolean;

  /* For Aggregated Box Plots, toggling linear/log mode display */
  wisChartScaleType: 'linear' | 'log';
  mapeChartScaleType: 'linear' | 'log';
}

const initialState: EvaluationsSeasonOverviewSettingsState = {
  /* Target Defaults */
  selectedTargetId: '',
  availableTargets: [],

  /* Model Defaults*/
  evaluationSeasonOverviewHorizon: [0, 1],
  // evaluationSeasonOverviewSelectedModels: [...modelNames],
  evaluationSeasonOverviewSelectedModels: [],

  /* Time Range Defaults*/
  evalSOTimeRangeOptions: [],
  selectedDynamicTimePeriod: '',

  mapSelectedModel: 'null', // Set default to first model
  mapSelectedScoringOption: 'WIS/Baseline', // Default scoring option. TODO: Make configurable via config.yaml
  useLogColorScale: false,

  wisChartScaleType: 'linear',
  mapeChartScaleType: 'linear',
};

const evaluationsSeasonOverviewSettingsSlice = createSlice({
  name: 'evaluations-season-overview-settings-slice',
  initialState,
  reducers: {
    // Initialize evaluation settings from config
    initializeEvaluationSeasonOverviewSettings: (
      state,
      action: PayloadAction<{
        models: string[];
        timeRangeOptions: EvaluationSeasonOverviewTimeRangeOption[];
        defaultModel?: string;
        targets?: { targetId: string; displayString: string }[];
        defaultTargetId?: string;
        defaultPeriodId?: string;
        horizons?: number[];
      }>
    ) => {
      const { models, timeRangeOptions, defaultModel, targets, defaultTargetId, defaultPeriodId, horizons } = action.payload;
      
      // Initialize all models as selected
      state.evaluationSeasonOverviewSelectedModels = models;
      
      // Set time range options
      state.evalSOTimeRangeOptions = timeRangeOptions;
      
      // Set default time period
      if (defaultPeriodId) {
        state.selectedDynamicTimePeriod = defaultPeriodId;
      } else if (timeRangeOptions.length > 0) {
        state.selectedDynamicTimePeriod = timeRangeOptions[0].name;
      }
      
      // Set default map model to first model if not specified
      state.mapSelectedModel = defaultModel || models[0] || 'null';
      
      // Set available targets and default selection
      if (targets && targets.length > 0) {
        state.availableTargets = targets;
        state.selectedTargetId = defaultTargetId || targets[0].targetId;
      }
      
      // Set default horizons if provided
      if (horizons && horizons.length > 0) {
        // Default to first two horizons or all if less than 2
        state.evaluationSeasonOverviewHorizon = horizons.slice(0, Math.min(2, horizons.length));
      }
    },
    
    // Set selected target
    setSelectedTargetId: (state, action: PayloadAction<string>) => {
      state.selectedTargetId = action.payload;
    },
    
    setEvaluationSeasonOverviewHorizon: (state, action: PayloadAction<number[]>) => {
      state.evaluationSeasonOverviewHorizon = action.payload;
    },
    updateEvaluationSeasonOverviewTimeRangeOptions: (
      state,
      action: PayloadAction<EvaluationSeasonOverviewTimeRangeOption[]>
    ) => {
      state.evalSOTimeRangeOptions = action.payload;
    },
    updateSelectedDynamicTimePeriod: (state, action: PayloadAction<string>) => {
      state.selectedDynamicTimePeriod = action.payload;
    },
    setMapSelectedModel: (state, action: PayloadAction<string>) => {
      state.mapSelectedModel = action.payload;
    },
    setMapSelectedScoringOption: (
      state,
      action: PayloadAction<'WIS/Baseline' | 'MAPE' | 'Coverage'>
    ) => {
      state.mapSelectedScoringOption = action.payload;
    },
    setUseLogColorScale: (state, action: PayloadAction<boolean>) => {
      state.useLogColorScale = action.payload;
    },
    toggleModelSelection: (state, action: PayloadAction<string>) => {
      const modelName = action.payload;
      const index = state.evaluationSeasonOverviewSelectedModels.indexOf(modelName);
      if (index === -1) {
        // Model not currently selected, add it
        state.evaluationSeasonOverviewSelectedModels.push(modelName);
      } else {
        // Model currently selected, remove it
        state.evaluationSeasonOverviewSelectedModels.splice(index, 1);
      }
    },
    selectAllModels: (state) => {
      state.evaluationSeasonOverviewSelectedModels = [];
    },
    setWisChartScaleType: (state, action: PayloadAction<'linear' | 'log'>) => {
      state.wisChartScaleType = action.payload;
    },
    setMapeChartScaleType: (state, action: PayloadAction<'linear' | 'log'>) => {
      state.mapeChartScaleType = action.payload;
    },
  },
});

export const {
  initializeEvaluationSeasonOverviewSettings,
  setSelectedTargetId,
  setEvaluationSeasonOverviewHorizon,
  updateEvaluationSeasonOverviewTimeRangeOptions,
  updateSelectedDynamicTimePeriod,
  setMapSelectedModel,
  setMapSelectedScoringOption,
  setUseLogColorScale,
  toggleModelSelection,
  selectAllModels,
  setWisChartScaleType,
  setMapeChartScaleType,
} = evaluationsSeasonOverviewSettingsSlice.actions;

export default evaluationsSeasonOverviewSettingsSlice.reducer;
