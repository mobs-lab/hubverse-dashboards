import { ForecastPeriod } from '@/types/domains/forecasting';
import { createSlice, PayloadAction } from '@reduxjs/toolkit';

interface ForecastSettingsState {
  // Location selection
  selectedLocationCode: string;

  // Model selection
  selectedModels: string[];

  // Target selection
  selectedTargetIds: string[];

  // Horizon selection
  selectedHorizons: number[];

  // Time filtering
  selectedForecastPeriod: ForecastPeriod | null;
  timeFilterRangeStart: Date;
  timeFilterRangeEnd: Date;

  // Visualization settings
  yAxisScale: 'linear' | 'log';

  // Prediction Interval settings
  selectedPredictionIntervals: string[];

  // Historical data mode
  historicalTargetDataMode: boolean;
  selectedHistoricalAsOfDate: string | null;

  // User interaction state
  userSelectedDate: Date;
}

// Minimal initial state - will be populated from config
const initialState: ForecastSettingsState = {
  selectedLocationCode: 'US',
  selectedModels: [],
  selectedTargetIds: [],
  selectedHorizons: [],
  selectedForecastPeriod: null,
  timeFilterRangeStart: new Date(),
  timeFilterRangeEnd: new Date(),
  yAxisScale: 'linear',
  selectedPredictionIntervals: [],
  historicalTargetDataMode: false,
  selectedHistoricalAsOfDate: null,
  userSelectedDate: new Date(),
};

const forecastSettingsSlice = createSlice({
  name: 'forecastSettings',
  initialState,
  reducers: {
    // Initialize settings from config
    initializeForecastSettings: (
      state,
      action: PayloadAction<{
        locationCode?: string;
        models: string[];
        targets: string[];
        horizons: number[];
        forecastPeriod: ForecastPeriod;
        predictionIntervals: string[];
        selectedDate?: Date;
      }>
    ) => {
      const {
        locationCode,
        models,
        targets,
        horizons,
        forecastPeriod,
        predictionIntervals,
        selectedDate,
      } = action.payload;

      state.selectedLocationCode = locationCode || 'US';
      state.selectedModels = models;
      state.selectedTargetIds = targets;
      state.selectedHorizons = horizons;
      state.selectedForecastPeriod = forecastPeriod;
      state.timeFilterRangeStart = forecastPeriod.startDate;
      state.timeFilterRangeEnd = forecastPeriod.endDate;
      state.selectedPredictionIntervals = predictionIntervals;
      if (selectedDate) {
        state.userSelectedDate = selectedDate;
      }
    },

    // Location
    updateSelectedLocation: (state, action: PayloadAction<string>) => {
      state.selectedLocationCode = action.payload;
    },

    // Legacy action for backward compatibility with old components
    // Maps old {stateName, stateNum} pattern to new selectedLocationCode
    updateSelectedState: (
      state,
      action: PayloadAction<{ stateName?: string; stateNum: string | number }>
    ) => {
      // In the new architecture, we use location codes (like FIPS codes)
      // The stateNum is the location code
      const locationCode = typeof action.payload.stateNum === 'number'
        ? String(action.payload.stateNum).padStart(2, '0')
        : action.payload.stateNum;
      state.selectedLocationCode = locationCode;
    },

    // Models
    updateSelectedModels: (state, action: PayloadAction<string[]>) => {
      state.selectedModels = action.payload;
    },

    // Targets
    updateSelectedTargets: (state, action: PayloadAction<string[]>) => {
      state.selectedTargetIds = action.payload;
    },

    // Horizons
    updateSelectedHorizons: (state, action: PayloadAction<number[]>) => {
      state.selectedHorizons = action.payload;
    },

    // Forecast period
    updateSelectedForecastPeriod: (state, action: PayloadAction<ForecastPeriod>) => {
      state.selectedForecastPeriod = action.payload;
      state.timeFilterRangeStart = action.payload.startDate;
      state.timeFilterRangeEnd = action.payload.endDate;
    },

    // Time range
    updateTimeFilterStart: (state, action: PayloadAction<Date>) => {
      state.timeFilterRangeStart = action.payload;
    },
    updateTimeFilterEnd: (state, action: PayloadAction<Date>) => {
      state.timeFilterRangeEnd = action.payload;
    },

    // Visualization
    updateYScale: (state, action: PayloadAction<'linear' | 'log'>) => {
      state.yAxisScale = action.payload;
    },
    updateSelectedPredictionIntervals: (state, action: PayloadAction<string[]>) => {
      state.selectedPredictionIntervals = action.payload;
    },

    // Historical data
    updateHistoricalDataMode: (state, action: PayloadAction<boolean>) => {
      state.historicalTargetDataMode = action.payload;
    },
    updateSelectedHistoricalAsOfDate: (state, action: PayloadAction<string | null>) => {
      state.selectedHistoricalAsOfDate = action.payload;
    },

    // User interaction
    updateUserSelectedDate: (state, action: PayloadAction<Date>) => {
      state.userSelectedDate = action.payload;
    },
  },
});

export const {
  initializeForecastSettings,
  updateSelectedLocation,
  updateSelectedState,
  updateSelectedModels,
  updateSelectedTargets,
  updateSelectedHorizons,
  updateSelectedForecastPeriod,
  updateTimeFilterStart,
  updateTimeFilterEnd,
  updateYScale,
  updateSelectedPredictionIntervals,
  updateHistoricalDataMode,
  updateSelectedHistoricalAsOfDate,
  updateUserSelectedDate,
} = forecastSettingsSlice.actions;

export default forecastSettingsSlice.reducer;
