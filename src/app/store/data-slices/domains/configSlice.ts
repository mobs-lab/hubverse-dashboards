import { ForecastPeriodOptions, LocationMappingData } from '@/types/domains/forecasting';
import { PayloadAction, createSlice } from '@reduxjs/toolkit';

// Config-driven metadata from Python processing
export interface PredictionIntervalConfig {
  level: string; // "50", "90", "95"
  quantiles: [string, string]; // ["0.25", "0.75"]
}

export interface ModelConfig {
  modelName: string;
  color?: string;
}

export interface TargetConfig {
  targetId: string;
  displayString: string;
}

export interface DashboardConfig {
  // Feature flags
  evaluationsEnabled: boolean;
  nowcastEnabled: boolean;

  // Spatial configuration
  isSingleLocation: boolean;
  singleLocationCode?: string;
  disableMapInDashboard: boolean;

  // Temporal configuration
  timeUnit: number; // in days
  horizons: number[];

  // Forecast periods
  forecastPeriodOptions: ForecastPeriodOptions;
  defaultForecastPeriodId: string;

  // Locations
  locationMapping: LocationMappingData;

  // Models
  models: ModelConfig[];
  modelColorMap: Record<string, string>;

  // Targets
  targets: TargetConfig[];
  defaultTargetId: string;

  // Prediction intervals
  predictionIntervals: PredictionIntervalConfig[];
  defaultPredictionIntervals: string[]; // ["90"]

  // Dates
  defaultSelectedDate?: string;
  earliestDate?: string;
  latestDate?: string;
}

interface ConfigState {
  isLoaded: boolean;
  config: DashboardConfig | null;
}

const initialState: ConfigState = {
  isLoaded: false,
  config: null,
};

const configSlice = createSlice({
  name: 'config',
  initialState,
  reducers: {
    setDashboardConfig: (state, action: PayloadAction<DashboardConfig>) => {
      state.config = action.payload;
      state.isLoaded = true;
    },
    clearConfig: (state) => {
      state.config = null;
      state.isLoaded = false;
    },
  },
});

export const { setDashboardConfig, clearConfig } = configSlice.actions;
export default configSlice.reducer;

// Selectors
export const selectConfig = (state: { config: ConfigState }) => state.config.config;
export const selectEvaluationsEnabled = (state: { config: ConfigState }) =>
  state.config.config?.evaluationsEnabled ?? false;
export const selectNowcastEnabled = (state: { config: ConfigState }) =>
  state.config.config?.nowcastEnabled ?? false;
export const selectModelNames = (state: { config: ConfigState }) =>
  state.config.config?.models.map((m) => m.modelName) ?? [];
export const selectModelColorMap = (state: { config: ConfigState }) =>
  state.config.config?.modelColorMap ?? {};
export const selectHorizons = (state: { config: ConfigState }) =>
  state.config.config?.horizons ?? [];
export const selectPredictionIntervalOptions = (state: { config: ConfigState }) =>
  state.config.config?.predictionIntervals ?? [];
export const selectTargets = (state: { config: ConfigState }) => state.config.config?.targets ?? [];
