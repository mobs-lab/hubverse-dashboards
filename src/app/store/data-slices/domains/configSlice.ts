import { ForecastPeriodOptions, LocationMappingData } from '@/types/domains/forecasting';
import { PayloadAction, createSlice } from '@reduxjs/toolkit';

// Config-driven metadata from Python processing
export interface PredictionIntervalConfig {
  level: string; // "50", "90", "95"
  quantiles: [string, string]; // ["0.25", "0.75"]
}

export interface TargetConfig {
  targetId: string;
  targetKeyInData: string;
  displayString: string;
  dataValueProcessing?: DataValueProcessingConfig;
}

export interface DataValueProcessingConfig {
  scaling_factor: {
    target_data: number;
    model_output: number;
  };
  rounding_decimals: {
    target_data: number;
    model_output: number;
  };
}

export interface InfoButtonContentConfig {
  title: string;
  content: string;
}

export interface NavButtonConfig {
  text: string;
  navToPage?: 'Forecast' | 'Evaluation';
  navToExternal: boolean;
  navToLink?: string;
}

export interface UICustomizationConfig {
  header: {
    titleName: string;
    navButtons: NavButtonConfig[];
  };
  forecastPage: {
    chartHeaderName: string;
    histTdToggleText: string;
    disableLocationInfo: boolean;
    infoButtons: {
      headerInfo?: InfoButtonContentConfig;
      horizonInfo?: InfoButtonContentConfig;
    };
  };
}

export interface DashboardConfig {
  // Feature flags
  evaluationsEnabled: boolean;
  historicalTargetDataEnabled: boolean;
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
  modelColorMap: Record<string, string>;

  // Targets
  targets: TargetConfig[];
  defaultTargetId: string;

  // Prediction intervals
  predictionIntervals: PredictionIntervalConfig[];
  defaultPredictionIntervals: string[];

  // Dates
  defaultSelectedDate?: string;
  earliestDate?: string;
  latestDate?: string;

  // Default selections for UI initialization
  defaultLocation?: string;
  defaultHorizon?: number;

  // UI Customization
  uiCustomization: UICustomizationConfig;
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
