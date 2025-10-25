/* src/app/store/forecast-settings-slice.ts */
import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { ForecastPeriodOption } from '@/types/domains/forecasting';
import { parseISO } from 'date-fns';

interface ForecastSettingsState {
  selectedLocationName: string;
  selectedLocationCode: string;
  selectedModels: string[];
  selectedHorizon: number;
  timeFilterRangeStart: Date;
  timeFilterRangeEnd: Date;
  timeFilterRange: string;
  yAxisScale: string;
  selectedPredictionInterval: string[];
  historicalTargetDataMode: boolean;
  forecastPeriodsOptions: ForecastPeriodOption[];
  /* 
  //  Note: For RiskLevel Visualization Widgets only, another variable to keep track of the selected Prediction model (a single one) that should only affects the RiskLevel Visualization Widgets themselves.
  userSelectedRiskLevelModel: string;
*/
  //  Note: For ForecastChart to report back the userSelectedWeek to the whole page, for sibling components to use, for example the NowcastGauge and RiskLevelThermometer (inside NowcastStateThermo.tsx)
  userSelectedWeek: Date;
}

const initialState: ForecastSettingsState = {
  selectedLocationName: 'United States',
  selectedLocationCode: 'US',
  selectedModels: [
    'MOBS-GLEAM_FLUH',
    'MIGHTE-Nsemble',
    'MIGHTE-Joint',
    'NU_UCSD-GLEAM_AI_FLUH',
    'CEPH-Rtrend_fluH',
    'NEU_ISI-FluBcast',
    'NEU_ISI-AdaptiveEnsemble',
    'FluSight-ensemble',
  ],
  selectedHorizon: 3,
  timeFilterRange: '2023-08-01/2024-05-18',
  timeFilterRangeStart: parseISO('2023-08-01T12:00:00Z'),
  timeFilterRangeEnd: parseISO('2024-05-04T12:00:00Z'),
  yAxisScale: 'linear',
  selectedPredictionInterval: ['90'],
  historicalTargetDataMode: false,
  forecastPeriodsOptions: [],
  userSelectedWeek: new Date(),
};

const forecastSettingsSlice = createSlice({
  name: 'forecast-settings-slice',
  initialState,
  reducers: {
    updateSelectedState: (
      state,
      action: PayloadAction<{ stateName: string; stateNum: string }>
    ) => {
      state.selectedLocationName = action.payload.stateName;
      state.selectedLocationCode = action.payload.stateNum;
    },
    updateSelectedForecastModels: (state, action: PayloadAction<string[]>) => {
      state.selectedModels = action.payload;
    },
    updateNumOfWeeksAhead: (state, action: PayloadAction<number>) => {
      state.selectedHorizon = action.payload;
    },
    updateDateStart: (state, action: PayloadAction<Date>) => {
      state.timeFilterRangeStart = action.payload;
    },
    updateDateEnd: (state, action: PayloadAction<Date>) => {
      state.timeFilterRangeEnd = action.payload;
    },
    updateYScale: (state, action: PayloadAction<string>) => {
      state.yAxisScale = action.payload;
    },
    updateConfidenceInterval: (state, action: PayloadAction<string[]>) => {
      state.selectedPredictionInterval = action.payload;
    },
    updateHistoricalDataMode: (state, action: PayloadAction<boolean>) => {
      state.historicalTargetDataMode = action.payload;
    },
    setSeasonOptions: (state, action: PayloadAction<ForecastPeriodOption[]>) => {
      state.forecastPeriodsOptions = action.payload;
    },
    updateDateRange: (state, action: PayloadAction<string>) => {
      state.timeFilterRange = action.payload;
    },
    updateUserSelectedWeek: (state, action: PayloadAction<Date>) => {
      state.userSelectedWeek = action.payload;
    },
  },
});

export const {
  updateSelectedState,
  updateSelectedForecastModels,
  updateNumOfWeeksAhead,
  updateDateStart,
  updateDateEnd,
  updateYScale,
  updateConfidenceInterval,
  updateHistoricalDataMode,
  updateDateRange,
  setSeasonOptions,
  updateUserSelectedWeek,
} = forecastSettingsSlice.actions;

export default forecastSettingsSlice.reducer;
