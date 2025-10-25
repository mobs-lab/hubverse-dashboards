import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import {
  LocationData,
  ForecastPeriodOption,
} from '@/types/domains/forecasting';

interface AuxiliaryDataState {
  isLoaded: boolean;
  locations: LocationData[];
  metadata: {
    forecastPeriod?: ForecastPeriodOption[];
    specialForecastPeriod?: ForecastPeriodOption[];
    modelNames?: string[];
    defaultSeasonTimeValue?: string;
    defaultSelectedDate?: string;
  };
}

const initialState: AuxiliaryDataState = {
  isLoaded: false,
  locations: [],
  metadata: {},
};

const auxiliaryDataSlice = createSlice({
  name: 'auxiliaryData',
  initialState,
  reducers: {
    setAuxiliaryJsonData: (state, action: PayloadAction<any>) => {
      state.locations = action.payload.locations || [];

      state.metadata = action.payload.metadata || {};
      state.isLoaded = true;
    },
    clearAuxiliaryData: (state) => {
      state.locations = [];

      state.metadata = {};
      state.isLoaded = false;
    },
  },
});

export const { setAuxiliaryJsonData, clearAuxiliaryData } = auxiliaryDataSlice.actions;
export default auxiliaryDataSlice.reducer;
