import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { LocationMappingData, ForecastPeriodOptions } from '@/types/domains/forecasting';

interface AuxiliaryDataState {
  isLoaded: boolean;
  locationMapping: LocationMappingData;
  forecastPeriodOptions: ForecastPeriodOptions;
  mapData: any | null; // TopoJSON/GeoJSON data
}

const initialState: AuxiliaryDataState = {
  isLoaded: false,
  locationMapping: {},
  forecastPeriodOptions: {},
  mapData: null,
};

const auxiliaryDataSlice = createSlice({
  name: 'auxiliaryData',
  initialState,
  reducers: {
    setAuxiliaryData: (
      state,
      action: PayloadAction<{
        locationMapping: LocationMappingData;
        forecastPeriodOptions: ForecastPeriodOptions;
      }>
    ) => {
      state.locationMapping = action.payload.locationMapping;
      state.forecastPeriodOptions = action.payload.forecastPeriodOptions;
      state.isLoaded = true;
    },
    setMapData: (state, action: PayloadAction<any>) => {
      state.mapData = action.payload;
    },
    clearAuxiliaryData: (state) => {
      state.locationMapping = {};
      state.forecastPeriodOptions = {};
      state.mapData = null;
      state.isLoaded = false;
    },
  },
});

export const { setAuxiliaryData, setMapData, clearAuxiliaryData } =
  auxiliaryDataSlice.actions;
export default auxiliaryDataSlice.reducer;
