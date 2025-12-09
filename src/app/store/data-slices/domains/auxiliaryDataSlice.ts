import { createSlice, PayloadAction } from '@reduxjs/toolkit';

interface AuxiliaryDataState {
  mapData: any | null; // TopoJSON/GeoJSON data
}

const initialState: AuxiliaryDataState = {
  mapData: null,
};

const auxiliaryDataSlice = createSlice({
  name: 'auxiliaryData',
  initialState,
  reducers: {
    setMapData: (state, action: PayloadAction<any>) => {
      state.mapData = action.payload;
    },
    clearAuxiliaryData: (state) => {
      state.mapData = null;
    },
  },
});

export const { setMapData, clearAuxiliaryData } =
  auxiliaryDataSlice.actions;
export default auxiliaryDataSlice.reducer;
