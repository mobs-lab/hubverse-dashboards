import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { HistoricalTargetDataCollection } from "@/types/domains/forecasting";

interface HistoricalTargetDataState {
  isLoaded: boolean;
  data: HistoricalTargetDataCollection;
}

const initialState: HistoricalTargetDataState = {
  isLoaded: false,
  data: {},
};

const historicalTargetDataSlice = createSlice({
  name: "historicalTargetData",
  initialState,
  reducers: {
    setHistoricalTargetData: (
      state,
      action: PayloadAction<HistoricalTargetDataCollection>
    ) => {
      state.data = action.payload;
      state.isLoaded = true;
    },
    clearHistoricalTargetData: (state) => {
      state.data = {};
      state.isLoaded = false;
    },
  },
});

export const { setHistoricalTargetData, clearHistoricalTargetData } =
  historicalTargetDataSlice.actions;
export default historicalTargetDataSlice.reducer;
