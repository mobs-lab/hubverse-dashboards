import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  TargetData,
  ModelOutputCollection
} from "@/types/domains/forecasting";

interface CoreDataState {
  isLoaded: boolean;
  // [REMOVED] loadedForecastPeriods - no longer tracking individual period loading

  // New simplified structure (Monolithic)
  targetData: TargetData; 
  modelOutput: ModelOutputCollection;
}

const initialState: CoreDataState = {
  isLoaded: false,
  targetData: {},
  modelOutput: {},
};

const coreDataSlice = createSlice({
  name: "coreData",
  initialState,
  reducers: {
    // [REMOVED] addForecastPeriodData - we now load everything at once

    // Bulk load all data (for initial load)
    setAllCoreData: (
      state,
      action: PayloadAction<{
        targetData: TargetData;
        modelOutput: ModelOutputCollection;
      }>
    ) => {
      state.targetData = action.payload.targetData;
      state.modelOutput = action.payload.modelOutput;
      state.isLoaded = true;
    },

    clearCoreData: (state) => {
      state.targetData = {};
      state.modelOutput = {};
      state.isLoaded = false;
    },
  },
});

export const { setAllCoreData, clearCoreData } = coreDataSlice.actions;
export default coreDataSlice.reducer;
