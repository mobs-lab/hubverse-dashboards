import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  TargetDataCollection,
  ModelOutputCollection
} from "@/types/domains/forecasting";

interface CoreDataState {
  isLoaded: boolean;
  loadedForecastPeriods: string[]; // Track which periods have been loaded

  // New simplified structure
  targetDataCollection: TargetDataCollection;
  modelOutputCollection: ModelOutputCollection;
}

const initialState: CoreDataState = {
  isLoaded: false,
  loadedForecastPeriods: [],
  targetDataCollection: {},
  modelOutputCollection: {},
};

const coreDataSlice = createSlice({
  name: "coreData",
  initialState,
  reducers: {
    // Add data for a specific forecast period
    addForecastPeriodData: (
      state,
      action: PayloadAction<{
        forecastPeriodId: string;
        targetData?: any;
        modelOutput?: any;
      }>
    ) => {
      const { forecastPeriodId, targetData, modelOutput } = action.payload;

      // Add target data for this period
      if (targetData) {
        state.targetDataCollection[forecastPeriodId] = targetData;
      }

      // Add model output data for this period
      if (modelOutput) {
        state.modelOutputCollection[forecastPeriodId] = modelOutput;
      }

      // Track that this period has been loaded
      if (!state.loadedForecastPeriods.includes(forecastPeriodId)) {
        state.loadedForecastPeriods.push(forecastPeriodId);
      }

      // Mark as loaded if at least one period is loaded
      if (state.loadedForecastPeriods.length > 0) {
        state.isLoaded = true;
      }
    },

    // Bulk load all data (for initial load)
    setAllCoreData: (
      state,
      action: PayloadAction<{
        targetData: TargetDataCollection;
        modelOutput: ModelOutputCollection;
      }>
    ) => {
      state.targetDataCollection = action.payload.targetData;
      state.modelOutputCollection = action.payload.modelOutput;
      state.loadedForecastPeriods = Object.keys(action.payload.targetData);
      state.isLoaded = true;
    },

    clearCoreData: (state) => {
      state.targetDataCollection = {};
      state.modelOutputCollection = {};
      state.loadedForecastPeriods = [];
      state.isLoaded = false;
    },
  },
});

export const { addForecastPeriodData, setAllCoreData, clearCoreData } = coreDataSlice.actions;
export default coreDataSlice.reducer;
