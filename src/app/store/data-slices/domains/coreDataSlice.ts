import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { TargetData, ModelOutputCollection } from '@/types/domains/forecasting';

interface CoreDataState {
  mainData: any;
  isLoaded: boolean;

  targetData: TargetData;
  modelOutput: ModelOutputCollection;
}

const initialState: CoreDataState = {
  isLoaded: false,
  mainData: {},
  targetData: {},
  modelOutput: {},
};

const coreDataSlice = createSlice({
  name: 'coreData',
  initialState,
  reducers: {
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
