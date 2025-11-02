// src/store/index.ts
import { configureStore } from '@reduxjs/toolkit';

import {
  auxiliaryDataReducer,
  coreDataReducer,
  evaluationDataReducer,
  evaluationsSeasonOverviewSettingsReducer,
  evaluationsSingleModelSettingsReducer,
  forecastSettingsReducer,
  historicalTargetDataReducer,
} from './data-slices';

import configReducer from './data-slices/domains/configSlice';

const store = configureStore({
  reducer: {
    configStore: configReducer,
    coreDataStore: coreDataReducer,
    evaluationDataStore: evaluationDataReducer,
    auxiliaryDataStore: auxiliaryDataReducer,
    historicalTargetDataStore: historicalTargetDataReducer,
    forecastSettings: forecastSettingsReducer,
    evaluationsSeasonOverviewSettings: evaluationsSeasonOverviewSettingsReducer,
    evaluationsSingleModelSettings: evaluationsSingleModelSettingsReducer,
  },
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: false, // Disable for Date objects
    }),
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

export default store;
