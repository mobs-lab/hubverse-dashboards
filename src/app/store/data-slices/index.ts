// src/store/data-slices/index.ts
// Export Data Slice
export { default as coreDataReducer } from './domains/coreDataSlice';
export { default as evaluationDataReducer } from './domains/evaluationDataSlice';
export { default as auxiliaryDataReducer } from './domains/auxiliaryDataSlice';
export { default as historicalTargetDataReducer } from './domains/historicalTargetDataSlice';
// Export Settings Variables Slice
export { default as forecastSettingsReducer } from './settings/SettingsSliceForecastPage';
export { default as evaluationsSeasonOverviewSettingsReducer } from './settings/SettingsSliceEvaluationSeasonOverview';
export { default as evaluationsSingleModelSettingsReducer } from './settings/SettingsSliceEvaluationSingleModel';
