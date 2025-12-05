import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import {
  AppDataEvaluationsPrecalculated,
  AppDataEvaluationsSingleModelRawScores,
} from '@/types/domains/evaluations';

// Evaluation data structure matching DataContract.md
interface EvaluationDataState {
  areAggregatesLoaded: boolean;
  areRawScoresLoaded: boolean;

  loadedPeriods: string[];
  loadedRawScoreSeasons: string[];

  // Pre-calculated evaluation data
  precalculated: AppDataEvaluationsPrecalculated;
  rawScores: AppDataEvaluationsSingleModelRawScores;
}

const initialState: EvaluationDataState = {
  areAggregatesLoaded: false,
  areRawScoresLoaded: false,

  loadedPeriods: [],
  loadedRawScoreSeasons: [],

  precalculated: {
    iqr: {},
    locationMap_aggregates: {},
    detailedCoverage_aggregates: {},
  },
  rawScores: {},
};

const evaluationDataSlice = createSlice({
  name: 'evaluationData',
  initialState,
  reducers: {
    setEvaluationJsonData: (state, action: PayloadAction<any>) => {
      if (action.payload.precalculated) {
        state.precalculated = action.payload.precalculated;
        state.areAggregatesLoaded = true;
      }

      if (action.payload.rawScores) {
        state.rawScores = action.payload.rawScores;
        state.areRawScoresLoaded = true;
      }

      if (!action.payload.precalculated && !action.payload.rawScores) {
        console.warn('No precalculated or rawScores data found in the payload');
        return;
      }
    },
    // Add precalculated data for a specific period/season
    // Data format from per-period file is flat (not nested by periodId):
    // { iqr: {...}, locationMap_aggregates: {...}, detailedCoverage_aggregates: {...} }
    addPrecalculatedData: (
      state,
      action: PayloadAction<{
        periodId: string;
        data: any;
      }>
    ) => {
      const { periodId, data } = action.payload;

      // Data comes directly as { iqr, locationMap_aggregates, detailedCoverage_aggregates }
      // Store under the periodId key
      if (data.iqr) {
        state.precalculated.iqr[periodId] = data.iqr;
      }

      if (data.locationMap_aggregates) {
        state.precalculated.locationMap_aggregates[periodId] = data.locationMap_aggregates;
      }

      if (data.detailedCoverage_aggregates) {
        state.precalculated.detailedCoverage_aggregates[periodId] = data.detailedCoverage_aggregates;
      }

      // Track that this period has been loaded
      if (!state.loadedPeriods.includes(periodId)) {
        state.loadedPeriods.push(periodId);
      }

      // Mark as loaded if we have at least one period
      state.areAggregatesLoaded = state.loadedPeriods.length > 0;
    },
    // Add raw scores for a specific season/period
    // Data format from per-period file is flat: { [targetId]: { [metric]: { [model]: ... } } }
    addRawScores: (
      state,
      action: PayloadAction<{
        seasonId: string;
        data: any;
      }>
    ) => {
      const { seasonId, data } = action.payload;

      // Data comes directly as raw scores structure (not wrapped in rawScores key)
      // Store under the seasonId key
      state.rawScores[seasonId] = data;

      // Track that this season's raw scores have been loaded
      if (!state.loadedRawScoreSeasons.includes(seasonId)) {
        state.loadedRawScoreSeasons.push(seasonId);
      }

      // Mark as loaded if we have at least one season
      state.areRawScoresLoaded = state.loadedRawScoreSeasons.length > 0;
    },
    clearEvaluationJsonData: (state) => {
      state.precalculated = {
        iqr: {},
        locationMap_aggregates: {},
        detailedCoverage_aggregates: {},
      };
      state.rawScores = {};
      state.loadedPeriods = [];
      state.loadedRawScoreSeasons = [];

      state.areAggregatesLoaded = false;
      state.areRawScoresLoaded = false;
    },
  },
});

export const {
  setEvaluationJsonData,
  addPrecalculatedData,
  addRawScores,
  clearEvaluationJsonData,
} = evaluationDataSlice.actions;

export default evaluationDataSlice.reducer;
