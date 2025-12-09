// src/app/store/forecastSettingsSlice.ts
import {createSlice, PayloadAction} from '@reduxjs/toolkit';
import { ForecastPeriodOption } from '@/types/domains/forecasting';
import {parseISO} from "date-fns";

interface EvaluationsSettingsState {
    /* Target Selection */
    selectedTargetId: string;
    availableTargets: { targetId: string; displayString: string }[];

    /* Location Related */
    evaluationsSingleModelViewSelectedLocationName: string; // Single model view's selected location name
    evaluationsSingleModelViewSelectedStateCode: string; // Location code

    /* Model Related*/
    evaluationsSingleModelViewModel: string; //Single Model view page allows only 1 model to be selected at a time
    evaluationSingleModelViewHorizon: number; //how many weeks ahead from reference date (matching surveillance week's number) should we look for as target_end_date in predictions to draw the intervals
    evaluationSingleModelViewScoresOption: any; 

    /* Time Range Related */
    evaluationsSingleModelViewDateStart: Date;
    evaluationSingleModelViewDateEnd: Date;
    evaluationsSingleModelViewSeasonId: string;
    evaluationSingleModelViewSeasonOptions: ForecastPeriodOption[];
}

const initialState: EvaluationsSettingsState = {
    /* Target Defaults */
    selectedTargetId: '',
    availableTargets: [],

    /* Location Defaults */
    evaluationsSingleModelViewSelectedLocationName: "US",
    evaluationsSingleModelViewSelectedStateCode: "US",

    /* Model Defaults*/
    evaluationsSingleModelViewModel: "",
    evaluationSingleModelViewHorizon: 0,
    evaluationSingleModelViewScoresOption: "MAPE", // TODO: Make configurable via config.yaml

    /* Time Range Defaults*/
    evaluationsSingleModelViewSeasonId: "", // Will be set by DataProvider
    evaluationsSingleModelViewDateStart: parseISO("2023-08-01T12:00:00Z"),
    evaluationSingleModelViewDateEnd: parseISO("2024-05-04T12:00:00Z"),
    evaluationSingleModelViewSeasonOptions: [],
};

const evaluationsSingleModelSettingsSlice = createSlice({
    name: 'evaluations-single-model-settings-slice',
    initialState,
    reducers: {
        // Initialize evaluation single model settings from config
        initializeEvaluationSingleModelSettings: (
            state,
            action: PayloadAction<{
                locationCode: string;
                locationName: string;
                defaultModel: string;
                seasonOptions: ForecastPeriodOption[];
                defaultSeasonId?: string;
                targets?: { targetId: string; displayString: string }[];
                defaultTargetId?: string;
                defaultHorizon?: number;
            }>
        ) => {
            const { locationCode, locationName, defaultModel, seasonOptions, defaultSeasonId, targets, defaultTargetId, defaultHorizon } = action.payload;
            
            state.evaluationsSingleModelViewSelectedStateCode = locationCode;
            state.evaluationsSingleModelViewSelectedLocationName = locationName;
            state.evaluationsSingleModelViewModel = defaultModel;
            state.evaluationSingleModelViewSeasonOptions = seasonOptions;
            
            // Set default season if provided
            if (defaultSeasonId) {
                state.evaluationsSingleModelViewSeasonId = defaultSeasonId;
                const selectedSeason = seasonOptions.find(s => s.forecastPeriodID === defaultSeasonId);
                if (selectedSeason) {
                    state.evaluationsSingleModelViewDateStart = selectedSeason.startDate;
                    state.evaluationSingleModelViewDateEnd = selectedSeason.endDate;
                }
            }
            
            // Set available targets and default selection
            if (targets && targets.length > 0) {
                state.availableTargets = targets;
                state.selectedTargetId = defaultTargetId || targets[0].targetId;
            }
            
            // Set default horizon if provided
            if (defaultHorizon !== undefined) {
                state.evaluationSingleModelViewHorizon = defaultHorizon;
            }
        },
        
        // Set selected target
        setSingleModelSelectedTargetId: (state, action: PayloadAction<string>) => {
            state.selectedTargetId = action.payload;
        },
        
        updateEvaluationSingleModelViewSelectedState: (state, action: PayloadAction<{
            stateName: string;
            stateNum: string
        }>) => {
            state.evaluationsSingleModelViewSelectedLocationName = action.payload.stateName;
            state.evaluationsSingleModelViewSelectedStateCode = action.payload.stateNum;
        },
        updateEvaluationsSingleModelViewModel: (state, action: PayloadAction<string>) => {
            state.evaluationsSingleModelViewModel = action.payload;
        },
        updateEvaluationSingleModelViewHorizon: (state, action: PayloadAction<number>) => {
            state.evaluationSingleModelViewHorizon = action.payload;
        },
        updateEvaluationSingleModelViewSeasonOptions: (state, action: PayloadAction<ForecastPeriodOption[]>) => {
            state.evaluationSingleModelViewSeasonOptions = action.payload;
        },
        updateEvaluationSingleModelViewDateStart: (state, action: PayloadAction<Date>) => {
            state.evaluationsSingleModelViewDateStart = action.payload;
        },
        updateEvaluationSingleModelViewDateEnd: (state, action: PayloadAction<Date>) => {
            state.evaluationSingleModelViewDateEnd = action.payload;
        },
        updateEvaluationsSingleModelViewSeasonId: (state, action: PayloadAction<string>) => { // <-- Renamed from updateEvaluationsSingleModelViewDateRange
            state.evaluationsSingleModelViewSeasonId = action.payload;
        },

        /*TODO: Implement reducer for scores once discussed*/
        updateEvaluationScores: (state, action: PayloadAction<any>) => {

            state.evaluationSingleModelViewScoresOption = action.payload;
        },
    },
});

export const {
    initializeEvaluationSingleModelSettings,
    setSingleModelSelectedTargetId,
    updateEvaluationsSingleModelViewModel,
    updateEvaluationSingleModelViewHorizon,
    updateEvaluationSingleModelViewSeasonOptions,
    updateEvaluationSingleModelViewSelectedState,
    updateEvaluationSingleModelViewDateStart,
    updateEvaluationSingleModelViewDateEnd,
    updateEvaluationsSingleModelViewSeasonId,
    updateEvaluationScores
} = evaluationsSingleModelSettingsSlice.actions;

export default evaluationsSingleModelSettingsSlice.reducer;